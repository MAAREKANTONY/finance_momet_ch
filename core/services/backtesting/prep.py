"""
Prerequisites preparation for backtests.

When a backtest is launched, we must ensure:
- DailyBar data is available for the universe and date range
- DailyMetric + Alert computations are available for the scenario configuration (same computations as alerts)

This module provides a conservative implementation that reuses existing tasks.

Design:
- We only check *coverage* at a coarse level to decide whether to run the existing tasks.
- Tasks are executed synchronously when called from the Celery backtest task to keep the run deterministic.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from django.db.models import Min, Max

from core.models import Backtest, DailyBar, DailyMetric, Scenario, Symbol
from core.services.backtesting.ohlc_readiness import ensure_ohlc_ready_for_backtest
from core.services.metrics_depth import check_metrics_depth


@dataclass
class BacktestPrepReport:
    did_fetch_bars: bool
    did_compute_metrics: bool
    notes: list[str]


def _tickers_from_universe_snapshot(raw_universe) -> list[str]:
    tickers: list[str] = []
    if not isinstance(raw_universe, list):
        return tickers
    for item in raw_universe:
        if isinstance(item, dict):
            ticker = item.get("ticker") or item.get("symbol") or item.get("code")
            if ticker:
                tickers.append(str(ticker).strip())
        elif item:
            tickers.append(str(item).strip())
    return [ticker for ticker in tickers if ticker]


def _bars_cover_range(symbol_id: int, start: date, end: date) -> bool:
    qs = DailyBar.objects.filter(symbol_id=symbol_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(agg["mn"] and agg["mx"] and agg["mn"] <= start and agg["mx"] >= end)


def _metrics_cover_range(symbol_id: int, scenario_id: int, start: date, end: date) -> bool:
    qs = DailyMetric.objects.filter(symbol_id=symbol_id, scenario_id=scenario_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(agg["mn"] and agg["mx"] and agg["mn"] <= start and agg["mx"] >= end)


def prepare_backtest_data(backtest: Backtest, *, force_full_recompute: bool = False) -> BacktestPrepReport:
    """
    Ensure data required for the backtest exists.

    - For dynamic S&P 500 universes, report missing OHLC coverage without auto-fetching.
    - For static universes, keep the historical behavior and run fetch_daily_bars_task()
      when DailyBar coverage is missing.
    - If DailyMetric/Alert coverage is missing for at least one symbol, run compute_metrics_task(recompute_all=False).

    Returns a report explaining what was executed.
    """
    notes: list[str] = []
    did_fetch = False
    did_compute = False

    # Lazy import to avoid circular imports (tasks -> prep -> tasks)
    from core.tasks import fetch_daily_bars_task
    from core.tasks import _compute_metrics_for_scenario

    # Determine universe (snapshot if present, else scenario symbols)
    tickers = _tickers_from_universe_snapshot(backtest.universe_snapshot or [])
    symbols = Symbol.objects.filter(ticker__in=tickers).all() if tickers else backtest.scenario.symbols.all()

    is_dynamic_sp500 = (
        backtest.scenario.universe_mode == Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
    )

    if is_dynamic_sp500:
        ohlc_report = ensure_ohlc_ready_for_backtest(
            backtest=backtest,
            symbols=symbols,
            start_date=backtest.start_date,
            end_date=backtest.end_date,
            block_on_missing=False,
        )
        did_fetch = ohlc_report.did_fetch
        notes.extend(ohlc_report.notes)
        missing_bars = ohlc_report.missing_after
    else:
        # Check bars coverage
        missing_bars = []
        for s in symbols:
            if not _bars_cover_range(s.id, backtest.start_date, backtest.end_date):
                missing_bars.append(s.ticker)

        if missing_bars:
            notes.append(
                f"Missing DailyBar coverage for {len(missing_bars)} symbols (sample: {', '.join(missing_bars[:10])}{'...' if len(missing_bars) > 10 else ''})."
            )
            # Run synchronously (we are already in a background task when called from run_backtest_task)
            fetch_daily_bars_task()
            did_fetch = True
            notes.append("Ran fetch_daily_bars_task().")

    # Check metrics depth (single grouped query) and decide whether we must full recompute.
    symbol_ids = list(symbols.values_list("id", flat=True))
    depth = check_metrics_depth(
        scenario_id=backtest.scenario_id,
        symbol_ids=symbol_ids,
        required_start=backtest.start_date,
        required_end=backtest.end_date,
    )

    needs_full = bool(force_full_recompute) or depth.needs_full_recompute()
    if needs_full:
        if force_full_recompute:
            notes.append("Force Full Recompute requested from UI.")
        if depth.needs_full_recompute():
            notes.append(
                f"Insufficient metrics depth for date range: missing coverage on {len(depth.missing_symbol_ids)}/{depth.total_symbols} symbols."
            )
        # Full recompute (scoped to the backtest universe only) – no formula change.
        _compute_metrics_for_scenario(
            symbols_qs=symbols,
            scenario=backtest.scenario,
            recompute_all=True,
            job=None,
        )
        did_compute = True
        notes.append("Ran full recompute for this scenario (scoped to backtest universe).")

    if not missing_bars and not needs_full:
        notes.append("All prerequisite data already present for the requested date range.")

    return BacktestPrepReport(did_fetch_bars=did_fetch, did_compute_metrics=did_compute, notes=notes)
