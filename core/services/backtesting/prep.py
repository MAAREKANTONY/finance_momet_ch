"""
Prerequisites preparation for backtests.

When a backtest is launched, we must ensure:
- DailyBar data is available for the universe and date range (Twelve Data sync)
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

from core.models import Backtest, DailyBar, DailyMetric, Symbol


@dataclass
class BacktestPrepReport:
    did_fetch_bars: bool
    did_compute_metrics: bool
    notes: list[str]


def _bars_cover_range(symbol_id: int, start: date, end: date) -> bool:
    qs = DailyBar.objects.filter(symbol_id=symbol_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(agg["mn"] and agg["mx"] and agg["mn"] <= start and agg["mx"] >= end)


def _metrics_cover_range(symbol_id: int, scenario_id: int, start: date, end: date) -> bool:
    qs = DailyMetric.objects.filter(symbol_id=symbol_id, scenario_id=scenario_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(agg["mn"] and agg["mx"] and agg["mn"] <= start and agg["mx"] >= end)


def prepare_backtest_data(backtest: Backtest) -> BacktestPrepReport:
    """
    Ensure data required for the backtest exists.

    - If DailyBar coverage is missing for at least one symbol, run fetch_daily_bars_task().
    - If DailyMetric/Alert coverage is missing for at least one symbol, run compute_metrics_task(recompute_all=False).

    Returns a report explaining what was executed.
    """
    notes: list[str] = []
    did_fetch = False
    did_compute = False

    # Lazy import to avoid circular imports (tasks -> prep -> tasks)
    from core.tasks import fetch_daily_bars_task, compute_metrics_task

    # Determine universe (snapshot if present, else scenario symbols)
    tickers = backtest.universe_snapshot or []
    symbols = Symbol.objects.filter(ticker__in=tickers).all() if tickers else backtest.scenario.symbols.all()

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

    # Check metrics coverage
    missing_metrics = []
    for s in symbols:
        if not _metrics_cover_range(s.id, backtest.scenario_id, backtest.start_date, backtest.end_date):
            missing_metrics.append(s.ticker)

    if missing_metrics:
        notes.append(
            f"Missing DailyMetric/Alert coverage for {len(missing_metrics)} symbols (sample: {', '.join(missing_metrics[:10])}{'...' if len(missing_metrics) > 10 else ''})."
        )
        compute_metrics_task(recompute_all=False)
        did_compute = True
        notes.append("Ran compute_metrics_task(recompute_all=False).")

    if not missing_bars and not missing_metrics:
        notes.append("All prerequisite data already present for the requested date range.")

    return BacktestPrepReport(did_fetch_bars=did_fetch, did_compute_metrics=did_compute, notes=notes)
