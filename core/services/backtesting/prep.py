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

from dataclasses import dataclass, field
from datetime import date, timedelta
import logging
import time

from django.db.models import Count, Min, Max

from core.models import Backtest, DailyBar, DailyMetric, Scenario, Symbol
from core.services.backtesting.ohlc_readiness import ensure_ohlc_ready_for_backtest
from core.services.metrics_depth import check_metrics_depth

logger = logging.getLogger(__name__)


@dataclass
class BacktestPrepReport:
    did_fetch_bars: bool
    did_compute_metrics: bool
    notes: list[str]


@dataclass
class MissingOHLCRange:
    ticker: str
    start: date
    end: date
    reason: str


@dataclass
class StaticOHLCCoverageDiagnostic:
    total_symbols: int
    requested_start: date
    requested_end: date
    effective_start: date | None
    effective_end: date | None
    ok: list[str] = field(default_factory=list)
    missing_start: list[str] = field(default_factory=list)
    missing_end: list[str] = field(default_factory=list)
    no_bars_in_range: list[str] = field(default_factory=list)
    no_bars_at_all: list[str] = field(default_factory=list)
    missing_ranges: list[MissingOHLCRange] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(
            self.missing_start
            or self.missing_end
            or self.no_bars_in_range
            or self.no_bars_at_all
        )

    @property
    def has_exploitable_data(self) -> bool:
        return bool(self.effective_start and self.effective_end)

    def missing_tickers(self) -> list[str]:
        ordered = []
        seen = set()
        for ticker in self.missing_start + self.missing_end + self.no_bars_in_range + self.no_bars_at_all:
            if ticker not in seen:
                ordered.append(ticker)
                seen.add(ticker)
        return ordered

    def message(self) -> str:
        examples = self.missing_tickers()[:10]
        range_examples = [
            f"{item.ticker} {item.start} → {item.end}"
            for item in self.missing_ranges[:5]
            if item.start <= item.end
        ]
        effective = (
            f"Période exploitable observée : {self.effective_start} → {self.effective_end}."
            if self.effective_start and self.effective_end
            else "Aucune période exploitable commune observée."
        )
        return (
            f"Couverture prix insuffisante pour la période {self.requested_start} → {self.requested_end}. "
            f"{self.total_symbols} symboles analysés. "
            f"{len(self.ok)} semblent couvrir la période exploitable. "
            f"{len(self.missing_start)} commencent après le début demandé. "
            f"{len(self.missing_end)} s’arrêtent avant la fin exploitable. "
            f"{len(self.no_bars_in_range)} n’ont aucun prix dans la période. "
            f"{len(self.no_bars_at_all)} n’ont aucun prix en base. "
            f"{effective} "
            "Les dates demandées ne sont pas forcément des jours de marché ; la vérification utilise les premières et dernières dates disponibles. "
            f"Exemples : {', '.join(examples) if examples else 'aucun'}. "
            f"Plages à télécharger : {'; '.join(range_examples) if range_examples else 'aucune'}. "
            "Préparez les prix manquants depuis Trigger > Télécharger les prix des actions."
        )

    def warning_message(self) -> str:
        effective = (
            f"{self.effective_start} → {self.effective_end}"
            if self.effective_start and self.effective_end
            else "non disponible"
        )
        return (
            "Attention : couverture prix partielle. "
            "Le backtest est lancé sur les données disponibles. "
            f"{len(self.ok)} symboles couvrent la période exploitable {effective}. "
            f"{len(self.missing_start)} commencent après le début demandé. "
            f"{len(self.missing_end)} s’arrêtent avant la fin exploitable. "
            f"{len(self.no_bars_in_range)} n’ont aucun prix dans la période. "
            f"{len(self.no_bars_at_all)} n’ont aucun prix en base. "
            "Les résultats peuvent être incomplets. "
            "Vous pouvez compléter les prix depuis Trigger > Télécharger les prix des actions."
        )

    def blocking_message(self) -> str:
        return (
            f"Impossible de lancer le backtest : aucun prix exploitable sur la période "
            f"{self.requested_start} → {self.requested_end}. "
            f"{self.total_symbols} symboles analysés. "
            f"{len(self.no_bars_in_range)} n’ont aucun prix dans la période. "
            f"{len(self.no_bars_at_all)} n’ont aucun prix en base. "
            "Préparez les prix depuis Trigger > Télécharger les prix des actions."
        )


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


def _static_ohlc_coverage_diagnostic(symbols, start: date, end: date) -> StaticOHLCCoverageDiagnostic:
    symbols_list = list(symbols)
    if not symbols_list:
        return StaticOHLCCoverageDiagnostic(0, start, end, None, None)

    symbol_ids = [symbol.id for symbol in symbols_list]
    range_rows = {
        row["symbol_id"]: row
        for row in DailyBar.objects.filter(
            symbol_id__in=symbol_ids,
            date__gte=start,
            date__lte=end,
        ).values("symbol_id").annotate(c=Count("id"), mn=Min("date"), mx=Max("date"))
    }
    all_rows = {
        row["symbol_id"]: row
        for row in DailyBar.objects.filter(symbol_id__in=symbol_ids)
        .values("symbol_id")
        .annotate(c=Count("id"), mn=Min("date"), mx=Max("date"))
    }
    effective_start = min((row["mn"] for row in range_rows.values() if row.get("mn")), default=None)
    effective_end = max((row["mx"] for row in range_rows.values() if row.get("mx")), default=None)
    report = StaticOHLCCoverageDiagnostic(
        total_symbols=len(symbols_list),
        requested_start=start,
        requested_end=end,
        effective_start=effective_start,
        effective_end=effective_end,
    )

    for symbol in symbols_list:
        range_row = range_rows.get(symbol.id)
        all_row = all_rows.get(symbol.id)
        if not all_row or not all_row.get("c"):
            report.no_bars_at_all.append(symbol.ticker)
            report.missing_ranges.append(MissingOHLCRange(symbol.ticker, start, end, "NO_BARS_AT_ALL"))
            continue
        if not range_row or not range_row.get("c"):
            report.no_bars_in_range.append(symbol.ticker)
            report.missing_ranges.append(MissingOHLCRange(symbol.ticker, start, end, "NO_BARS_IN_RANGE"))
            continue

        has_issue = False
        if effective_start and range_row["mn"] > effective_start:
            has_issue = True
            report.missing_start.append(symbol.ticker)
            missing_end = range_row["mn"] - timedelta(days=1)
            if start <= missing_end:
                report.missing_ranges.append(MissingOHLCRange(symbol.ticker, start, missing_end, "MISSING_START"))
        if effective_end and range_row["mx"] < effective_end:
            has_issue = True
            report.missing_end.append(symbol.ticker)
            missing_start = range_row["mx"] + timedelta(days=1)
            if missing_start <= effective_end:
                report.missing_ranges.append(MissingOHLCRange(symbol.ticker, missing_start, effective_end, "MISSING_END"))
        if not has_issue:
            report.ok.append(symbol.ticker)
    return report


def _missing_bar_coverage_symbols(symbols, start: date, end: date) -> list[str]:
    return _static_ohlc_coverage_diagnostic(symbols, start, end).missing_tickers()


def _metrics_cover_range(symbol_id: int, scenario_id: int, start: date, end: date) -> bool:
    qs = DailyMetric.objects.filter(symbol_id=symbol_id, scenario_id=scenario_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(agg["mn"] and agg["mx"] and agg["mn"] <= start and agg["mx"] >= end)


def prepare_backtest_data(backtest: Backtest, *, force_full_recompute: bool = False) -> BacktestPrepReport:
    """
    Ensure data required for the backtest exists.

    - For historical dynamic universes, report missing OHLC coverage without auto-fetching.
    - For static universes, fail fast with a clear message when DailyBar coverage
      is missing. Backtests must not launch a global provider fetch implicitly.
    - If DailyMetric/Alert coverage is missing for at least one symbol, run compute_metrics_task(recompute_all=False).

    Returns a report explaining what was executed.
    """
    notes: list[str] = []
    did_fetch = False
    did_compute = False

    # Lazy import to avoid circular imports (tasks -> prep -> tasks)
    from core.tasks import _compute_metrics_for_scenario

    # Determine universe (snapshot if present, else scenario symbols)
    tickers = _tickers_from_universe_snapshot(backtest.universe_snapshot or [])
    symbols = Symbol.objects.filter(ticker__in=tickers).all() if tickers else backtest.scenario.symbols.all()

    is_dynamic_universe = backtest.scenario.universe_mode in {
        Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
        Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
    }

    ohlc_started = time.monotonic()
    exclude_metric_tickers_for_missing_ohlc: set[str] = set()
    if is_dynamic_universe:
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
        logger.warning(
            "[backtest timing] step=ohlc_check mode=dynamic backtest_id=%s duration=%.3fs missing_after=%s",
            getattr(backtest, "id", None),
            time.monotonic() - ohlc_started,
            len(missing_bars or []),
        )
    else:
        ohlc_diagnostic = _static_ohlc_coverage_diagnostic(symbols, backtest.start_date, backtest.end_date)
        missing_bars = ohlc_diagnostic.missing_tickers()
        logger.warning(
            "[backtest timing] step=ohlc_check mode=static backtest_id=%s duration=%.3fs total=%s ok=%s missing_start=%s missing_end=%s no_range=%s no_bars=%s",
            getattr(backtest, "id", None),
            time.monotonic() - ohlc_started,
            ohlc_diagnostic.total_symbols,
            len(ohlc_diagnostic.ok),
            len(ohlc_diagnostic.missing_start),
            len(ohlc_diagnostic.missing_end),
            len(ohlc_diagnostic.no_bars_in_range),
            len(ohlc_diagnostic.no_bars_at_all),
        )
        if ohlc_diagnostic.has_issues:
            if not ohlc_diagnostic.has_exploitable_data:
                raise ValueError(ohlc_diagnostic.blocking_message())
            notes.append(ohlc_diagnostic.warning_message())
            notes.append(ohlc_diagnostic.message())
            exclude_metric_tickers_for_missing_ohlc.update(ohlc_diagnostic.no_bars_in_range)
            exclude_metric_tickers_for_missing_ohlc.update(ohlc_diagnostic.no_bars_at_all)

    metric_symbols = symbols
    if exclude_metric_tickers_for_missing_ohlc:
        metric_symbols = symbols.exclude(ticker__in=exclude_metric_tickers_for_missing_ohlc)
        examples = ", ".join(sorted(exclude_metric_tickers_for_missing_ohlc)[:10])
        notes.append(
            "Certaines métriques ne sont pas recalculées car les prix OHLC sources sont absents. "
            "Récupérez d’abord les prix manquants, puis relancez le calcul des métriques."
            + (f" Symboles concernés : {examples}." if examples else "")
        )

    # Check metrics depth (single grouped query) and decide whether we must full recompute.
    symbol_ids = list(metric_symbols.values_list("id", flat=True))
    metrics_started = time.monotonic()
    depth = check_metrics_depth(
        scenario_id=backtest.scenario_id,
        symbol_ids=symbol_ids,
        required_start=backtest.start_date,
        required_end=backtest.end_date,
    )
    logger.warning(
        "[backtest timing] step=metrics_depth backtest_id=%s duration=%.3fs total=%s covered=%s missing=%s no_metrics_at_all=%s no_metrics_in_range=%s missing_start=%s missing_end=%s effective_start=%s effective_end=%s",
        getattr(backtest, "id", None),
        time.monotonic() - metrics_started,
        depth.total_symbols,
        depth.covered_symbols,
        len(depth.missing_symbol_ids),
        len(depth.no_metrics_at_all_symbol_ids),
        len(depth.no_metrics_in_range_symbol_ids),
        len(depth.missing_start_symbol_ids),
        len(depth.missing_end_symbol_ids),
        depth.effective_start,
        depth.effective_end,
    )

    if depth.has_partial_coverage and not depth.needs_full_recompute():
        notes.append(
            "Attention : couverture indicateurs partielle. "
            f"{depth.covered_symbols}/{depth.total_symbols} symboles couvrent la période exploitable "
            f"{depth.effective_start} → {depth.effective_end}. "
            f"{len(depth.missing_start_symbol_ids)} commencent après le début exploitable. "
            f"{len(depth.missing_end_symbol_ids)} s’arrêtent avant la fin exploitable. "
            f"{len(depth.no_metrics_in_range_symbol_ids)} n’ont aucun indicateur dans la période. "
            "Aucun recalcul massif n’a été lancé automatiquement."
        )

    needs_full = bool(force_full_recompute) or depth.needs_full_recompute()
    if needs_full:
        if force_full_recompute:
            notes.append("Force Full Recompute requested from UI.")
        if depth.needs_full_recompute():
            notes.append(
                f"Insufficient metrics depth for date range: missing coverage on {len(depth.missing_symbol_ids)}/{depth.total_symbols} symbols."
            )
        compute_symbols = metric_symbols
        recompute_all = True
        if (
            not force_full_recompute
            and depth.has_exploitable_metrics
            and depth.no_metrics_at_all_symbol_ids
        ):
            compute_symbols = metric_symbols.filter(id__in=depth.no_metrics_at_all_symbol_ids)
            recompute_all = False
            notes.append(
                f"Recomputing metrics only for {len(depth.no_metrics_at_all_symbol_ids)} symbols without any metric rows."
            )
        compute_started = time.monotonic()
        _compute_metrics_for_scenario(
            symbols_qs=compute_symbols,
            scenario=backtest.scenario,
            recompute_all=recompute_all,
            job=None,
        )
        did_compute = True
        logger.warning(
            "[backtest timing] step=metrics_recompute backtest_id=%s duration=%.3fs symbols=%s recompute_all=%s",
            getattr(backtest, "id", None),
            time.monotonic() - compute_started,
            compute_symbols.count() if hasattr(compute_symbols, "count") else len(list(compute_symbols)),
            recompute_all,
        )
        notes.append("Ran metrics recompute for this scenario (scoped to required symbols).")

    if not missing_bars and not needs_full:
        notes.append("All prerequisite data already present for the requested date range.")

    return BacktestPrepReport(did_fetch_bars=did_fetch, did_compute_metrics=did_compute, notes=notes)
