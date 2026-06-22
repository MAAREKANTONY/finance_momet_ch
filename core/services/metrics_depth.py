"""Metrics depth checks.

Purpose
-------
When users increase the *requested* historical depth (e.g. extending a Backtest range
or increasing GameScenario.study_days), the existing DailyMetric/Alert rows might not
cover the newly-required period.

This module provides a **side-effect-free** check (no recompute, no deletes):
it only inspects DB coverage and returns a report that callers can use to decide
whether a *full recompute* is required.

Important
---------
- No formula / math engine changes here.
- Callers decide what to do (auto full recompute, show a button, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from django.db.models import Max, Min

from core.models import DailyMetric


@dataclass(frozen=True)
class MetricsDepthReport:
    """Coverage report for DailyMetric for a given scenario/universe and date window."""

    scenario_id: int
    required_start: date
    required_end: date

    total_symbols: int
    covered_symbols: int

    # Symbols that do NOT fully cover [required_start, required_end]
    missing_symbol_ids: list[int]

    # A few sample ids for logs/UI
    sample_missing_symbol_ids: list[int]

    effective_start: date | None = None
    effective_end: date | None = None
    ok_symbol_ids: list[int] = field(default_factory=list)
    missing_start_symbol_ids: list[int] = field(default_factory=list)
    missing_end_symbol_ids: list[int] = field(default_factory=list)
    no_metrics_in_range_symbol_ids: list[int] = field(default_factory=list)
    no_metrics_at_all_symbol_ids: list[int] = field(default_factory=list)

    @property
    def has_exploitable_metrics(self) -> bool:
        return bool(self.effective_start and self.effective_end)

    @property
    def has_partial_coverage(self) -> bool:
        return bool(self.missing_symbol_ids) and self.has_exploitable_metrics

    def needs_full_recompute(self) -> bool:
        if not self.total_symbols:
            return False
        if not self.has_exploitable_metrics:
            return True
        return bool(self.no_metrics_at_all_symbol_ids)


def check_metrics_depth(
    *,
    scenario_id: int,
    symbol_ids: list[int],
    required_start: date,
    required_end: date,
) -> MetricsDepthReport:
    """Check whether DailyMetric coverage is complete for a given window.

    This function is intentionally **read-only**.

    The requested start/end may be non-market calendar dates. Coverage is checked
    against the effective metric window observed in the requested scope:
      - effective_start = first DailyMetric date found in the window
      - effective_end = last DailyMetric date found in the window

    A symbol is considered covered when its rows span that effective window.
    """
    # Defensive normalizations
    symbol_ids = [int(x) for x in (symbol_ids or []) if x is not None]
    if not symbol_ids:
        return MetricsDepthReport(
            scenario_id=int(scenario_id),
            required_start=required_start,
            required_end=required_end,
            total_symbols=0,
            covered_symbols=0,
            missing_symbol_ids=[],
            sample_missing_symbol_ids=[],
        )

    # One grouped query instead of N per-symbol queries.
    rows = list(
        DailyMetric.objects.filter(
            scenario_id=scenario_id,
            symbol_id__in=symbol_ids,
            date__gte=required_start,
            date__lte=required_end,
        )
        .values("symbol_id")
        .annotate(mn=Min("date"), mx=Max("date"))
    )
    all_rows = list(
        DailyMetric.objects.filter(
            scenario_id=scenario_id,
            symbol_id__in=symbol_ids,
        )
        .values("symbol_id")
        .annotate(mn=Min("date"), mx=Max("date"))
    )

    by_symbol = {r["symbol_id"]: (r.get("mn"), r.get("mx")) for r in rows}
    all_by_symbol = {r["symbol_id"]: (r.get("mn"), r.get("mx")) for r in all_rows}
    effective_start = min((r.get("mn") for r in rows if r.get("mn")), default=None)
    effective_end = max((r.get("mx") for r in rows if r.get("mx")), default=None)

    missing: list[int] = []
    ok_symbol_ids: list[int] = []
    missing_start: list[int] = []
    missing_end: list[int] = []
    no_metrics_in_range: list[int] = []
    no_metrics_at_all: list[int] = []

    for sid in symbol_ids:
        mn, mx = by_symbol.get(sid, (None, None))
        all_mn, all_mx = all_by_symbol.get(sid, (None, None))
        if not all_mn or not all_mx:
            missing.append(sid)
            no_metrics_at_all.append(sid)
            continue
        if not mn or not mx:
            missing.append(sid)
            no_metrics_in_range.append(sid)
            continue

        has_issue = False
        if effective_start and mn > effective_start:
            has_issue = True
            missing_start.append(sid)
        if effective_end and mx < effective_end:
            has_issue = True
            missing_end.append(sid)
        if has_issue:
            missing.append(sid)
        else:
            ok_symbol_ids.append(sid)

    return MetricsDepthReport(
        scenario_id=int(scenario_id),
        required_start=required_start,
        required_end=required_end,
        total_symbols=len(symbol_ids),
        covered_symbols=len(ok_symbol_ids),
        missing_symbol_ids=missing,
        sample_missing_symbol_ids=missing[:20],
        effective_start=effective_start,
        effective_end=effective_end,
        ok_symbol_ids=ok_symbol_ids,
        missing_start_symbol_ids=missing_start,
        missing_end_symbol_ids=missing_end,
        no_metrics_in_range_symbol_ids=no_metrics_in_range,
        no_metrics_at_all_symbol_ids=no_metrics_at_all,
    )
