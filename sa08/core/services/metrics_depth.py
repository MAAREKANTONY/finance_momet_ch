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

from dataclasses import dataclass
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

    def needs_full_recompute(self) -> bool:
        return bool(self.missing_symbol_ids)


def check_metrics_depth(
    *,
    scenario_id: int,
    symbol_ids: list[int],
    required_start: date,
    required_end: date,
) -> MetricsDepthReport:
    """Check whether DailyMetric coverage is complete for a given window.

    This function is intentionally **read-only**.

    A symbol is considered "covered" iff:
      - it has at least one DailyMetric row in the window
      - min(date) <= required_start
      - max(date) >= required_end
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
    rows = (
        DailyMetric.objects.filter(
            scenario_id=scenario_id,
            symbol_id__in=symbol_ids,
            date__gte=required_start,
            date__lte=required_end,
        )
        .values("symbol_id")
        .annotate(mn=Min("date"), mx=Max("date"))
    )

    by_symbol = {r["symbol_id"]: (r.get("mn"), r.get("mx")) for r in rows}

    missing: list[int] = []
    covered = 0
    for sid in symbol_ids:
        mn, mx = by_symbol.get(sid, (None, None))
        if not mn or not mx:
            missing.append(sid)
            continue
        if mn > required_start or mx < required_end:
            missing.append(sid)
            continue
        covered += 1

    return MetricsDepthReport(
        scenario_id=int(scenario_id),
        required_start=required_start,
        required_end=required_end,
        total_symbols=len(symbol_ids),
        covered_symbols=covered,
        missing_symbol_ids=missing,
        sample_missing_symbol_ids=missing[:20],
    )
