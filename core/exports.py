"""Export helpers.

We keep these helpers outside views/tasks to avoid circular imports.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable

from openpyxl import Workbook

from .models import DailyBar, DailyMetric, Scenario, Symbol, Alert


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # Accept ISO YYYY-MM-DD
    return date.fromisoformat(s)


def build_scenario_workbook_write_only(
    *,
    scenario: Scenario,
    symbols_qs,
    date_from: str | None = None,
    date_to: str | None = None,
) -> Workbook:
    """Build a scenario export workbook in write-only mode.

    This is designed to be memory efficient for large exports.
    """

    d_from = _parse_date(date_from)
    d_to = _parse_date(date_to)

    wb = Workbook(write_only=True)

    # Preload alerts for all symbols in one pass (bounded by optional date filters)
    alerts_qs = Alert.objects.filter(scenario=scenario, symbol__in=symbols_qs).values_list("symbol_id", "date", "alerts")
    if d_from:
        alerts_qs = alerts_qs.filter(date__gte=d_from)
    if d_to:
        alerts_qs = alerts_qs.filter(date__lte=d_to)
    alerts_map = {(sid, dt): al for (sid, dt, al) in alerts_qs.iterator(chunk_size=5000)}

    first = True
    for sym in symbols_qs.order_by("ticker", "exchange").iterator(chunk_size=200):
        # Bars (streaming)
        bars = DailyBar.objects.filter(symbol=sym).order_by("date")
        if d_from:
            bars = bars.filter(date__gte=d_from)
        if d_to:
            bars = bars.filter(date__lte=d_to)
        bars = bars.only("date", "open", "high", "low", "close", "volume", "change_amount", "change_pct")

        # Metrics for this symbol+scenario
        metrics = DailyMetric.objects.filter(symbol=sym, scenario=scenario).order_by("date")
        if d_from:
            metrics = metrics.filter(date__gte=d_from)
        if d_to:
            metrics = metrics.filter(date__lte=d_to)
        metrics = metrics.only(
            "date",
            "V",
            "slope_P",
            "sum_pos_P",
            "nb_pos_P",
            "ratio_P",
            "amp_h",
            "slope_vrai",
            "P",
            "M",
            "M1",
            "X",
            "X1",
            "T",
            "Q",
            "S",
            "K1",
            "K1f",
            "K2",
            "K3",
            "K4",
            "K2f",
            "K2f_pre",
            "Kf2bis",
        )

        # Build a date->metric mapping (can be large but bounded per symbol).
        metrics_by_date = {m.date: m for m in metrics.iterator(chunk_size=5000)}

        title = (sym.ticker or "")[:28] or f"SYM_{sym.id}"
        ws = wb.create_sheet(title=title)

        ws.append([f"Scenario: {scenario.name}"])
        ws.append([f"Description: {scenario.description}"])
        ws.append([
            f"Vars: a={scenario.a} b={scenario.b} c={scenario.c} d={scenario.d} e={scenario.e} "
            f"| N1={scenario.n1} N2={scenario.n2} "
            f"| SUM_SLOPE/SLOPE_VRAI: Npente={getattr(scenario,'npente',None)} seuil={getattr(scenario,'slope_threshold',None)} "
            f"| history_years={scenario.history_years}"
        ])
        ws.append([f"Ticker: {sym.ticker}  Exchange: {sym.exchange}  Name: {sym.name}"])
        ws.append([])

        header = [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "change_amount",
            "change_pct",
            "V",
            "slope_P",
            "sum_pos_P",
            "nb_pos_P",
            "ratio_P",
            "amp_h",
            "slope_vrai",
            "P",
            "M",
            "M1",
            "X",
            "X1",
            "T",
            "Q",
            "S",
            "K1",
            "K1f",
            "K2f",
            "K2f_pre",
            "Kf2bis",
            "K2",
            "K3",
            "K4",
            "alerts",
        ]
        ws.append(header)

        def f(x):
            return float(x) if x is not None else None

        for b in bars.iterator(chunk_size=5000):
            m = metrics_by_date.get(b.date)
            ws.append([
                b.date.isoformat(),
                f(b.open),
                f(b.high),
                f(b.low),
                f(b.close),
                (int(b.volume) if b.volume is not None else None),
                f(b.change_amount),
                f(b.change_pct),
                f(m.V) if m else None,
                f(m.slope_P) if m else None,
                f(m.sum_pos_P) if m else None,
                (m.nb_pos_P if m and m.nb_pos_P is not None else None),
                f(m.ratio_P) if m else None,
                f(m.amp_h) if m else None,
                f(getattr(m, "slope_vrai", None)) if m else None,
                f(m.P) if m else None,
                f(m.M) if m else None,
                f(m.M1) if m else None,
                f(m.X) if m else None,
                f(m.X1) if m else None,
                f(m.T) if m else None,
                f(m.Q) if m else None,
                f(m.S) if m else None,
                f(m.K1) if m else None,
                f(m.K1f) if m else None,
                f(getattr(m, "K2f", None)) if m else None,
                f(getattr(m, "K2f_pre", None)) if m else None,
                f(getattr(m, "Kf2bis", None)) if m else None,
                f(m.K2) if m else None,
                f(m.K3) if m else None,
                f(m.K4) if m else None,
                alerts_map.get((sym.id, b.date), ""),
            ])

    return wb
