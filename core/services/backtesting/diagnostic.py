from __future__ import annotations

from datetime import date
from typing import Any

from core.models import DailyMetric, HistoricalMarketCap, Symbol


_SIGNAL_SERIES_FIELDS = {
    "AF": ("P", "Kf2bis"),
    "BF": ("P", "Kf2bis"),
    "SPA": ("SUM_SLOPE",),
    "SPV": ("SUM_SLOPE",),
    "SPVA": ("SLOPE_VRAI",),
    "SPVV": ("SLOPE_VRAI",),
    "SPA_BASSE": ("SUM_SLOPE_BASSE",),
    "SPV_BASSE": ("SUM_SLOPE_BASSE",),
    "SPVA_BASSE": ("SLOPE_VRAI_BASSE",),
    "SPVV_BASSE": ("SLOPE_VRAI_BASSE",),
    "A1": ("K1",),
    "B1": ("K1",),
    "C1": ("K2",),
    "D1": ("K2",),
    "E1": ("K3",),
    "F1": ("K3",),
    "G1": ("K4",),
    "H1": ("K4",),
}

_ACTION_MARKERS = {"BUY", "SELL", "FORCED_SELL"}

_METRIC_VALUE_FIELDS = (
    "date",
    "P",
    "K1",
    "K2",
    "K3",
    "K4",
    "Kf2bis",
    "sum_slope",
    "slope_vrai",
    "sum_slope_basse",
    "slope_vrai_basse",
)

_SERIES_TO_METRIC_FIELD = {
    "P": "P",
    "K1": "K1",
    "K2": "K2",
    "K3": "K3",
    "K4": "K4",
    "Kf2bis": "Kf2bis",
    "SUM_SLOPE": "sum_slope",
    "SLOPE_VRAI": "slope_vrai",
    "SUM_SLOPE_BASSE": "sum_slope_basse",
    "SLOPE_VRAI_BASSE": "slope_vrai_basse",
}

_MARKET_CAP_PROVIDER = "eodhd"
_MARKET_CAP_MIN_KEY = "market_cap_min"
_MARKET_CAP_MAX_KEY = "market_cap_max"
_MARKET_CAP_MISSING_POLICY_KEY = "market_cap_missing_policy"


def _metric_series_value(metric_row: dict[str, Any] | None, series_key: str) -> str | None:
    if not metric_row:
        return None
    value = metric_row.get(_SERIES_TO_METRIC_FIELD[series_key])
    return None if value in (None, "") else str(value)


def _decimal_str(value: Any) -> str | None:
    dec = value
    if dec in (None, ""):
        return None
    try:
        return format(dec.normalize(), "f")
    except Exception:
        return str(dec)


def _build_markers(daily: list[dict[str, Any]]) -> list[dict[str, str]]:
    markers: list[dict[str, str]] = []
    for row in daily:
        date = str((row or {}).get("date") or "")
        action = str((row or {}).get("action") or "")
        if not date or not action:
            continue
        for part in action.split("+"):
            marker_type = part.strip().upper()
            if marker_type in _ACTION_MARKERS:
                markers.append({"date": date, "type": marker_type})
    return markers


def _signal_series_keys(buy_codes: list[str]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for code in buy_codes:
        normalized = str(code or "").strip().upper()
        for series_key in _SIGNAL_SERIES_FIELDS.get(normalized, ()):
            if series_key not in seen:
                keys.append(series_key)
                seen.add(series_key)
    return keys


def _find_symbol_for_ticker(backtest, ticker: str) -> Symbol | None:
    symbol = backtest.scenario.symbols.filter(ticker=ticker).order_by("id").first()
    if symbol is not None:
        return symbol
    return Symbol.objects.filter(ticker=ticker).order_by("id").first()


def _build_market_cap_payload(*, backtest, symbol: Symbol | None, dates: list[str]) -> dict[str, Any] | None:
    settings = dict(getattr(backtest, "settings", {}) or {})
    market_cap_min = _decimal_str(settings.get(_MARKET_CAP_MIN_KEY))
    market_cap_max = _decimal_str(settings.get(_MARKET_CAP_MAX_KEY))
    filter_configured = market_cap_min is not None or market_cap_max is not None
    missing_policy = None
    if filter_configured:
        raw_policy = str(settings.get(_MARKET_CAP_MISSING_POLICY_KEY) or "BLOCK").strip().upper()
        missing_policy = raw_policy if raw_policy in {"BLOCK", "ALLOW"} else "BLOCK"

    if not dates:
        return None

    last_date = date.fromisoformat(dates[-1])
    history: list[tuple[date, Any]] = []
    if symbol is not None:
        history = list(
            HistoricalMarketCap.objects
            .filter(symbol=symbol, provider=_MARKET_CAP_PROVIDER, date__lte=last_date)
            .order_by("date")
            .values_list("date", "market_cap")
        )

    if not history and not filter_configured:
        return None

    values: list[str | None] = []
    history_idx = -1
    for current_date in dates:
        current_date_obj = date.fromisoformat(current_date)
        while history_idx + 1 < len(history) and history[history_idx + 1][0] <= current_date_obj:
            history_idx += 1
        values.append(_decimal_str(history[history_idx][1]) if history_idx >= 0 else None)
    has_data = any(value is not None for value in values)

    return {
        "label": "Historical Market Cap",
        "values": values,
        "min": market_cap_min,
        "max": market_cap_max,
        "missing_policy": missing_policy,
        "has_data": has_data,
    }


def build_diagnostic_chart_payload(*, backtest, ticker: str, line_index: int, line: dict[str, Any] | None,
                                   daily: list[dict[str, Any]] | None, portfolio_daily: list[dict[str, Any]] | None):
    if not line or not daily:
        return None

    dates = [str((row or {}).get("date") or "") for row in daily if (row or {}).get("date")]
    if not dates:
        return None

    symbol = _find_symbol_for_ticker(backtest, ticker)
    metrics_by_date: dict[str, dict[str, Any]] = {}
    if symbol is not None:
        metric_rows = (
            DailyMetric.objects.filter(symbol=symbol, scenario=backtest.scenario, date__in=dates)
            .values(*_METRIC_VALUE_FIELDS)
        )
        metrics_by_date = {str(row["date"]): row for row in metric_rows}

    buy_codes = list((line or {}).get("buy") or [])
    signal_series = {}
    for series_key in _signal_series_keys(buy_codes):
        signal_series[series_key] = {
            "key": series_key,
            "values": [_metric_series_value(metrics_by_date.get(date), series_key) for date in dates],
        }

    reference_price = [_metric_series_value(metrics_by_date.get(date), "P") for date in dates]
    close_price = [None if (row or {}).get("price_close") in (None, "") else str((row or {}).get("price_close")) for row in daily]

    buy_gm_filter = str((line or {}).get("buy_gm_filter") or "IGNORE").strip() or "IGNORE"
    gm = None
    if buy_gm_filter != "IGNORE":
        gm_by_date = {
            str((row or {}).get("date") or ""): None if (row or {}).get("avg_global_nglobal") in (None, "") else str((row or {}).get("avg_global_nglobal"))
            for row in (portfolio_daily or [])
            if (row or {}).get("date")
        }
        gm = {
            "role": "filter",
            "filter_code": buy_gm_filter,
            "label": "Filtre GM",
            "values": [gm_by_date.get(date) for date in dates],
        }

    thresholds = {
        "slope_threshold": _decimal_str(getattr(backtest.scenario, "slope_threshold", None)),
        "slope_threshold_basse": _decimal_str(getattr(backtest.scenario, "slope_threshold_basse", None)),
    }
    market_cap = _build_market_cap_payload(backtest=backtest, symbol=symbol, dates=dates)

    return {
        "ticker": ticker,
        "line_index": int(line_index),
        "dates": dates,
        "reference_price": reference_price,
        "close_price": close_price,
        "markers": _build_markers(daily),
        "signal_series": signal_series,
        "gm": gm,
        "market_cap": market_cap,
        "thresholds": thresholds,
    }
