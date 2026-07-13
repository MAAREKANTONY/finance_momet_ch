from __future__ import annotations

from bisect import bisect_right
from datetime import date
from decimal import Decimal
from typing import Any

from core.models import Alert, DailyBar, DailyMetric, HistoricalMarketCap, Symbol
from core.services.couloir import is_couloir_line
from core.services.market_cap import preload_market_cap_series
from core.services.recent_high_drawdown import (
    compute_recent_high_drawdown_condition,
    normalize_recent_high_drawdown_params,
)
from core.services.gm_push import compute_current_push_values_by_date, compute_push_state_by_date, compute_push_values_for_series
from core.services.china_benchmark_registry import csi300_market_benchmark_exchange, csi300_market_benchmark_ticker
from core.services.trend_filters import (
    collect_distinct_benchmark_tickers,
    evaluate_trend_filters_for_symbol,
    market_benchmark_ticker_for_symbol,
    normalize_trend_filter_settings,
    preload_benchmark_price_cache,
    sector_benchmark_ticker_for_symbol,
    summarize_benchmark_usage,
    trend_return_from_cache,
)


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


def _universe_code_for_backtest(backtest) -> str | None:
    meta = ((getattr(backtest, "results", None) or {}).get("meta") or {}).get("universe") or {}
    if meta.get("universe_code"):
        return str(meta["universe_code"]).strip().upper()
    mode = str(getattr(getattr(backtest, "scenario", None), "universe_mode", "") or "").strip().upper()
    if mode == "CSI300_HISTORICAL_DYNAMIC":
        return "CSI300"
    if mode == "SP500_HISTORICAL_DYNAMIC":
        return "SP500"
    return None


def _should_replace_benchmark_symbol(existing: Symbol | None, candidate: Symbol) -> bool:
    if existing is None:
        return True
    if str(candidate.ticker).upper() == csi300_market_benchmark_ticker().upper():
        return str(candidate.exchange or "").upper() == csi300_market_benchmark_exchange().upper()
    return False


def _load_benchmark_symbols_by_ticker(benchmark_tickers: list[str]) -> dict[str, Symbol]:
    symbols_by_ticker: dict[str, Symbol] = {}
    if not benchmark_tickers:
        return symbols_by_ticker
    for benchmark_symbol in Symbol.objects.filter(ticker__in=benchmark_tickers).order_by("ticker", "id"):
        existing = symbols_by_ticker.get(benchmark_symbol.ticker)
        if _should_replace_benchmark_symbol(existing, benchmark_symbol):
            symbols_by_ticker[benchmark_symbol.ticker] = benchmark_symbol
    return symbols_by_ticker

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
_GM_FAMILIES = ("current", "market", "sector")
_GM_FAMILY_TO_SETTING_KEY = {
    "current": "trend_filter_gm_current",
    "market": "trend_filter_gm_market",
    "sector": "trend_filter_gm_sector",
}
_GM_FAMILY_LABELS = {
    "current": "GM actuel",
    "market": "GM marché",
    "sector": "GM secteur",
}
_GM_PUSH_FAMILY_LABELS = {
    "current": "GM_push actuel",
    "market": "GM_push marché",
    "sector": "GM_push secteur",
}


def _alerts_set(alerts_str: str) -> set[str]:
    if not alerts_str:
        return set()
    return {a.strip() for a in str(alerts_str).split(",") if a.strip()}


def _build_action_by_date_from_events(events: list[dict[str, Any]] | None) -> dict[str, str]:
    actions_by_date: dict[str, list[str]] = {}
    for event in events or []:
        row_date = str((event or {}).get("date") or "").strip()
        action = str((event or {}).get("action") or "").strip().upper()
        if not row_date or not action:
            continue
        actions_by_date.setdefault(row_date, []).append(action)
    return {row_date: "+".join(parts) for row_date, parts in actions_by_date.items()}


def _market_cap_at_or_before_from_series(series: list[tuple[date, Decimal]] | None, as_of: date) -> Decimal | None:
    if not series or not as_of:
        return None
    dates = [row_date for row_date, _market_cap in series]
    idx = bisect_right(dates, as_of) - 1
    if idx < 0:
        return None
    return series[idx][1]


def build_backtest_ticker_diagnostic_on_demand(*, backtest, ticker: str, line_index: int, line: dict[str, Any] | None) -> list[dict[str, Any]]:
    from core.services.backtesting.engine import (
        _buy_tradability_for_day,
        _market_cap_bounds_from_settings,
        _market_cap_filter_enabled,
        _market_cap_missing_policy_from_settings,
        _price_bounds_from_settings,
    )

    symbol = _find_symbol_for_ticker(backtest, ticker)
    if symbol is None:
        return []

    start_date = getattr(backtest, "start_date", None)
    end_date = getattr(backtest, "end_date", None)
    if not start_date or not end_date:
        return []

    bar_rows = list(
        DailyBar.objects
        .filter(symbol=symbol, date__gte=start_date, date__lte=end_date)
        .order_by("date")
        .values("date", "close")
    )
    if not bar_rows:
        return []

    dates = [row["date"] for row in bar_rows]
    metric_rows = {
        row["date"]: row
        for row in DailyMetric.objects.filter(symbol=symbol, scenario=backtest.scenario, date__in=dates).values("date", "ratio_P", "P")
    }
    alert_rows = {
        row["date"]: _alerts_set(row.get("alerts") or "")
        for row in Alert.objects.filter(symbol=symbol, scenario=backtest.scenario, date__in=dates).values("date", "alerts")
    }
    event_rows = list((line or {}).get("events") or [])
    actions_by_date = _build_action_by_date_from_events(event_rows)
    events_by_date: dict[str, list[dict[str, Any]]] = {}
    for event in event_rows:
        row_date_str = str((event or {}).get("date") or "").strip()
        if row_date_str:
            events_by_date.setdefault(row_date_str, []).append(event)

    settings = dict(getattr(backtest, "settings", {}) or {})
    include_all = bool(getattr(backtest, "include_all_tickers", False))
    ratio_threshold = _to_decimal_or_none(getattr(backtest, "ratio_threshold", None)) or Decimal("0")
    min_price, max_price = _price_bounds_from_settings(settings)
    min_market_cap, max_market_cap = _market_cap_bounds_from_settings(settings)
    market_cap_missing_policy = _market_cap_missing_policy_from_settings(settings)
    market_cap_filter_enabled = _market_cap_filter_enabled(min_market_cap, max_market_cap)
    market_cap_series = []
    if market_cap_filter_enabled and getattr(symbol, "id", None):
        market_cap_series = preload_market_cap_series([symbol], date.min, end_date).get(symbol.id, [])

    shares_open = False
    trade_count = 0
    sum_g = Decimal("0")
    tradable_days = 0
    tradable_days_in_position = 0
    rhd_lookback_days, rhd_max_drop_pct = normalize_recent_high_drawdown_params(backtest.scenario)
    prior_reference_prices: list[Decimal] = []

    daily: list[dict[str, Any]] = []
    for row in bar_rows:
        row_date = row["date"]
        metric_row = metric_rows.get(row_date) or {}
        reference_price = _to_decimal_or_none(metric_row.get("P"))
        ratio_raw = _to_decimal_or_none(metric_row.get("ratio_P"))
        row_date_str = str(row_date)
        action_g = None
        action_pnl_amount = None
        day_events = events_by_date.get(row_date_str, [])
        for event in day_events:
            action = str((event or {}).get("action") or "").strip().upper()
            if action == "BUY":
                shares_open = True
            elif action in {"SELL", "FORCED_SELL"}:
                shares_open = False
                g_value = _to_decimal_or_none((event or {}).get("action_G"))
                pnl_value = _to_decimal_or_none((event or {}).get("action_PNL_AMOUNT"))
                if g_value is not None:
                    sum_g += g_value
                    trade_count += 1
                    action_g = str(g_value)
                if pnl_value is not None:
                    action_pnl_amount = str(pnl_value)

        market_cap_value = _market_cap_at_or_before_from_series(market_cap_series, row_date) if market_cap_filter_enabled else None
        tradable, ratio_pct, _ratio_raw = _buy_tradability_for_day(
            price_value=row.get("close"),
            ratio_p_val=metric_row.get("ratio_P"),
            market_cap_value=market_cap_value,
            include_all=include_all,
            ratio_threshold=ratio_threshold,
            min_price=min_price,
            max_price=max_price,
            min_market_cap=min_market_cap,
            max_market_cap=max_market_cap,
            market_cap_missing_policy=market_cap_missing_policy,
        )
        if tradable:
            tradable_days += 1
            if shares_open:
                tradable_days_in_position += 1
        rhd_state = compute_recent_high_drawdown_condition(
            previous_prices=prior_reference_prices,
            current_price=reference_price,
            lookback_days=rhd_lookback_days,
            max_drop_pct=rhd_max_drop_pct,
        )
        not_in_position_days = max(0, tradable_days - tradable_days_in_position)
        bt_value = sum_g
        s_g_n = None if trade_count == 0 else (sum_g / Decimal(trade_count))
        bmj = None if not_in_position_days == 0 else (bt_value / Decimal(not_in_position_days))
        bmd = None if tradable_days_in_position == 0 else (bt_value / Decimal(tradable_days_in_position))

        daily.append({
            "date": row_date_str,
            "price_close": None if row.get("close") in (None, "") else str(row.get("close")),
            "reference_price": None if reference_price is None else str(reference_price),
            "ratio_P": None if ratio_raw is None else str(ratio_raw),
            "ratio_P_pct": None if ratio_pct is None else str(ratio_pct),
            "tradable": tradable,
            "recent_high_drawdown_enabled": rhd_state["enabled"],
            "recent_high_drawdown_passed": rhd_state["passed"],
            "recent_high_drawdown_recent_high": None if rhd_state["recent_high"] is None else str(rhd_state["recent_high"]),
            "recent_high_drawdown_threshold_price": None if rhd_state["threshold_price"] is None else str(rhd_state["threshold_price"]),
            "alerts": sorted(list(alert_rows.get(row_date, set()))),
            "action": actions_by_date.get(row_date_str),
            "action_G": action_g,
            "action_PNL_AMOUNT": action_pnl_amount,
            "forced_close": "FORCED_SELL" in str(actions_by_date.get(row_date_str) or ""),
            "shares": 1 if shares_open else 0,
            "N": trade_count,
            "S_G_N": None if s_g_n is None else str(s_g_n),
            "BT": str(bt_value),
            "TRADABLE_DAYS": tradable_days,
            "TRADABLE_DAYS_NOT_IN_POSITION": not_in_position_days,
            "TRADABLE_DAYS_IN_POSITION_CLOSED": tradable_days_in_position,
            "NB_JOUR_OUVRES": not_in_position_days,
            "BMJ": None if bmj is None else str(bmj),
            "BMD": None if bmd is None else str(bmd),
            "BUY_DAYS_CLOSED": tradable_days_in_position,
            "RATIO_NOT_IN_POSITION": None if tradable_days == 0 else str((Decimal(not_in_position_days) / Decimal(tradable_days)) * Decimal("100")),
            "RATIO_IN_POSITION": None if tradable_days == 0 else str((Decimal(tradable_days_in_position) / Decimal(tradable_days)) * Decimal("100")),
        })
        if reference_price is not None:
            prior_reference_prices.append(reference_price)
    return daily


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


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _normalize_gm_condition_mode(value: Any) -> str:
    code = str(value or "IGNORE").strip().upper()
    if code.startswith("GM_"):
        code = code[3:]
    mapping = {
        "POSITIVE": "POS",
        "POSITIF": "POS",
        "NEGATIVE": "NEG",
        "NEGATIF": "NEG",
        "NEUTRAL": "NEU",
        "NEUTRE": "NEU",
        "POS_OR_NEU": "POS_OR_NEU",
        "NEG_OR_NEU": "NEG_OR_NEU",
    }
    code = mapping.get(code, code)
    return code if code in {"IGNORE", "POS", "NEG", "NEU", "POS_OR_NEU", "NEG_OR_NEU"} else "IGNORE"


def _gm_mode_to_filter_code(mode: Any) -> str:
    normalized = _normalize_gm_condition_mode(mode)
    return {
        "POS": "GM_POS",
        "NEG": "GM_NEG",
        "NEU": "GM_NEU",
        "POS_OR_NEU": "GM_POS_OR_NEU",
        "NEG_OR_NEU": "GM_NEG_OR_NEU",
    }.get(normalized, "IGNORE")


def _gm_condition_entry(config: dict[str, Any] | None, family: str) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {"mode": "IGNORE", "threshold": None, "explicit_threshold": False}
    raw = config.get(family)
    if not isinstance(raw, dict):
        return {"mode": "IGNORE", "threshold": None, "explicit_threshold": False}
    return {
        "mode": _normalize_gm_condition_mode(raw.get("mode") or raw.get("direction") or raw.get("code")),
        "threshold": None if raw.get("threshold") in (None, "") else str(raw.get("threshold")),
        "explicit_threshold": bool(raw.get("explicit_threshold")) and raw.get("threshold") not in (None, ""),
    }


def _line_gm_diagnostic_config(line: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(line, dict):
        return None
    gm_buy = line.get("gm_buy_conditions") if isinstance(line.get("gm_buy_conditions"), dict) else {}
    gm_sell = line.get("gm_sell_market_exit_conditions") if isinstance(line.get("gm_sell_market_exit_conditions"), dict) else {}
    legacy_buy = {
        "current": line.get("buy_market_gm_current", line.get("buy_gm_filter")),
        "market": line.get("buy_market_gm_market"),
        "sector": line.get("buy_market_gm_sector"),
    }

    settings = {"trend_filter_operator": "OR"}
    roles_by_family: dict[str, list[dict[str, Any]]] = {family: [] for family in _GM_FAMILIES}
    for family in _GM_FAMILIES:
        buy_entry = _gm_condition_entry(gm_buy, family)
        if buy_entry["mode"] == "IGNORE":
            legacy_code = _gm_mode_to_filter_code(legacy_buy.get(family))
            if legacy_code != "IGNORE":
                buy_entry = {"mode": _normalize_gm_condition_mode(legacy_code), "threshold": None, "explicit_threshold": False}
        if buy_entry["mode"] != "IGNORE":
            roles_by_family[family].append({"role": "BUY", **buy_entry})

        sell_entry = _gm_condition_entry(gm_sell, family)
        if sell_entry["mode"] != "IGNORE":
            roles_by_family[family].append({"role": "SELL", **sell_entry})

        selected = roles_by_family[family][0] if roles_by_family[family] else {"mode": "IGNORE"}
        settings[_GM_FAMILY_TO_SETTING_KEY[family]] = _gm_mode_to_filter_code(selected.get("mode"))

    if not any(roles_by_family.values()):
        return None
    return {"settings": settings, "roles_by_family": roles_by_family}


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
        action_reason = str((row or {}).get("action_reason") or "").upper()
        if "GM_PUSH_MARKET_EXIT" in action_reason:
            markers.append({"date": date, "type": "GM_PUSH_MARKET_EXIT"})
    return markers


def _daily_series(daily: list[dict[str, Any]], key: str) -> list[str | None]:
    return [None if (row or {}).get(key) in (None, "") else str((row or {}).get(key)) for row in daily]


def _build_couloir_payload(line: dict[str, Any] | None, daily: list[dict[str, Any]], dates: list[str]) -> dict[str, Any] | None:
    active = is_couloir_line(line)
    has_trace = any((row or {}).get("couloir_state") for row in daily)
    if not active and not has_trace:
        return None

    markers: list[dict[str, str]] = []
    if has_trace:
        for row in daily:
            row = row or {}
            row_date = str(row.get("date") or "")
            if not row_date:
                continue
            if row.get("couloir_buy_candidate"):
                markers.append({"date": row_date, "type": "COULOIR_BUY_CANDIDATE"})
            if row.get("couloir_blocked_reason"):
                markers.append({
                    "date": row_date,
                    "type": "COULOIR_BUY_BLOCKED",
                    "reason": str(row.get("couloir_blocked_reason") or ""),
                })
            if row.get("couloir_buy_executed"):
                markers.append({"date": row_date, "type": "COULOIR_BUY_EXECUTED"})
            if row.get("couloir_sell_executed"):
                source = str(row.get("couloir_sell_source") or "COULOIR").strip().upper() or "COULOIR"
                marker_type = {
                    "GM": "COULOIR_SELL_GM",
                    "GM_PUSH": "COULOIR_SELL_GM_PUSH",
                    "FORCED": "COULOIR_SELL_FORCED",
                }.get(source, "COULOIR_SELL")
                markers.append({"date": row_date, "type": marker_type, "source": source})
            if row.get("couloir_reset_after_sell"):
                markers.append({"date": row_date, "type": "COULOIR_RESET"})

    return {
        "active": bool(active),
        "has_trace": bool(has_trace),
        "state": _daily_series(daily, "couloir_state"),
        "low_ref": _daily_series(daily, "couloir_low_ref"),
        "high_ref": _daily_series(daily, "couloir_high_ref"),
        "buy_threshold_price": _daily_series(daily, "couloir_buy_threshold_price"),
        "sell_threshold_price": _daily_series(daily, "couloir_sell_threshold_price"),
        "buy_candidate": [bool((row or {}).get("couloir_buy_candidate")) for row in daily],
        "sell_candidate": [bool((row or {}).get("couloir_sell_candidate")) for row in daily],
        "buy_executed": [bool((row or {}).get("couloir_buy_executed")) for row in daily],
        "sell_executed": [bool((row or {}).get("couloir_sell_executed")) for row in daily],
        "sell_source": _daily_series(daily, "couloir_sell_source"),
        "blocked_reason": _daily_series(daily, "couloir_blocked_reason"),
        "reset_after_sell": [bool((row or {}).get("couloir_reset_after_sell")) for row in daily],
        "markers": markers,
    }


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


def _build_trend_filter_payload(*, backtest, symbol: Symbol | None, dates: list[str], portfolio_daily: list[dict[str, Any]] | None, line: dict[str, Any] | None):
    line_config = _line_gm_diagnostic_config(line)
    if not line_config or not dates:
        return None

    settings = line_config["settings"]
    roles_by_family = line_config["roles_by_family"]
    universe_code = _universe_code_for_backtest(backtest)
    normalized = normalize_trend_filter_settings(settings)
    gm_values_by_date = {
        str((row or {}).get("date") or ""): None if (row or {}).get("avg_global_nglobal") in (None, "") else str((row or {}).get("avg_global_nglobal"))
        for row in (portfolio_daily or [])
        if (row or {}).get("date")
    }
    # Reuse the backend evaluator by passing the actual regime at each date.
    # The evaluator only needs the selected date's current GM regime, while
    # diagnostics need the full current-GM curve, so we keep both forms here.
    from core.services.global_momentum import regime_for_value

    gm_regime_by_date = {
        row_date: regime_for_value(_to_decimal_or_none(gm_value))
        for row_date, gm_value in gm_values_by_date.items()
    }

    universe_tickers = []
    raw_universe = getattr(backtest, "universe_snapshot", None) or []
    if isinstance(raw_universe, list):
        for item in raw_universe:
            if isinstance(item, dict):
                ticker = item.get("ticker") or item.get("symbol") or item.get("code")
                if ticker:
                    universe_tickers.append(str(ticker))
            elif item:
                universe_tickers.append(str(item))
    universe_symbols = list(Symbol.objects.filter(ticker__in=universe_tickers).order_by("ticker", "id"))

    benchmark_tickers = sorted(collect_distinct_benchmark_tickers(universe_symbols, settings, universe_code=universe_code))
    benchmark_symbols_by_ticker = _load_benchmark_symbols_by_ticker(benchmark_tickers)
    benchmark_cache = preload_benchmark_price_cache(
        symbols=list(benchmark_symbols_by_ticker.values()),
        scenario=backtest.scenario,
        start_date=date.fromisoformat(dates[0]),
        end_date=date.fromisoformat(dates[-1]),
    )

    evaluated = evaluate_trend_filters_for_symbol(
        symbol=symbol,
        settings=settings,
        as_of=date.fromisoformat(dates[-1]),
        nglobal=int(getattr(backtest.scenario, "nglobal", 20) or 20),
        gm_current_regime=gm_regime_by_date.get(dates[-1]),
        benchmark_cache_by_ticker=benchmark_cache,
        universe_code=universe_code,
    )

    filters = evaluated["filters"]

    def _series_payload(key: str, family: str, *, values: list[str | None], benchmark_ticker: str | None = None):
        payload = filters.get(key) or {}
        roles = roles_by_family.get(family) or []
        thresholds = [
            {
                "role": role.get("role"),
                "label": f"Seuil {role.get('role')} {_GM_FAMILY_LABELS[family]}",
                "mode": role.get("mode"),
                "threshold": role.get("threshold"),
            }
            for role in roles
            if role.get("explicit_threshold") and role.get("threshold") not in (None, "")
        ]
        return {
            "active": bool(roles),
            "label": _GM_FAMILY_LABELS.get(family) or payload.get("label") or key,
            "filter_code": payload.get("filter_code"),
            "benchmark_ticker": benchmark_ticker or payload.get("benchmark_ticker"),
            "values": values,
            "status": payload.get("status"),
            "reason": payload.get("reason"),
            "roles": roles,
            "thresholds": thresholds,
        }

    market_benchmark_ticker = (filters.get("trend_filter_gm_market") or {}).get("benchmark_ticker")
    sector_benchmark_ticker = (filters.get("trend_filter_gm_sector") or {}).get("benchmark_ticker")
    nglobal = int(getattr(backtest.scenario, "nglobal", 20) or 20)

    return {
        "operator": normalized["trend_filter_operator"],
        "zero_line": "0",
        "current": _series_payload(
            "trend_filter_gm_current",
            "current",
            values=[gm_values_by_date.get(row_date) for row_date in dates],
        ),
        "market": _series_payload(
            "trend_filter_gm_market",
            "market",
            benchmark_ticker=market_benchmark_ticker,
            values=[
                _decimal_str(
                    trend_return_from_cache(
                        benchmark_cache.get(market_benchmark_ticker),
                        as_of=date.fromisoformat(row_date),
                        nglobal=nglobal,
                    )
                )
                if market_benchmark_ticker
                else None
                for row_date in dates
            ],
        ),
        "sector": _series_payload(
            "trend_filter_gm_sector",
            "sector",
            benchmark_ticker=sector_benchmark_ticker,
            values=[
                _decimal_str(
                    trend_return_from_cache(
                        benchmark_cache.get(sector_benchmark_ticker),
                        as_of=date.fromisoformat(row_date),
                        nglobal=nglobal,
                    )
                )
                if sector_benchmark_ticker
                else None
                for row_date in dates
            ],
        ),
        "universe": summarize_benchmark_usage(symbols=universe_symbols, settings=settings, universe_code=universe_code),
    }


def _gm_push_condition_entry(config: dict[str, Any] | None, family: str) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {"mode": "IGNORE", "buy_threshold": None, "sell_threshold": None, "explicit_threshold": False}
    raw = config.get(family)
    if not isinstance(raw, dict):
        return {"mode": "IGNORE", "buy_threshold": None, "sell_threshold": None, "explicit_threshold": False}
    mode = _normalize_gm_condition_mode(raw.get("mode") or raw.get("direction") or raw.get("code"))
    threshold = _to_decimal_or_none(raw.get("threshold"))
    buy_threshold = _to_decimal_or_none(raw.get("buy_threshold"))
    sell_threshold = _to_decimal_or_none(raw.get("sell_threshold"))
    if threshold is not None:
        buy_threshold = threshold
        sell_threshold = threshold
    elif buy_threshold is not None and sell_threshold is None:
        sell_threshold = buy_threshold
    elif sell_threshold is not None and buy_threshold is None:
        buy_threshold = sell_threshold
    if mode in {"POS", "NEG"} and buy_threshold is None and sell_threshold is None:
        threshold = Decimal("0")
        buy_threshold = Decimal("0")
        sell_threshold = Decimal("0")
    return {
        "mode": mode,
        "threshold": None if threshold is None else str(threshold),
        "buy_threshold": None if buy_threshold is None else str(buy_threshold),
        "sell_threshold": None if sell_threshold is None else str(sell_threshold),
        "explicit_threshold": bool(raw.get("explicit_threshold")) and (buy_threshold is not None or sell_threshold is not None),
    }


def _line_gm_push_diagnostic_config(line: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(line, dict):
        return None
    gm_push_buy = line.get("gm_push_buy_conditions") if isinstance(line.get("gm_push_buy_conditions"), dict) else {}
    gm_push_sell = (
        line.get("gm_push_sell_market_exit_conditions")
        if isinstance(line.get("gm_push_sell_market_exit_conditions"), dict)
        else {}
    )
    roles_by_family: dict[str, list[dict[str, Any]]] = {family: [] for family in _GM_FAMILIES}
    for family in _GM_FAMILIES:
        buy_entry = _gm_push_condition_entry(gm_push_buy, family)
        if buy_entry["mode"] != "IGNORE":
            roles_by_family[family].append({"role": "BUY", **buy_entry})

        sell_entry = _gm_push_condition_entry(gm_push_sell, family)
        if sell_entry["mode"] != "IGNORE":
            roles_by_family[family].append({"role": "SELL", **sell_entry})

    if not any(roles_by_family.values()):
        return None
    return {
        "operator_buy": str((gm_push_buy or {}).get("operator") or "AND").strip().upper(),
        "operator_sell": str((gm_push_sell or {}).get("operator") or "AND").strip().upper(),
        "roles_by_family": roles_by_family,
    }


def _push_state_series(values_by_date: dict[date, Decimal | None], roles: list[dict[str, Any]]) -> dict[date, str]:
    buy_threshold = next((role.get("buy_threshold") for role in roles if role.get("buy_threshold") not in (None, "")), None)
    sell_threshold = next((role.get("sell_threshold") for role in roles if role.get("sell_threshold") not in (None, "")), None)
    if buy_threshold in (None, "") or sell_threshold in (None, ""):
        return {}
    return compute_push_state_by_date(values_by_date, buy_threshold=buy_threshold, sell_threshold=sell_threshold)


def _build_gm_push_payload(*, backtest, symbol: Symbol | None, dates: list[str], portfolio_daily: list[dict[str, Any]] | None, line: dict[str, Any] | None):
    line_config = _line_gm_push_diagnostic_config(line)
    if not line_config or not dates:
        return None

    roles_by_family = line_config["roles_by_family"]
    universe_code = _universe_code_for_backtest(backtest)
    nglobal = int(getattr(backtest.scenario, "nglobal", 20) or 20)
    date_objects = [date.fromisoformat(row_date) for row_date in dates]

    current_values_by_date = {
        str((row or {}).get("date") or ""): None if (row or {}).get("gm_push_current") in (None, "") else str((row or {}).get("gm_push_current"))
        for row in (portfolio_daily or [])
        if (row or {}).get("date")
    }
    if not any(current_values_by_date.get(row_date) is not None for row_date in dates):
        metrics_by_symbol: dict[int, dict[date, Any]] = {}
        for row in DailyMetric.objects.filter(scenario=backtest.scenario, date__in=date_objects).values("symbol_id", "date", "P"):
            metrics_by_symbol.setdefault(int(row["symbol_id"]), {})[row["date"]] = row.get("P")
        current_recomputed = compute_current_push_values_by_date(metrics_by_symbol, nglobal=nglobal)
        current_values_by_date = {
            row_date: _decimal_str(current_recomputed.get(date.fromisoformat(row_date)))
            for row_date in dates
        }

    universe_tickers = []
    raw_universe = getattr(backtest, "universe_snapshot", None) or []
    if isinstance(raw_universe, list):
        for item in raw_universe:
            if isinstance(item, dict):
                ticker = item.get("ticker") or item.get("symbol") or item.get("code")
                if ticker:
                    universe_tickers.append(str(ticker))
            elif item:
                universe_tickers.append(str(item))
    universe_symbols = list(Symbol.objects.filter(ticker__in=universe_tickers).order_by("ticker", "id"))

    settings = {"trend_filter_operator": "OR"}
    for family in _GM_FAMILIES:
        roles = roles_by_family.get(family) or []
        settings[_GM_FAMILY_TO_SETTING_KEY[family]] = _gm_mode_to_filter_code(roles[0].get("mode")) if roles else "IGNORE"

    benchmark_tickers = sorted(collect_distinct_benchmark_tickers(universe_symbols, settings, universe_code=universe_code))
    benchmark_symbols_by_ticker = _load_benchmark_symbols_by_ticker(benchmark_tickers)
    benchmark_cache = preload_benchmark_price_cache(
        symbols=list(benchmark_symbols_by_ticker.values()),
        scenario=backtest.scenario,
        start_date=date_objects[0],
        end_date=date_objects[-1],
    )

    def _benchmark_series(benchmark_ticker: str | None) -> dict[date, Decimal | None]:
        if not benchmark_ticker:
            return {}
        cache_entry = benchmark_cache.get(benchmark_ticker) or {}
        return compute_push_values_for_series(
            list(zip(cache_entry.get("dates") or [], cache_entry.get("values") or [])),
            nglobal=nglobal,
        )

    def _series_payload(family: str, *, values_by_date: dict[date, Decimal | None] | dict[str, str | None], benchmark_ticker: str | None = None):
        roles = roles_by_family.get(family) or []
        state_source = {
            (date.fromisoformat(row_date) if isinstance(row_date, str) else row_date): _to_decimal_or_none(value)
            for row_date, value in (values_by_date or {}).items()
        }
        states_by_date = _push_state_series(state_source, roles)
        thresholds = []
        for role in roles:
            if role.get("buy_threshold") not in (None, ""):
                thresholds.append({
                    "role": "BUY",
                    "label": f"Seuil BUY {_GM_PUSH_FAMILY_LABELS[family]} : au-dessus de {role.get('buy_threshold')}",
                    "threshold": role.get("buy_threshold"),
                    "user_threshold": role.get("threshold"),
                    "mode": role.get("mode"),
                    "source_role": role.get("role"),
                })
            if role.get("sell_threshold") not in (None, ""):
                thresholds.append({
                    "role": "SELL",
                    "label": f"Seuil SELL {_GM_PUSH_FAMILY_LABELS[family]} : en dessous de {role.get('sell_threshold')}",
                    "threshold": role.get("sell_threshold"),
                    "user_threshold": role.get("threshold"),
                    "mode": role.get("mode"),
                    "source_role": role.get("role"),
                })
        return {
            "active": bool(roles),
            "label": _GM_PUSH_FAMILY_LABELS[family],
            "benchmark_ticker": benchmark_ticker,
            "values": [
                _decimal_str(state_source.get(date.fromisoformat(row_date)))
                for row_date in dates
            ],
            "states": [states_by_date.get(date.fromisoformat(row_date), "UNKNOWN") for row_date in dates],
            "roles": roles,
            "thresholds": thresholds,
        }

    market_benchmark_ticker = None
    sector_benchmark_ticker = None
    if symbol is not None:
        market_benchmark_ticker = market_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
        sector_benchmark_ticker = sector_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)

    return {
        "operator_buy": line_config["operator_buy"] if line_config["operator_buy"] in {"AND", "OR"} else "AND",
        "operator_sell": line_config["operator_sell"] if line_config["operator_sell"] in {"AND", "OR"} else "AND",
        "zero_line": "0",
        "current": _series_payload(
            "current",
            values_by_date=current_values_by_date,
        ),
        "market": _series_payload(
            "market",
            benchmark_ticker=market_benchmark_ticker,
            values_by_date=_benchmark_series(market_benchmark_ticker),
        ),
        "sector": _series_payload(
            "sector",
            benchmark_ticker=sector_benchmark_ticker,
            values_by_date=_benchmark_series(sector_benchmark_ticker),
        ),
        "universe": summarize_benchmark_usage(symbols=universe_symbols, settings=settings, universe_code=universe_code),
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
    couloir = _build_couloir_payload(line, daily, dates)

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
        "slope_sell_threshold": _decimal_str(getattr(backtest.scenario, "slope_sell_threshold", None)),
        "slope_threshold_basse": _decimal_str(getattr(backtest.scenario, "slope_threshold_basse", None)),
        "slope_sell_threshold_basse": _decimal_str(getattr(backtest.scenario, "slope_sell_threshold_basse", None)),
    }
    market_cap = _build_market_cap_payload(backtest=backtest, symbol=symbol, dates=dates)
    trend_filters = _build_trend_filter_payload(
        backtest=backtest,
        symbol=symbol,
        dates=dates,
        portfolio_daily=portfolio_daily,
        line=line,
    )
    gm_push = _build_gm_push_payload(
        backtest=backtest,
        symbol=symbol,
        dates=dates,
        portfolio_daily=portfolio_daily,
        line=line,
    )
    rhd_lookback_days, rhd_max_drop_pct = normalize_recent_high_drawdown_params(backtest.scenario)
    recent_high_drawdown = None
    if rhd_lookback_days is not None and rhd_max_drop_pct is not None:
        prior_prices: list[Any] = []
        rhd_passed: list[bool] = []
        rhd_recent_high: list[str | None] = []
        rhd_thresholds: list[str | None] = []
        for date_value in dates:
            current_reference_price = _metric_series_value(metrics_by_date.get(date_value), "P")
            rhd_state = compute_recent_high_drawdown_condition(
                previous_prices=prior_prices,
                current_price=current_reference_price,
                lookback_days=rhd_lookback_days,
                max_drop_pct=rhd_max_drop_pct,
            )
            rhd_passed.append(bool(rhd_state["passed"]))
            rhd_recent_high.append(_decimal_str(rhd_state["recent_high"]))
            rhd_thresholds.append(_decimal_str(rhd_state["threshold_price"]))
            if current_reference_price not in (None, ""):
                prior_prices.append(current_reference_price)
        recent_high_drawdown = {
            "lookback_days": rhd_lookback_days,
            "max_drop_pct": _decimal_str(rhd_max_drop_pct),
            "passed": rhd_passed,
            "recent_high": rhd_recent_high,
            "threshold_price": rhd_thresholds,
        }

    return {
        "ticker": ticker,
        "line_index": int(line_index),
        "dates": dates,
        "reference_price": reference_price,
        "close_price": close_price,
        "markers": _build_markers(daily),
        "signal_series": signal_series,
        "gm": gm,
        "trend_filters": trend_filters,
        "gm_push": gm_push,
        "couloir": couloir,
        "market_cap": market_cap,
        "recent_high_drawdown": recent_high_drawdown,
        "thresholds": thresholds,
    }
