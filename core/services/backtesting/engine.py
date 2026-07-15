"""
Backtesting engine (Feature 3 - minimal viable implementation).

Scope:
- Compute backtest results for the configured universe and selected signal lines.
- Uses existing computed data:
  - DailyBar (prices)
  - DailyMetric (ratio_P)
  - Alert (alerts codes like A1,B1,...)

Important:
- This implementation intentionally stays simple (no fees, no slippage, close price only).
- One position at a time per (ticker, signal line).
- Sell is processed before Buy on the same day.

Future iterations will extend:
- CP global capital constraints across tickers (selection by ratio_P)
- multi-position / sizing variants
- richer analytics & exports
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import time
from decimal import Decimal
from typing import Any

from django.db import transaction

from core.models import Alert, Backtest, DailyBar, DailyMetric, Symbol
from core.services.global_momentum import (
    compute_global_momentum_values_by_date,
    regime_for_value,
)
from core.services.gm_push import (
    GM_PUSH_NEG_ACTIVE,
    GM_PUSH_POS_ACTIVE,
    GM_PUSH_UNKNOWN,
    compute_current_push_values_by_date,
    compute_push_state_by_date,
    compute_push_values_for_series,
)
from core.services.china_benchmark_registry import (
    CSI300_MARKET_BENCHMARK,
    csi300_market_benchmark_exchange,
    csi300_market_benchmark_ticker,
)
from core.services.csi300_sector_gm import (
    SECTOR_REASON_BENCHMARK_MISSING,
    SECTOR_REASON_BENCHMARK_OHLC_MISSING,
    build_csi300_sector_gm_coverage,
    csi300_benchmark_exchange_for_ticker,
    resolve_csi300_sector_benchmark,
)
from core.services.backtest_currency import effective_currency_for_new_result
from tools.csi300_policy import CSI300_SUPPORTED_HISTORY_START_ISO
from core.services.market_cap import preload_market_cap_series
from core.services.couloir import (
    COULOIR_SIGNAL_CODE,
    CouloirConfig,
    CouloirState,
    is_couloir_line,
    normalize_couloir_line_config,
)
logger = logging.getLogger(__name__)

from core.services.trend_filters import (
    collect_distinct_benchmark_tickers,
    evaluate_trend_filters_for_symbol,
    gm_condition_matches,
    market_benchmark_ticker_for_symbol,
    preload_benchmark_price_cache,
    sector_benchmark_ticker_for_symbol,
    TREND_FILTER_GM_CURRENT_KEY,
    TREND_FILTER_GM_MARKET_KEY,
    TREND_FILTER_GM_SECTOR_KEY,
    TREND_FILTER_OPERATOR_KEY,
    trend_return_from_cache,
)
from core.utils.numbers import format_decimal_plain
from core.trading_model_config import (
    SIGNAL_LATCH_OPPOSITES,
    TRADING_MODEL_AUTO_SELL_VALUES,
    TRADING_MODEL_EXPLICIT_SELL_VALUES,
    SIGNAL_LATCH_INVALIDATORS,
    SIGNAL_LATCH_STATE_PAIRS,
    SPECIAL_SELL_K1F_UPPER_DOWN_B1F,
    TRADING_MODEL_PROGRESSIVE_AUTO_SELL,
    TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL,
    resolve_trading_model,
    validate_explicit_latch_config,
    validate_progressive_explicit_sell_config,
)

GLOBAL_REGIME_FILTER_CODES = {"IGNORE", "GM_POS", "GM_NEG", "GM_NEU", "GM_POS_OR_NEU", "GM_NEG_OR_NEU"}
GM_CONDITION_FAMILIES = ("current", "market", "sector")
MARKET_CAP_MISSING_POLICY_ALLOW = "ALLOW"
MARKET_CAP_MISSING_POLICY_BLOCK = "BLOCK"
MAX_EXPLAIN_BLOCKERS = 10
REENTRY_WARNING_WINDOW_DAYS = 1
REENTRY_WARNING_CODE = "IMMEDIATE_REENTRY"


def _to_dec(v) -> Decimal | None:
    """Best-effort conversion to Decimal."""
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _normalize_price_bound(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _price_bounds_from_settings(settings: Any) -> tuple[Decimal, Decimal | None]:
    settings = settings if isinstance(settings, dict) else {}
    min_price = _normalize_price_bound(settings.get("min_price"), Decimal("0"))
    max_price = _normalize_price_bound(settings.get("max_price"), None)
    return min_price if min_price is not None else Decimal("0"), max_price


def _market_cap_bounds_from_settings(settings: Any) -> tuple[Decimal | None, Decimal | None]:
    settings = settings if isinstance(settings, dict) else {}
    return (
        _normalize_price_bound(settings.get("market_cap_min"), None),
        _normalize_price_bound(settings.get("market_cap_max"), None),
    )


def _market_cap_filter_enabled(min_market_cap: Decimal | None, max_market_cap: Decimal | None) -> bool:
    return min_market_cap is not None or max_market_cap is not None


def _market_cap_missing_policy_from_settings(settings: Any) -> str:
    settings = settings if isinstance(settings, dict) else {}
    raw = settings.get("market_cap_missing_policy")
    policy = str(raw or "").strip().upper()
    return MARKET_CAP_MISSING_POLICY_ALLOW if policy == MARKET_CAP_MISSING_POLICY_ALLOW else MARKET_CAP_MISSING_POLICY_BLOCK


def _trade_price_in_bounds(price_value: Any, *, min_price: Decimal, max_price: Decimal | None) -> bool:
    price = _to_dec(price_value)
    if price is None:
        return False
    if price < min_price:
        return False
    if max_price is not None and price > max_price:
        return False
    return True


def _market_cap_in_bounds(
    market_cap_value: Any,
    *,
    min_market_cap: Decimal | None,
    max_market_cap: Decimal | None,
    missing_policy: str,
) -> bool:
    if not _market_cap_filter_enabled(min_market_cap, max_market_cap):
        return True
    market_cap = _to_dec(market_cap_value)
    if market_cap is None:
        return missing_policy == MARKET_CAP_MISSING_POLICY_ALLOW
    if min_market_cap is not None and market_cap < min_market_cap:
        return False
    if max_market_cap is not None and market_cap > max_market_cap:
        return False
    return True


def _ratio_values_for_tradability(ratio_p_val) -> tuple[Decimal | None, Decimal | None]:
    if isinstance(ratio_p_val, dict):
        ratio_p_val = ratio_p_val.get("ratio_P")
    elif isinstance(ratio_p_val, tuple):
        ratio_p_val = _metric_val(ratio_p_val, _M_RATIO_P)

    if ratio_p_val is None:
        return None, None
    try:
        r_raw = Decimal(str(ratio_p_val))
        return r_raw, r_raw
    except Exception:
        return None, None


def _buy_tradability_for_day(
    *,
    price_value: Any,
    ratio_p_val: Any,
    market_cap_value: Any = None,
    include_all: bool,
    ratio_threshold: Decimal,
    min_price: Decimal,
    max_price: Decimal | None,
    min_market_cap: Decimal | None = None,
    max_market_cap: Decimal | None = None,
    market_cap_missing_policy: str = MARKET_CAP_MISSING_POLICY_BLOCK,
) -> tuple[bool, Decimal | None, Decimal | None]:
    ratio_pct, ratio_raw = _ratio_values_for_tradability(ratio_p_val)
    if not _trade_price_in_bounds(price_value, min_price=min_price, max_price=max_price):
        return False, ratio_pct, ratio_raw
    if not _market_cap_in_bounds(
        market_cap_value,
        min_market_cap=min_market_cap,
        max_market_cap=max_market_cap,
        missing_policy=market_cap_missing_policy,
    ):
        return False, ratio_pct, ratio_raw

    eligibility_filter_active = (not include_all) and ratio_threshold > 0
    if ratio_pct is None:
        return (not eligibility_filter_active, ratio_pct, ratio_raw)
    if not eligibility_filter_active:
        return True, ratio_pct, ratio_raw
    return ratio_pct >= ratio_threshold, ratio_pct, ratio_raw


def _cross_down(prev_a: Decimal | None, a: Decimal | None, prev_b: Decimal | None, b: Decimal | None) -> bool:
    """Return True if A crosses down B between t-1 and t."""
    if prev_a is None or a is None or prev_b is None or b is None:
        return False
    return (prev_a >= prev_b) and (a < b)


@dataclass
class BacktestEngineResult:
    results: dict[str, Any]
    logs: list[str]


def _alerts_set(alerts_str: str) -> set[str]:
    if not alerts_str:
        return set()
    return {a.strip() for a in alerts_str.split(",") if a.strip()}


# Compact metrics tuple layout: (ratio_P, K1, K1f, K2f, K2, K3, K4, P, Kf2bis, sum_slope, slope_vrai, sum_slope_basse, slope_vrai_basse)
_M_RATIO_P = 0
_M_K1 = 1
_M_K1F = 2
_M_K2F = 3
_M_K2 = 4
_M_K3 = 5
_M_K4 = 6
_M_P = 7
_M_KF = 8
_M_SUM_SLOPE = 9
_M_SLOPE_VRAI = 10
_M_SUM_SLOPE_BASSE = 11
_M_SLOPE_VRAI_BASSE = 12


def _metric_val(mtuple: tuple | None, idx: int) -> Any:
    """Return raw value from compact metrics tuple."""
    if not mtuple:
        return None
    try:
        return mtuple[idx]
    except Exception:
        return None


def _normalize_codes(value: Any) -> list[str]:
    if value in (None, ""):
        return []

    def _append_token(out: list[str], raw: Any) -> None:
        if raw in (None, ""):
            return
        text = str(raw).strip()
        if not text:
            return
        parts = [part.strip().upper() for part in text.split(",")] if "," in text else [text.upper()]
        for code in parts:
            if code and code not in out:
                out.append(code)

    if isinstance(value, str):
        out: list[str] = []
        _append_token(out, value)
        return out
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            _append_token(out, item)
        return out
    out: list[str] = []
    _append_token(out, value)
    return out


def _normalize_logic(value: Any, default: str) -> str:
    logic = str(value or default).strip().upper()
    return logic if logic in {"AND", "OR"} else default


def _normalize_global_regime_filter(value: Any) -> str:
    code = str(value or "IGNORE").strip().upper()
    return code if code in GLOBAL_REGIME_FILTER_CODES else "IGNORE"


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


def _normalize_gm_condition_entry(raw: Any = None, *, legacy_code: Any = None) -> dict[str, Any]:
    if isinstance(raw, dict):
        mode = _normalize_gm_condition_mode(raw.get("mode") or raw.get("direction") or raw.get("code"))
        threshold = raw.get("threshold")
        buy_max_threshold = raw.get("buy_max_threshold")
        explicit_raw = raw.get("explicit_threshold")
    else:
        mode = _normalize_gm_condition_mode(raw if raw not in (None, "") else legacy_code)
        threshold = None
        buy_max_threshold = None
        explicit_raw = False

    threshold_dec = _to_dec(threshold)
    buy_max_threshold_dec = _to_dec(buy_max_threshold)
    explicit_threshold = bool(explicit_raw) or threshold not in (None, "")
    if threshold_dec is None:
        explicit_threshold = False
    return {
        "mode": mode,
        "threshold": None if threshold_dec is None else format_decimal_plain(threshold_dec),
        "buy_max_threshold": None if mode != "POS" or buy_max_threshold_dec is None else format_decimal_plain(buy_max_threshold_dec),
        "explicit_threshold": bool(explicit_threshold),
    }


def _normalize_gm_conditions_config(
    raw: Any = None,
    *,
    operator: Any = None,
    current: Any = None,
    market: Any = None,
    sector: Any = None,
) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    normalized = {
        "operator": _normalize_logic(payload.get("operator", operator), "AND"),
    }
    legacy = {"current": current, "market": market, "sector": sector}
    for family in GM_CONDITION_FAMILIES:
        normalized[family] = _normalize_gm_condition_entry(
            payload.get(family),
            legacy_code=legacy.get(family),
        )
    return normalized


def _normalize_gm_push_condition_entry(raw: Any = None) -> dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    mode = _normalize_gm_condition_mode(raw.get("mode") or raw.get("direction") or raw.get("code"))
    normalized_mode = mode if mode in {"IGNORE", "POS", "NEG"} else "IGNORE"
    threshold = _to_dec(raw.get("threshold"))
    buy_threshold = _to_dec(raw.get("buy_threshold"))
    sell_threshold = _to_dec(raw.get("sell_threshold"))
    buy_max_threshold = _to_dec(raw.get("buy_max_threshold"))
    if threshold is not None:
        buy_threshold = threshold
        sell_threshold = threshold
    elif buy_threshold is not None and sell_threshold is None:
        sell_threshold = buy_threshold
    elif sell_threshold is not None and buy_threshold is None:
        buy_threshold = sell_threshold
    explicit_threshold = bool(raw.get("explicit_threshold")) or any(
        value not in (None, "")
        for value in (raw.get("threshold"), raw.get("buy_threshold"), raw.get("sell_threshold"))
    )
    if normalized_mode in {"POS", "NEG"} and buy_threshold is None and sell_threshold is None:
        buy_threshold = Decimal("0")
        sell_threshold = Decimal("0")
    return {
        "mode": normalized_mode,
        "threshold": None if threshold is None else format_decimal_plain(threshold),
        "buy_threshold": None if buy_threshold is None else format_decimal_plain(buy_threshold),
        "sell_threshold": None if sell_threshold is None else format_decimal_plain(sell_threshold),
        "buy_max_threshold": None if normalized_mode != "POS" or buy_max_threshold is None else format_decimal_plain(buy_max_threshold),
        "explicit_threshold": bool(explicit_threshold and buy_threshold is not None and sell_threshold is not None),
    }


def _normalize_gm_push_conditions_config(raw: Any = None, *, operator: Any = None) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    normalized = {
        "operator": _normalize_logic(payload.get("operator", operator), "AND"),
    }
    for family in GM_CONDITION_FAMILIES:
        normalized[family] = _normalize_gm_push_condition_entry(payload.get(family))
    return normalized


def _gm_conditions_has_active(config: dict[str, Any] | None) -> bool:
    if not isinstance(config, dict):
        return False
    return any(
        _normalize_gm_condition_mode((config.get(family) or {}).get("mode")) != "IGNORE"
        for family in GM_CONDITION_FAMILIES
    )


def _gm_push_conditions_has_active(config: dict[str, Any] | None) -> bool:
    if not isinstance(config, dict):
        return False
    return any(
        _normalize_gm_condition_mode((config.get(family) or {}).get("mode")) in {"POS", "NEG"}
        for family in GM_CONDITION_FAMILIES
    )


def _signal_lines_have_gm_conditions(signal_lines: list[dict[str, Any]] | None) -> bool:
    return any(
        _gm_conditions_has_active((line or {}).get("gm_buy_conditions"))
        or _gm_conditions_has_active((line or {}).get("gm_sell_market_exit_conditions"))
        for line in (signal_lines or [])
    )


def _signal_lines_have_gm_push_conditions(signal_lines: list[dict[str, Any]] | None) -> bool:
    return any(
        _gm_push_conditions_has_active((line or {}).get("gm_push_buy_conditions"))
        or _gm_push_conditions_has_active((line or {}).get("gm_push_sell_market_exit_conditions"))
        for line in (signal_lines or [])
    )


def _signal_lines_sector_gm_operators(signal_lines: list[dict[str, Any]] | None) -> list[str]:
    operators: set[str] = set()
    for line in signal_lines or []:
        for config_key in (
            "gm_buy_conditions",
            "gm_sell_market_exit_conditions",
            "gm_push_buy_conditions",
            "gm_push_sell_market_exit_conditions",
        ):
            config = line.get(config_key) if isinstance(line, dict) else None
            if not isinstance(config, dict):
                continue
            sector_entry = config.get("sector") if isinstance(config.get("sector"), dict) else {}
            if _normalize_gm_condition_mode(sector_entry.get("mode")) != "IGNORE":
                operators.add(_normalize_logic(config.get("operator"), "AND"))
    return sorted(operators)


def _legacy_filter_to_gm_condition(filter_code: Any) -> dict[str, Any]:
    return _normalize_gm_condition_entry(legacy_code=filter_code)


def _normalize_signal_lines_config(signal_lines: Any) -> list[dict[str, Any]]:
    if not isinstance(signal_lines, list) or not signal_lines:
        default_buy = ["AF"]
        return [{"mode": "standard", "trading_model": TRADING_MODEL_PROGRESSIVE_AUTO_SELL, "buy": default_buy, "sell": ["BF"], "buy_logic": "AND", "sell_logic": "OR"}]
    out: list[dict[str, Any]] = []
    for raw in signal_lines:
        if not isinstance(raw, dict):
            continue
        mode = str(raw.get("mode") or "standard").strip() or "standard"
        buy_codes = _normalize_codes(raw.get("buy") or raw.get("buy_conditions"))
        sell_codes = _normalize_codes(raw.get("sell") or raw.get("sell_conditions"))
        raw_is_couloir = is_couloir_line(raw)
        if raw_is_couloir:
            mode = "couloir"
            buy_codes = [COULOIR_SIGNAL_CODE]
            sell_codes = []
        gm_buy_conditions = _normalize_gm_conditions_config(
            raw.get("gm_buy_conditions"),
            operator=raw.get("buy_market_operator"),
            current=raw.get("buy_market_gm_current", raw.get("buy_gm_filter")),
            market=raw.get("buy_market_gm_market"),
            sector=raw.get("buy_market_gm_sector"),
        )
        gm_sell_market_exit_conditions = _normalize_gm_conditions_config(
            raw.get("gm_sell_market_exit_conditions"),
            operator=raw.get("gm_sell_market_exit_operator"),
        )
        gm_push_buy_conditions = _normalize_gm_push_conditions_config(raw.get("gm_push_buy_conditions"))
        gm_push_sell_market_exit_conditions = _normalize_gm_push_conditions_config(
            raw.get("gm_push_sell_market_exit_conditions"),
            operator=raw.get("gm_push_sell_market_exit_operator"),
        )
        if (
            buy_codes
            or sell_codes
            or _gm_conditions_has_active(gm_buy_conditions)
            or _gm_conditions_has_active(gm_sell_market_exit_conditions)
            or _gm_push_conditions_has_active(gm_push_buy_conditions)
            or _gm_push_conditions_has_active(gm_push_sell_market_exit_conditions)
        ):
            trading_model, explicit_trading_model = resolve_trading_model("LEGACY_DAILY" if raw_is_couloir else raw.get("trading_model"), buy_codes)
            buy_logic = _normalize_logic(raw.get("buy_logic"), "AND")
            sell_logic = _normalize_logic(raw.get("sell_logic"), "OR")
            sell_gm_filter = _normalize_global_regime_filter(raw.get("sell_gm_filter"))
            if explicit_trading_model and trading_model in TRADING_MODEL_AUTO_SELL_VALUES:
                validate_explicit_latch_config(
                    buy_codes=buy_codes,
                    buy_logic=buy_logic,
                    sell_codes=sell_codes,
                    sell_gm_filter=sell_gm_filter,
                )
            if explicit_trading_model and trading_model == TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL:
                validate_progressive_explicit_sell_config(
                    buy_codes=buy_codes,
                    buy_logic=buy_logic,
                    sell_codes=sell_codes,
                    sell_gm_filter=sell_gm_filter,
                    has_gm_sell_market_exit=(
                        _gm_conditions_has_active(gm_sell_market_exit_conditions)
                        or _gm_push_conditions_has_active(gm_push_sell_market_exit_conditions)
                    ),
                )
            payload = {
                "mode": mode,
                "trading_model": trading_model,
                "buy": buy_codes,
                "sell": sell_codes,
                "buy_logic": buy_logic,
                "sell_logic": sell_logic,
                "buy_gm_filter": _normalize_global_regime_filter(raw.get("buy_gm_filter")),
                "buy_gm_operator": _normalize_logic(raw.get("buy_gm_operator"), "AND"),
                "buy_market_gm_current": _normalize_global_regime_filter(raw.get("buy_market_gm_current")),
                "buy_market_gm_market": _normalize_global_regime_filter(raw.get("buy_market_gm_market")),
                "buy_market_gm_sector": _normalize_global_regime_filter(raw.get("buy_market_gm_sector")),
                "buy_market_operator": _normalize_logic(raw.get("buy_market_operator"), "AND"),
                "gm_buy_conditions": gm_buy_conditions,
                "gm_sell_market_exit_conditions": gm_sell_market_exit_conditions,
                "gm_push_buy_conditions": gm_push_buy_conditions,
                "gm_push_sell_market_exit_conditions": gm_push_sell_market_exit_conditions,
                "sell_gm_filter": sell_gm_filter,
                "sell_gm_operator": _normalize_logic(raw.get("sell_gm_operator"), "AND"),
            }
            if raw_is_couloir:
                payload.update(normalize_couloir_line_config(raw))
            out.append(payload)
    return out or [{"mode": "standard", "trading_model": TRADING_MODEL_PROGRESSIVE_AUTO_SELL, "buy": ["AF"], "sell": ["BF"], "buy_logic": "AND", "sell_logic": "OR", "buy_gm_filter": "IGNORE", "buy_gm_operator": "AND", "buy_market_gm_current": "IGNORE", "buy_market_gm_market": "IGNORE", "buy_market_gm_sector": "IGNORE", "buy_market_operator": "AND", "gm_buy_conditions": _normalize_gm_conditions_config(), "gm_sell_market_exit_conditions": _normalize_gm_conditions_config(), "gm_push_buy_conditions": _normalize_gm_push_conditions_config(), "gm_push_sell_market_exit_conditions": _normalize_gm_push_conditions_config(), "sell_gm_filter": "IGNORE", "sell_gm_operator": "AND"}]


def _match_codes(day_alerts: set[str], codes: list[str], logic: str = "AND") -> bool:
    codes = _normalize_codes(codes)
    if not codes:
        return False
    logic = _normalize_logic(logic, "AND")
    if logic == "OR":
        return any(code in day_alerts for code in codes)
    return all(code in day_alerts for code in codes)


_SIGNAL_STATE_PAIRS: tuple[tuple[str, str], ...] = (
    ("AF", "BF"),
    ("SPA", "SPV"),
    ("SPVA", "SPVV"),
    ("SPA_BASSE", "SPV_BASSE"),
    ("SPVA_BASSE", "SPVV_BASSE"),
    ("RHD_OK", "RHD_FAIL"),
)

# For AND conditions, non-persistent crossing signals must also be allowed to
# accumulate over time until their opposite signal invalidates them. Example:
# A1 on day T, then C1 on day T+X => A1 AND C1 becomes true on T+X if B1 did not
# occur in-between. We keep this memory separate from the persistent AF/SP*
# states so that single-signal strategies keep their historical event semantics.
_AND_LATCH_STATE_PAIRS = SIGNAL_LATCH_STATE_PAIRS

_AND_LATCH_OPPOSITE: dict[str, str] = {}
for _pos, _neg in _AND_LATCH_STATE_PAIRS:
    _AND_LATCH_OPPOSITE[_pos] = _neg
    _AND_LATCH_OPPOSITE[_neg] = _pos

_SIGNAL_LATCH_INVALIDATORS = SIGNAL_LATCH_INVALIDATORS


def _apply_signal_state_transitions(active_states: dict[str, bool], day_alerts: set[str]) -> set[str]:
    """Update persistent signal states and return *effective* codes for the day.

    Important distinction:
    - event signals like A1/B1/C1/... are only true on the crossing day and must stay
      directly matchable on that day;
    - persistent states like AF/BF, SPA/SPV, SPVA/SPVV... remain active until the
      opposite event occurs.

    The returned set therefore contains the union of:
    1) today's raw event alerts
    2) currently active persistent states
    """
    normalized = {str(code).strip().upper() for code in (day_alerts or set()) if str(code).strip()}
    for positive_code, negative_code in _SIGNAL_STATE_PAIRS:
        pos_seen = positive_code in normalized
        neg_seen = negative_code in normalized
        if pos_seen and not neg_seen:
            active_states[positive_code] = True
            active_states[negative_code] = False
        elif neg_seen and not pos_seen:
            active_states[positive_code] = False
            active_states[negative_code] = True
    active_now = {code for code, is_active in active_states.items() if is_active}
    return normalized | active_now


def _update_and_latched_states(latched_states: dict[str, bool], day_alerts: set[str]) -> set[str]:
    normalized = {str(code).strip().upper() for code in (day_alerts or set()) if str(code).strip()}
    for positive_code, negative_code in _AND_LATCH_STATE_PAIRS:
        pos_seen = positive_code in normalized
        neg_seen = negative_code in normalized
        if pos_seen and not neg_seen:
            latched_states[positive_code] = True
            latched_states[negative_code] = False
        elif neg_seen and not pos_seen:
            latched_states[positive_code] = False
            latched_states[negative_code] = True
    return {code for code, is_active in latched_states.items() if is_active}




def _reset_trade_signal_memory(state_row: dict[str, Any]) -> None:
    """Clear persisted signal memory after an executed trade.

    This prevents an opposite-side trade from reusing stale AND/persistent states
    from the previous cycle and immediately re-triggering on the same day or the
    following days without a fresh signal sequence.
    """
    state_row["active_signal_states"] = {}
    state_row["and_latched_states"] = {}
    state_row["signal_latch_state"] = {}
    state_row["signal_latch_invalidated_today"] = set()
    state_row["signal_latch_last_date"] = None
    state_row["sell_signal_latch_state"] = {}
    state_row["sell_signal_latch_last_date"] = None


def _retain_non_invalidated_latch_signals_after_sell(
    state_row: dict[str, Any],
    invalidated_signals: set[str] | None,
    previous_latch_state: dict[str, bool] | None = None,
) -> None:
    """After a latch-model SELL, clear only the invalidated buy signals.

    Product rule:
    - when a latch-model position is sold because one latched BUY signal is invalidated,
      other still-valid BUY latches must remain available for the next cycle;
    - GM remains outside latch memory and is re-evaluated only at BUY time.
    """
    invalidated = {str(code).strip().upper() for code in (invalidated_signals or set()) if str(code).strip()}
    if not invalidated:
        return
    latch_state = dict(previous_latch_state or state_row.get("signal_latch_state") or {})
    for code in invalidated:
        latch_state[code] = False
    state_row["signal_latch_state"] = latch_state
    state_row["signal_latch_invalidated_today"] = set()
    state_row["signal_latch_last_date"] = None


def _line_uses_signal_latch_model(state_row: dict[str, Any]) -> bool:
    return state_row.get("trading_model") in (
        TRADING_MODEL_AUTO_SELL_VALUES | TRADING_MODEL_EXPLICIT_SELL_VALUES
    )


def _line_uses_auto_sell_model(state_row: dict[str, Any]) -> bool:
    return state_row.get("trading_model") in TRADING_MODEL_AUTO_SELL_VALUES


def _line_uses_progressive_explicit_sell_model(state_row: dict[str, Any]) -> bool:
    return state_row.get("trading_model") == TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL


def _line_allows_same_day_reentry(state_row: dict[str, Any]) -> bool:
    return _line_uses_progressive_explicit_sell_model(state_row)


def _apply_signal_events_to_latch_state(
    state: dict[str, bool],
    required_codes: list[str],
    events: set[str],
) -> tuple[dict[str, bool], set[str]]:
    updated = dict(state or {})
    normalized_events = {str(code).strip().upper() for code in (events or set()) if str(code).strip()}
    invalidated: set[str] = set()
    for code in _normalize_codes(required_codes):
        opposite = _SIGNAL_LATCH_INVALIDATORS.get(code)
        if not opposite:
            continue
        pos_seen = code in normalized_events
        neg_seen = opposite in normalized_events
        if pos_seen and neg_seen:
            if updated.get(code):
                invalidated.add(code)
            updated[code] = False
        elif neg_seen:
            if updated.get(code):
                invalidated.add(code)
            updated[code] = False
        elif pos_seen:
            updated[code] = True
    return updated, invalidated


def _get_signal_latch_day_state(state_row: dict[str, Any], day_alerts: set[str], as_of_date: date) -> tuple[dict[str, bool], set[str]]:
    if state_row.get("signal_latch_last_date") != as_of_date:
        latch_state, invalidated = _apply_signal_events_to_latch_state(
            state_row.get("signal_latch_state") or {},
            state_row.get("buy_codes") or [],
            day_alerts,
        )
        state_row["signal_latch_state"] = latch_state
        state_row["signal_latch_invalidated_today"] = invalidated
        state_row["signal_latch_last_date"] = as_of_date
    return state_row.get("signal_latch_state") or {}, state_row.get("signal_latch_invalidated_today") or set()


def _apply_bidirectional_latch_events_to_state(
    state: dict[str, bool],
    required_codes: list[str],
    events: set[str],
) -> dict[str, bool]:
    updated = dict(state or {})
    normalized_events = {str(code).strip().upper() for code in (events or set()) if str(code).strip()}
    for code in _normalize_codes(required_codes):
        opposite = SIGNAL_LATCH_OPPOSITES.get(code)
        if not opposite:
            continue
        code_seen = code in normalized_events
        opposite_seen = opposite in normalized_events
        if code_seen and opposite_seen:
            updated[code] = False
        elif opposite_seen:
            updated[code] = False
        elif code_seen:
            updated[code] = True
    return updated


def _get_sell_signal_latch_day_state(state_row: dict[str, Any], day_alerts: set[str], as_of_date: date) -> dict[str, bool]:
    if state_row.get("sell_signal_latch_last_date") != as_of_date:
        state_row["sell_signal_latch_state"] = _apply_bidirectional_latch_events_to_state(
            state_row.get("sell_signal_latch_state") or {},
            state_row.get("sell_codes") or [],
            day_alerts,
        )
        state_row["sell_signal_latch_last_date"] = as_of_date
    return state_row.get("sell_signal_latch_state") or {}


def _signal_latch_sell_ready(state_row: dict[str, Any]) -> bool:
    sell_codes = _normalize_codes(state_row.get("sell_codes"))
    if not sell_codes:
        return False
    latch_state = state_row.get("sell_signal_latch_state") or {}
    if _normalize_logic(state_row.get("sell_logic"), "OR") == "OR":
        return any(bool(latch_state.get(code)) for code in sell_codes)
    return all(bool(latch_state.get(code)) for code in sell_codes)


def _consume_sell_latch_state(state_row: dict[str, Any]) -> None:
    state_row["sell_signal_latch_state"] = {}
    state_row["sell_signal_latch_last_date"] = None


def _signal_latch_buy_ready(state_row: dict[str, Any], gm_code: str | None) -> bool:
    buy_codes = _normalize_codes(state_row.get("buy_codes"))
    if not buy_codes:
        return False
    latch_state = state_row.get("signal_latch_state") or {}
    local_ok = all(bool(latch_state.get(code)) for code in buy_codes)
    gm_filter = _normalize_global_regime_filter(state_row.get("buy_gm_filter"))
    if gm_filter == "IGNORE":
        return local_ok
    return local_ok and _gm_filter_match(gm_code, gm_filter)


def _format_warning_date(value: Any) -> str | None:
    if isinstance(value, date):
        return str(value)
    if value in (None, ""):
        return None
    return str(value)


def _arm_reentry_warning_if_needed(state_row: dict[str, Any], *, sell_date: date, ticker: str, line_index: int) -> None:
    if not _line_uses_progressive_explicit_sell_model(state_row):
        return
    if not _signal_latch_buy_ready(state_row, None):
        return
    state_row["_reentry_warning_candidate"] = {
        "sell_date": sell_date,
        "ticker": ticker,
        "line_index": line_index,
        "buy_latch_state": dict(state_row.get("signal_latch_state") or {}),
    }


def _record_reentry_warning_if_needed(state_row: dict[str, Any], *, buy_date: date, ticker: str, line_index: int) -> None:
    candidate = state_row.get("_reentry_warning_candidate")
    if not isinstance(candidate, dict):
        return
    sell_date = candidate.get("sell_date")
    if not isinstance(sell_date, date):
        state_row["_reentry_warning_candidate"] = None
        return
    if (buy_date - sell_date).days > REENTRY_WARNING_WINDOW_DAYS:
        state_row["_reentry_warning_candidate"] = None
        return
    warnings = state_row.setdefault("warnings", [])
    warning = {
        "code": REENTRY_WARNING_CODE,
        "title": "Réentrées immédiates détectées",
        "message": (
            "Certaines conditions BUY restent actives après déclenchement des conditions SELL. "
            "Vérifiez que vos conditions SELL invalident naturellement vos conditions BUY."
        ),
        "ticker": ticker,
        "line_index": line_index,
        "sell_date": _format_warning_date(sell_date),
        "buy_date": _format_warning_date(buy_date),
        "window_days": REENTRY_WARNING_WINDOW_DAYS,
        "buy_latch_state": candidate.get("buy_latch_state") or {},
    }
    if warning not in warnings:
        warnings.append(warning)
    state_row["_reentry_warning_candidate"] = None


def _collect_backtest_warnings(state: dict[tuple[str, int], dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for st in state.values():
        for warning in st.get("warnings") or []:
            if warning not in warnings:
                warnings.append(warning)
    return warnings


def _serialize_warnings(warnings: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for warning in warnings or []:
        if not isinstance(warning, dict):
            continue
        payload = dict(warning)
        for key in ("sell_date", "buy_date"):
            payload[key] = _format_warning_date(payload.get(key))
        out.append(payload)
    return out

def _close_open_position(
    state_row: dict[str, Any],
    *,
    close_price: Decimal,
    close_date: date,
    CT: Decimal,
    fixed_capital: bool,
    reset_signal_memory: bool = True,
) -> tuple[Decimal | None, Decimal] | None:
    """Close an open position and fully update realized-trade state."""
    if (not state_row.get("position_open")) or state_row.get("entry_price") is None or int(state_row.get("shares") or 0) <= 0:
        return None

    shares = int(state_row.get("shares") or 0)
    close_price = Decimal(close_price)
    proceeds = Decimal(shares) * close_price

    if fixed_capital:
        total_after = Decimal(state_row.get("cash_ticker") or 0) + proceeds
        state_row["bank"] = Decimal(state_row.get("bank") or 0) + (total_after - CT)
        state_row["cash_ticker"] = CT
    else:
        state_row["cash_ticker"] = Decimal(state_row.get("cash_ticker") or 0) + proceeds

    entry = Decimal(state_row["entry_price"])
    pnl_amount_today = Decimal(shares) * (close_price - entry)
    G_today = None if entry == 0 else ((close_price - entry) / entry)

    state_row["pnl_amount_total"] = Decimal(state_row.get("pnl_amount_total") or 0) + pnl_amount_today
    if pnl_amount_today > 0:
        state_row["total_gain_amount"] = Decimal(state_row.get("total_gain_amount") or 0) + pnl_amount_today
        state_row["win_trades"] = int(state_row.get("win_trades") or 0) + 1
        current_max_gain = state_row.get("max_gain_amount")
        if current_max_gain is None or pnl_amount_today > current_max_gain:
            state_row["max_gain_amount"] = pnl_amount_today
    elif pnl_amount_today < 0:
        state_row["total_loss_amount"] = Decimal(state_row.get("total_loss_amount") or 0) + pnl_amount_today
        state_row["loss_trades"] = int(state_row.get("loss_trades") or 0) + 1
        current_max_loss = state_row.get("max_loss_amount")
        if current_max_loss is None or pnl_amount_today < current_max_loss:
            state_row["max_loss_amount"] = pnl_amount_today

    entry_date = state_row.get("entry_date")
    if entry_date is not None:
        try:
            state_row["buy_days_closed"] = int(state_row.get("buy_days_closed") or 0) + int((close_date - entry_date).days) + 1
        except Exception:
            pass

    state_row["entry_date"] = None
    state_row["trade_count"] = int(state_row.get("trade_count") or 0) + 1
    state_row["sum_g"] = Decimal(state_row.get("sum_g") or 0) + (G_today or Decimal("0"))
    if reset_signal_memory:
        _reset_trade_signal_memory(state_row)
    state_row["position_open"] = False
    state_row["entry_price"] = None
    state_row["shares"] = 0
    couloir_state = state_row.get("couloir_state")
    if couloir_state is not None:
        couloir_state.on_sell_executed(close_price)
    return G_today, pnl_amount_today


def _build_shared_line_kpi_values(state_row: dict[str, Any]) -> dict[str, Any]:
    """Return the shared per-line KPI values used by both backtest result paths."""
    trade_count = int(state_row["trade_count"])
    bt = state_row["sum_g"]
    tradable_days = int(state_row.get("tradable_days") or 0)
    in_position_days = int(state_row.get("tradable_days_in_position") or 0)
    not_in_position_days = max(0, tradable_days - in_position_days)
    return {
        "N": trade_count,
        "S_G_N": None if trade_count == 0 else (bt / Decimal(trade_count)),
        "BT": bt,
        "TRADABLE_DAYS": tradable_days,
        "TRADABLE_DAYS_NOT_IN_POSITION": not_in_position_days,
        "TRADABLE_DAYS_IN_POSITION_CLOSED": in_position_days,
        "BMJ": None if not_in_position_days == 0 else (bt / Decimal(not_in_position_days)),
        "BMD": None if in_position_days == 0 else (bt / Decimal(in_position_days)),
    }


def _account_for_forced_close_day(state_row: dict[str, Any], *, was_tradable: bool) -> None:
    """Keep end-of-day holding counters consistent after a forced close."""
    in_position_days = int(state_row.get("tradable_days_in_position") or 0)
    if was_tradable and in_position_days > 0:
        state_row["tradable_days_in_position"] = in_position_days - 1


def _sync_daily_row_with_shared_line_kpis(row: dict[str, Any], state_row: dict[str, Any]) -> None:
    """Project the state-backed line KPIs onto a retained detailed row."""
    shared_kpis = _build_shared_line_kpi_values(state_row)
    tradable_days = shared_kpis["TRADABLE_DAYS"]
    not_in_position_days = shared_kpis["TRADABLE_DAYS_NOT_IN_POSITION"]
    in_position_days = shared_kpis["TRADABLE_DAYS_IN_POSITION_CLOSED"]
    row.update({
        "N": shared_kpis["N"],
        "S_G_N": None if shared_kpis["S_G_N"] is None else str(shared_kpis["S_G_N"]),
        "BT": str(shared_kpis["BT"]),
        "TRADABLE_DAYS": tradable_days,
        "TRADABLE_DAYS_NOT_IN_POSITION": not_in_position_days,
        "TRADABLE_DAYS_IN_POSITION_CLOSED": in_position_days,
        "NB_JOUR_OUVRES": not_in_position_days,
        "BUY_DAYS_CLOSED": in_position_days,
        "BMJ": None if shared_kpis["BMJ"] is None else str(shared_kpis["BMJ"]),
        "BMD": None if shared_kpis["BMD"] is None else str(shared_kpis["BMD"]),
        "RATIO_NOT_IN_POSITION": None if tradable_days == 0 else str((Decimal(not_in_position_days) / Decimal(tradable_days)) * Decimal("100")),
        "RATIO_IN_POSITION": None if tradable_days == 0 else str((Decimal(in_position_days) / Decimal(tradable_days)) * Decimal("100")),
    })


def _compute_portfolio_bt_ratio(equity_end: Decimal | None, invested_end: Decimal | None) -> Decimal | None:
    """Return the validated portfolio BT ratio.

    BT = (equity_end - invested_end) / invested_end when invested_end > 0.
    """
    if equity_end is None or invested_end is None or invested_end <= 0:
        return None
    return (equity_end - invested_end) / invested_end


def _match_codes_with_memory(day_alerts: set[str], latched_alerts: set[str], codes: list[str], logic: str = "AND") -> bool:
    codes = _normalize_codes(codes)
    if not codes:
        return False
    logic = _normalize_logic(logic, "AND")
    if logic == "OR":
        return any(code in day_alerts for code in codes)
    effective = set(day_alerts or set()) | set(latched_alerts or set())
    return all(code in effective for code in codes)


def _gm_filter_match(gm_code: str | None, gm_filter: Any) -> bool:
    gm_filter = _normalize_global_regime_filter(gm_filter)
    if gm_filter == "IGNORE":
        return True
    gm_code = str(gm_code or "").strip().upper() or None
    if gm_code is None:
        return False
    if gm_filter == "GM_POS":
        return gm_code == "GM_POS"
    if gm_filter == "GM_NEG":
        return gm_code == "GM_NEG"
    if gm_filter == "GM_NEU":
        return gm_code == "GM_NEU"
    if gm_filter == "GM_POS_OR_NEU":
        return gm_code in {"GM_POS", "GM_NEU"}
    if gm_filter == "GM_NEG_OR_NEU":
        return gm_code in {"GM_NEG", "GM_NEU"}
    return False


def _match_line_with_global_filter(
    day_alerts: set[str],
    latched_alerts: set[str],
    codes: list[str],
    logic: str,
    gm_code: str | None,
    gm_filter: Any,
    gm_operator: Any,
) -> bool:
    norm_codes = _normalize_codes(codes)
    local_ok = _match_codes_with_memory(day_alerts, latched_alerts, norm_codes, logic) if norm_codes else False
    gm_filter = _normalize_global_regime_filter(gm_filter)
    if gm_filter == "IGNORE":
        return local_ok
    gm_ok = _gm_filter_match(gm_code, gm_filter)
    if not norm_codes:
        return gm_ok
    operator = _normalize_logic(gm_operator, "AND")
    return (local_ok and gm_ok) if operator == "AND" else (local_ok or gm_ok)


def _global_regime_filter_label(gm_filter: Any) -> str:
    gm_filter = _normalize_global_regime_filter(gm_filter)
    if gm_filter == "IGNORE":
        return ""
    labels = {
        "GM_POS": "GM_POS",
        "GM_NEG": "GM_NEG",
        "GM_NEU": "GM_NEU",
        "GM_POS_OR_NEU": "GM_POS OU GM_NEU",
        "GM_NEG_OR_NEU": "GM_NEG OU GM_NEU",
    }
    return labels.get(gm_filter, gm_filter)


def _compose_condition_label(codes: list[str], logic: str = "AND", gm_filter: Any = "IGNORE", gm_operator: Any = "AND") -> str:
    base = _codes_label(codes, logic)
    gm_label = _global_regime_filter_label(gm_filter)
    if not gm_label:
        return base
    if not base:
        return gm_label
    operator = _normalize_logic(gm_operator, "AND")
    op_txt = " & " if operator == "AND" else " | "
    return f"({base}){op_txt}{gm_label}"


def _line_market_conditions_settings(state_row: dict[str, Any]) -> dict[str, Any]:
    return {
        TREND_FILTER_OPERATOR_KEY: _normalize_logic(state_row.get("buy_market_operator"), "AND"),
        TREND_FILTER_GM_CURRENT_KEY: _normalize_global_regime_filter(state_row.get("buy_market_gm_current")),
        TREND_FILTER_GM_MARKET_KEY: _normalize_global_regime_filter(state_row.get("buy_market_gm_market")),
        TREND_FILTER_GM_SECTOR_KEY: _normalize_global_regime_filter(state_row.get("buy_market_gm_sector")),
    }


def _gm_conditions_to_trend_settings(config: dict[str, Any] | None) -> dict[str, Any]:
    def code_for(family: str) -> str:
        mode = _normalize_gm_condition_mode(((config or {}).get(family) or {}).get("mode"))
        return {
            "POS": "GM_POS",
            "NEG": "GM_NEG",
            "NEU": "GM_NEU",
            "POS_OR_NEU": "GM_POS_OR_NEU",
            "NEG_OR_NEU": "GM_NEG_OR_NEU",
        }.get(mode, "IGNORE")

    return {
        TREND_FILTER_OPERATOR_KEY: _normalize_logic((config or {}).get("operator"), "AND"),
        TREND_FILTER_GM_CURRENT_KEY: code_for("current"),
        TREND_FILTER_GM_MARKET_KEY: code_for("market"),
        TREND_FILTER_GM_SECTOR_KEY: code_for("sector"),
    }


def _collect_distinct_benchmark_tickers_for_line_market_conditions(
    symbols: list[Symbol],
    signal_lines: list[dict[str, Any]] | None,
    *,
    universe_code: str | None = None,
) -> set[str]:
    benchmark_tickers: set[str] = set()
    for line in signal_lines or []:
        benchmark_tickers |= collect_distinct_benchmark_tickers(
            symbols,
            _line_market_conditions_settings(line),
            universe_code=universe_code,
        )
        benchmark_tickers |= collect_distinct_benchmark_tickers(
            symbols,
            _gm_conditions_to_trend_settings(line.get("gm_buy_conditions")),
            universe_code=universe_code,
        )
        benchmark_tickers |= collect_distinct_benchmark_tickers(
            symbols,
            _gm_conditions_to_trend_settings(line.get("gm_sell_market_exit_conditions")),
            universe_code=universe_code,
        )
        benchmark_tickers |= collect_distinct_benchmark_tickers(
            symbols,
            _gm_conditions_to_trend_settings(line.get("gm_push_buy_conditions")),
            universe_code=universe_code,
        )
        benchmark_tickers |= collect_distinct_benchmark_tickers(
            symbols,
            _gm_conditions_to_trend_settings(line.get("gm_push_sell_market_exit_conditions")),
            universe_code=universe_code,
        )
    return benchmark_tickers


def _universe_code_for_backtest(backtest: Backtest, resolved_universe=None) -> str | None:
    if resolved_universe is not None:
        universe_code = getattr(resolved_universe, "universe_code", None)
        if universe_code:
            return str(universe_code).strip().upper()
    mode = str(getattr(getattr(backtest, "scenario", None), "universe_mode", "") or "").strip().upper()
    if mode == "CSI300_HISTORICAL_DYNAMIC":
        return "CSI300"
    if mode == "SP500_HISTORICAL_DYNAMIC":
        return "SP500"
    return None


def _should_replace_benchmark_symbol(
    existing: Symbol | None,
    candidate: Symbol,
    *,
    universe_code: str | None = None,
) -> bool:
    if str(universe_code or "").strip().upper() == "CSI300":
        expected_exchange = csi300_benchmark_exchange_for_ticker(candidate.ticker)
        if expected_exchange:
            return str(candidate.exchange or "").strip().upper() == expected_exchange
    if existing is None:
        return True
    if str(candidate.ticker).upper() == csi300_market_benchmark_ticker().upper():
        return str(candidate.exchange or "").upper() == csi300_market_benchmark_exchange().upper()
    return False


def _load_benchmark_symbols_by_ticker(
    benchmark_tickers: list[str],
    *,
    universe_code: str | None = None,
) -> dict[str, Symbol]:
    symbols_by_ticker: dict[str, Symbol] = {}
    if not benchmark_tickers:
        return symbols_by_ticker
    for benchmark_symbol in Symbol.objects.filter(ticker__in=benchmark_tickers).order_by("ticker", "id"):
        existing = symbols_by_ticker.get(benchmark_symbol.ticker)
        if _should_replace_benchmark_symbol(existing, benchmark_symbol, universe_code=universe_code):
            symbols_by_ticker[benchmark_symbol.ticker] = benchmark_symbol
    return symbols_by_ticker


def _line_market_conditions_label(state_row: dict[str, Any]) -> str:
    settings = _line_market_conditions_settings(state_row)
    labels = []
    mapping = (
        (TREND_FILTER_GM_CURRENT_KEY, "GM actuel"),
        (TREND_FILTER_GM_MARKET_KEY, "GM marché"),
        (TREND_FILTER_GM_SECTOR_KEY, "GM secteur"),
    )
    for key, label in mapping:
        code = settings.get(key)
        if code and code != "IGNORE":
            labels.append(f"{label}={_global_regime_filter_label(code)}")
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    op_txt = " ET " if settings[TREND_FILTER_OPERATOR_KEY] == "AND" else " OU "
    return op_txt.join(labels)


def _gm_condition_entry_label(entry: dict[str, Any] | None) -> str:
    entry = entry or {}
    mode = _normalize_gm_condition_mode(entry.get("mode"))
    if mode == "IGNORE":
        return ""
    label = {
        "POS": "positif",
        "NEG": "négatif",
        "NEU": "neutre",
        "POS_OR_NEU": "positif ou neutre",
        "NEG_OR_NEU": "négatif ou neutre",
    }.get(mode, mode)
    if entry.get("explicit_threshold"):
        threshold = entry.get("threshold")
        if threshold not in (None, ""):
            buy_max = entry.get("buy_max_threshold") if mode == "POS" else None
            if buy_max not in (None, ""):
                return f"{label} (> {threshold}, achat bloqué > {buy_max})"
            op = ">" if mode == "POS" else "<" if mode == "NEG" else "autour de"
            return f"{label} ({op} {threshold})"
    return label


def _gm_conditions_label(config: dict[str, Any] | None) -> str:
    if not _gm_conditions_has_active(config):
        return ""
    parts = []
    for family, label in (("current", "GM actuel"), ("market", "GM marché"), ("sector", "GM secteur")):
        entry_label = _gm_condition_entry_label((config or {}).get(family))
        if entry_label:
            parts.append(f"{label}: {entry_label}")
    op_txt = " ET " if _normalize_logic((config or {}).get("operator"), "AND") == "AND" else " OU "
    return op_txt.join(parts)


def _gm_push_condition_entry_label(entry: dict[str, Any] | None) -> str:
    entry = entry or {}
    mode = _normalize_gm_condition_mode(entry.get("mode"))
    if mode == "IGNORE":
        return ""
    label = {"POS": "impulsion positive", "NEG": "impulsion négative"}.get(mode, mode)
    threshold = entry.get("buy_threshold") if mode == "POS" else entry.get("sell_threshold")
    if threshold not in (None, ""):
        op = ">" if mode == "POS" else "<"
        buy_max = entry.get("buy_max_threshold") if mode == "POS" else None
        if buy_max not in (None, ""):
            return f"{label} ({op} {threshold}, achat bloqué > {buy_max})"
        return f"{label} ({op} {threshold})"
    return label


def _gm_push_conditions_label(config: dict[str, Any] | None) -> str:
    if not _gm_push_conditions_has_active(config):
        return ""
    parts = []
    for family, label in (("current", "GM_push actuel"), ("market", "GM_push marché"), ("sector", "GM_push secteur")):
        entry_label = _gm_push_condition_entry_label((config or {}).get(family))
        if entry_label:
            parts.append(f"{label}: {entry_label}")
    op_txt = " ET " if _normalize_logic((config or {}).get("operator"), "AND") == "AND" else " OU "
    return op_txt.join(parts)


def _gm_value_for_condition_family(
    *,
    family: str,
    symbol: Symbol | None,
    as_of: date,
    nglobal: int,
    gm_current_value: Decimal | None,
    benchmark_cache_by_ticker: dict[str, dict[str, list[Any]]] | None,
    universe_code: str | None = None,
) -> tuple[Decimal | None, str | None]:
    if family == "current":
        return gm_current_value, None
    if family == "market":
        benchmark_ticker = market_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
    elif family == "sector":
        benchmark_ticker = sector_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
    else:
        return None, None
    if not benchmark_ticker:
        return None, None
    return (
        trend_return_from_cache(
            (benchmark_cache_by_ticker or {}).get(benchmark_ticker),
            as_of=as_of,
            nglobal=nglobal,
        ),
        benchmark_ticker,
    )


def _build_gm_push_values_from_benchmark_cache(
    benchmark_cache_by_ticker: dict[str, dict[str, list[Any]]] | None,
    *,
    nglobal: int,
) -> dict[str, dict[date, Decimal | None]]:
    out: dict[str, dict[date, Decimal | None]] = {}
    for ticker, cache_entry in (benchmark_cache_by_ticker or {}).items():
        dates = cache_entry.get("dates") or []
        values = cache_entry.get("values") or []
        out[ticker] = compute_push_values_for_series(list(zip(dates, values)), nglobal=nglobal)
    return out


def _gm_push_value_for_condition_family(
    *,
    family: str,
    symbol: Symbol | None,
    as_of: date,
    gm_push_current_values: dict[date, Decimal | None] | None,
    gm_push_benchmark_values_by_ticker: dict[str, dict[date, Decimal | None]] | None,
    universe_code: str | None = None,
) -> tuple[Decimal | None, str | None]:
    if family == "current":
        return (gm_push_current_values or {}).get(as_of), None
    if family == "market":
        benchmark_ticker = market_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
    elif family == "sector":
        benchmark_ticker = sector_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
    else:
        return None, None
    if not benchmark_ticker:
        return None, None
    return (
        (gm_push_benchmark_values_by_ticker or {}).get(benchmark_ticker, {}).get(as_of),
        benchmark_ticker,
    )


def _gm_push_thresholds_for_entry(entry: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    buy_threshold = _to_dec(entry.get("buy_threshold"))
    sell_threshold = _to_dec(entry.get("sell_threshold"))
    return buy_threshold, sell_threshold


def _buy_condition_exceeds_max_threshold(*, mode: Any, actual_value: Decimal | None, buy_max_threshold: Any) -> bool:
    if _normalize_gm_condition_mode(mode) != "POS":
        return False
    max_threshold = _to_dec(buy_max_threshold)
    if actual_value is None or max_threshold is None:
        return False
    return actual_value > max_threshold


def _evaluation_blocked_by_buy_max_threshold(evaluation: dict[str, Any]) -> bool:
    return any(
        bool((family or {}).get("blocked_by_buy_max_threshold"))
        for family in (evaluation.get("families") or {}).values()
        if isinstance(family, dict)
    )


def _gm_buy_decision_reason(family_payload: dict[str, Any]) -> str:
    if family_payload.get("blocked_by_buy_max_threshold"):
        return "GM au-dessus du seuil haut"
    if family_payload.get("passed") is True:
        if family_payload.get("buy_max_threshold") not in (None, ""):
            return "GM dans le tunnel d'achat"
        return "GM au-dessus du seuil d'activation"
    if family_payload.get("value") in (None, ""):
        unavailable_reason = str(family_payload.get("unavailable_reason") or "")
        return {
            "SECTOR_MISSING": "GM secteur indisponible : secteur absent",
            "SECTOR_GENERIC": "GM secteur indisponible : secteur générique",
            "SECTOR_UNSUPPORTED": "GM secteur indisponible : secteur non supporté",
            SECTOR_REASON_BENCHMARK_MISSING: "GM secteur indisponible : benchmark absent",
            SECTOR_REASON_BENCHMARK_OHLC_MISSING: "GM secteur indisponible : OHLC benchmark absentes",
            "BENCHMARK_OHLC_UNAVAILABLE_AS_OF": "GM secteur indisponible à cette date",
        }.get(unavailable_reason, "GM indisponible")
    mode = _normalize_gm_condition_mode(family_payload.get("mode"))
    if bool(family_payload.get("explicit_threshold")):
        if mode == "POS":
            return "GM sous le seuil d'activation"
        if mode == "NEG":
            return "GM au-dessus du seuil d'activation"
    return "GM ne correspond pas au régime attendu"


def _compact_gm_buy_debug(evaluation: dict[str, Any], state_row: dict[str, Any] | None = None) -> dict[str, Any] | None:
    families = {}
    for family, payload in (evaluation.get("families") or {}).items():
        if not isinstance(payload, dict) or not payload.get("active"):
            continue
        families[family] = {
            "type": "GM_XXXX",
            "scope": family,
            "mode": payload.get("mode"),
            "value": payload.get("value"),
            "threshold": payload.get("threshold"),
            "buy_max_threshold": payload.get("buy_max_threshold"),
            "explicit_threshold": bool(payload.get("explicit_threshold")),
            "benchmark_ticker": payload.get("benchmark_ticker"),
            "expected_benchmark": payload.get("expected_benchmark"),
            "sector": payload.get("sector"),
            "canonical_sector": payload.get("canonical_sector"),
            "unavailable_reason": payload.get("unavailable_reason"),
            "blocked_by_buy_max_threshold": bool(payload.get("blocked_by_buy_max_threshold")),
            "decision": "passed" if payload.get("passed") else "blocked",
            "reason": _gm_buy_decision_reason(payload),
        }
    if not families:
        return None
    debug = {
        "type": "GM_XXXX",
        "operator": evaluation.get("operator"),
        "passed": bool(evaluation.get("passed")),
        "families": families,
    }
    legacy_fields = {
        key: value
        for key, value in {
            "buy_market_gm_current": (state_row or {}).get("buy_market_gm_current"),
            "buy_market_gm_market": (state_row or {}).get("buy_market_gm_market"),
            "buy_market_gm_sector": (state_row or {}).get("buy_market_gm_sector"),
        }.items()
        if value not in (None, "", "IGNORE")
    }
    if legacy_fields:
        debug["legacy_compatibility_warning"] = "Champ GM legacy présent mais ignoré au profit de gm_buy_conditions structuré."
        debug["legacy_fields"] = legacy_fields
    return debug


def _gm_push_decision_reason(family_payload: dict[str, Any]) -> str:
    if family_payload.get("blocked_by_buy_max_threshold"):
        return "GM_push au-dessus du seuil haut"
    if family_payload.get("passed") is True:
        return "GM_push actif"
    if family_payload.get("value") in (None, ""):
        return "GM_push indisponible"
    mode = _normalize_gm_condition_mode(family_payload.get("mode"))
    if mode == "POS":
        return "GM_push positif non déclenché"
    if mode == "NEG":
        return "GM_push négatif non déclenché"
    return "GM_push ne correspond pas au régime attendu"


def _compact_gm_push_buy_debug(evaluation: dict[str, Any]) -> dict[str, Any] | None:
    families = {}
    for family, payload in (evaluation.get("families") or {}).items():
        if not isinstance(payload, dict) or not payload.get("active"):
            continue
        families[family] = {
            "type": "GM_PUSH",
            "scope": family,
            "mode": payload.get("mode"),
            "value": payload.get("value"),
            "state": payload.get("state"),
            "buy_threshold": payload.get("buy_threshold"),
            "sell_threshold": payload.get("sell_threshold"),
            "buy_max_threshold": payload.get("buy_max_threshold"),
            "benchmark_ticker": payload.get("benchmark_ticker"),
            "blocked_by_buy_max_threshold": bool(payload.get("blocked_by_buy_max_threshold")),
            "decision": "passed" if payload.get("passed") else "blocked",
            "reason": _gm_push_decision_reason(payload),
        }
    if not families:
        return None
    return {
        "type": "GM_PUSH",
        "operator": evaluation.get("operator"),
        "passed": bool(evaluation.get("passed")),
        "families": families,
    }


def _evaluate_gm_push_conditions(
    *,
    config: dict[str, Any] | None,
    symbol: Symbol | None,
    as_of: date,
    gm_push_current_values: dict[date, Decimal | None] | None,
    gm_push_benchmark_values_by_ticker: dict[str, dict[date, Decimal | None]] | None,
    gm_push_state_cache: dict[tuple[Any, ...], dict[date, str]],
    apply_buy_max_threshold: bool = False,
    universe_code: str | None = None,
) -> dict[str, Any]:
    config = config if isinstance(config, dict) else {}
    active_results = []
    family_results = {}
    for family in GM_CONDITION_FAMILIES:
        entry = (config.get(family) or {}) if isinstance(config.get(family), dict) else {}
        mode = _normalize_gm_condition_mode(entry.get("mode"))
        if mode not in {"POS", "NEG"}:
            family_results[family] = {"active": False, "passed": None, "value": None, "state": GM_PUSH_UNKNOWN, "benchmark_ticker": None}
            continue
        actual_value, benchmark_ticker = _gm_push_value_for_condition_family(
            family=family,
            symbol=symbol,
            as_of=as_of,
            gm_push_current_values=gm_push_current_values,
            gm_push_benchmark_values_by_ticker=gm_push_benchmark_values_by_ticker,
            universe_code=universe_code,
        )
        buy_threshold, sell_threshold = _gm_push_thresholds_for_entry(entry)
        if family == "current":
            series_key = ("current", str(buy_threshold), str(sell_threshold))
            series = gm_push_current_values or {}
        else:
            series_key = (family, benchmark_ticker, str(buy_threshold), str(sell_threshold))
            series = (gm_push_benchmark_values_by_ticker or {}).get(benchmark_ticker or "", {})
        if series_key not in gm_push_state_cache:
            gm_push_state_cache[series_key] = compute_push_state_by_date(
                series,
                buy_threshold=buy_threshold,
                sell_threshold=sell_threshold,
            )
        state_value = gm_push_state_cache[series_key].get(as_of, GM_PUSH_UNKNOWN)
        passed_before_buy_max_threshold = (
            state_value == GM_PUSH_POS_ACTIVE
            if mode == "POS"
            else state_value == GM_PUSH_NEG_ACTIVE
        )
        blocked_by_buy_max_threshold = bool(
            apply_buy_max_threshold
            and passed_before_buy_max_threshold
            and _buy_condition_exceeds_max_threshold(
                mode=mode,
                actual_value=actual_value,
                buy_max_threshold=entry.get("buy_max_threshold"),
            )
        )
        passed = passed_before_buy_max_threshold and not blocked_by_buy_max_threshold
        payload = {
            "active": True,
            "mode": mode,
            "threshold": entry.get("threshold"),
            "buy_threshold": entry.get("buy_threshold"),
            "sell_threshold": entry.get("sell_threshold"),
            "buy_max_threshold": entry.get("buy_max_threshold"),
            "explicit_threshold": bool(entry.get("explicit_threshold")),
            "value": None if actual_value is None else str(actual_value),
            "state": state_value,
            "benchmark_ticker": benchmark_ticker,
            "passed_before_buy_max_threshold": bool(passed_before_buy_max_threshold),
            "blocked_by_buy_max_threshold": bool(blocked_by_buy_max_threshold),
            "passed": bool(passed),
        }
        family_results[family] = payload
        active_results.append(payload)
    if not active_results:
        passed_all = True
    elif _normalize_logic(config.get("operator"), "AND") == "OR":
        passed_all = any(item["passed"] is True for item in active_results)
    else:
        passed_all = all(item["passed"] is True for item in active_results)
    return {
        "operator": _normalize_logic(config.get("operator"), "AND"),
        "has_active": bool(active_results),
        "passed": bool(passed_all),
        "families": family_results,
        "label": _gm_push_conditions_label(config),
    }


def _evaluate_gm_conditions(
    *,
    config: dict[str, Any] | None,
    symbol: Symbol | None,
    as_of: date,
    nglobal: int,
    gm_current_value: Decimal | None,
    gm_current_regime: str | None = None,
    benchmark_cache_by_ticker: dict[str, dict[str, list[Any]]] | None,
    apply_buy_max_threshold: bool = False,
    universe_code: str | None = None,
) -> dict[str, Any]:
    config = config if isinstance(config, dict) else {}
    active_results = []
    family_results = {}
    for family in GM_CONDITION_FAMILIES:
        entry = (config.get(family) or {}) if isinstance(config.get(family), dict) else {}
        mode = _normalize_gm_condition_mode(entry.get("mode"))
        if mode == "IGNORE":
            family_results[family] = {"active": False, "passed": None, "value": None, "benchmark_ticker": None}
            continue
        actual_value, benchmark_ticker = _gm_value_for_condition_family(
            family=family,
            symbol=symbol,
            as_of=as_of,
            nglobal=nglobal,
            gm_current_value=gm_current_value,
            benchmark_cache_by_ticker=benchmark_cache_by_ticker,
            universe_code=universe_code,
        )
        unavailable_reason = ""
        sector = ""
        canonical_sector = ""
        expected_benchmark = ""
        if family == "sector" and str(universe_code or "").strip().upper() == "CSI300":
            resolution = resolve_csi300_sector_benchmark(symbol)
            sector = resolution.raw_sector
            canonical_sector = resolution.canonical_sector
            expected_benchmark = resolution.provider_symbol
            if actual_value is None:
                if resolution.reason:
                    unavailable_reason = resolution.reason
                elif benchmark_ticker not in (benchmark_cache_by_ticker or {}):
                    unavailable_reason = SECTOR_REASON_BENCHMARK_MISSING
                elif not ((benchmark_cache_by_ticker or {}).get(benchmark_ticker) or {}).get("dates"):
                    unavailable_reason = SECTOR_REASON_BENCHMARK_OHLC_MISSING
                else:
                    unavailable_reason = "BENCHMARK_OHLC_UNAVAILABLE_AS_OF"
        if family == "current" and not bool(entry.get("explicit_threshold")) and gm_current_regime:
            passed = _gm_filter_match(gm_current_regime, f"GM_{mode}")
        else:
            passed = gm_condition_matches(
                actual_value=actual_value,
                mode=mode,
                threshold=_to_dec(entry.get("threshold")),
                explicit_threshold=bool(entry.get("explicit_threshold")),
            )
        passed_before_buy_max_threshold = bool(passed)
        blocked_by_buy_max_threshold = bool(
            apply_buy_max_threshold
            and passed_before_buy_max_threshold
            and _buy_condition_exceeds_max_threshold(
                mode=mode,
                actual_value=actual_value,
                buy_max_threshold=entry.get("buy_max_threshold"),
            )
        )
        passed = passed_before_buy_max_threshold and not blocked_by_buy_max_threshold
        payload = {
            "active": True,
            "mode": mode,
            "threshold": entry.get("threshold"),
            "buy_max_threshold": entry.get("buy_max_threshold"),
            "explicit_threshold": bool(entry.get("explicit_threshold")),
            "value": None if actual_value is None else str(actual_value),
            "benchmark_ticker": benchmark_ticker,
            "expected_benchmark": expected_benchmark,
            "sector": sector,
            "canonical_sector": canonical_sector,
            "unavailable_reason": unavailable_reason,
            "passed_before_buy_max_threshold": bool(passed_before_buy_max_threshold),
            "blocked_by_buy_max_threshold": bool(blocked_by_buy_max_threshold),
            "passed": bool(passed),
        }
        family_results[family] = payload
        active_results.append(payload)
    if not active_results:
        passed_all = True
    elif _normalize_logic(config.get("operator"), "AND") == "OR":
        passed_all = any(item["passed"] is True for item in active_results)
    else:
        passed_all = all(item["passed"] is True for item in active_results)
    return {
        "operator": _normalize_logic(config.get("operator"), "AND"),
        "has_active": bool(active_results),
        "passed": bool(passed_all),
        "families": family_results,
        "label": _gm_conditions_label(config),
    }


def _initialize_active_signal_states(active_states: dict[str, bool], metrics_tuple: tuple | None, price_value: Any, *, slope_threshold: Any = None, slope_threshold_basse: Any = None) -> set[str]:
    """Initialize persistent states from the first in-range day values.

    This avoids missing opportunities when conditions are already true on the
    first day of the backtest/game range, without forcing a blanket buy.
    """
    price = _to_dec(price_value if price_value is not None else _metric_val(metrics_tuple, _M_P))
    kf = _to_dec(_metric_val(metrics_tuple, _M_KF))
    sum_slope = _to_dec(_metric_val(metrics_tuple, _M_SUM_SLOPE))
    slope_vrai = _to_dec(_metric_val(metrics_tuple, _M_SLOPE_VRAI))
    sum_slope_basse = _to_dec(_metric_val(metrics_tuple, _M_SUM_SLOPE_BASSE))
    slope_vrai_basse = _to_dec(_metric_val(metrics_tuple, _M_SLOPE_VRAI_BASSE))
    threshold = _to_dec(slope_threshold)
    threshold_basse = _to_dec(slope_threshold_basse)

    def set_pair(positive_code: str, negative_code: str, left: Decimal | None, right: Decimal | None):
        if left is None or right is None:
            return
        if left > right:
            active_states[positive_code] = True
            active_states[negative_code] = False
        elif left < right:
            active_states[positive_code] = False
            active_states[negative_code] = True

    set_pair("AF", "BF", price, kf)
    set_pair("SPA", "SPV", sum_slope, threshold)
    set_pair("SPVA", "SPVV", slope_vrai, threshold)
    set_pair("SPA_BASSE", "SPV_BASSE", sum_slope_basse, threshold_basse)
    set_pair("SPVA_BASSE", "SPVV_BASSE", slope_vrai_basse, threshold_basse)
    return {code for code, is_active in active_states.items() if is_active}

def _codes_label(codes: list[str], logic: str = "AND") -> str:
    norm = _normalize_codes(codes)
    return " & ".join(norm) if norm else ""



def _build_global_momentum_values_from_ticker_data(data_by_ticker: dict[str, dict[str, Any]], nglobal: int) -> dict[date, Decimal | None]:
    metrics_by_ticker: dict[str, dict[date, tuple[Any, ...]]] = {}
    for ticker, tdata in (data_by_ticker or {}).items():
        metrics = tdata.get("metrics") or {}
        if metrics:
            metrics_by_ticker[ticker] = metrics
    return compute_global_momentum_values_by_date(
        metrics_by_ticker,
        nglobal=int(nglobal or 0),
        p_getter=lambda mt: _metric_val(mt, _M_P),
    )


def _build_gm_push_current_values_from_ticker_data(data_by_ticker: dict[str, dict[str, Any]], nglobal: int) -> dict[date, Decimal | None]:
    metrics_by_ticker: dict[str, dict[date, tuple[Any, ...]]] = {}
    for ticker, tdata in (data_by_ticker or {}).items():
        metrics = tdata.get("metrics") or {}
        if metrics:
            metrics_by_ticker[ticker] = metrics
    return compute_current_push_values_by_date(
        metrics_by_ticker,
        nglobal=int(nglobal or 0),
        p_getter=lambda mt: _metric_val(mt, _M_P),
    )




def _preload_backtest_ticker_data(*, symbols: list[Symbol], scenario_id: int, fetch_start_d, end_d, include_compact_extras: bool = True) -> tuple[dict[str, dict[str, Any]], set[date]]:
    """Bulk-preload bars / metrics / alerts for a set of symbols.

    This avoids 3*N queryset loops when a scenario/backtest contains thousands of
    tickers. Returned structure intentionally matches the legacy per-ticker shape
    so downstream engine logic stays unchanged.
    """
    data_by_ticker: dict[str, dict[str, Any]] = {}
    all_dates: set[date] = set()
    if not symbols:
        return data_by_ticker, all_dates

    symbol_ids = [s.id for s in symbols]
    ticker_by_symbol_id = {s.id: s.ticker for s in symbols}

    for s in symbols:
        data_by_ticker[s.ticker] = {
            "symbol_id": s.id,
            "price_by_date": {},
            "metrics": {},
            "alerts": {},
        }

    bars_rows = DailyBar.objects.filter(
        symbol_id__in=symbol_ids,
        date__gte=fetch_start_d,
        date__lte=end_d,
    ).order_by("symbol_id", "date").values("symbol_id", "date", "close")
    for row in bars_rows:
        ticker = ticker_by_symbol_id.get(row["symbol_id"])
        if not ticker:
            continue
        d = row["date"]
        data_by_ticker[ticker]["price_by_date"][d] = row.get("close")
        all_dates.add(d)

    metric_fields = ["symbol_id", "date", "ratio_P", "K1", "K1f", "K2f", "K2", "K3", "K4", "P"]
    if include_compact_extras:
        metric_fields.extend(["Kf2bis", "sum_slope", "slope_vrai", "sum_slope_basse", "slope_vrai_basse"])
    metrics_rows = DailyMetric.objects.filter(
        symbol_id__in=symbol_ids,
        scenario_id=scenario_id,
        date__gte=fetch_start_d,
        date__lte=end_d,
    ).order_by("symbol_id", "date").values(*metric_fields)
    for row in metrics_rows:
        ticker = ticker_by_symbol_id.get(row["symbol_id"])
        if not ticker:
            continue
        if include_compact_extras:
            payload = (
                row.get("ratio_P"),
                row.get("K1"),
                row.get("K1f"),
                row.get("K2f"),
                row.get("K2"),
                row.get("K3"),
                row.get("K4"),
                row.get("P"),
                row.get("Kf2bis"),
                row.get("sum_slope"),
                row.get("slope_vrai"),
                row.get("sum_slope_basse"),
                row.get("slope_vrai_basse"),
            )
        else:
            payload = (
                row.get("ratio_P"),
                row.get("K1"),
                row.get("K1f"),
                row.get("K2f"),
                row.get("K2"),
                row.get("K3"),
                row.get("K4"),
                row.get("P"),
            )
        data_by_ticker[ticker]["metrics"][row["date"]] = payload
        all_dates.add(row["date"])

    alerts_rows = Alert.objects.filter(
        symbol_id__in=symbol_ids,
        scenario_id=scenario_id,
        date__gte=fetch_start_d,
        date__lte=end_d,
    ).order_by("symbol_id", "date").values("symbol_id", "date", "alerts")
    for row in alerts_rows:
        ticker = ticker_by_symbol_id.get(row["symbol_id"])
        if not ticker:
            continue
        data_by_ticker[ticker]["alerts"][row["date"]] = _alerts_set(row["alerts"])
        all_dates.add(row["date"])

    data_by_ticker = {ticker: payload for ticker, payload in data_by_ticker.items() if payload["price_by_date"]}
    return data_by_ticker, all_dates


def _preload_market_cap_cache(symbols: list[Symbol], end_d, *, provider: str = "eodhd") -> dict[int, dict[str, list[Any]]]:
    # One bounded preload query per simulation; hot loops use _market_cap_from_cache.
    if not symbols or not end_d:
        return {}
    raw_series = preload_market_cap_series(symbols, date.min, end_d, provider=provider)
    return {
        symbol_id: {
            "dates": [row_date for row_date, _market_cap in series],
            "values": [market_cap for _row_date, market_cap in series],
        }
        for symbol_id, series in raw_series.items()
    }


def _market_cap_from_cache(cache_entry: dict[str, list[Any]] | None, as_of) -> Decimal | None:
    # O(log n) lookup over each symbol's pre-sorted historical rows.
    if not cache_entry or not as_of:
        return None
    dates = cache_entry.get("dates") or []
    idx = bisect_right(dates, as_of) - 1
    if idx < 0:
        return None
    values = cache_entry.get("values") or []
    return values[idx] if idx < len(values) else None


def _build_global_momentum_regime_from_values(
    values_by_date: dict[date, Decimal | None],
) -> dict[date, str]:
    out: dict[date, str] = {}
    for d, v in (values_by_date or {}).items():
        regime = regime_for_value(v)
        if regime:
            out[d] = regime
    return out


def run_backtest(
    backtest: Backtest,
    checkpoint=None,
    *,
    large_result_mode: bool = False,
    estimated_daily_rows: int | None = None,
    resolved_universe=None,
) -> BacktestEngineResult:

    """
    Feature 4:
    - Adds global capital constraint (CP) and daily selection of new allocations by highest ratio_p.
    - Keeps per-(ticker,line) independent cash re-investment once allocated.
    """
    logs: list[str] = []

    def _checkpoint():
        if checkpoint is not None:
            checkpoint()

    def _append_daily_row(st: dict[str, Any], row: dict[str, Any]) -> dict[str, Any] | None:
        if large_result_mode:
            return None
        st["daily_rows"].append(row)
        return row

    def _record_line_event(
        st: dict[str, Any],
        *,
        as_of: date,
        action: str,
        price_close: Decimal | None = None,
        action_g: Decimal | None = None,
        action_pnl_amount: Decimal | None = None,
        action_reason: str | None = None,
        gm_buy_debug: dict[str, Any] | None = None,
    ) -> None:
        event = {
            "date": str(as_of),
            "action": str(action),
        }
        if action_reason:
            event["action_reason"] = str(action_reason)
        if price_close is not None:
            event["price_close"] = str(price_close)
        if action_g is not None:
            event["action_G"] = str(action_g)
        if action_pnl_amount is not None:
            event["action_PNL_AMOUNT"] = str(action_pnl_amount)
        if gm_buy_debug:
            event["gm_buy_debug"] = gm_buy_debug
        st.setdefault("events", []).append(event)

    def _couloir_threshold_value(couloir_state: CouloirState | None, ref_name: str) -> Decimal | None:
        if couloir_state is None:
            return None
        if ref_name == "low_ref":
            low_ref = couloir_state.low_ref
            if low_ref is None or low_ref <= 0:
                return None
            return low_ref * (Decimal("1") + couloir_state.config.buy_rebound_threshold)
        if ref_name == "high_ref":
            high_ref = couloir_state.high_ref
            if high_ref is None or high_ref <= 0:
                return None
            return high_ref * (Decimal("1") - couloir_state.config.sell_drawdown_threshold)
        return None

    def _couloir_debug_snapshot(st: dict[str, Any]) -> dict[str, Any]:
        couloir_state = st.get("couloir_state")
        if large_result_mode or couloir_state is None:
            return {}
        buy_threshold = _couloir_threshold_value(couloir_state, "low_ref")
        sell_threshold = _couloir_threshold_value(couloir_state, "high_ref")
        return {
            "couloir_state": couloir_state.position_state,
            "couloir_low_ref": None if couloir_state.low_ref is None else str(couloir_state.low_ref),
            "couloir_high_ref": None if couloir_state.high_ref is None else str(couloir_state.high_ref),
            "couloir_buy_threshold_price": None if buy_threshold is None else str(buy_threshold),
            "couloir_sell_threshold_price": None if sell_threshold is None else str(sell_threshold),
        }

    def _start_couloir_day_debug(st: dict[str, Any]) -> None:
        if large_result_mode or st.get("couloir_state") is None:
            return
        st["_couloir_day_debug"] = {
            **_couloir_debug_snapshot(st),
            "couloir_buy_candidate": False,
            "couloir_sell_candidate": False,
            "couloir_buy_executed": False,
            "couloir_sell_executed": False,
            "couloir_sell_source": None,
            "couloir_blocked_reason": None,
            "couloir_reset_after_sell": False,
        }

    def _merge_couloir_day_debug(st: dict[str, Any], **updates: Any) -> None:
        if large_result_mode or st.get("couloir_state") is None:
            return
        debug = st.setdefault("_couloir_day_debug", {})
        snapshot = _couloir_debug_snapshot(st)
        preserve_when_missing = {
            "couloir_low_ref",
            "couloir_high_ref",
            "couloir_buy_threshold_price",
            "couloir_sell_threshold_price",
        }
        for key, value in snapshot.items():
            if key in preserve_when_missing and value is None and debug.get(key) is not None:
                continue
            debug[key] = value
        for key, value in updates.items():
            if key == "couloir_blocked_reason" and debug.get(key):
                continue
            debug[key] = value

    def _apply_couloir_day_debug_to_last_row(st: dict[str, Any], as_of: date) -> None:
        if large_result_mode:
            return
        debug = st.get("_couloir_day_debug")
        rows = st.get("daily_rows") or []
        if not debug or not rows:
            return
        if rows[-1].get("date") != str(as_of):
            return
        rows[-1].update(debug)

    def _new_explain_summary() -> dict[str, Any]:
        return {
            "played": False,
            "buy_candidates": 0,
            "buy_executed": 0,
            "sell_executed": 0,
            "blocked_counts": {},
            "last_blockers": [],
        }

    def _explain_for_state(st: dict[str, Any]) -> dict[str, Any]:
        explain = st.setdefault("explain", _new_explain_summary())
        explain.setdefault("blocked_counts", {})
        explain.setdefault("last_blockers", [])
        return explain

    def _record_explain_buy_candidate(st: dict[str, Any], as_of: date) -> None:
        key = str(as_of)
        seen = st.setdefault("_explain_buy_candidate_dates", set())
        if key in seen:
            return
        seen.add(key)
        explain = _explain_for_state(st)
        explain["buy_candidates"] = int(explain.get("buy_candidates") or 0) + 1

    def _record_explain_trade(st: dict[str, Any], action: str) -> None:
        explain = _explain_for_state(st)
        action_key = str(action or "").upper()
        if action_key == "BUY":
            explain["buy_executed"] = int(explain.get("buy_executed") or 0) + 1
        elif action_key in {"SELL", "FORCED_SELL"}:
            explain["sell_executed"] = int(explain.get("sell_executed") or 0) + 1
        explain["played"] = True

    def _record_explain_blocker(
        st: dict[str, Any],
        as_of: date,
        reason_code: str,
        reason_label: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        try:
            code = str(reason_code or "UNKNOWN").strip() or "UNKNOWN"
            payload = payload if isinstance(payload, dict) else {}
            scope = str(payload.get("scope") or "").strip()
            dedupe_key = (str(as_of), code, scope)
            seen = st.setdefault("_explain_blocker_keys", set())
            if dedupe_key in seen:
                return
            seen.add(dedupe_key)
            explain = _explain_for_state(st)
            counts = explain.setdefault("blocked_counts", {})
            counts[code] = int(counts.get(code) or 0) + 1
            blocker = {
                "date": str(as_of),
                "code": code,
                "decision": "blocked",
                "reason": str(reason_label or code),
            }
            for key, value in payload.items():
                if value in (None, "") or key in blocker:
                    continue
                if isinstance(value, (str, int, float, bool)):
                    blocker[key] = value
                else:
                    blocker[key] = str(value)
            blockers = list(explain.setdefault("last_blockers", []))
            blockers.append(blocker)
            explain["last_blockers"] = blockers[-MAX_EXPLAIN_BLOCKERS:]
        except Exception:
            return

    def _record_gm_buy_blockers(st: dict[str, Any], as_of: date) -> None:
        gm_debug = st.get("_last_gm_buy_debug") or {}
        for scope, payload in (gm_debug.get("families") or {}).items():
            if not isinstance(payload, dict) or payload.get("decision") != "blocked":
                continue
            if scope == "sector" and payload.get("unavailable_reason"):
                code = "GM_SECTOR_UNAVAILABLE"
            else:
                code = "GM_XXXX_BUY_MAX" if payload.get("blocked_by_buy_max_threshold") else "GM_XXXX_THRESHOLD"
            _record_explain_blocker(
                st,
                as_of,
                code if code == "GM_SECTOR_UNAVAILABLE" else f"{code}_{str(scope).upper()}",
                payload.get("reason") or "GM_XXXX bloque l'achat",
                {**payload, "scope": scope},
            )

    def _record_gm_push_buy_blockers(st: dict[str, Any], as_of: date) -> None:
        gm_push_debug = st.get("_last_gm_push_buy_debug") or {}
        for scope, payload in (gm_push_debug.get("families") or {}).items():
            if not isinstance(payload, dict) or payload.get("decision") != "blocked":
                continue
            code = "GM_PUSH_BUY_MAX" if payload.get("blocked_by_buy_max_threshold") else "GM_PUSH_NOT_TRIGGERED"
            _record_explain_blocker(
                st,
                as_of,
                f"{code}_{str(scope).upper()}",
                payload.get("reason") or "GM_push bloque l'achat",
                {**payload, "scope": scope},
            )

    def _record_rhd_signal_blocker_if_relevant(st: dict[str, Any], as_of: date, buy_codes: list[str], alerts: set[str]) -> None:
        normalized_buy = {str(code).upper() for code in buy_codes or []}
        normalized_alerts = {str(code).upper() for code in alerts or set()}
        if "RHD_OK" in normalized_buy and "RHD_FAIL" in normalized_alerts and "RHD_OK" not in normalized_alerts:
            _record_explain_blocker(
                st,
                as_of,
                "RHD_FAIL_SIGNAL",
                "RHD_FAIL présent, RHD_OK absent",
                {"type": "RHD", "decision": "blocked"},
            )

    def _serialized_explain(st: dict[str, Any]) -> dict[str, Any]:
        explain = _explain_for_state(st)
        buy_executed = int(explain.get("buy_executed") or 0)
        sell_executed = int(explain.get("sell_executed") or 0)
        return {
            "played": bool(explain.get("played") or buy_executed or sell_executed),
            "buy_candidates": int(explain.get("buy_candidates") or 0),
            "buy_executed": buy_executed,
            "sell_executed": sell_executed,
            "blocked_counts": dict(explain.get("blocked_counts") or {}),
            "last_blockers": list(explain.get("last_blockers") or [])[-MAX_EXPLAIN_BLOCKERS:],
        }

    def _universe_allows_new_buy(ticker: str, as_of: date) -> bool:
        if resolved_universe is None:
            return True
        active_by_date = getattr(resolved_universe, "active_by_date", None) or {}
        return ticker in active_by_date.get(as_of, frozenset())

    def _universe_member(ticker: str, as_of: date) -> bool | None:
        if resolved_universe is None:
            return None
        return _universe_allows_new_buy(ticker, as_of)

    def _universe_daily_fields(ticker: str, as_of: date) -> dict[str, Any]:
        member = _universe_member(ticker, as_of)
        if member is None:
            return {}
        return {
            "universe_member": member,
            "buy_blocked_by_universe": False,
            "buy_blocked_reason": None,
        }

    def _mark_universe_buy_blocked(st: dict[str, Any], as_of: date) -> None:
        if resolved_universe is None or large_result_mode:
            return
        rows = st.get("daily_rows") or []
        if not rows:
            return
        last = rows[-1]
        if str(last.get("date") or "") != str(as_of):
            return
        last["buy_blocked_by_universe"] = True
        last["buy_blocked_reason"] = "not_active_in_universe"

    def _mark_gm_buy_blocked(st: dict[str, Any], as_of: date) -> None:
        if large_result_mode:
            return
        rows = st.get("daily_rows") or []
        if not rows:
            return
        last = rows[-1]
        if str(last.get("date") or "") != str(as_of):
            return
        gm_debug = st.get("_last_gm_buy_debug")
        if gm_debug:
            last["gm_buy_debug"] = gm_debug
        if st.get("_last_gm_buy_max_blocked"):
            last["buy_blocked_by_gm_buy_max"] = True
            last["buy_blocked_reason"] = "gm_buy_max_threshold"
            last["buy_blocked_message"] = "Achat bloqué : GM au-dessus du seuil haut d’achat."
        elif gm_debug:
            last["buy_blocked_by_gm"] = True
            last["buy_blocked_reason"] = "gm_threshold"
            last["buy_blocked_message"] = "Achat bloqué : GM sous le seuil d’activation."

    def _mark_gm_buy_max_blocked(st: dict[str, Any], as_of: date) -> None:
        if large_result_mode:
            return
        rows = st.get("daily_rows") or []
        if not rows:
            return
        last = rows[-1]
        if str(last.get("date") or "") != str(as_of):
            return
        last["buy_blocked_by_gm_buy_max"] = True
        last["buy_blocked_reason"] = "gm_buy_max_threshold"
        last["buy_blocked_message"] = "Achat bloqué : GM au-dessus du seuil haut d’achat."

    def _resolved_universe_meta() -> dict[str, Any] | None:
        if resolved_universe is None:
            return None
        metadata = getattr(resolved_universe, "metadata", {}) or {}
        universe_code_value = getattr(resolved_universe, "universe_code", None)
        meta = {
            "mode": getattr(resolved_universe, "mode", None),
            "universe_code": universe_code_value,
            "coverage_start": str(getattr(resolved_universe, "coverage_start", "")),
            "coverage_end": str(getattr(resolved_universe, "coverage_end", "")),
            "superset_count": len(getattr(resolved_universe, "tickers", ()) or ()),
            "ticker_count": len(getattr(resolved_universe, "tickers", ()) or ()),
            "source": metadata.get("source"),
        }
        if str(universe_code_value or "").strip().upper() == "CSI300":
            meta["supported_history_start"] = CSI300_SUPPORTED_HISTORY_START_ISO
            provider_symbol = CSI300_MARKET_BENCHMARK.provider_symbol or (
                f"{csi300_market_benchmark_ticker()}.{csi300_market_benchmark_exchange()}"
            )
            meta["market_benchmark"] = {
                "name": "CSI 300",
                "ticker": csi300_market_benchmark_ticker(),
                "exchange": csi300_market_benchmark_exchange(),
                "provider_symbol": provider_symbol,
                "label": f"CSI 300 / {provider_symbol}",
            }
            sector_operators = _signal_lines_sector_gm_operators(signal_lines)
            if sector_operators:
                active_by_date = getattr(resolved_universe, "active_by_date", {}) or {}
                sector_coverage = build_csi300_sector_gm_coverage(
                    symbols=list(sym_by_ticker.values()),
                    coverage_start=backtest.start_date,
                    coverage_end=backtest.end_date,
                    active_members_expected=max((len(items) for items in active_by_date.values()), default=0),
                )
                settings_payload = backtest.settings if isinstance(backtest.settings, dict) else {}
                sector_coverage.update({
                    "active": True,
                    "operators": sector_operators,
                    "partial_coverage_confirmed": bool(
                        settings_payload.get("dynamic_universe_readiness_ack")
                    ),
                })
                meta["sector_gm"] = sector_coverage
                meta["sector_benchmark_status"] = (
                    f"{sector_coverage['status']} — "
                    f"{sector_coverage['members_with_usable_sector_gm']}/"
                    f"{sector_coverage['symbols_considered']} tickers historiques couverts; "
                    "aucun fallback."
                )
        return meta

    def _state_copy_for_buy_probe(st: dict[str, Any]) -> dict[str, Any]:
        probe = dict(st)
        probe["active_signal_states"] = dict(st.get("active_signal_states") or {})
        probe["and_latched_states"] = dict(st.get("and_latched_states") or {})
        probe["signal_latch_state"] = dict(st.get("signal_latch_state") or {})
        probe["signal_latch_invalidated_today"] = set(st.get("signal_latch_invalidated_today") or set())
        return probe

    def _would_be_buy_candidate_without_universe(
        ticker: str,
        d: date,
        st: dict[str, Any],
        tdata: dict[str, Any],
        *,
        require_allocated: bool,
    ) -> bool:
        if require_allocated and not st.get("allocated"):
            return False

        price_by_date = tdata["price_by_date"]
        if d not in price_by_date:
            return False

        probe = _state_copy_for_buy_probe(st)
        buy_codes = probe["buy_codes"]
        day_alerts_raw = tdata["alerts"].get(d, set())
        local_event_alerts = {a.upper() for a in day_alerts_raw}
        event_alerts = set(local_event_alerts)
        gm_code = global_momentum_regime_by_date.get(d)
        if gm_code:
            event_alerts.add(gm_code)
        day_alerts = _apply_signal_state_transitions(probe["active_signal_states"], event_alerts)
        latched_alerts = _update_and_latched_states(probe["and_latched_states"], event_alerts)
        if probe["use_signal_latch_model"]:
            _get_signal_latch_day_state(probe, local_event_alerts, d)
            if not _signal_latch_buy_ready(probe, gm_code):
                return False
        elif not _match_line_with_global_filter(
            day_alerts,
            latched_alerts,
            buy_codes,
            probe["buy_logic"],
            gm_code,
            probe["buy_gm_filter"],
            probe["buy_gm_operator"],
        ):
            return False

        tradable, _ratio_pct, _ = _ratio_tradable(ticker, d, price_by_date.get(d), tdata["metrics"].get(d))
        if not tradable:
            return False
        if not _trend_filter_allows_buy(ticker, d, gm_code, probe["buy_gm_filter"]):
            return False
        if not _line_market_conditions_allow_buy(probe, ticker, d, gm_code):
            return False
        if not _line_gm_push_conditions_allow_buy(probe, ticker, d):
            return False

        if not require_allocated and not probe.get("allocated"):
            return True

        close_d = _to_dec(price_by_date[d])
        if close_d is None or close_d <= 0:
            return False
        cash = probe["cash_ticker"]
        if fixed_capital and probe.get("allocated"):
            cash = CT
        elif (not require_allocated) and CP_infinite and not probe.get("allocated"):
            cash = CT
        try:
            shares = int((cash / close_d).to_integral_value(rounding="ROUND_FLOOR"))
        except Exception:
            shares = 0
        return shares > 0

    # Universe
    raw_universe = backtest.universe_snapshot or list(backtest.scenario.symbols.values_list("ticker", flat=True))
    tickers: list[str] = []
    if isinstance(raw_universe, list):
        for item in raw_universe:
            if isinstance(item, dict):
                t = item.get("ticker") or item.get("symbol") or item.get("code")
                if t is not None:
                    tickers.append(str(t).strip())
            else:
                tickers.append(str(item).strip())
    else:
        try:
            tickers = [str(x).strip() for x in list(raw_universe)]
        except Exception:
            tickers = [str(raw_universe).strip()]
    tickers = [t for t in tickers if t]

    if not tickers:
        return BacktestEngineResult(results={"error": "No tickers in scenario/universe."}, logs=["No tickers found."])

    # Params
    CP_raw = Decimal(str(backtest.capital_total or 0))
    CP_infinite = (CP_raw == 0)
    global_cash = None if CP_infinite else CP_raw

    CT = Decimal(str(backtest.capital_per_ticker or 0))
    capital_mode = str(getattr(backtest, "capital_mode", "REINVEST") or "REINVEST").upper()
    fixed_capital = (capital_mode == "FIXED")
    X = Decimal(str(backtest.ratio_threshold or 0))  # percent threshold
    include_all = bool(getattr(backtest, "include_all_tickers", False))
    settings = getattr(backtest, "settings", {}) or {}
    min_price, max_price = _price_bounds_from_settings(settings)
    min_market_cap, max_market_cap = _market_cap_bounds_from_settings(settings)
    market_cap_missing_policy = _market_cap_missing_policy_from_settings(settings)
    market_cap_filter_enabled = _market_cap_filter_enabled(min_market_cap, max_market_cap)
    trend_filters_enabled = False

    signal_lines = _normalize_signal_lines_config(backtest.signal_lines)
    needs_gm_conditions = _signal_lines_have_gm_conditions(signal_lines)
    needs_gm_push_conditions = _signal_lines_have_gm_push_conditions(signal_lines)
    universe_code = _universe_code_for_backtest(backtest, resolved_universe)

    # Resolve symbols in one query
    symbols = list(Symbol.objects.filter(ticker__in=tickers))
    sym_by_ticker = {s.ticker: s for s in symbols}

    # Preload all data per ticker for date range
    start_d = backtest.start_date
    end_d = backtest.end_date
    warmup_days = int(getattr(backtest, "warmup_days", 0) or 0)
    fetch_start_d = (start_d - timedelta(days=warmup_days)) if (start_d and warmup_days > 0) else start_d

    # NOTE (performance/memory): for large universes we bulk-preload bars / metrics /
    # alerts in 3 queries instead of doing 3 queries per ticker. The in-memory
    # structure remains backward compatible with the legacy engine logic.
    missing_tickers = [ticker for ticker in tickers if ticker not in sym_by_ticker]
    for ticker in missing_tickers:
        logs.append(f"Ticker {ticker} not found/active; skipped.")

    preload_started = time.monotonic()
    data_by_ticker, all_dates = _preload_backtest_ticker_data(
        symbols=list(sym_by_ticker.values()),
        scenario_id=backtest.scenario_id,
        fetch_start_d=fetch_start_d,
        end_d=end_d,
        include_compact_extras=True,
    )
    logger.warning(
        "[backtest timing] step=engine_preload backtest_id=%s duration=%.3fs requested_tickers=%s loaded_tickers=%s dates=%s large_result_mode=%s",
        getattr(backtest, "id", None),
        time.monotonic() - preload_started,
        len(tickers),
        len(data_by_ticker),
        len(all_dates),
        "on" if large_result_mode else "off",
    )
    market_cap_cache = (
        _preload_market_cap_cache(list(sym_by_ticker.values()), end_d)
        if market_cap_filter_enabled
        else {}
    )
    benchmark_tickers = sorted(
        _collect_distinct_benchmark_tickers_for_line_market_conditions(
            list(sym_by_ticker.values()),
            signal_lines,
            universe_code=universe_code,
        )
        if (needs_gm_conditions or needs_gm_push_conditions)
        else set()
    )
    benchmark_symbols_by_ticker = _load_benchmark_symbols_by_ticker(
        benchmark_tickers,
        universe_code=universe_code,
    )
    benchmark_price_cache = (
        preload_benchmark_price_cache(
            symbols=list(benchmark_symbols_by_ticker.values()),
            scenario=backtest.scenario,
            start_date=fetch_start_d,
            end_date=end_d,
        )
        if benchmark_tickers
        else {}
    )
    for benchmark_ticker in benchmark_symbols_by_ticker:
        benchmark_price_cache.setdefault(benchmark_ticker, {"dates": [], "values": []})
    for ticker in tickers:
        if ticker in sym_by_ticker and ticker not in data_by_ticker:
            logs.append(f"No DailyBar data for {ticker} in range; skipped.")

    if not data_by_ticker:
        return BacktestEngineResult(results={"error": "No usable tickers with data in range."}, logs=logs)

    nglobal = int(getattr(backtest.scenario, "nglobal", 20) or 20)
    gm_started = time.monotonic()
    global_momentum_values_by_date = (
        _build_global_momentum_values_from_ticker_data(data_by_ticker, nglobal)
        if needs_gm_conditions
        else {}
    )
    global_momentum_regime_by_date = (
        _build_global_momentum_regime_from_values(global_momentum_values_by_date)
        if needs_gm_conditions
        else {}
    )
    gm_push_current_values_by_date = (
        _build_gm_push_current_values_from_ticker_data(data_by_ticker, nglobal)
        if needs_gm_push_conditions
        else {}
    )
    gm_push_benchmark_values_by_ticker = (
        _build_gm_push_values_from_benchmark_cache(
            benchmark_price_cache,
            nglobal=nglobal,
        )
        if needs_gm_push_conditions
        else {}
    )
    logger.warning(
        "[backtest timing] step=engine_gm_compute backtest_id=%s duration=%.3fs gm=%s gm_push=%s",
        getattr(backtest, "id", None),
        time.monotonic() - gm_started,
        needs_gm_conditions,
        needs_gm_push_conditions,
    )
    gm_push_state_cache: dict[tuple[Any, ...], dict[date, str]] = {}

    dates_sorted = sorted(all_dates)
    if not dates_sorted:
        return BacktestEngineResult(results={"error": "No market dates found in range."}, logs=logs)

    warmup_dates = [d for d in dates_sorted if (start_d is not None and d < start_d)]
    real_dates_sorted = [d for d in dates_sorted if (start_d is None or d >= start_d)]
    if not real_dates_sorted:
        return BacktestEngineResult(results={"error": "No market dates found in effective backtest range."}, logs=logs)

    # Per (ticker, line_index) state
    state: dict[tuple[str, int], dict[str, Any]] = {}

    def _market_cap_for_day(ticker: str, d) -> Decimal | None:
        if not market_cap_filter_enabled:
            return None
        symbol = sym_by_ticker.get(ticker)
        if not symbol:
            return None
        return _market_cap_from_cache(market_cap_cache.get(symbol.id), d)

    def _ratio_tradable(ticker: str, d, price_value, ratio_p_val) -> tuple[bool, Decimal | None, Decimal | None]:
        """Return (tradable, ratio_percent, ratio_raw).

        If include_all is enabled, tradable is always True (eligibility bypass),
        while ratio values are kept for ranking/display when available.
        """
        return _buy_tradability_for_day(
            price_value=price_value,
            ratio_p_val=ratio_p_val,
            market_cap_value=_market_cap_for_day(ticker, d),
            include_all=include_all,
            ratio_threshold=X,
            min_price=min_price,
            max_price=max_price,
            min_market_cap=min_market_cap,
            max_market_cap=max_market_cap,
            market_cap_missing_policy=market_cap_missing_policy,
        )

    def _trend_filter_allows_buy(ticker: str, d, gm_code: str | None, buy_gm_filter: str | None) -> bool:
        if not trend_filters_enabled:
            return True
        return bool(
            evaluate_trend_filters_for_symbol(
                symbol=sym_by_ticker.get(ticker),
                settings=settings,
                as_of=d,
                nglobal=nglobal,
                gm_current_regime=gm_code,
                benchmark_cache_by_ticker=benchmark_price_cache,
                suppress_gm_current=_normalize_global_regime_filter(buy_gm_filter) != "IGNORE",
                universe_code=universe_code,
            )["passed"]
        )

    def _line_market_conditions_allow_buy(st: dict[str, Any], ticker: str, d, gm_code: str | None) -> bool:
        st["_last_gm_buy_max_blocked"] = False
        st["_last_gm_buy_debug"] = None
        gm_buy_conditions = st.get("gm_buy_conditions") or _normalize_gm_conditions_config(
            operator=st.get("buy_market_operator"),
            current=st.get("buy_market_gm_current"),
            market=st.get("buy_market_gm_market"),
            sector=st.get("buy_market_gm_sector"),
        )
        if not _gm_conditions_has_active(gm_buy_conditions):
            return True
        evaluation = _evaluate_gm_conditions(
            config=gm_buy_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            nglobal=nglobal,
            gm_current_value=_to_dec(global_momentum_values_by_date.get(d)),
            gm_current_regime=gm_code,
            benchmark_cache_by_ticker=benchmark_price_cache,
            apply_buy_max_threshold=True,
            universe_code=universe_code,
        )
        st["_last_gm_buy_max_blocked"] = _evaluation_blocked_by_buy_max_threshold(evaluation)
        st["_last_gm_buy_debug"] = _compact_gm_buy_debug(evaluation, st)
        return bool(evaluation["passed"])

    def _gm_market_exit_sell_evaluation(st: dict[str, Any], ticker: str, d) -> dict[str, Any]:
        gm_sell_conditions = st.get("gm_sell_market_exit_conditions") or _normalize_gm_conditions_config()
        if not _gm_conditions_has_active(gm_sell_conditions):
            return {"has_active": False, "passed": False, "label": ""}
        evaluation = _evaluate_gm_conditions(
            config=gm_sell_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            nglobal=nglobal,
            gm_current_value=_to_dec(global_momentum_values_by_date.get(d)),
            gm_current_regime=global_momentum_regime_by_date.get(d),
            benchmark_cache_by_ticker=benchmark_price_cache,
            universe_code=universe_code,
        )
        evaluation["passed"] = bool(evaluation.get("passed"))
        return evaluation

    def _line_gm_push_conditions_allow_buy(st: dict[str, Any], ticker: str, d) -> bool:
        st["_last_gm_push_buy_max_blocked"] = False
        st["_last_gm_push_buy_debug"] = None
        gm_push_buy_conditions = st.get("gm_push_buy_conditions") or _normalize_gm_push_conditions_config()
        if not _gm_push_conditions_has_active(gm_push_buy_conditions):
            return True
        evaluation = _evaluate_gm_push_conditions(
            config=gm_push_buy_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            gm_push_current_values=gm_push_current_values_by_date,
            gm_push_benchmark_values_by_ticker=gm_push_benchmark_values_by_ticker,
            gm_push_state_cache=gm_push_state_cache,
            apply_buy_max_threshold=True,
            universe_code=universe_code,
        )
        st["_last_gm_push_buy_max_blocked"] = _evaluation_blocked_by_buy_max_threshold(evaluation)
        st["_last_gm_push_buy_debug"] = _compact_gm_push_buy_debug(evaluation)
        return bool(evaluation["passed"])

    def _gm_push_market_exit_sell_evaluation(st: dict[str, Any], ticker: str, d) -> dict[str, Any]:
        gm_push_sell_conditions = st.get("gm_push_sell_market_exit_conditions") or _normalize_gm_push_conditions_config()
        if not _gm_push_conditions_has_active(gm_push_sell_conditions):
            return {"has_active": False, "passed": False, "label": ""}
        evaluation = _evaluate_gm_push_conditions(
            config=gm_push_sell_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            gm_push_current_values=gm_push_current_values_by_date,
            gm_push_benchmark_values_by_ticker=gm_push_benchmark_values_by_ticker,
            gm_push_state_cache=gm_push_state_cache,
            universe_code=universe_code,
        )
        evaluation["passed"] = bool(evaluation.get("passed"))
        return evaluation

    for ticker in data_by_ticker.keys():
        for li, line in enumerate(signal_lines):
            state[(ticker, li)] = {
                "buy_codes": _normalize_codes(line.get("buy")),
                "sell_codes": _normalize_codes(line.get("sell")),
                "buy_logic": _normalize_logic(line.get("buy_logic"), "AND"),
                "sell_logic": _normalize_logic(line.get("sell_logic"), "OR"),
                "buy_gm_filter": _normalize_global_regime_filter(line.get("buy_gm_filter")),
                "buy_gm_operator": _normalize_logic(line.get("buy_gm_operator"), "AND"),
                "buy_market_gm_current": _normalize_global_regime_filter(
                    line.get("buy_market_gm_current", line.get("buy_gm_filter"))
                ),
                "buy_market_gm_market": _normalize_global_regime_filter(line.get("buy_market_gm_market")),
                "buy_market_gm_sector": _normalize_global_regime_filter(line.get("buy_market_gm_sector")),
                "buy_market_operator": _normalize_logic(line.get("buy_market_operator"), "AND"),
                "gm_buy_conditions": _normalize_gm_conditions_config(
                    line.get("gm_buy_conditions"),
                    operator=line.get("buy_market_operator"),
                    current=line.get("buy_market_gm_current", line.get("buy_gm_filter")),
                    market=line.get("buy_market_gm_market"),
                    sector=line.get("buy_market_gm_sector"),
                ),
                "gm_sell_market_exit_conditions": _normalize_gm_conditions_config(
                    line.get("gm_sell_market_exit_conditions"),
                    operator=line.get("gm_sell_market_exit_operator"),
                ),
                "gm_push_buy_conditions": _normalize_gm_push_conditions_config(line.get("gm_push_buy_conditions")),
                "gm_push_sell_market_exit_conditions": _normalize_gm_push_conditions_config(
                    line.get("gm_push_sell_market_exit_conditions"),
                    operator=line.get("gm_push_sell_market_exit_operator"),
                ),
                "sell_gm_filter": _normalize_global_regime_filter(line.get("sell_gm_filter")),
                "sell_gm_operator": _normalize_logic(line.get("sell_gm_operator"), "AND"),
                "trading_model": line.get("trading_model"),
                "allocated": False,
                "cash_ticker": Decimal("0"),
                # Realized PnL that is NOT reinvested when fixed_capital is enabled.
                # Always included in portfolio equity.
                "bank": Decimal("0"),
                "position_open": False,
                "entry_price": None,
                "shares": 0,
                "trade_count": 0,
                "sum_g": Decimal("0"),
                "pnl_amount_total": Decimal("0"),
                "total_gain_amount": Decimal("0"),
                "total_loss_amount": Decimal("0"),
                "win_trades": 0,
                "loss_trades": 0,
                "max_gain_amount": None,
                "max_loss_amount": None,
                # Legacy counters historically shown as NB_JOUR_OUVRES / BUY_DAYS_CLOSED.
                # They were poorly named in the UI. In V5.2.30 we introduce clearer names
                # and ratios for the UI while keeping the underlying behaviour unchanged
                # unless explicitly recomputed from daily rows.
                "nb_jours_ouvres": 0,
                "buy_days_closed": 0,
                # New counters (V5.2.30): cumulative counts over "tradable" days (ratio_p >= X
                # unless include_all is enabled). These are used for UI display and ratios.
                "tradable_days": 0,
                "tradable_days_in_position": 0,
                "entry_date": None,
                "prev_k": None,  # previous day's K-values dict (K1,K1f,K2,K3,K4)
                "active_signal_states": {},
                "and_latched_states": {},
                "use_signal_latch_model": _line_uses_signal_latch_model(line),
                "signal_latch_state": {},
                "signal_latch_invalidated_today": set(),
                "signal_latch_last_date": None,
                "sell_signal_latch_state": {},
                "sell_signal_latch_last_date": None,
                "warnings": [],
                "_reentry_warning_candidate": None,
                "_sold_today": False,
                "couloir_state": CouloirState(CouloirConfig.from_line(line)) if is_couloir_line(line) else None,
                "daily_rows": [],
                "events": [],
                "explain": _new_explain_summary(),
                "_explain_buy_candidate_dates": set(),
                "_explain_blocker_keys": set(),
            }

    # Warmup phase: reconstruct persistent states before the real backtest period.
    # No allocation, no trades, no counters during warmup.
    for d in warmup_dates:
        _checkpoint()
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata or d not in tdata.get("price_by_date", {}):
                continue
            local_event_alerts = {a.upper() for a in tdata.get("alerts", {}).get(d, set())}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            _update_and_latched_states(st["and_latched_states"], event_alerts)
            if st["use_signal_latch_model"]:
                _get_signal_latch_day_state(st, local_event_alerts, d)
            if _line_uses_progressive_explicit_sell_model(st):
                _get_sell_signal_latch_day_state(st, local_event_alerts, d)
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                couloir_state.observe_warmup_price(tdata["price_by_date"].get(d))
            if tdata.get("metrics") and (tdata["metrics"].get(d) is not None):
                st["prev_k"] = tdata["metrics"].get(d)

    # Portfolio tracking (Feature 8)
    portfolio_daily: list[dict[str, Any]] = []
    last_price_by_ticker: dict[str, Decimal] = {}
    invested_total = Decimal("0")  # dynamic invested capital for CP infinite
    peak_equity: Decimal | None = None
    max_drawdown: Decimal = Decimal("0")
    max_drawdown_amount: Decimal = Decimal("0")

    def _snapshot_portfolio(d: date):
        """Compute end-of-day portfolio snapshot.

        Portfolio is the aggregation of all allocated (ticker,line) cash + market value,
        plus remaining global cash when CP is limited.
        """
        nonlocal peak_equity, max_drawdown, max_drawdown_amount, invested_total

        # update last prices for tickers that have a bar today
        for tk, tdata in data_by_ticker.items():
            px = tdata["price_by_date"].get(d)
            if px is not None:
                last_price_by_ticker[tk] = px

        cash_allocated = Decimal("0")
        positions_value = Decimal("0")
        bank_total = Decimal("0")

        for (tk, _li), st in state.items():
            if not st.get("allocated"):
                continue
            cash_allocated += Decimal(st.get("cash_ticker") or 0)
            bank_total += Decimal(st.get("bank") or 0)
            shares = int(st.get("shares") or 0)
            if shares > 0:
                px = data_by_ticker.get(tk, {}).get("price_by_date", {}).get(d)
                if px is None:
                    px = last_price_by_ticker.get(tk)
                if px is not None:
                    positions_value += (Decimal(shares) * Decimal(px))

        global_cash_val = Decimal("0") if global_cash is None else Decimal(global_cash)

        if CP_infinite:
            invested = invested_total
            capital_total = invested_total
        else:
            invested = CP_raw - global_cash_val
            capital_total = CP_raw

        equity = global_cash_val + cash_allocated + positions_value + bank_total
        # Finite-capital backtests have a stable portfolio baseline: the initial
        # capital. Unallocated global cash is part of equity, not profit.
        # Unlimited-capital backtests keep their historical dynamic baseline.
        pnl_global = equity - (invested if CP_infinite else CP_raw)
        portfolio_return_global = None
        if capital_total and capital_total != 0:
            portfolio_return_global = (equity - capital_total) / capital_total

        avg_global_nglobal = _to_dec((global_momentum_values_by_date or {}).get(d))
        gm_push_current_value = _to_dec((gm_push_current_values_by_date or {}).get(d))

        if peak_equity is None or equity > peak_equity:
            peak_equity = equity
        dd = Decimal("0")
        if peak_equity and peak_equity != 0:
            dd = (equity - peak_equity) / peak_equity
        dd_amount = Decimal("0")
        if peak_equity is not None and equity < peak_equity:
            dd_amount = peak_equity - equity
        if dd < max_drawdown:
            max_drawdown = dd
        if dd_amount > max_drawdown_amount:
            max_drawdown_amount = dd_amount

        portfolio_daily.append(
            {
                "date": str(d),
                "global_cash": str(global_cash_val),
                "cash_allocated": str(cash_allocated),
                "bank_total": str(bank_total),
                "positions_value": str(positions_value),
                "equity": str(equity),
                "invested": str(invested),
                "pnl_global": str(pnl_global),
                "portfolio_return_global": None if portfolio_return_global is None else str(portfolio_return_global),
                "avg_global_nglobal": None if avg_global_nglobal is None else str(avg_global_nglobal),
                "gm_push_current": None if gm_push_current_value is None else str(gm_push_current_value),
                "drawdown": str(dd),
            }
        )

    # Daily loop
    engine_loop_started = time.monotonic()
    for d in real_dates_sorted:
        _checkpoint()

        # 1) SELL phase (sell before buy)
        for (ticker, li), st in state.items():
            st["_sold_today"] = False
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                ratio_pct, ratio_raw = _ratio_values_for_tradability(tdata["metrics"].get(d))
                _append_daily_row(st, {
                    "date": str(d),
                    "price_close": None,
                    "ratio_P": None if ratio_raw is None else str(ratio_raw),
                    "ratio_P_pct": None if ratio_pct is None else str(ratio_pct),
                    "tradable": False,
                    "alerts": sorted(list(tdata["alerts"].get(d, set()))),
                    **_universe_daily_fields(ticker, d),
                    "buy_code": _compose_condition_label(st["buy_codes"], st["buy_logic"], st["buy_gm_filter"], st["buy_gm_operator"]),
                    "sell_code": _compose_condition_label(st["sell_codes"], st["sell_logic"], st["sell_gm_filter"], st["sell_gm_operator"]),
                    "buy_codes": st["buy_codes"],
                    "sell_codes": st["sell_codes"],
                    "buy_logic": st["buy_logic"],
                    "sell_logic": st["sell_logic"],
                    "buy_gm_filter": st["buy_gm_filter"],
                    "buy_gm_operator": st["buy_gm_operator"],
                    "buy_market_gm_current": st["buy_market_gm_current"],
                    "buy_market_gm_market": st["buy_market_gm_market"],
                    "buy_market_gm_sector": st["buy_market_gm_sector"],
                    "buy_market_operator": st["buy_market_operator"],
                    "gm_buy_conditions": st["gm_buy_conditions"],
                    "gm_sell_market_exit_conditions": st["gm_sell_market_exit_conditions"],
                    "gm_sell_market_exit_label": _gm_conditions_label(st["gm_sell_market_exit_conditions"]),
                    "gm_push_buy_conditions": st["gm_push_buy_conditions"],
                    "gm_push_sell_market_exit_conditions": st["gm_push_sell_market_exit_conditions"],
                    "gm_push_buy_label": _gm_push_conditions_label(st["gm_push_buy_conditions"]),
                    "gm_push_sell_market_exit_label": _gm_push_conditions_label(st["gm_push_sell_market_exit_conditions"]),
                    "sell_gm_filter": st["sell_gm_filter"],
                    "sell_gm_operator": st["sell_gm_operator"],
                    "action": None,
                    "action_G": None,
                    "action_PNL_AMOUNT": None,
                    "forced_close": False,
                    "allocated": st["allocated"],
                    "cash_ticker": str(st["cash_ticker"]),
                    "bank": str(st.get("bank") or Decimal("0")),
                    "shares": st["shares"],
                    "N": st["trade_count"],
                    "S_G_N": None if int(st["trade_count"]) == 0 else str(st["sum_g"] / Decimal(int(st["trade_count"]))),
                    "BT": str(st["sum_g"]),
                    "NB_JOUR_OUVRES": max(0, int(st.get("tradable_days") or 0) - int(st.get("tradable_days_in_position") or 0)),
                    "BMJ": None,
                    "BMD": None,
                    "BUY_DAYS_CLOSED": int(st.get("tradable_days_in_position") or 0),
                })
                continue  # no market data for this ticker that day
            close_d = _to_dec(price_by_date[d])
            if close_d is None:
                continue
            _start_couloir_day_debug(st)
            day_alerts_raw = tdata["alerts"].get(d, set())
            local_event_alerts = {a.upper() for a in day_alerts_raw}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if st["use_signal_latch_model"]:
                _get_signal_latch_day_state(st, local_event_alerts, d)

            # tradable status computed for NB_JOUR_OUVRES before actions
            tradable, ratio_pct, ratio_raw = _ratio_tradable(ticker, d, price_by_date.get(d), tdata["metrics"].get(d))
            if tradable and not st["position_open"]:
                st["nb_jours_ouvres"] += 1

            G_today = None
            pnl_amount_today = None
            forced_close = False

            def _do_sell(reason: str, *, reset_signal_memory: bool = True):
                nonlocal G_today, pnl_amount_today
                closed = _close_open_position(
                    st,
                    close_price=close_d,
                    close_date=d,
                    CT=CT,
                    fixed_capital=fixed_capital,
                    reset_signal_memory=reset_signal_memory,
                )
                if closed is None:
                    return
                G_today, pnl_amount_today = closed
                st["_last_action_reason"] = reason
                logs.append(f"{ticker}[L{li+1}] SELL {reason} on {d} close={close_d} G={G_today}")

            sell_codes = st["sell_codes"]
            sell_code = sell_codes[0] if sell_codes else ""

            # Special sell mode: K1f crosses down either (1) 0 (B1f) or (2) the closest
            # "line above" among K1/K2/K3/K4 as of t-1.
            couloir_state = st.get("couloir_state")
            if st["position_open"] and couloir_state is not None:
                couloir_sell_candidate = couloir_state.evaluate_sell_candidate(d, close_d)
                _merge_couloir_day_debug(st, couloir_sell_candidate=bool(couloir_sell_candidate))
                if couloir_sell_candidate:
                    _do_sell("Couloir : seuil de repli vente atteint")
                    if G_today is not None:
                        _merge_couloir_day_debug(
                            st,
                            couloir_sell_executed=True,
                            couloir_sell_source="COULOIR",
                            couloir_reset_after_sell=True,
                        )
            elif st["position_open"] and sell_code == SPECIAL_SELL_K1F_UPPER_DOWN_B1F:
                k_today = (tdata["metrics"].get(d) or None)
                k_prev = st.get("prev_k") or None
                k1f_prev = _to_dec(_metric_val(k_prev, _M_K1F))
                k1f_today = _to_dec(_metric_val(k_today, _M_K1F))

                # 1) B1f fallback: K1f cross 0 down
                if _cross_down(k1f_prev, k1f_today, Decimal("0"), Decimal("0")):
                    _do_sell("AUTO (B1f: K1f cross 0 down)")
                else:
                    # 2) Find the closest line above K1f at t-1 among K1/K2/K3/K4
                    candidates_prev: list[tuple[str, Decimal]] = []
                    for key, idx in (("K1", _M_K1), ("K2", _M_K2), ("K3", _M_K3), ("K4", _M_K4)):
                        v = _to_dec(_metric_val(k_prev, idx))
                        if v is None or k1f_prev is None:
                            continue
                        if v > k1f_prev:
                            candidates_prev.append((key, v))

                    if candidates_prev and k1f_prev is not None and k1f_today is not None:
                        target_key, _ = min(candidates_prev, key=lambda x: x[1])
                        idx_map = {"K1": _M_K1, "K2": _M_K2, "K3": _M_K3, "K4": _M_K4}
                        target_prev = _to_dec(_metric_val(k_prev, idx_map.get(target_key, _M_K1)))
                        target_today = _to_dec(_metric_val(k_today, idx_map.get(target_key, _M_K1)))
                        if _cross_down(k1f_prev, k1f_today, target_prev, target_today):
                            _do_sell(f"AUTO ({target_key}: K1f cross down)")

            elif st["position_open"] and _line_uses_progressive_explicit_sell_model(st):
                _get_sell_signal_latch_day_state(st, local_event_alerts, d)
                if _signal_latch_sell_ready(st):
                    _do_sell(
                        f"signal {_compose_condition_label(sell_codes, st['sell_logic'], st['sell_gm_filter'], st['sell_gm_operator'])}",
                        reset_signal_memory=False,
                    )
                    _consume_sell_latch_state(st)
                    _arm_reentry_warning_if_needed(st, sell_date=d, ticker=ticker, line_index=li + 1)
            elif st["position_open"] and _line_uses_auto_sell_model(st):
                _latch_state, invalidated_signals = _get_signal_latch_day_state(st, local_event_alerts, d)
                if invalidated_signals:
                    previous_latch_state = dict(_latch_state or {})
                    _do_sell(f"signal invalidation {','.join(sorted(invalidated_signals))}")
                    _retain_non_invalidated_latch_signals_after_sell(st, invalidated_signals, previous_latch_state)
            elif st["position_open"] and _match_line_with_global_filter(day_alerts, latched_alerts, sell_codes, st["sell_logic"], gm_code, st["sell_gm_filter"], st["sell_gm_operator"]):
                _do_sell(f"signal {_compose_condition_label(sell_codes, st['sell_logic'], st['sell_gm_filter'], st['sell_gm_operator'])}")
            if st["position_open"] and G_today is None:
                gm_sell_evaluation = _gm_market_exit_sell_evaluation(st, ticker, d)
                if gm_sell_evaluation.get("has_active") and gm_sell_evaluation.get("passed"):
                    _do_sell(
                        f"Protection marché GM ({gm_sell_evaluation.get('label') or 'active'})",
                        reset_signal_memory=not bool(st.get("use_signal_latch_model")),
                    )
                    if G_today is not None:
                        _merge_couloir_day_debug(
                            st,
                            couloir_sell_executed=True,
                            couloir_sell_source="GM",
                            couloir_reset_after_sell=True,
                        )
                    if _line_uses_progressive_explicit_sell_model(st):
                        _arm_reentry_warning_if_needed(st, sell_date=d, ticker=ticker, line_index=li + 1)
            if st["position_open"] and G_today is None:
                gm_push_sell_evaluation = _gm_push_market_exit_sell_evaluation(st, ticker, d)
                if gm_push_sell_evaluation.get("has_active") and gm_push_sell_evaluation.get("passed"):
                    _do_sell(
                        f"GM_PUSH_MARKET_EXIT ({gm_push_sell_evaluation.get('label') or 'active'})",
                        reset_signal_memory=not bool(st.get("use_signal_latch_model")),
                    )
                    if G_today is not None:
                        _merge_couloir_day_debug(
                            st,
                            couloir_sell_executed=True,
                            couloir_sell_source="GM_PUSH",
                            couloir_reset_after_sell=True,
                        )
                    if _line_uses_progressive_explicit_sell_model(st):
                        _arm_reentry_warning_if_needed(st, sell_date=d, ticker=ticker, line_index=li + 1)
            if G_today is not None:
                st["_sold_today"] = True
                _record_explain_trade(st, "SELL")
                _record_line_event(
                    st,
                    as_of=d,
                    action="SELL",
                    price_close=close_d,
                    action_g=G_today,
                    action_pnl_amount=pnl_amount_today,
                    action_reason=st.get("_last_action_reason"),
                )

            # record daily row (we may update with buy action later, but keep as dict to mutate)
            N = st["trade_count"]
            S_G_N = None if N == 0 else (st["sum_g"] / Decimal(N))
            BT = st["sum_g"]  # == S_G_N*N
            # Clarified day counters (V5.2.30)
            # - TRADABLE_DAYS: number of days where the ticker is tradable (ratio_p >= X, or include_all)
            # - TRADABLE_DAYS_IN_POSITION_CLOSED: number of tradable days where we end the day in position (shares > 0)
            # - TRADABLE_DAYS_NOT_IN_POSITION: remaining tradable days (flat)
            tradable_days = int(st.get("tradable_days") or 0)
            in_pos_days = int(st.get("tradable_days_in_position") or 0)
            not_in_pos_days = max(0, tradable_days - in_pos_days)

            # Keep legacy keys for backward compatibility.
            nb = not_in_pos_days
            bmd_days = in_pos_days

            BMJ = None if nb == 0 else (BT / Decimal(nb))
            BMD = None if bmd_days == 0 else (BT / Decimal(bmd_days))

            daily_row = _append_daily_row(st, {
                "date": str(d),
                "price_close": str(close_d),
                "ratio_P": None if ratio_raw is None else str(ratio_raw),
                "ratio_P_pct": None if ratio_pct is None else str(ratio_pct),
                "tradable": tradable,
                "alerts": sorted(list(day_alerts_raw)),
                **_universe_daily_fields(ticker, d),
                "buy_code": _compose_condition_label(st["buy_codes"], st["buy_logic"], st["buy_gm_filter"], st["buy_gm_operator"]),
                "sell_code": _compose_condition_label(st["sell_codes"], st["sell_logic"], st["sell_gm_filter"], st["sell_gm_operator"]),
                "buy_codes": st["buy_codes"],
                "sell_codes": st["sell_codes"],
                "buy_logic": st["buy_logic"],
                "sell_logic": st["sell_logic"],
                "buy_gm_filter": st["buy_gm_filter"],
                "buy_gm_operator": st["buy_gm_operator"],
                "buy_market_gm_current": st["buy_market_gm_current"],
                "buy_market_gm_market": st["buy_market_gm_market"],
                "buy_market_gm_sector": st["buy_market_gm_sector"],
                "buy_market_operator": st["buy_market_operator"],
                "gm_buy_conditions": st["gm_buy_conditions"],
                "gm_sell_market_exit_conditions": st["gm_sell_market_exit_conditions"],
                "gm_sell_market_exit_label": _gm_conditions_label(st["gm_sell_market_exit_conditions"]),
                "gm_push_buy_conditions": st["gm_push_buy_conditions"],
                "gm_push_sell_market_exit_conditions": st["gm_push_sell_market_exit_conditions"],
                "gm_push_buy_label": _gm_push_conditions_label(st["gm_push_buy_conditions"]),
                "gm_push_sell_market_exit_label": _gm_push_conditions_label(st["gm_push_sell_market_exit_conditions"]),
                "sell_gm_filter": st["sell_gm_filter"],
                "sell_gm_operator": st["sell_gm_operator"],
                "action": "SELL" if G_today is not None else None,
                "action_reason": st.get("_last_action_reason") if G_today is not None else None,
                "action_G": None if G_today is None else str(G_today),
                "action_PNL_AMOUNT": None if pnl_amount_today is None else str(pnl_amount_today),
                "forced_close": forced_close,
                "allocated": st["allocated"],
                "cash_ticker": str(st["cash_ticker"]),
                "bank": str(st.get("bank") or Decimal("0")),
                "shares": st["shares"],
                "N": N,
                "S_G_N": None if S_G_N is None else str(S_G_N),
                "BT": str(BT),
                "NB_JOUR_OUVRES": nb,
                "BMJ": None if BMJ is None else str(BMJ),
                "BMD": None if BMD is None else str(BMD),
                "BUY_DAYS_CLOSED": bmd_days,
            })
            if daily_row is not None:
                _apply_couloir_day_debug_to_last_row(st, d)

            # Keep previous day's indicator values (used by special sell modes).
            # We update it only when metrics exist for this day.
            if tdata.get("metrics") and (tdata["metrics"].get(d) is not None):
                st["prev_k"] = tdata["metrics"].get(d)

        # 2) BUY allocation selection phase (for not-yet-allocated strategies, limited CP)
        candidates_need_alloc = []
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue
            if st["position_open"] or (st.get("_sold_today") and not _line_allows_same_day_reentry(st)):
                continue
            if not _universe_allows_new_buy(ticker, d):
                if _would_be_buy_candidate_without_universe(ticker, d, st, tdata, require_allocated=False):
                    _mark_universe_buy_blocked(st, d)
                continue
            buy_codes = st["buy_codes"]
            day_alerts_raw = tdata["alerts"].get(d, set())
            local_event_alerts = {a.upper() for a in day_alerts_raw}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                couloir_buy_candidate = couloir_state.evaluate_buy_candidate(d, price_by_date.get(d))
                if not couloir_buy_candidate:
                    continue
                _merge_couloir_day_debug(st, couloir_buy_candidate=True)
                _apply_couloir_day_debug_to_last_row(st, d)
            else:
                day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
                latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
                if st["use_signal_latch_model"]:
                    _get_signal_latch_day_state(st, local_event_alerts, d)
                    if not _signal_latch_buy_ready(st, gm_code):
                        _record_rhd_signal_blocker_if_relevant(st, d, buy_codes, local_event_alerts)
                        continue
                elif not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                    _record_rhd_signal_blocker_if_relevant(st, d, buy_codes, local_event_alerts)
                    continue

            _record_explain_buy_candidate(st, d)
            tradable, ratio_pct, _ = _ratio_tradable(ticker, d, price_by_date.get(d), tdata["metrics"].get(d))
            if not tradable:
                _merge_couloir_day_debug(st, couloir_blocked_reason="TRADABILITY")
                _apply_couloir_day_debug_to_last_row(st, d)
                _record_explain_blocker(st, d, "NOT_TRADABLE", "Ticker non tradable ce jour", {"type": "TRADABILITY"})
                continue
            if not _trend_filter_allows_buy(ticker, d, gm_code, st["buy_gm_filter"]):
                _merge_couloir_day_debug(st, couloir_blocked_reason="TREND_FILTER")
                _apply_couloir_day_debug_to_last_row(st, d)
                _record_explain_blocker(st, d, "TREND_FILTER_BLOCKED", "Filtre de tendance bloquant", {"type": "TREND_FILTER"})
                continue
            if not _line_market_conditions_allow_buy(st, ticker, d, gm_code):
                _merge_couloir_day_debug(st, couloir_blocked_reason="GM")
                _apply_couloir_day_debug_to_last_row(st, d)
                _mark_gm_buy_blocked(st, d)
                _record_gm_buy_blockers(st, d)
                continue
            if not _line_gm_push_conditions_allow_buy(st, ticker, d):
                _merge_couloir_day_debug(st, couloir_blocked_reason="GM_PUSH")
                _apply_couloir_day_debug_to_last_row(st, d)
                if st.get("_last_gm_push_buy_max_blocked"):
                    _mark_gm_buy_max_blocked(st, d)
                _record_gm_push_buy_blockers(st, d)
                continue

            if not st["allocated"]:
                # Needs CT allocation to be able to buy
                if CP_infinite:
                    # allocate immediately
                    st["allocated"] = True
                    st["cash_ticker"] = CT
                else:
                    # will be considered by selection
                    # use ratio_pct for ranking; None already filtered out
                    candidates_need_alloc.append((ratio_pct or Decimal("0"), ticker, li))

        if (not CP_infinite) and candidates_need_alloc:
            # Sort by highest ratio_p
            candidates_need_alloc.sort(key=lambda x: x[0], reverse=True)
            for ratio_pct, ticker, li in candidates_need_alloc:
                if global_cash is None:
                    break
                if global_cash < CT or CT <= 0:
                    break
                st = state[(ticker, li)]
                if st["allocated"]:
                    continue
                # allocate
                st["allocated"] = True
                st["cash_ticker"] = CT
                global_cash -= CT
                # for KPI / equity baseline tracking
                # (for CP limited, invested is derived from CP - global_cash)
                logs.append(f"ALLOC {ticker}[L{li+1}] on {d} ratio={ratio_pct}% global_cash={global_cash}")

        # also track invested capital for CP infinite allocations (immediate or ranked)
        if CP_infinite:
            for (ticker, li), st in state.items():
                if st.get("allocated") and st.get("_counted_alloc") is not True:
                    # allocated now (first time)
                    if CT > 0:
                        invested_total += CT
                    st["_counted_alloc"] = True

        # 3) BUY execution phase (for allocated or already allocated strategies)
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue
            if st["position_open"] or (st.get("_sold_today") and not _line_allows_same_day_reentry(st)):
                continue
            if not _universe_allows_new_buy(ticker, d):
                if _would_be_buy_candidate_without_universe(ticker, d, st, tdata, require_allocated=True):
                    _mark_universe_buy_blocked(st, d)
                continue

            buy_codes = st["buy_codes"]
            day_alerts_raw = tdata["alerts"].get(d, set())
            local_event_alerts = {a.upper() for a in day_alerts_raw}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                couloir_buy_candidate = couloir_state.evaluate_buy_candidate(d, price_by_date.get(d))
                if not couloir_buy_candidate:
                    continue
                _merge_couloir_day_debug(st, couloir_buy_candidate=True)
                _apply_couloir_day_debug_to_last_row(st, d)
            else:
                day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
                latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
                if st["use_signal_latch_model"]:
                    _get_signal_latch_day_state(st, local_event_alerts, d)
                    if not _signal_latch_buy_ready(st, gm_code):
                        _record_rhd_signal_blocker_if_relevant(st, d, buy_codes, local_event_alerts)
                        continue
                elif not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                    _record_rhd_signal_blocker_if_relevant(st, d, buy_codes, local_event_alerts)
                    continue

            _record_explain_buy_candidate(st, d)
            tradable, _, _ = _ratio_tradable(ticker, d, price_by_date.get(d), tdata["metrics"].get(d))
            if not tradable:
                _merge_couloir_day_debug(st, couloir_blocked_reason="TRADABILITY")
                _apply_couloir_day_debug_to_last_row(st, d)
                _record_explain_blocker(st, d, "NOT_TRADABLE", "Ticker non tradable ce jour", {"type": "TRADABILITY"})
                continue
            if not _trend_filter_allows_buy(ticker, d, gm_code, st["buy_gm_filter"]):
                _merge_couloir_day_debug(st, couloir_blocked_reason="TREND_FILTER")
                _apply_couloir_day_debug_to_last_row(st, d)
                _record_explain_blocker(st, d, "TREND_FILTER_BLOCKED", "Filtre de tendance bloquant", {"type": "TREND_FILTER"})
                continue
            if not _line_market_conditions_allow_buy(st, ticker, d, gm_code):
                _merge_couloir_day_debug(st, couloir_blocked_reason="GM")
                _apply_couloir_day_debug_to_last_row(st, d)
                _mark_gm_buy_blocked(st, d)
                _record_gm_buy_blockers(st, d)
                continue
            if not _line_gm_push_conditions_allow_buy(st, ticker, d):
                _merge_couloir_day_debug(st, couloir_blocked_reason="GM_PUSH")
                _apply_couloir_day_debug_to_last_row(st, d)
                if st.get("_last_gm_push_buy_max_blocked"):
                    _mark_gm_buy_max_blocked(st, d)
                _record_gm_push_buy_blockers(st, d)
                continue

            if not st["allocated"]:
                # no allocation available (limited CP)
                _merge_couloir_day_debug(st, couloir_blocked_reason="ALLOCATION")
                _apply_couloir_day_debug_to_last_row(st, d)
                _record_explain_blocker(
                    st,
                    d,
                    "INSUFFICIENT_CASH",
                    "Cash disponible insuffisant pour allouer le capital par action",
                    {
                        "type": "ALLOCATION",
                        "capital_total": str(CP_raw),
                        "capital_per_ticker": str(CT),
                        "cash": None if global_cash is None else str(global_cash),
                    },
                )
                continue

            close_d = _to_dec(price_by_date[d])
            if close_d is None or close_d <= 0:
                _record_explain_blocker(
                    st,
                    d,
                    "INVALID_EXECUTION_PRICE",
                    "Prix d'exécution absent ou invalide",
                    {
                        "type": "EXECUTION_PRICE",
                        "capital_total": str(CP_raw),
                        "capital_per_ticker": str(CT),
                        "price": None if close_d is None else str(close_d),
                    },
                )
                continue

            cash = st["cash_ticker"]
            if fixed_capital and st.get("allocated"):
                # In fixed mode, each new BUY starts from the initial CT (no reinvest).
                cash = CT
                st["cash_ticker"] = CT
            shares = int((cash / close_d).to_integral_value(rounding="ROUND_FLOOR"))
            if shares <= 0:
                if cash <= 0:
                    _record_explain_blocker(
                        st,
                        d,
                        "ZERO_EFFECTIVE_CAPITAL",
                        "Capital alloué nul ou négatif",
                        {
                            "type": "ALLOCATION",
                            "capital_total": str(CP_raw),
                            "capital_per_ticker": str(CT),
                            "cash": str(cash),
                            "price": str(close_d),
                            "quantity": shares,
                        },
                    )
                else:
                    _record_explain_blocker(
                        st,
                        d,
                        "ORDER_QUANTITY_ZERO",
                        "Quantité d'achat arrondie à zéro",
                        {
                            "type": "SIZING",
                            "capital_total": str(CP_raw),
                            "capital_per_ticker": str(CT),
                            "cash": str(cash),
                            "price": str(close_d),
                            "quantity": shares,
                        },
                    )
                continue

            st["shares"] = shares
            st["cash_ticker"] = cash - (Decimal(shares) * close_d)
            st["position_open"] = True
            st["entry_price"] = str(close_d)
            st["entry_date"] = d
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                couloir_state.on_buy_executed(close_d)
                _merge_couloir_day_debug(st, couloir_buy_executed=True)
                _apply_couloir_day_debug_to_last_row(st, d)
            if not st["use_signal_latch_model"]:
                _reset_trade_signal_memory(st)
            _record_reentry_warning_if_needed(st, buy_date=d, ticker=ticker, line_index=li + 1)

            logs.append(f"{ticker}[L{li+1}] BUY signal {_compose_condition_label(buy_codes, st['buy_logic'], st['buy_gm_filter'], st['buy_gm_operator'])} on {d} close={close_d} shares={shares} cash_left={st['cash_ticker']}")
            _record_explain_trade(st, "BUY")
            _record_line_event(st, as_of=d, action="BUY", price_close=close_d, gm_buy_debug=st.get("_last_gm_buy_debug"))

            # mutate last daily row to add action
            if st["daily_rows"]:
                last = st["daily_rows"][-1]
                # If already had SELL action same day, keep SELL as priority but record buy too
                if last.get("action") == "SELL":
                    last["action"] = "SELL+BUY"
                else:
                    last["action"] = "BUY"
                last["shares"] = st["shares"]
                last["cash_ticker"] = str(st["cash_ticker"])
                last["allocated"] = st["allocated"]
                if st.get("_last_gm_buy_debug"):
                    last["gm_buy_debug"] = st.get("_last_gm_buy_debug")

        # 3.b) End-of-day counters for UI (tradable days / in-position ratios)
        # We update them AFTER the BUY phase so that a BUY on day D counts the day as
        # "in position" for end-of-day state (using shares > 0).
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue
            is_tradable, _ratio_pct, _ratio_raw = _ratio_tradable(ticker, d, price_by_date.get(d), tdata["metrics"].get(d))
            in_position_eod = bool(st.get("shares") or 0) > 0

            if is_tradable:
                st["tradable_days"] += 1
                if in_position_eod:
                    st["tradable_days_in_position"] += 1

            tradable_days = st["tradable_days"]
            in_pos_days = st["tradable_days_in_position"]
            not_in_pos_days = max(0, tradable_days - in_pos_days)

            # Keep legacy keys for compatibility, but values now match the clarified
            # UI names (TRADABLE_DAYS_NOT_IN_POSITION / TRADABLE_DAYS_IN_POSITION_CLOSED).
            if st["daily_rows"]:
                last = st["daily_rows"][-1]
                last["TRADABLE_DAYS"] = tradable_days
                last["TRADABLE_DAYS_NOT_IN_POSITION"] = not_in_pos_days
                last["TRADABLE_DAYS_IN_POSITION_CLOSED"] = in_pos_days
                last["NB_JOUR_OUVRES"] = not_in_pos_days
                last["BUY_DAYS_CLOSED"] = in_pos_days
                if tradable_days > 0:
                    last["RATIO_NOT_IN_POSITION"] = str((Decimal(not_in_pos_days) / Decimal(tradable_days)) * Decimal("100"))
                    last["RATIO_IN_POSITION"] = str((Decimal(in_pos_days) / Decimal(tradable_days)) * Decimal("100"))
                else:
                    last["RATIO_NOT_IN_POSITION"] = None
                    last["RATIO_IN_POSITION"] = None

                # Recompute BMJ/BMD display values using the clarified denominators.
                BT = _to_dec(last.get("BT")) or Decimal("0")
                last["BMJ"] = None if not_in_pos_days == 0 else str(BT / Decimal(not_in_pos_days))
                last["BMD"] = None if in_pos_days == 0 else str(BT / Decimal(in_pos_days))

                _merge_couloir_day_debug(st)
                _apply_couloir_day_debug_to_last_row(st, d)

        # 4) Portfolio daily snapshot (end-of-day)
        _snapshot_portfolio(d)

    logger.warning(
        "[backtest timing] step=engine_loop backtest_id=%s duration=%.3fs dates=%s states=%s loaded_tickers=%s large_result_mode=%s",
        getattr(backtest, "id", None),
        time.monotonic() - engine_loop_started,
        len(real_dates_sorted),
        len(state),
        len(data_by_ticker),
        "on" if large_result_mode else "off",
    )

    # Forced close at end (per ticker,line) on last available price date
    if backtest.close_positions_at_end:
        for (ticker, li), st in state.items():
            if not st["position_open"] or st["entry_price"] is None or st["shares"] <= 0:
                continue
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            # pick last date with price for this ticker within global dates
            price_by_date = tdata["price_by_date"]
            last_date = None
            for d in reversed(dates_sorted):
                if d in price_by_date:
                    last_date = d
                    break
            if last_date is None:
                continue
            close_d = _to_dec(price_by_date[last_date])
            if close_d is None:
                continue
            closed = _close_open_position(
                st,
                close_price=close_d,
                close_date=last_date,
                CT=CT,
                fixed_capital=fixed_capital,
                reset_signal_memory=True,
            )
            if closed is None:
                continue
            G_today, pnl_amount_today = closed
            was_tradable, _, _ = _ratio_tradable(
                ticker,
                last_date,
                price_by_date.get(last_date),
                tdata["metrics"].get(last_date),
            )
            _account_for_forced_close_day(st, was_tradable=was_tradable)
            _merge_couloir_day_debug(
                st,
                couloir_sell_executed=True,
                couloir_sell_source="FORCED",
                couloir_reset_after_sell=True,
            )
            logs.append(f"{ticker}[L{li+1}] FORCED SELL on {last_date} close={close_d} G={G_today}")
            _record_explain_trade(st, "FORCED_SELL")
            _record_line_event(
                st,
                as_of=last_date,
                action="FORCED_SELL",
                price_close=close_d,
                action_g=G_today,
                action_pnl_amount=pnl_amount_today,
            )

            # Update last daily row for that ticker/line
            # Find last row with that date (if any), else append
            rows = st["daily_rows"]
            if rows and rows[-1]["date"] == str(last_date):
                rows[-1]["forced_close"] = True
                rows[-1]["action"] = "FORCED_SELL" if rows[-1].get("action") is None else f"{rows[-1].get('action')}+FORCED_SELL"
                rows[-1]["action_G"] = None if G_today is None else str(G_today)
                rows[-1]["action_PNL_AMOUNT"] = str(pnl_amount_today)
                rows[-1]["shares"] = 0
                rows[-1]["cash_ticker"] = str(st["cash_ticker"])
                rows[-1]["bank"] = str(st.get("bank") or Decimal("0"))
                # recompute cumulative metrics after forced close
                N = st["trade_count"]
                S_G_N = None if N == 0 else (st["sum_g"] / Decimal(N))
                BT = st["sum_g"]
                nb = st["nb_jours_ouvres"]
                BMJ = None if nb == 0 else (BT / Decimal(nb))
                bmd_days = st.get("buy_days_closed") or 0
                BMD = None if bmd_days == 0 else (BT / Decimal(bmd_days))
                rows[-1]["N"] = N
                rows[-1]["S_G_N"] = None if S_G_N is None else str(S_G_N)
                rows[-1]["BT"] = str(BT)
                rows[-1]["NB_JOUR_OUVRES"] = nb
                rows[-1]["BMJ"] = None if BMJ is None else str(BMJ)
                rows[-1]["BMD"] = None if BMD is None else str(BMD)
                rows[-1]["BUY_DAYS_CLOSED"] = bmd_days
                _apply_couloir_day_debug_to_last_row(st, last_date)
                _sync_daily_row_with_shared_line_kpis(rows[-1], st)
            elif rows:
                N = st["trade_count"]
                S_G_N = None if N == 0 else (st["sum_g"] / Decimal(N))
                BT = st["sum_g"]
                nb = st["nb_jours_ouvres"]
                BMJ = None if nb == 0 else (BT / Decimal(nb))
                bmd_days = st.get("buy_days_closed") or 0
                BMD = None if bmd_days == 0 else (BT / Decimal(bmd_days))
                rows.append({
                    "date": str(last_date),
                    "price_close": str(close_d),
                    "ratio_P": None,
                    "ratio_P_pct": None,
                    "tradable": False,
                    "alerts": [],
                    **_universe_daily_fields(ticker, last_date),
                    "buy_code": _compose_condition_label(st["buy_codes"], st["buy_logic"], st["buy_gm_filter"], st["buy_gm_operator"]),
                    "sell_code": _compose_condition_label(st["sell_codes"], st["sell_logic"], st["sell_gm_filter"], st["sell_gm_operator"]),
                    "buy_codes": st["buy_codes"],
                    "sell_codes": st["sell_codes"],
                    "buy_logic": st["buy_logic"],
                    "sell_logic": st["sell_logic"],
                    "buy_gm_filter": st["buy_gm_filter"],
                    "buy_gm_operator": st["buy_gm_operator"],
                    "buy_market_gm_current": st["buy_market_gm_current"],
                    "buy_market_gm_market": st["buy_market_gm_market"],
                    "buy_market_gm_sector": st["buy_market_gm_sector"],
                    "buy_market_operator": st["buy_market_operator"],
                    "gm_buy_conditions": st["gm_buy_conditions"],
                    "gm_sell_market_exit_conditions": st["gm_sell_market_exit_conditions"],
                    "gm_sell_market_exit_label": _gm_conditions_label(st["gm_sell_market_exit_conditions"]),
                    "gm_push_buy_conditions": st["gm_push_buy_conditions"],
                    "gm_push_sell_market_exit_conditions": st["gm_push_sell_market_exit_conditions"],
                    "gm_push_buy_label": _gm_push_conditions_label(st["gm_push_buy_conditions"]),
                    "gm_push_sell_market_exit_label": _gm_push_conditions_label(st["gm_push_sell_market_exit_conditions"]),
                    "sell_gm_filter": st["sell_gm_filter"],
                    "sell_gm_operator": st["sell_gm_operator"],
                    "action": "FORCED_SELL",
                    "action_G": None if G_today is None else str(G_today),
                    "action_PNL_AMOUNT": str(pnl_amount_today),
                    "forced_close": True,
                    "allocated": st["allocated"],
                    "cash_ticker": str(st["cash_ticker"]),
                    "bank": str(st.get("bank") or Decimal("0")),
                    "shares": 0,
                    "N": N,
                    "S_G_N": None if S_G_N is None else str(S_G_N),
                    "BT": str(BT),
                    "NB_JOUR_OUVRES": nb,
                    "BMJ": None if BMJ is None else str(BMJ),
                    "BMD": None if BMD is None else str(BMD),
                    "BUY_DAYS_CLOSED": bmd_days,
                })
                _apply_couloir_day_debug_to_last_row(st, last_date)
                _sync_daily_row_with_shared_line_kpis(rows[-1], st)

    all_warnings = _serialize_warnings(_collect_backtest_warnings(state))

    # Build results structure compatible with previous output
    results: dict[str, Any] = {
        "meta": {
            "backtest_id": backtest.id,
            "scenario_id": backtest.scenario_id,
            "start_date": str(start_d),
            "end_date": str(end_d),
            "CP": str(CP_raw),
            "CP_infinite": CP_infinite,
            "CT": str(CT),
            "capital_mode": "FIXED" if fixed_capital else "REINVEST",
            "X": str(X),
            "signal_lines": signal_lines,
            "global_cash_end": None if global_cash is None else str(global_cash),
            "engine_version": "5.2.3",
            "large_result_mode": bool(large_result_mode),
            "detailed_daily_rows_omitted": bool(large_result_mode),
            "estimated_daily_rows": estimated_daily_rows,
            "warning_count": len(all_warnings),
            "warnings": all_warnings,
        },
        "tickers": {},
    }
    universe_meta = _resolved_universe_meta()
    if universe_meta is not None:
        results["meta"]["universe"] = universe_meta
    effective_currency = effective_currency_for_new_result(universe_meta)
    if effective_currency:
        results["meta"]["effective_currency"] = effective_currency

    # Organize by ticker
    for ticker in data_by_ticker.keys():
        tentry = {"lines": []}
        for li, line in enumerate(signal_lines):
            st = state[(ticker, li)]
            shared_kpis = _build_shared_line_kpi_values(st)
            N = shared_kpis["N"]
            S_G_N = shared_kpis["S_G_N"]
            BT = shared_kpis["BT"]
            # State counters track tradable days and whether a position is held at end of day.
            tradable_days = shared_kpis["TRADABLE_DAYS"]
            in_pos_days = shared_kpis["TRADABLE_DAYS_IN_POSITION_CLOSED"]
            not_in_pos_days = shared_kpis["TRADABLE_DAYS_NOT_IN_POSITION"]

            # Keep legacy keys in the JSON for backward compatibility.
            nb = not_in_pos_days
            bmd_days = in_pos_days

            BMJ = shared_kpis["BMJ"]
            BMD = shared_kpis["BMD"]
            pnl_amount_total = Decimal(st.get("pnl_amount_total") or 0)
            total_gain_amount = Decimal(st.get("total_gain_amount") or 0)
            total_loss_amount = Decimal(st.get("total_loss_amount") or 0)
            win_trades = int(st.get("win_trades") or 0)
            loss_trades = int(st.get("loss_trades") or 0)
            total_trades_amount = int(N)
            flat_trades = max(0, total_trades_amount - win_trades - loss_trades)
            avg_trade_amount = None if total_trades_amount == 0 else (pnl_amount_total / Decimal(total_trades_amount))
            profit_factor_amount = None
            if total_loss_amount < 0:
                profit_factor_amount = total_gain_amount / abs(total_loss_amount)
            elif total_gain_amount > 0 and total_loss_amount == 0:
                profit_factor_amount = None
            win_rate_amount = None if total_trades_amount == 0 else ((Decimal(win_trades) / Decimal(total_trades_amount)) * Decimal("100"))
            tentry["lines"].append({
                "line_index": li + 1,
                "buy": st["buy_codes"],
                "sell": st["sell_codes"],
                "buy_logic": st["buy_logic"],
                "sell_logic": st["sell_logic"],
                "buy_gm_filter": st["buy_gm_filter"],
                "buy_gm_operator": st["buy_gm_operator"],
                "buy_market_gm_current": st["buy_market_gm_current"],
                "buy_market_gm_market": st["buy_market_gm_market"],
                "buy_market_gm_sector": st["buy_market_gm_sector"],
                "buy_market_operator": st["buy_market_operator"],
                "gm_buy_conditions": st["gm_buy_conditions"],
                "gm_sell_market_exit_conditions": st["gm_sell_market_exit_conditions"],
                "gm_sell_market_exit_label": _gm_conditions_label(st["gm_sell_market_exit_conditions"]),
                "gm_push_buy_conditions": st["gm_push_buy_conditions"],
                "gm_push_sell_market_exit_conditions": st["gm_push_sell_market_exit_conditions"],
                "gm_push_buy_label": _gm_push_conditions_label(st["gm_push_buy_conditions"]),
                "gm_push_sell_market_exit_label": _gm_push_conditions_label(st["gm_push_sell_market_exit_conditions"]),
                "sell_gm_filter": st["sell_gm_filter"],
                "sell_gm_operator": st["sell_gm_operator"],
                "trading_model": st["trading_model"],
                "allocated": st["allocated"],
                "daily_rows_omitted": bool(large_result_mode),
                "events": list(st.get("events") or []),
                "warning_count": len(st.get("warnings") or []),
                "warnings": _serialize_warnings(st.get("warnings") or []),
                "explain": _serialized_explain(st),
                "final": {
                    "N": N,
                    "S_G_N": None if S_G_N is None else str(S_G_N),
                    "BT": str(BT),
                    # UI-renamed in V5.2.30 (display): TRADABLE_DAYS_NOT_IN_POSITION
                    "NB_JOUR_OUVRES": nb,
                    "TRADABLE_DAYS": tradable_days,
                    "TRADABLE_DAYS_NOT_IN_POSITION": nb,
                    # UI-renamed in V5.2.30 (display): TRADABLE_DAYS_IN_POSITION_CLOSED
                    "TRADABLE_DAYS_IN_POSITION_CLOSED": bmd_days,
                    "BMJ": None if BMJ is None else str(BMJ),
                    "BMD": None if BMD is None else str(BMD),
                    "BUY_DAYS_CLOSED": bmd_days,
                    "RATIO_NOT_IN_POSITION": None if tradable_days == 0 else str((Decimal(nb) / Decimal(tradable_days)) * Decimal("100")),
                    "RATIO_IN_POSITION": None if tradable_days == 0 else str((Decimal(bmd_days) / Decimal(tradable_days)) * Decimal("100")),
                    "cash_ticker_end": str(st["cash_ticker"]),
                    "PNL_AMOUNT": str(pnl_amount_total),
                    "TOTAL_GAIN_AMOUNT": str(total_gain_amount),
                    "TOTAL_LOSS_AMOUNT": str(total_loss_amount),
                    "AVG_TRADE_AMOUNT": None if avg_trade_amount is None else str(avg_trade_amount),
                    "PROFIT_FACTOR_AMOUNT": None if profit_factor_amount is None else str(profit_factor_amount),
                    "MAX_GAIN_AMOUNT": None if st.get("max_gain_amount") is None else str(st.get("max_gain_amount")),
                    "MAX_LOSS_AMOUNT": None if st.get("max_loss_amount") is None else str(st.get("max_loss_amount")),
                    "WIN_TRADES": win_trades,
                    "LOSS_TRADES": loss_trades,
                    "FLAT_TRADES": flat_trades,
                    "WIN_RATE_AMOUNT": None if win_rate_amount is None else str(win_rate_amount),
                    "FINAL_EQUITY": str(Decimal(st.get("cash_ticker") or 0) + Decimal(st.get("bank") or 0)),
                },
                "daily": [] if large_result_mode else st["daily_rows"],
            })
        results["tickers"][ticker] = tentry

    # --- Feature 8: Portfolio synthesis ---
    # Compute KPIs from daily equity curve
    invested_end = Decimal("0")
    equity_end = Decimal("0")
    nb_days_invested = 0
    if portfolio_daily:
        last = portfolio_daily[-1]
        try:
            invested_end = Decimal(str(last.get("invested") or 0))
        except Exception:
            invested_end = Decimal("0")
        try:
            equity_end = Decimal(str(last.get("equity") or 0))
        except Exception:
            equity_end = Decimal("0")

        for row in portfolio_daily:
            try:
                inv = Decimal(str(row.get("invested") or 0))
            except Exception:
                inv = Decimal("0")
            if inv > 0:
                nb_days_invested += 1

    # Portfolio-level synthesis across played tickers only.
    # A ticker is considered "played" if at least one line has N > 0.
    # If several signal lines exist for the same ticker, we first average
    # the played-line metrics ticker by ticker, then aggregate globally.
    played_ticker_count = 0
    played_ticker_ratios: list[Decimal] = []
    played_ticker_bmds: list[Decimal] = []
    positive_ticker_count = 0
    positive_ticker_bmds: list[Decimal] = []
    positive_ticker_ratios: list[Decimal] = []
    non_positive_ticker_count = 0
    non_positive_ticker_bmds: list[Decimal] = []
    non_positive_ticker_ratios: list[Decimal] = []

    for _ticker, tentry in results.get("tickers", {}).items():
        ticker_ratios: list[Decimal] = []
        ticker_bmds: list[Decimal] = []
        ticker_played = False
        for line in (tentry.get("lines") or []):
            final = line.get("final") or {}
            try:
                n_trades = int(final.get("N") or 0)
            except Exception:
                n_trades = 0
            if n_trades <= 0:
                continue

            ticker_played = True
            ratio_raw = final.get("RATIO_IN_POSITION")
            if ratio_raw not in (None, ""):
                try:
                    ticker_ratios.append(Decimal(str(ratio_raw)))
                except Exception:
                    pass

            bmd_raw = final.get("BMD")
            if bmd_raw not in (None, ""):
                try:
                    ticker_bmds.append(Decimal(str(bmd_raw)))
                except Exception:
                    pass

        if not ticker_played:
            continue

        played_ticker_count += 1
        ticker_avg_ratio = None
        if ticker_ratios:
            ticker_avg_ratio = sum(ticker_ratios) / Decimal(len(ticker_ratios))
            played_ticker_ratios.append(ticker_avg_ratio)

        ticker_avg_bmd = None
        if ticker_bmds:
            ticker_avg_bmd = sum(ticker_bmds) / Decimal(len(ticker_bmds))
            played_ticker_bmds.append(ticker_avg_bmd)

        # Split played tickers into BMD > 0 vs BMD <= 0 (or null).
        # If BMD is missing for a played ticker, classify it in the non-positive bucket.
        if ticker_avg_bmd is not None and ticker_avg_bmd > 0:
            positive_ticker_count += 1
            positive_ticker_bmds.append(ticker_avg_bmd)
            if ticker_avg_ratio is not None:
                positive_ticker_ratios.append(ticker_avg_ratio)
        else:
            non_positive_ticker_count += 1
            if ticker_avg_bmd is not None:
                non_positive_ticker_bmds.append(ticker_avg_bmd)
            if ticker_avg_ratio is not None:
                non_positive_ticker_ratios.append(ticker_avg_ratio)

    avg_ratio_in_position_played = None
    if played_ticker_ratios:
        avg_ratio_in_position_played = sum(played_ticker_ratios) / Decimal(len(played_ticker_ratios))

    avg_bmd_positive = None
    if positive_ticker_bmds:
        avg_bmd_positive = sum(positive_ticker_bmds) / Decimal(len(positive_ticker_bmds))

    avg_ratio_positive = None
    if positive_ticker_ratios:
        avg_ratio_positive = sum(positive_ticker_ratios) / Decimal(len(positive_ticker_ratios))

    avg_bmd_non_positive = None
    if non_positive_ticker_bmds:
        avg_bmd_non_positive = sum(non_positive_ticker_bmds) / Decimal(len(non_positive_ticker_bmds))

    avg_ratio_non_positive = None
    if non_positive_ticker_ratios:
        avg_ratio_non_positive = sum(non_positive_ticker_ratios) / Decimal(len(non_positive_ticker_ratios))

    portfolio_capital_base = CP_raw
    portfolio_n = 0
    portfolio_bt = Decimal("0")
    portfolio_tradable_days_not_in_position = 0
    portfolio_tradable_days_in_position_closed = 0
    total_gain_amount = Decimal("0")
    total_loss_amount = Decimal("0")
    total_trades_amount = 0
    win_trades_amount = 0
    loss_trades_amount = 0
    flat_trades_amount = 0
    max_gain_amount = None
    max_loss_amount = None
    for _ticker, tentry in results.get("tickers", {}).items():
        for line in (tentry.get("lines") or []):
            final = line.get("final") or {}
            try:
                portfolio_n += int(final.get("N") or 0)
            except Exception:
                pass
            try:
                portfolio_bt += Decimal(str(final.get("BT") or 0))
            except Exception:
                pass
            try:
                portfolio_tradable_days_not_in_position += int(final.get("TRADABLE_DAYS_NOT_IN_POSITION") or 0)
            except Exception:
                pass
            try:
                portfolio_tradable_days_in_position_closed += int(final.get("TRADABLE_DAYS_IN_POSITION_CLOSED") or 0)
            except Exception:
                pass
            try:
                line_gain = Decimal(str(final.get("TOTAL_GAIN_AMOUNT") or 0))
            except Exception:
                line_gain = Decimal("0")
            try:
                line_loss = Decimal(str(final.get("TOTAL_LOSS_AMOUNT") or 0))
            except Exception:
                line_loss = Decimal("0")
            try:
                line_win = int(final.get("WIN_TRADES") or 0)
            except Exception:
                line_win = 0
            try:
                line_loss_n = int(final.get("LOSS_TRADES") or 0)
            except Exception:
                line_loss_n = 0
            try:
                line_total_trades = int(final.get("N") or 0)
            except Exception:
                line_total_trades = 0
            line_flat_n = max(0, line_total_trades - line_win - line_loss_n)
            line_max_gain = final.get("MAX_GAIN_AMOUNT")
            line_max_loss = final.get("MAX_LOSS_AMOUNT")
            total_gain_amount += line_gain
            total_loss_amount += line_loss
            win_trades_amount += line_win
            loss_trades_amount += line_loss_n
            flat_trades_amount += line_flat_n
            total_trades_amount += line_total_trades
            if line_max_gain not in (None, ""):
                try:
                    d = Decimal(str(line_max_gain))
                    if max_gain_amount is None or d > max_gain_amount:
                        max_gain_amount = d
                except Exception:
                    pass
            if line_max_loss not in (None, ""):
                try:
                    d = Decimal(str(line_max_loss))
                    if max_loss_amount is None or d < max_loss_amount:
                        max_loss_amount = d
                except Exception:
                    pass

    total_pnl_amount = total_gain_amount + total_loss_amount
    total_return_on_capital = None if portfolio_capital_base <= 0 else (total_pnl_amount / portfolio_capital_base)
    avg_trade_amount = None if total_trades_amount == 0 else (total_pnl_amount / Decimal(total_trades_amount))
    profit_factor_amount = None
    if total_loss_amount < 0:
        profit_factor_amount = total_gain_amount / abs(total_loss_amount)
    win_rate_amount = None if total_trades_amount == 0 else ((Decimal(win_trades_amount) / Decimal(total_trades_amount)) * Decimal("100"))
    portfolio_s_g_n = None if portfolio_n == 0 else (portfolio_bt / Decimal(portfolio_n))
    portfolio_bt_display = None
    if total_trades_amount > 0:
        portfolio_bt_display = _compute_portfolio_bt_ratio(equity_end, invested_end)
    portfolio_bmj = None
    if portfolio_bt_display is not None and nb_days_invested > 0:
        portfolio_bmj = portfolio_bt_display / Decimal(nb_days_invested)
    portfolio_bmd = None
    if portfolio_bt_display is not None and portfolio_tradable_days_in_position_closed > 0:
        portfolio_bmd = portfolio_bt_display / Decimal(portfolio_tradable_days_in_position_closed)

    results["portfolio"] = {
        "kpi": {
            "capital_total": str(CP_raw),
            "invested_end": str(invested_end),
            "equity_end": str(equity_end),
            "N": portfolio_n,
            "S_G_N": None if portfolio_s_g_n is None else str(portfolio_s_g_n),
            "BT": None if portfolio_bt_display is None else str(portfolio_bt_display),
            "TRADABLE_DAYS_NOT_IN_POSITION": portfolio_tradable_days_not_in_position,
            "TRADABLE_DAYS_IN_POSITION_CLOSED": portfolio_tradable_days_in_position_closed,
            "BMJ": None if portfolio_bmj is None else str(portfolio_bmj),
            "BMD": None if portfolio_bmd is None else str(portfolio_bmd),
            "NB_DAYS": nb_days_invested,
            "AVG_RATIO_IN_POSITION_PLAYED": None if avg_ratio_in_position_played is None else str(avg_ratio_in_position_played),
            "NB_PLAYED_TICKERS": played_ticker_count,
            "POSITIVE_BMD_TICKERS": positive_ticker_count,
            "POSITIVE_BMD_AVG_GAIN": None if avg_bmd_positive is None else str(avg_bmd_positive),
            "POSITIVE_BMD_AVG_RATIO_IN_POSITION": None if avg_ratio_positive is None else str(avg_ratio_positive),
            "NON_POSITIVE_BMD_TICKERS": non_positive_ticker_count,
            "NON_POSITIVE_BMD_AVG_GAIN": None if avg_bmd_non_positive is None else str(avg_bmd_non_positive),
            "NON_POSITIVE_BMD_AVG_RATIO_IN_POSITION": None if avg_ratio_non_positive is None else str(avg_ratio_non_positive),
            "max_drawdown": str(max_drawdown),
            "TOTAL_PNL_AMOUNT": str(total_pnl_amount),
            "TOTAL_RETURN_ON_CAPITAL": None if total_return_on_capital is None else str(total_return_on_capital),
            "FINAL_EQUITY": str(equity_end),
            "TOTAL_GAIN_AMOUNT": str(total_gain_amount),
            "TOTAL_LOSS_AMOUNT": str(total_loss_amount),
            "AVG_TRADE_AMOUNT": None if avg_trade_amount is None else str(avg_trade_amount),
            "PROFIT_FACTOR_AMOUNT": None if profit_factor_amount is None else str(profit_factor_amount),
            "MAX_GAIN_AMOUNT": None if max_gain_amount is None else str(max_gain_amount),
            "MAX_LOSS_AMOUNT": None if max_loss_amount is None else str(max_loss_amount),
            "TOTAL_TRADES": total_trades_amount,
            "WIN_TRADES": win_trades_amount,
            "LOSS_TRADES": loss_trades_amount,
            "FLAT_TRADES": flat_trades_amount,
            "WIN_RATE_AMOUNT": None if win_rate_amount is None else str(win_rate_amount),
            "max_drawdown_amount": str(max_drawdown_amount),
        },
        "daily": portfolio_daily,
    }

    return BacktestEngineResult(results=results, logs=logs)


def run_backtest_kpi_only(backtest: Backtest, checkpoint=None, *, max_days: int | None = None) -> dict[str, dict[str, Any]]:
    """Compute ONLY per-ticker KPI finals (no per-day rows, no portfolio).

    Additive helper used by "GameScenario" to avoid huge memory usage.

    Returns: {"TICKER": {"lines": [{"line_index":1, "BMD":"...", ...}, ...], "best_bmd":"..."}}
    """
    logs: list[str] = []

    def _checkpoint():
        if checkpoint is not None:
            checkpoint()

    # Universe
    raw_universe = backtest.universe_snapshot or list(backtest.scenario.symbols.values_list("ticker", flat=True))
    tickers: list[str] = []
    if isinstance(raw_universe, list):
        for item in raw_universe:
            if isinstance(item, dict):
                t = item.get("ticker") or item.get("symbol") or item.get("code")
                if t is not None:
                    tickers.append(str(t).strip())
            else:
                tickers.append(str(item).strip())
    else:
        try:
            tickers = [str(x).strip() for x in list(raw_universe)]
        except Exception:
            tickers = [str(raw_universe).strip()]
    tickers = [t for t in tickers if t]
    if not tickers:
        return {}

    # Params
    CP_raw = Decimal(str(backtest.capital_total or 0))
    CP_infinite = (CP_raw == 0)
    global_cash = None if CP_infinite else CP_raw
    CT = Decimal(str(backtest.capital_per_ticker or 0))
    capital_mode = str(getattr(backtest, "capital_mode", "REINVEST") or "REINVEST").upper()
    fixed_capital = (capital_mode == "FIXED")
    X = Decimal("0")  # Game mode: eligibility bypass (include_all)
    include_all = True
    settings = getattr(backtest, "settings", {}) or {}
    min_price, max_price = _price_bounds_from_settings(settings)
    min_market_cap, max_market_cap = _market_cap_bounds_from_settings(settings)
    market_cap_missing_policy = _market_cap_missing_policy_from_settings(settings)
    market_cap_filter_enabled = _market_cap_filter_enabled(min_market_cap, max_market_cap)
    trend_filters_enabled = False

    signal_lines = _normalize_signal_lines_config(backtest.signal_lines)
    needs_gm_conditions = _signal_lines_have_gm_conditions(signal_lines)
    needs_gm_push_conditions = _signal_lines_have_gm_push_conditions(signal_lines)
    universe_code = _universe_code_for_backtest(backtest)

    symbols = list(Symbol.objects.filter(ticker__in=tickers))
    sym_by_ticker = {s.ticker: s for s in symbols}

    start_d = backtest.start_date
    end_d = backtest.end_date
    warmup_days = int(getattr(backtest, "warmup_days", 0) or 0)
    fetch_start_d = (start_d - timedelta(days=warmup_days)) if (start_d and warmup_days > 0) else start_d

    data_by_ticker, all_dates = _preload_backtest_ticker_data(
        symbols=list(sym_by_ticker.values()),
        scenario_id=backtest.scenario_id,
        fetch_start_d=fetch_start_d,
        end_d=end_d,
        include_compact_extras=False,
    )
    market_cap_cache = (
        _preload_market_cap_cache(list(sym_by_ticker.values()), end_d)
        if market_cap_filter_enabled
        else {}
    )
    benchmark_tickers = sorted(
        _collect_distinct_benchmark_tickers_for_line_market_conditions(
            list(sym_by_ticker.values()),
            signal_lines,
            universe_code=universe_code,
        )
        if (needs_gm_conditions or needs_gm_push_conditions)
        else set()
    )
    benchmark_symbols_by_ticker = _load_benchmark_symbols_by_ticker(
        benchmark_tickers,
        universe_code=universe_code,
    )
    benchmark_price_cache = (
        preload_benchmark_price_cache(
            symbols=list(benchmark_symbols_by_ticker.values()),
            scenario=backtest.scenario,
            start_date=fetch_start_d,
            end_date=end_d,
        )
        if benchmark_tickers
        else {}
    )
    for benchmark_ticker in benchmark_symbols_by_ticker:
        benchmark_price_cache.setdefault(benchmark_ticker, {"dates": [], "values": []})

    if not data_by_ticker:
        return {}

    nglobal = int(getattr(backtest.scenario, "nglobal", 20) or 20)
    global_momentum_values_by_date = (
        _build_global_momentum_values_from_ticker_data(data_by_ticker, nglobal)
        if needs_gm_conditions
        else {}
    )
    global_momentum_regime_by_date = (
        _build_global_momentum_regime_from_values(global_momentum_values_by_date)
        if needs_gm_conditions
        else {}
    )
    gm_push_current_values_by_date = (
        _build_gm_push_current_values_from_ticker_data(data_by_ticker, nglobal)
        if needs_gm_push_conditions
        else {}
    )
    gm_push_benchmark_values_by_ticker = (
        _build_gm_push_values_from_benchmark_cache(
            benchmark_price_cache,
            nglobal=nglobal,
        )
        if needs_gm_push_conditions
        else {}
    )
    gm_push_state_cache: dict[tuple[Any, ...], dict[date, str]] = {}

    dates_sorted = sorted(all_dates)
    warmup_dates = [d for d in dates_sorted if (start_d is not None and d < start_d)]
    real_dates_sorted = [d for d in dates_sorted if (start_d is None or d >= start_d)]
    if max_days and max_days > 0 and len(real_dates_sorted) > max_days:
        real_dates_sorted = real_dates_sorted[-int(max_days):]
    if not real_dates_sorted:
        return {}

    # State per (ticker,line)
    state: dict[tuple[str, int], dict[str, Any]] = {}
    for ticker in data_by_ticker.keys():
        for li, line in enumerate(signal_lines):
            state[(ticker, li)] = {
                "buy_codes": _normalize_codes(line.get("buy")),
                "sell_codes": _normalize_codes(line.get("sell")),
                "buy_logic": _normalize_logic(line.get("buy_logic"), "AND"),
                "sell_logic": _normalize_logic(line.get("sell_logic"), "OR"),
                "buy_gm_filter": _normalize_global_regime_filter(line.get("buy_gm_filter")),
                "buy_gm_operator": _normalize_logic(line.get("buy_gm_operator"), "AND"),
                "buy_market_gm_current": _normalize_global_regime_filter(
                    line.get("buy_market_gm_current", line.get("buy_gm_filter"))
                ),
                "buy_market_gm_market": _normalize_global_regime_filter(line.get("buy_market_gm_market")),
                "buy_market_gm_sector": _normalize_global_regime_filter(line.get("buy_market_gm_sector")),
                "buy_market_operator": _normalize_logic(line.get("buy_market_operator"), "AND"),
                "gm_buy_conditions": _normalize_gm_conditions_config(
                    line.get("gm_buy_conditions"),
                    operator=line.get("buy_market_operator"),
                    current=line.get("buy_market_gm_current", line.get("buy_gm_filter")),
                    market=line.get("buy_market_gm_market"),
                    sector=line.get("buy_market_gm_sector"),
                ),
                "gm_sell_market_exit_conditions": _normalize_gm_conditions_config(
                    line.get("gm_sell_market_exit_conditions"),
                    operator=line.get("gm_sell_market_exit_operator"),
                ),
                "gm_push_buy_conditions": _normalize_gm_push_conditions_config(line.get("gm_push_buy_conditions")),
                "gm_push_sell_market_exit_conditions": _normalize_gm_push_conditions_config(
                    line.get("gm_push_sell_market_exit_conditions"),
                    operator=line.get("gm_push_sell_market_exit_operator"),
                ),
                "sell_gm_filter": _normalize_global_regime_filter(line.get("sell_gm_filter")),
                "sell_gm_operator": _normalize_logic(line.get("sell_gm_operator"), "AND"),
                "trading_model": line.get("trading_model"),
                "allocated": False,
                "cash_ticker": Decimal("0"),
                "bank": Decimal("0"),
                "shares": 0,
                "position_open": False,
                "entry_price": None,
                "entry_date": None,
                "trade_count": 0,
                "sum_g": Decimal("0"),
                "pnl_amount_total": Decimal("0"),
                "total_gain_amount": Decimal("0"),
                "total_loss_amount": Decimal("0"),
                "win_trades": 0,
                "loss_trades": 0,
                "max_gain_amount": None,
                "max_loss_amount": None,
                "tradable_days": 0,
                "tradable_days_in_position": 0,
                "prev_k": None,
                "active_signal_states": {},
                "and_latched_states": {},
                "use_signal_latch_model": _line_uses_signal_latch_model(line),
                "signal_latch_state": {},
                "signal_latch_invalidated_today": set(),
                "signal_latch_last_date": None,
                "sell_signal_latch_state": {},
                "sell_signal_latch_last_date": None,
                "warnings": [],
                "_reentry_warning_candidate": None,
                "_sold_today": False,
                "couloir_state": CouloirState(CouloirConfig.from_line(line)) if is_couloir_line(line) else None,
            }

    def _market_cap_for_day(ticker: str, d) -> Decimal | None:
        if not market_cap_filter_enabled:
            return None
        symbol = sym_by_ticker.get(ticker)
        if not symbol:
            return None
        return _market_cap_from_cache(market_cap_cache.get(symbol.id), d)

    def _ratio_tradable(ticker: str, d, price_value, ratio_p_val) -> tuple[bool, Decimal | None, Decimal | None]:
        return _buy_tradability_for_day(
            price_value=price_value,
            ratio_p_val=ratio_p_val,
            market_cap_value=_market_cap_for_day(ticker, d),
            include_all=include_all,
            ratio_threshold=X,
            min_price=min_price,
            max_price=max_price,
            min_market_cap=min_market_cap,
            max_market_cap=max_market_cap,
            market_cap_missing_policy=market_cap_missing_policy,
        )

    def _trend_filter_allows_buy(ticker: str, d, gm_code: str | None, buy_gm_filter: str | None) -> bool:
        if not trend_filters_enabled:
            return True
        return bool(
            evaluate_trend_filters_for_symbol(
                symbol=sym_by_ticker.get(ticker),
                settings=settings,
                as_of=d,
                nglobal=nglobal,
                gm_current_regime=gm_code,
                benchmark_cache_by_ticker=benchmark_price_cache,
                suppress_gm_current=_normalize_global_regime_filter(buy_gm_filter) != "IGNORE",
                universe_code=universe_code,
            )["passed"]
        )

    def _line_market_conditions_allow_buy(st: dict[str, Any], ticker: str, d, gm_code: str | None) -> bool:
        st["_last_gm_buy_max_blocked"] = False
        st["_last_gm_buy_debug"] = None
        gm_buy_conditions = st.get("gm_buy_conditions") or _normalize_gm_conditions_config(
            operator=st.get("buy_market_operator"),
            current=st.get("buy_market_gm_current"),
            market=st.get("buy_market_gm_market"),
            sector=st.get("buy_market_gm_sector"),
        )
        if not _gm_conditions_has_active(gm_buy_conditions):
            return True
        evaluation = _evaluate_gm_conditions(
            config=gm_buy_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            nglobal=nglobal,
            gm_current_value=_to_dec(global_momentum_values_by_date.get(d)),
            gm_current_regime=gm_code,
            benchmark_cache_by_ticker=benchmark_price_cache,
            apply_buy_max_threshold=True,
            universe_code=universe_code,
        )
        st["_last_gm_buy_max_blocked"] = _evaluation_blocked_by_buy_max_threshold(evaluation)
        return bool(evaluation["passed"])

    def _gm_market_exit_sell_evaluation(st: dict[str, Any], ticker: str, d) -> dict[str, Any]:
        gm_sell_conditions = st.get("gm_sell_market_exit_conditions") or _normalize_gm_conditions_config()
        if not _gm_conditions_has_active(gm_sell_conditions):
            return {"has_active": False, "passed": False, "label": ""}
        evaluation = _evaluate_gm_conditions(
            config=gm_sell_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            nglobal=nglobal,
            gm_current_value=_to_dec(global_momentum_values_by_date.get(d)),
            gm_current_regime=global_momentum_regime_by_date.get(d),
            benchmark_cache_by_ticker=benchmark_price_cache,
            universe_code=universe_code,
        )
        evaluation["passed"] = bool(evaluation.get("passed"))
        return evaluation

    def _line_gm_push_conditions_allow_buy(st: dict[str, Any], ticker: str, d) -> bool:
        st["_last_gm_push_buy_max_blocked"] = False
        gm_push_buy_conditions = st.get("gm_push_buy_conditions") or _normalize_gm_push_conditions_config()
        if not _gm_push_conditions_has_active(gm_push_buy_conditions):
            return True
        evaluation = _evaluate_gm_push_conditions(
            config=gm_push_buy_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            gm_push_current_values=gm_push_current_values_by_date,
            gm_push_benchmark_values_by_ticker=gm_push_benchmark_values_by_ticker,
            gm_push_state_cache=gm_push_state_cache,
            apply_buy_max_threshold=True,
            universe_code=universe_code,
        )
        st["_last_gm_push_buy_max_blocked"] = _evaluation_blocked_by_buy_max_threshold(evaluation)
        return bool(evaluation["passed"])

    def _gm_push_market_exit_sell_evaluation(st: dict[str, Any], ticker: str, d) -> dict[str, Any]:
        gm_push_sell_conditions = st.get("gm_push_sell_market_exit_conditions") or _normalize_gm_push_conditions_config()
        if not _gm_push_conditions_has_active(gm_push_sell_conditions):
            return {"has_active": False, "passed": False, "label": ""}
        evaluation = _evaluate_gm_push_conditions(
            config=gm_push_sell_conditions,
            symbol=sym_by_ticker.get(ticker),
            as_of=d,
            gm_push_current_values=gm_push_current_values_by_date,
            gm_push_benchmark_values_by_ticker=gm_push_benchmark_values_by_ticker,
            gm_push_state_cache=gm_push_state_cache,
            universe_code=universe_code,
        )
        evaluation["passed"] = bool(evaluation.get("passed"))
        return evaluation

    # Warmup phase: reconstruct persistent states before the real game period.
    # No allocation, no trades, no counters during warmup.
    for d in warmup_dates:
        _checkpoint()
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata or d not in tdata.get("price_by_date", {}):
                continue
            local_event_alerts = {a.upper() for a in tdata.get("alerts", {}).get(d, set())}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            _update_and_latched_states(st["and_latched_states"], event_alerts)
            if st["use_signal_latch_model"]:
                _get_signal_latch_day_state(st, local_event_alerts, d)
            if _line_uses_progressive_explicit_sell_model(st):
                _get_sell_signal_latch_day_state(st, local_event_alerts, d)
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                couloir_state.observe_warmup_price(tdata["price_by_date"].get(d))
            if tdata.get("metrics") and (tdata["metrics"].get(d) is not None):
                st["prev_k"] = tdata["metrics"].get(d)

    # Daily loop
    for d in real_dates_sorted:
        _checkpoint()
        # SELL phase
        for (ticker, li), st in state.items():
            st["_sold_today"] = False
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue
            close_d = _to_dec(price_by_date[d])
            if close_d is None:
                continue

            local_event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if st["use_signal_latch_model"]:
                _get_signal_latch_day_state(st, local_event_alerts, d)
            tradable, _, _ = _ratio_tradable(ticker, d, price_by_date.get(d), tdata["metrics"].get(d))

            G_today: Decimal | None = None

            def _do_sell(reason: str, *, reset_signal_memory: bool = True):
                nonlocal G_today
                closed = _close_open_position(
                    st,
                    close_price=close_d,
                    close_date=d,
                    CT=CT,
                    fixed_capital=fixed_capital,
                    reset_signal_memory=reset_signal_memory,
                )
                if closed is None:
                    return
                G_today, _pnl_amount_today = closed
                st["_last_action_reason"] = reason
                logs.append(f"{ticker}[L{li+1}] SELL {reason} on {d} close={close_d} G={G_today}")

            sell_codes = st["sell_codes"]
            sell_code = sell_codes[0] if sell_codes else ""
            couloir_state = st.get("couloir_state")
            if st["position_open"] and couloir_state is not None:
                if couloir_state.evaluate_sell_candidate(d, close_d):
                    _do_sell("Couloir : seuil de repli vente atteint")
            elif st["position_open"] and sell_code == SPECIAL_SELL_K1F_UPPER_DOWN_B1F:
                k_today = (tdata["metrics"].get(d) or None)
                k_prev = st.get("prev_k") or None
                k1f_prev = _to_dec(_metric_val(k_prev, _M_K1F))
                k1f_today = _to_dec(_metric_val(k_today, _M_K1F))
                if _cross_down(k1f_prev, k1f_today, Decimal("0"), Decimal("0")):
                    _do_sell("AUTO (B1f: K1f cross 0 down)")
                else:
                    candidates_prev: list[tuple[str, Decimal]] = []
                    for key, idx in (("K1", _M_K1), ("K2", _M_K2), ("K3", _M_K3), ("K4", _M_K4)):
                        v = _to_dec(_metric_val(k_prev, idx))
                        if v is None or k1f_prev is None:
                            continue
                        if v > k1f_prev:
                            candidates_prev.append((key, v))
                    if candidates_prev and k1f_prev is not None and k1f_today is not None:
                        target_key, _ = min(candidates_prev, key=lambda x: x[1])
                        idx_map = {"K1": _M_K1, "K2": _M_K2, "K3": _M_K3, "K4": _M_K4}
                        target_prev = _to_dec(_metric_val(k_prev, idx_map.get(target_key, _M_K1)))
                        target_today = _to_dec(_metric_val(k_today, idx_map.get(target_key, _M_K1)))
                        if _cross_down(k1f_prev, k1f_today, target_prev, target_today):
                            _do_sell(f"AUTO ({target_key}: K1f cross down)")

            elif st["position_open"] and _line_uses_progressive_explicit_sell_model(st):
                _get_sell_signal_latch_day_state(st, local_event_alerts, d)
                if _signal_latch_sell_ready(st):
                    _do_sell(
                        f"signal {_compose_condition_label(sell_codes, st['sell_logic'], st['sell_gm_filter'], st['sell_gm_operator'])}",
                        reset_signal_memory=False,
                    )
                    _consume_sell_latch_state(st)
                    _arm_reentry_warning_if_needed(st, sell_date=d, ticker=ticker, line_index=li + 1)
            elif st["position_open"] and _line_uses_auto_sell_model(st):
                _latch_state, invalidated_signals = _get_signal_latch_day_state(st, local_event_alerts, d)
                if invalidated_signals:
                    previous_latch_state = dict(_latch_state or {})
                    _do_sell(f"signal invalidation {','.join(sorted(invalidated_signals))}")
                    _retain_non_invalidated_latch_signals_after_sell(st, invalidated_signals, previous_latch_state)
            elif st["position_open"] and _match_line_with_global_filter(day_alerts, latched_alerts, sell_codes, st["sell_logic"], gm_code, st["sell_gm_filter"], st["sell_gm_operator"]):
                _do_sell(f"signal {_compose_condition_label(sell_codes, st['sell_logic'], st['sell_gm_filter'], st['sell_gm_operator'])}")
            if st["position_open"] and G_today is None:
                gm_sell_evaluation = _gm_market_exit_sell_evaluation(st, ticker, d)
                if gm_sell_evaluation.get("has_active") and gm_sell_evaluation.get("passed"):
                    _do_sell(
                        f"Protection marché GM ({gm_sell_evaluation.get('label') or 'active'})",
                        reset_signal_memory=not bool(st.get("use_signal_latch_model")),
                    )
                    if _line_uses_progressive_explicit_sell_model(st):
                        _arm_reentry_warning_if_needed(st, sell_date=d, ticker=ticker, line_index=li + 1)
            if st["position_open"] and G_today is None:
                gm_push_sell_evaluation = _gm_push_market_exit_sell_evaluation(st, ticker, d)
                if gm_push_sell_evaluation.get("has_active") and gm_push_sell_evaluation.get("passed"):
                    _do_sell(
                        f"GM_PUSH_MARKET_EXIT ({gm_push_sell_evaluation.get('label') or 'active'})",
                        reset_signal_memory=not bool(st.get("use_signal_latch_model")),
                    )
                    if _line_uses_progressive_explicit_sell_model(st):
                        _arm_reentry_warning_if_needed(st, sell_date=d, ticker=ticker, line_index=li + 1)
            if G_today is not None:
                st["_sold_today"] = True

            # Keep prev_k for special sells
            if tdata.get("metrics") and (tdata["metrics"].get(d) is not None):
                st["prev_k"] = tdata["metrics"].get(d)

        # BUY allocation phase
        candidates_need_alloc = []
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            if d not in tdata["price_by_date"]:
                continue
            if st["position_open"] or (st.get("_sold_today") and not _line_allows_same_day_reentry(st)):
                continue
            buy_codes = st["buy_codes"]
            local_event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                if not couloir_state.evaluate_buy_candidate(d, tdata["price_by_date"].get(d)):
                    continue
            else:
                day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
                latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
                if st["use_signal_latch_model"]:
                    _get_signal_latch_day_state(st, local_event_alerts, d)
                    if not _signal_latch_buy_ready(st, gm_code):
                        continue
                elif not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                    continue
            tradable, ratio_pct, _ = _ratio_tradable(ticker, d, tdata["price_by_date"].get(d), tdata["metrics"].get(d))
            if not tradable:
                continue
            if not _trend_filter_allows_buy(ticker, d, gm_code, st["buy_gm_filter"]):
                continue
            if not _line_market_conditions_allow_buy(st, ticker, d, gm_code):
                continue
            if not _line_gm_push_conditions_allow_buy(st, ticker, d):
                continue

            if not st["allocated"]:
                if CP_infinite:
                    st["allocated"] = True
                    st["cash_ticker"] = CT
                else:
                    candidates_need_alloc.append((ratio_pct or Decimal("0"), ticker, li))

        if (not CP_infinite) and candidates_need_alloc:
            candidates_need_alloc.sort(key=lambda x: x[0], reverse=True)
            for _ratio_pct, ticker, li in candidates_need_alloc:
                if global_cash is None:
                    break
                if global_cash < CT or CT <= 0:
                    break
                st = state[(ticker, li)]
                if st["allocated"]:
                    continue
                st["allocated"] = True
                st["cash_ticker"] = CT
                global_cash -= CT

        # BUY execution
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            if d not in tdata["price_by_date"]:
                continue
            if st["position_open"] or (st.get("_sold_today") and not _line_allows_same_day_reentry(st)):
                continue
            buy_codes = st["buy_codes"]
            local_event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            event_alerts = set(local_event_alerts)
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                if not couloir_state.evaluate_buy_candidate(d, tdata["price_by_date"].get(d)):
                    continue
            else:
                day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
                latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
                if st["use_signal_latch_model"]:
                    _get_signal_latch_day_state(st, local_event_alerts, d)
                    if not _signal_latch_buy_ready(st, gm_code):
                        continue
                elif not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                    continue
            tradable, _, _ = _ratio_tradable(ticker, d, tdata["price_by_date"].get(d), tdata["metrics"].get(d))
            if not tradable:
                continue
            if not _trend_filter_allows_buy(ticker, d, gm_code, st["buy_gm_filter"]):
                continue
            if not _line_market_conditions_allow_buy(st, ticker, d, gm_code):
                continue
            if not _line_gm_push_conditions_allow_buy(st, ticker, d):
                continue
            if not st["allocated"]:
                continue
            close_d = _to_dec(tdata["price_by_date"][d])
            if close_d is None or close_d <= 0:
                continue
            cash = st["cash_ticker"]
            if fixed_capital and st.get("allocated"):
                cash = CT
                st["cash_ticker"] = CT
            shares = int((cash / close_d).to_integral_value(rounding="ROUND_FLOOR"))
            if shares <= 0:
                continue
            st["shares"] = shares
            st["cash_ticker"] = cash - (Decimal(shares) * close_d)
            st["position_open"] = True
            st["entry_price"] = str(close_d)
            st["entry_date"] = d
            couloir_state = st.get("couloir_state")
            if couloir_state is not None:
                couloir_state.on_buy_executed(close_d)
            if not st["use_signal_latch_model"]:
                _reset_trade_signal_memory(st)
            _record_reentry_warning_if_needed(st, buy_date=d, ticker=ticker, line_index=li + 1)

        # End-of-day counters
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            if d not in tdata["price_by_date"]:
                continue
            tradable, _, _ = _ratio_tradable(ticker, d, tdata["price_by_date"].get(d), tdata["metrics"].get(d))
            if tradable:
                st["tradable_days"] += 1
                if st["position_open"] and st["shares"] > 0:
                    st["tradable_days_in_position"] += 1

    # Force close if requested
    if backtest.close_positions_at_end and real_dates_sorted:
        last_date = real_dates_sorted[-1]
        for (ticker, li), st in state.items():
            if not st["position_open"] or st["entry_price"] is None or st["shares"] <= 0:
                continue
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            close_d = _to_dec(tdata["price_by_date"].get(last_date))
            if close_d is None:
                continue
            closed = _close_open_position(
                st,
                close_price=close_d,
                close_date=last_date,
                CT=CT,
                fixed_capital=fixed_capital,
                reset_signal_memory=True,
            )
            if closed is None:
                continue
            tradable, _, _ = _ratio_tradable(ticker, last_date, tdata["price_by_date"].get(last_date), tdata["metrics"].get(last_date))
            _account_for_forced_close_day(st, was_tradable=tradable)

    # Build finals
    out: dict[str, dict[str, Any]] = {}
    for ticker in data_by_ticker.keys():
        tentry: dict[str, Any] = {"lines": []}
        best_bmd: Decimal | None = None
        for li, _line in enumerate(signal_lines):
            st = state[(ticker, li)]
            shared_kpis = _build_shared_line_kpi_values(st)
            N = shared_kpis["N"]
            BT = shared_kpis["BT"]
            tradable_days = shared_kpis["TRADABLE_DAYS"]
            in_pos_days = shared_kpis["TRADABLE_DAYS_IN_POSITION_CLOSED"]
            not_in_pos_days = shared_kpis["TRADABLE_DAYS_NOT_IN_POSITION"]
            BMJ = shared_kpis["BMJ"]
            BMD = shared_kpis["BMD"]
            if BMD is not None:
                if best_bmd is None or BMD > best_bmd:
                    best_bmd = BMD
            tentry["lines"].append(
                {
                    "line_index": li + 1,
                    "buy": st["buy_codes"],
                    "sell": st["sell_codes"],
                    "buy_logic": st["buy_logic"],
                    "sell_logic": st["sell_logic"],
                    "buy_gm_filter": st["buy_gm_filter"],
                    "buy_gm_operator": st["buy_gm_operator"],
                    "buy_market_gm_current": st["buy_market_gm_current"],
                    "buy_market_gm_market": st["buy_market_gm_market"],
                    "buy_market_gm_sector": st["buy_market_gm_sector"],
                    "buy_market_operator": st["buy_market_operator"],
                    "gm_buy_conditions": st["gm_buy_conditions"],
                    "gm_sell_market_exit_conditions": st["gm_sell_market_exit_conditions"],
                    "gm_sell_market_exit_label": _gm_conditions_label(st["gm_sell_market_exit_conditions"]),
                    "gm_push_buy_conditions": st["gm_push_buy_conditions"],
                    "gm_push_sell_market_exit_conditions": st["gm_push_sell_market_exit_conditions"],
                    "gm_push_buy_label": _gm_push_conditions_label(st["gm_push_buy_conditions"]),
                    "gm_push_sell_market_exit_label": _gm_push_conditions_label(st["gm_push_sell_market_exit_conditions"]),
                    "sell_gm_filter": st["sell_gm_filter"],
                    "sell_gm_operator": st["sell_gm_operator"],
                    "trading_model": st["trading_model"],
                    "warning_count": len(st.get("warnings") or []),
                    "warnings": _serialize_warnings(st.get("warnings") or []),
                    "final": {
                        "N": N,
                        "BT": str(BT),
                        "TRADABLE_DAYS": tradable_days,
                        "TRADABLE_DAYS_NOT_IN_POSITION": not_in_pos_days,
                        "TRADABLE_DAYS_IN_POSITION_CLOSED": in_pos_days,
                        "BMJ": None if BMJ is None else str(BMJ),
                        "BMD": None if BMD is None else str(BMD),
                    },
                }
            )
        tentry["best_bmd"] = None if best_bmd is None else str(best_bmd)
        out[ticker] = tentry
    return out
