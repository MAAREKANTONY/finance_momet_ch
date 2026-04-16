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

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from django.db import transaction

from core.models import Alert, Backtest, DailyBar, DailyMetric, Symbol
from core.services.global_momentum import (
    compute_global_momentum_values_by_date,
    regime_for_value,
)

# Pseudo signal used by UI to activate the special sell rule.
# IMPORTANT: additive only; legacy backtests ignore it unless explicitly chosen.
SPECIAL_SELL_K1F_UPPER_DOWN_B1F = "AUTO_K1F_UPPER_DOWN_B1F"

GLOBAL_REGIME_FILTER_CODES = {"IGNORE", "GM_POS", "GM_NEG", "GM_NEU", "GM_POS_OR_NEU", "GM_NEG_OR_NEU"}


def _to_dec(v) -> Decimal | None:
    """Best-effort conversion to Decimal."""
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


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


def _normalize_signal_lines_config(signal_lines: Any) -> list[dict[str, Any]]:
    if not isinstance(signal_lines, list) or not signal_lines:
        return [{"mode": "standard", "buy": ["AF"], "sell": ["BF"], "buy_logic": "AND", "sell_logic": "OR"}]
    out: list[dict[str, Any]] = []
    for raw in signal_lines:
        if not isinstance(raw, dict):
            continue
        mode = str(raw.get("mode") or "standard").strip() or "standard"
        buy_codes = _normalize_codes(raw.get("buy") or raw.get("buy_conditions"))
        sell_codes = _normalize_codes(raw.get("sell") or raw.get("sell_conditions"))
        if buy_codes or sell_codes:
            out.append({
                "mode": mode,
                "buy": buy_codes,
                "sell": sell_codes,
                "buy_logic": _normalize_logic(raw.get("buy_logic"), "AND"),
                "sell_logic": _normalize_logic(raw.get("sell_logic"), "OR"),
                "buy_gm_filter": _normalize_global_regime_filter(raw.get("buy_gm_filter")),
                "buy_gm_operator": _normalize_logic(raw.get("buy_gm_operator"), "AND"),
                "sell_gm_filter": _normalize_global_regime_filter(raw.get("sell_gm_filter")),
                "sell_gm_operator": _normalize_logic(raw.get("sell_gm_operator"), "AND"),
            })
    return out or [{"mode": "standard", "buy": ["AF"], "sell": ["BF"], "buy_logic": "AND", "sell_logic": "OR", "buy_gm_filter": "IGNORE", "buy_gm_operator": "AND", "sell_gm_filter": "IGNORE", "sell_gm_operator": "AND"}]


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
)

# For AND conditions, non-persistent crossing signals must also be allowed to
# accumulate over time until their opposite signal invalidates them. Example:
# A1 on day T, then C1 on day T+X => A1 AND C1 becomes true on T+X if B1 did not
# occur in-between. We keep this memory separate from the persistent AF/SP*
# states so that single-signal strategies keep their historical event semantics.
_AND_LATCH_STATE_PAIRS: tuple[tuple[str, str], ...] = (
    ("A1", "B1"),
    ("C1", "D1"),
    ("E1", "F1"),
    ("G1", "H1"),
    ("AF", "BF"),
    ("SPA", "SPV"),
    ("SPVA", "SPVV"),
    ("SPA_BASSE", "SPV_BASSE"),
    ("SPVA_BASSE", "SPVV_BASSE"),
)

_AND_LATCH_OPPOSITE: dict[str, str] = {}
for _pos, _neg in _AND_LATCH_STATE_PAIRS:
    _AND_LATCH_OPPOSITE[_pos] = _neg
    _AND_LATCH_OPPOSITE[_neg] = _pos


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

    data_by_ticker = {ticker: payload for ticker, payload in data_by_ticker.items() if payload["price_by_date"]}
    return data_by_ticker, all_dates
def _build_global_momentum_regime_from_values(
    values_by_date: dict[date, Decimal | None],
) -> dict[date, str]:
    out: dict[date, str] = {}
    for d, v in (values_by_date or {}).items():
        regime = regime_for_value(v)
        if regime:
            out[d] = regime
    return out


def run_backtest(backtest: Backtest, checkpoint=None) -> BacktestEngineResult:

    """
    Feature 4:
    - Adds global capital constraint (CP) and daily selection of new allocations by highest ratio_p.
    - Keeps per-(ticker,line) independent cash re-investment once allocated.
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

    signal_lines = _normalize_signal_lines_config(backtest.signal_lines)

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

    data_by_ticker, all_dates = _preload_backtest_ticker_data(
        symbols=list(sym_by_ticker.values()),
        scenario_id=backtest.scenario_id,
        fetch_start_d=fetch_start_d,
        end_d=end_d,
        include_compact_extras=True,
    )
    for ticker in tickers:
        if ticker in sym_by_ticker and ticker not in data_by_ticker:
            logs.append(f"No DailyBar data for {ticker} in range; skipped.")

    if not data_by_ticker:
        return BacktestEngineResult(results={"error": "No usable tickers with data in range."}, logs=logs)

    nglobal = int(getattr(backtest.scenario, "nglobal", 20) or 20)
    global_momentum_values_by_date = _build_global_momentum_values_from_ticker_data(data_by_ticker, nglobal)
    global_momentum_regime_by_date = _build_global_momentum_regime_from_values(global_momentum_values_by_date)

    dates_sorted = sorted(all_dates)
    if not dates_sorted:
        return BacktestEngineResult(results={"error": "No market dates found in range."}, logs=logs)

    warmup_dates = [d for d in dates_sorted if (start_d is not None and d < start_d)]
    real_dates_sorted = [d for d in dates_sorted if (start_d is None or d >= start_d)]
    if not real_dates_sorted:
        return BacktestEngineResult(results={"error": "No market dates found in effective backtest range."}, logs=logs)

    # Per (ticker, line_index) state
    state: dict[tuple[str, int], dict[str, Any]] = {}

    def _ratio_tradable(ratio_p_val) -> tuple[bool, Decimal | None, Decimal | None]:
        """Return (tradable, ratio_percent, ratio_raw).

        If include_all is enabled, tradable is always True (eligibility bypass),
        while ratio values are kept for ranking/display when available.
        """
        # Accept either:
        # - legacy scalar ratio_P
        # - legacy dict like {"ratio_P": ...}
        # - compact tuple (ratio_P, K1, K1f, K2f, K2, K3, K4)
        if isinstance(ratio_p_val, dict):
            ratio_p_val = ratio_p_val.get("ratio_P")
        elif isinstance(ratio_p_val, tuple):
            ratio_p_val = _metric_val(ratio_p_val, _M_RATIO_P)

        if ratio_p_val is None:
            return (True, None, None) if include_all else (False, None, None)
        try:
            r_raw = Decimal(str(ratio_p_val))
            # ratio_P is already stored as a percentage (0-100)
            r_pct = r_raw
            if include_all:
                return (True, r_pct, r_raw)
            return (r_pct >= X, r_pct, r_raw)
        except Exception:
            return (True, None, None) if include_all else (False, None, None)

    for ticker in data_by_ticker.keys():
        for li, line in enumerate(signal_lines):
            state[(ticker, li)] = {
                "buy_codes": _normalize_codes(line.get("buy")),
                "sell_codes": _normalize_codes(line.get("sell")),
                "buy_logic": _normalize_logic(line.get("buy_logic"), "AND"),
                "sell_logic": _normalize_logic(line.get("sell_logic"), "OR"),
                "buy_gm_filter": _normalize_global_regime_filter(line.get("buy_gm_filter")),
                "buy_gm_operator": _normalize_logic(line.get("buy_gm_operator"), "AND"),
                "sell_gm_filter": _normalize_global_regime_filter(line.get("sell_gm_filter")),
                "sell_gm_operator": _normalize_logic(line.get("sell_gm_operator"), "AND"),
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
                "daily_rows": [],
            }

    # Warmup phase: reconstruct persistent states before the real backtest period.
    # No allocation, no trades, no counters during warmup.
    for d in warmup_dates:
        _checkpoint()
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata or d not in tdata.get("price_by_date", {}):
                continue
            event_alerts = {a.upper() for a in tdata.get("alerts", {}).get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            _update_and_latched_states(st["and_latched_states"], event_alerts)
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
        pnl_global = equity - invested
        portfolio_return_global = None
        if capital_total and capital_total != 0:
            portfolio_return_global = (equity - capital_total) / capital_total

        avg_global_nglobal = _to_dec((global_momentum_values_by_date or {}).get(d))

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
                "drawdown": str(dd),
            }
        )

    # Daily loop
    for d in real_dates_sorted:
        _checkpoint()

        # 1) SELL phase (sell before buy)
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue  # no market data for this ticker that day
            close_d = _to_dec(price_by_date[d])
            if close_d is None:
                continue
            day_alerts_raw = tdata["alerts"].get(d, set())
            event_alerts = {a.upper() for a in day_alerts_raw}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)

            # tradable status computed for NB_JOUR_OUVRES before actions
            tradable, ratio_pct, ratio_raw = _ratio_tradable(tdata["metrics"].get(d))
            if tradable and not st["position_open"]:
                st["nb_jours_ouvres"] += 1

            G_today = None
            pnl_amount_today = None
            forced_close = False

            def _do_sell(reason: str):
                nonlocal G_today, pnl_amount_today
                if not st["position_open"] or st["entry_price"] is None or st["shares"] <= 0:
                    return
                proceeds = Decimal(st["shares"]) * close_d
                if fixed_capital:
                    # Total value after closing the position (includes any cash left from rounding on BUY).
                    total_after = Decimal(st["cash_ticker"]) + proceeds
                    # Keep realized PnL in bank; reset cash_ticker back to CT for the next BUY.
                    st["bank"] = Decimal(st.get("bank") or 0) + (total_after - CT)
                    st["cash_ticker"] = CT
                else:
                    # Legacy behaviour: reinvest everything (capital evolves with PnL).
                    st["cash_ticker"] = st["cash_ticker"] + proceeds
                entry = Decimal(st["entry_price"])
                pnl_amount_today = Decimal(st["shares"]) * (close_d - entry)
                if entry != 0:
                    G_today = (close_d - entry) / entry
                st["pnl_amount_total"] = Decimal(st.get("pnl_amount_total") or 0) + pnl_amount_today
                if pnl_amount_today > 0:
                    st["total_gain_amount"] = Decimal(st.get("total_gain_amount") or 0) + pnl_amount_today
                    st["win_trades"] = int(st.get("win_trades") or 0) + 1
                    current_max_gain = st.get("max_gain_amount")
                    if current_max_gain is None or pnl_amount_today > current_max_gain:
                        st["max_gain_amount"] = pnl_amount_today
                elif pnl_amount_today < 0:
                    st["total_loss_amount"] = Decimal(st.get("total_loss_amount") or 0) + pnl_amount_today
                    st["loss_trades"] = int(st.get("loss_trades") or 0) + 1
                    current_max_loss = st.get("max_loss_amount")
                    if current_max_loss is None or pnl_amount_today < current_max_loss:
                        st["max_loss_amount"] = pnl_amount_today

                # Count holding days ONLY for completed (buy->sell) trades
                if st.get("entry_date") is not None:
                    try:
                        st["buy_days_closed"] += int((d - st["entry_date"]).days) + 1
                    except Exception:
                        pass
                st["entry_date"] = None
                st["trade_count"] += 1
                st["sum_g"] += (G_today or Decimal("0"))
                _reset_trade_signal_memory(st)
                st["position_open"] = False
                st["entry_price"] = None
                st["shares"] = 0
                logs.append(f"{ticker}[L{li+1}] SELL {reason} on {d} close={close_d} G={G_today}")

            sell_codes = st["sell_codes"]
            sell_code = sell_codes[0] if sell_codes else ""

            # Special sell mode: K1f crosses down either (1) 0 (B1f) or (2) the closest
            # "line above" among K1/K2/K3/K4 as of t-1.
            if st["position_open"] and sell_code == SPECIAL_SELL_K1F_UPPER_DOWN_B1F:
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

            elif st["position_open"] and _match_line_with_global_filter(day_alerts, latched_alerts, sell_codes, st["sell_logic"], gm_code, st["sell_gm_filter"], st["sell_gm_operator"]):
                _do_sell(f"signal {_compose_condition_label(sell_codes, st['sell_logic'], st['sell_gm_filter'], st['sell_gm_operator'])}")

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

            st["daily_rows"].append({
                "date": str(d),
                "price_close": str(close_d),
                "ratio_P": None if ratio_raw is None else str(ratio_raw),
                "ratio_P_pct": None if ratio_pct is None else str(ratio_pct),
                "tradable": tradable,
                "alerts": sorted(list(day_alerts_raw)),
                "buy_code": _compose_condition_label(st["buy_codes"], st["buy_logic"], st["buy_gm_filter"], st["buy_gm_operator"]),
                "sell_code": _compose_condition_label(st["sell_codes"], st["sell_logic"], st["sell_gm_filter"], st["sell_gm_operator"]),
                "buy_codes": st["buy_codes"],
                "sell_codes": st["sell_codes"],
                "buy_logic": st["buy_logic"],
                "sell_logic": st["sell_logic"],
                "buy_gm_filter": st["buy_gm_filter"],
                "buy_gm_operator": st["buy_gm_operator"],
                "sell_gm_filter": st["sell_gm_filter"],
                "sell_gm_operator": st["sell_gm_operator"],
                "action": "SELL" if G_today is not None else None,
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
            if st["position_open"]:
                continue
            buy_codes = st["buy_codes"]
            day_alerts_raw = tdata["alerts"].get(d, set())
            event_alerts = {a.upper() for a in day_alerts_raw}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                continue

            tradable, ratio_pct, _ = _ratio_tradable(tdata["metrics"].get(d))
            if not tradable:
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
            if st["position_open"]:
                continue

            buy_codes = st["buy_codes"]
            day_alerts_raw = tdata["alerts"].get(d, set())
            event_alerts = {a.upper() for a in day_alerts_raw}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                continue

            tradable, _, _ = _ratio_tradable(tdata["metrics"].get(d))
            if not tradable:
                continue

            if not st["allocated"]:
                # no allocation available (limited CP)
                continue

            close_d = _to_dec(price_by_date[d])
            if close_d is None or close_d <= 0:
                continue

            cash = st["cash_ticker"]
            if fixed_capital and st.get("allocated"):
                # In fixed mode, each new BUY starts from the initial CT (no reinvest).
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
            _reset_trade_signal_memory(st)

            logs.append(f"{ticker}[L{li+1}] BUY signal {_compose_condition_label(buy_codes, st['buy_logic'], st['buy_gm_filter'], st['buy_gm_operator'])} on {d} close={close_d} shares={shares} cash_left={st['cash_ticker']}")

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
            if not st["daily_rows"]:
                continue
            last = st["daily_rows"][-1]
            is_tradable = bool(last.get("tradable"))
            try:
                shares_eod = int(last.get("shares") or 0)
            except Exception:
                shares_eod = 0
            in_position_eod = shares_eod > 0

            if is_tradable:
                st["tradable_days"] += 1
                if in_position_eod:
                    st["tradable_days_in_position"] += 1

            tradable_days = st["tradable_days"]
            in_pos_days = st["tradable_days_in_position"]
            not_in_pos_days = max(0, tradable_days - in_pos_days)

            # Keep legacy keys for compatibility, but values now match the clarified
            # UI names (TRADABLE_DAYS_NOT_IN_POSITION / TRADABLE_DAYS_IN_POSITION_CLOSED).
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

        # 4) Portfolio daily snapshot (end-of-day)
        _snapshot_portfolio(d)

    def _recompute_tradable_counters_from_rows(st: dict[str, Any]) -> None:
        """Recompute cumulative tradable day counters from existing daily rows.

        This is used as a safety net for cases where rows are mutated after the main
        per-day accounting (ex: forced close at end). It keeps UI metrics consistent.
        """
        st["tradable_days"] = 0
        st["tradable_days_in_position"] = 0
        for row in st.get("daily_rows") or []:
            is_tradable = bool(row.get("tradable"))
            try:
                shares_eod = int(row.get("shares") or 0)
            except Exception:
                shares_eod = 0
            in_pos = shares_eod > 0
            if is_tradable:
                st["tradable_days"] += 1
                if in_pos:
                    st["tradable_days_in_position"] += 1
            td = int(st["tradable_days"])
            ip = int(st["tradable_days_in_position"])
            nip = max(0, td - ip)

            row["TRADABLE_DAYS"] = td
            row["TRADABLE_DAYS_NOT_IN_POSITION"] = nip
            row["TRADABLE_DAYS_IN_POSITION_CLOSED"] = ip
            row["NB_JOUR_OUVRES"] = nip
            row["BUY_DAYS_CLOSED"] = ip

            if td > 0:
                row["RATIO_NOT_IN_POSITION"] = str((Decimal(nip) / Decimal(td)) * Decimal("100"))
                row["RATIO_IN_POSITION"] = str((Decimal(ip) / Decimal(td)) * Decimal("100"))
            else:
                row["RATIO_NOT_IN_POSITION"] = None
                row["RATIO_IN_POSITION"] = None

            BT = _to_dec(row.get("BT")) or Decimal("0")
            row["BMJ"] = None if nip == 0 else str(BT / Decimal(nip))
            row["BMD"] = None if ip == 0 else str(BT / Decimal(ip))

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
            proceeds = Decimal(st["shares"]) * close_d
            if fixed_capital:
                total_after = Decimal(st["cash_ticker"]) + proceeds
                st["bank"] = Decimal(st.get("bank") or 0) + (total_after - CT)
                st["cash_ticker"] = CT
            else:
                st["cash_ticker"] = st["cash_ticker"] + proceeds
            entry = Decimal(st["entry_price"])
            G_today = None
            pnl_amount_today = Decimal(st["shares"]) * (close_d - entry)
            if entry != 0:
                G_today = (close_d - entry) / entry
            st["pnl_amount_total"] = Decimal(st.get("pnl_amount_total") or 0) + pnl_amount_today
            if pnl_amount_today > 0:
                st["total_gain_amount"] = Decimal(st.get("total_gain_amount") or 0) + pnl_amount_today
                st["win_trades"] = int(st.get("win_trades") or 0) + 1
                current_max_gain = st.get("max_gain_amount")
                if current_max_gain is None or pnl_amount_today > current_max_gain:
                    st["max_gain_amount"] = pnl_amount_today
            elif pnl_amount_today < 0:
                st["total_loss_amount"] = Decimal(st.get("total_loss_amount") or 0) + pnl_amount_today
                st["loss_trades"] = int(st.get("loss_trades") or 0) + 1
                current_max_loss = st.get("max_loss_amount")
                if current_max_loss is None or pnl_amount_today < current_max_loss:
                    st["max_loss_amount"] = pnl_amount_today
            st["entry_price"] = None
            # Count holding days for forced-close completed trade
            if st.get("entry_date") is not None:
                try:
                    st["buy_days_closed"] += int((last_date - st["entry_date"]).days) + 1
                except Exception:
                    pass
            st["entry_date"] = None
            st["shares"] = 0
            logs.append(f"{ticker}[L{li+1}] FORCED SELL on {last_date} close={close_d} G={G_today}")

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
                # Ensure tradable day counters remain consistent after mutating EOD shares.
                _recompute_tradable_counters_from_rows(st)
            else:
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
                    "buy_code": _compose_condition_label(st["buy_codes"], st["buy_logic"], st["buy_gm_filter"], st["buy_gm_operator"]),
                    "sell_code": _compose_condition_label(st["sell_codes"], st["sell_logic"], st["sell_gm_filter"], st["sell_gm_operator"]),
                    "buy_codes": st["buy_codes"],
                    "sell_codes": st["sell_codes"],
                    "buy_logic": st["buy_logic"],
                    "sell_logic": st["sell_logic"],
                    "buy_gm_filter": st["buy_gm_filter"],
                    "buy_gm_operator": st["buy_gm_operator"],
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
                _recompute_tradable_counters_from_rows(st)
                _recompute_tradable_counters_from_rows(st)

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
            "engine_version": "5.2.2",
        },
        "tickers": {},
    }

    # Organize by ticker
    for ticker in data_by_ticker.keys():
        tentry = {"lines": []}
        for li, line in enumerate(signal_lines):
            st = state[(ticker, li)]
            N = st["trade_count"]
            S_G_N = None if N == 0 else (st["sum_g"] / Decimal(N))
            BT = st["sum_g"]
            # Day counters are derived from the end-of-day rows (see section 3.b above).
            # They count "tradable" days (ratio_p >= X unless include_all) and whether a position
            # is held at the end of the day (shares > 0).
            tradable_days = int(st.get("tradable_days") or 0)
            in_pos_days = int(st.get("tradable_days_in_position") or 0)
            not_in_pos_days = max(0, tradable_days - in_pos_days)

            # Keep legacy keys in the JSON for backward compatibility.
            nb = not_in_pos_days
            bmd_days = in_pos_days

            BMJ = None if nb == 0 else (BT / Decimal(nb))
            BMD = None if bmd_days == 0 else (BT / Decimal(bmd_days))
            pnl_amount_total = Decimal(st.get("pnl_amount_total") or 0)
            total_gain_amount = Decimal(st.get("total_gain_amount") or 0)
            total_loss_amount = Decimal(st.get("total_loss_amount") or 0)
            win_trades = int(st.get("win_trades") or 0)
            loss_trades = int(st.get("loss_trades") or 0)
            total_trades_amount = win_trades + loss_trades
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
                "sell_gm_filter": st["sell_gm_filter"],
                "sell_gm_operator": st["sell_gm_operator"],
                "allocated": st["allocated"],
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
                    "TOTAL_GAIN_AMOUNT": amount_stats.get("TOTAL_GAIN_AMOUNT"),
                    "TOTAL_LOSS_AMOUNT": amount_stats.get("TOTAL_LOSS_AMOUNT"),
                    "AVG_TRADE_AMOUNT": None if avg_trade_amount is None else str(avg_trade_amount),
                    "PROFIT_FACTOR_AMOUNT": amount_stats.get("PROFIT_FACTOR_AMOUNT"),
                    "MAX_GAIN_AMOUNT": None if st.get("max_gain_amount") is None else str(st.get("max_gain_amount")),
                    "MAX_LOSS_AMOUNT": None if st.get("max_loss_amount") is None else str(st.get("max_loss_amount")),
                    "WIN_TRADES": win_trades,
                    "LOSS_TRADES": loss_trades,
                    "WIN_RATE_AMOUNT": amount_stats.get("WIN_RATE_AMOUNT"),
                    "FINAL_EQUITY": str(Decimal(st.get("cash_ticker") or 0) + Decimal(st.get("bank") or 0)),
                },
                "daily": st["daily_rows"],
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

    played_stats = aggregate_played_ticker_stats(results.get("tickers", {}))

    bt_return = compute_total_return(invested_end, equity_end)
    bmj_return = compute_daily_average(bt_return, nb_days_invested)

    portfolio_capital_base = (CP_raw if not CP_infinite else invested_total)
    total_pnl_amount = equity_end - portfolio_capital_base
    amount_stats = aggregate_amount_stats_from_ticker_entries(results.get("tickers", {}))

    total_gain_amount = Decimal(str(amount_stats.get("TOTAL_GAIN_AMOUNT") or 0))
    total_loss_amount = Decimal(str(amount_stats.get("TOTAL_LOSS_AMOUNT") or 0))
    total_trades_amount = int(amount_stats.get("TOTAL_TRADES") or 0)
    win_trades_amount = int(amount_stats.get("WIN_TRADES") or 0)
    loss_trades_amount = int(amount_stats.get("LOSS_TRADES") or 0)
    max_gain_amount = amount_stats.get("MAX_GAIN_AMOUNT")
    max_loss_amount = amount_stats.get("MAX_LOSS_AMOUNT")

    avg_trade_amount = None if total_trades_amount == 0 else (total_pnl_amount / Decimal(total_trades_amount))
    profit_factor_amount = None
    if total_loss_amount < 0:
        profit_factor_amount = total_gain_amount / abs(total_loss_amount)
    win_rate_amount = None if total_trades_amount == 0 else ((Decimal(win_trades_amount) / Decimal(total_trades_amount)) * Decimal("100"))

    results["portfolio"] = {
        "kpi": {
            "capital_total": str(CP_raw if not CP_infinite else invested_total),
            "invested_end": str(invested_end),
            "equity_end": str(equity_end),
            "BT": None if bt_return is None else str(bt_return),
            "BMJ": None if bmj_return is None else str(bmj_return),
            "NB_DAYS": nb_days_invested,
            "AVG_RATIO_IN_POSITION_PLAYED": played_stats.get("AVG_RATIO_IN_POSITION_PLAYED"),
            "NB_PLAYED_TICKERS": played_stats.get("NB_PLAYED_TICKERS"),
            "POSITIVE_BMD_TICKERS": played_stats.get("POSITIVE_BMD_TICKERS"),
            "POSITIVE_BMD_AVG_GAIN": played_stats.get("POSITIVE_BMD_AVG_GAIN"),
            "POSITIVE_BMD_AVG_RATIO_IN_POSITION": played_stats.get("POSITIVE_BMD_AVG_RATIO_IN_POSITION"),
            "NON_POSITIVE_BMD_TICKERS": played_stats.get("NON_POSITIVE_BMD_TICKERS"),
            "NON_POSITIVE_BMD_AVG_GAIN": played_stats.get("NON_POSITIVE_BMD_AVG_GAIN"),
            "NON_POSITIVE_BMD_AVG_RATIO_IN_POSITION": played_stats.get("NON_POSITIVE_BMD_AVG_RATIO_IN_POSITION"),
            "max_drawdown": str(max_drawdown),
            "TOTAL_PNL_AMOUNT": str(total_pnl_amount),
            "FINAL_EQUITY": str(equity_end),
            "TOTAL_GAIN_AMOUNT": amount_stats.get("TOTAL_GAIN_AMOUNT"),
            "TOTAL_LOSS_AMOUNT": amount_stats.get("TOTAL_LOSS_AMOUNT"),
            "AVG_TRADE_AMOUNT": None if avg_trade_amount is None else str(avg_trade_amount),
            "PROFIT_FACTOR_AMOUNT": amount_stats.get("PROFIT_FACTOR_AMOUNT"),
            "MAX_GAIN_AMOUNT": amount_stats.get("MAX_GAIN_AMOUNT"),
            "MAX_LOSS_AMOUNT": amount_stats.get("MAX_LOSS_AMOUNT"),
            "TOTAL_TRADES": amount_stats.get("TOTAL_TRADES"),
            "WIN_TRADES": amount_stats.get("WIN_TRADES"),
            "LOSS_TRADES": amount_stats.get("LOSS_TRADES"),
            "WIN_RATE_AMOUNT": amount_stats.get("WIN_RATE_AMOUNT"),
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

    signal_lines = _normalize_signal_lines_config(backtest.signal_lines)

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

    if not data_by_ticker:
        return {}

    nglobal = int(getattr(backtest.scenario, "nglobal", 20) or 20)
    global_momentum_values_by_date = _build_global_momentum_values_from_ticker_data(data_by_ticker, nglobal)
    global_momentum_regime_by_date = _build_global_momentum_regime_from_values(global_momentum_values_by_date)

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
                "sell_gm_filter": _normalize_global_regime_filter(line.get("sell_gm_filter")),
                "sell_gm_operator": _normalize_logic(line.get("sell_gm_operator"), "AND"),
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
            }

    def _ratio_tradable(_mtuple) -> tuple[bool, Decimal | None, Decimal | None]:
        # Game mode: always tradable
        return True, None, None

    # Warmup phase: reconstruct persistent states before the real game period.
    # No allocation, no trades, no counters during warmup.
    for d in warmup_dates:
        _checkpoint()
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata or d not in tdata.get("price_by_date", {}):
                continue
            event_alerts = {a.upper() for a in tdata.get("alerts", {}).get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            _update_and_latched_states(st["and_latched_states"], event_alerts)
            if tdata.get("metrics") and (tdata["metrics"].get(d) is not None):
                st["prev_k"] = tdata["metrics"].get(d)

    # Daily loop
    for d in real_dates_sorted:
        _checkpoint()
        # SELL phase
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue
            close_d = _to_dec(price_by_date[d])
            if close_d is None:
                continue

            event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            tradable, _, _ = _ratio_tradable(tdata["metrics"].get(d))

            G_today: Decimal | None = None

            def _do_sell(reason: str):
                nonlocal G_today
                if not st["position_open"] or st["entry_price"] is None or st["shares"] <= 0:
                    return
                proceeds = Decimal(st["shares"]) * close_d
                if fixed_capital:
                    total_after = Decimal(st["cash_ticker"]) + proceeds
                    st["bank"] = Decimal(st.get("bank") or 0) + (total_after - CT)
                    st["cash_ticker"] = CT
                else:
                    st["cash_ticker"] = st["cash_ticker"] + proceeds
                entry = Decimal(st["entry_price"])
                if entry != 0:
                    G_today = (close_d - entry) / entry
                st["trade_count"] += 1
                st["sum_g"] += (G_today or Decimal("0"))
                _reset_trade_signal_memory(st)
                st["position_open"] = False
                st["entry_price"] = None
                st["shares"] = 0
                st["entry_date"] = None
                logs.append(f"{ticker}[L{li+1}] SELL {reason} on {d} close={close_d} G={G_today}")

            sell_codes = st["sell_codes"]
            sell_code = sell_codes[0] if sell_codes else ""
            if st["position_open"] and sell_code == SPECIAL_SELL_K1F_UPPER_DOWN_B1F:
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

            elif st["position_open"] and _match_line_with_global_filter(day_alerts, latched_alerts, sell_codes, st["sell_logic"], gm_code, st["sell_gm_filter"], st["sell_gm_operator"]):
                _do_sell(f"signal {_compose_condition_label(sell_codes, st['sell_logic'], st['sell_gm_filter'], st['sell_gm_operator'])}")

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
            if st["position_open"]:
                continue
            buy_codes = st["buy_codes"]
            event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                continue
            tradable, ratio_pct, _ = _ratio_tradable(tdata["metrics"].get(d))
            if not tradable:
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
            if st["position_open"]:
                continue
            buy_codes = st["buy_codes"]
            event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_line_with_global_filter(day_alerts, latched_alerts, buy_codes, st["buy_logic"], gm_code, st["buy_gm_filter"], st["buy_gm_operator"]):
                continue
            tradable, _, _ = _ratio_tradable(tdata["metrics"].get(d))
            if not tradable:
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
            _reset_trade_signal_memory(st)

        # End-of-day counters
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            if d not in tdata["price_by_date"]:
                continue
            tradable, _, _ = _ratio_tradable(tdata["metrics"].get(d))
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
            entry = Decimal(st["entry_price"])
            G_today = None
            if entry != 0:
                G_today = (close_d - entry) / entry
            st["trade_count"] += 1
            st["sum_g"] += (G_today or Decimal("0"))
            st["position_open"] = False
            st["entry_price"] = None
            st["shares"] = 0
            st["entry_date"] = None

    # Build finals
    out: dict[str, dict[str, Any]] = {}
    for ticker in data_by_ticker.keys():
        tentry: dict[str, Any] = {"lines": []}
        best_bmd: Decimal | None = None
        for li, _line in enumerate(signal_lines):
            st = state[(ticker, li)]
            N = st["trade_count"]
            BT = st["sum_g"]
            tradable_days = int(st.get("tradable_days") or 0)
            in_pos_days = int(st.get("tradable_days_in_position") or 0)
            not_in_pos_days = max(0, tradable_days - in_pos_days)
            BMJ = None if not_in_pos_days == 0 else (BT / Decimal(not_in_pos_days))
            BMD = None if in_pos_days == 0 else (BT / Decimal(in_pos_days))
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
                    "sell_gm_filter": st["sell_gm_filter"],
                    "sell_gm_operator": st["sell_gm_operator"],
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
