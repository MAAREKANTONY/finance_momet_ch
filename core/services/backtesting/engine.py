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
    if isinstance(value, str):
        code = value.strip().upper()
        return [code] if code else []
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for item in value:
            if item in (None, ""):
                continue
            code = str(item).strip().upper()
            if code:
                out.append(code)
        return out
    return [str(value).strip().upper()] if str(value).strip() else []


def _normalize_logic(value: Any, default: str) -> str:
    logic = str(value or default).strip().upper()
    return logic if logic in {"AND", "OR"} else default


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
            })
    return out or [{"mode": "standard", "buy": ["AF"], "sell": ["BF"], "buy_logic": "AND", "sell_logic": "OR"}]


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


def _match_codes_with_memory(day_alerts: set[str], latched_alerts: set[str], codes: list[str], logic: str = "AND") -> bool:
    codes = _normalize_codes(codes)
    if not codes:
        return False
    logic = _normalize_logic(logic, "AND")
    if logic == "OR":
        return any(code in day_alerts for code in codes)
    effective = set(day_alerts or set()) | set(latched_alerts or set())
    return all(code in effective for code in codes)


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


def _build_global_momentum_regime_from_values(
    values_by_date: dict[date, Decimal | None],
) -> dict[date, str]:
    out: dict[date, str] = {}
    for d, v in (values_by_date or {}).items():
        regime = regime_for_value(v)
        if regime:
            out[d] = regime
    return out


def run_backtest(backtest: Backtest) -> BacktestEngineResult:

    """
    Feature 4:
    - Adds global capital constraint (CP) and daily selection of new allocations by highest ratio_p.
    - Keeps per-(ticker,line) independent cash re-investment once allocated.
    """
    logs: list[str] = []

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

    # NOTE (performance/memory):
    # For large universes (e.g. 500+ tickers over ~10y daily), fully materializing
    # per-day objects (Decimal + nested dicts) can blow up RAM and trigger OOM kills
    # in Celery workers.
    #
    # To keep results identical while reducing memory:
    # - store close/metric values in compact tuples (raw values) instead of nested dicts
    # - convert to Decimal only when the value is actually used
    data_by_ticker: dict[str, dict[str, Any]] = {}
    all_dates: set = set()

    for ticker in tickers:
        sym = sym_by_ticker.get(ticker)
        if not sym:
            logs.append(f"Ticker {ticker} not found/active; skipped.")
            continue

        bars_qs = (
            DailyBar.objects.filter(symbol=sym, date__gte=fetch_start_d, date__lte=end_d)
            .order_by("date")
            .values("date", "close")
        )
        # Materialize once (we still need to collect all dates for the engine),
        # but keep values compact (avoid Decimal objects in memory).
        bars = list(bars_qs)
        if not bars:
            logs.append(f"No DailyBar data for {ticker} in range; skipped.")
            continue

        # Store close as raw (typically Decimal from ORM) to avoid huge Decimal object
        # graphs in Python. We convert with Decimal(str(v)) only when used.
        price_by_date: dict[date, Any] = {}
        for b in bars:
            d = b["date"]
            all_dates.add(d)
            price_by_date[d] = b.get("close")

        # Metrics: store as compact tuples indexed by date to reduce memory.
        # Tuple layout: (ratio_P, K1, K1f, K2f, K2, K3, K4, P, Kf2bis, sum_slope, slope_vrai, sum_slope_basse, slope_vrai_basse)
        metrics: dict[date, tuple[Any, ...]] = {
            m["date"]: (
                m.get("ratio_P"),
                m.get("K1"),
                m.get("K1f"),
                m.get("K2f"),
                m.get("K2"),
                m.get("K3"),
                m.get("K4"),
                m.get("P"),
                m.get("Kf2bis"),
                m.get("sum_slope"),
                m.get("slope_vrai"),
                m.get("sum_slope_basse"),
                m.get("slope_vrai_basse"),
            )
            for m in DailyMetric.objects.filter(
                symbol=sym,
                scenario_id=backtest.scenario_id,
                date__gte=fetch_start_d,
                date__lte=end_d,
            ).values("date", "ratio_P", "K1", "K1f", "K2f", "K2", "K3", "K4", "P", "Kf2bis", "sum_slope", "slope_vrai", "sum_slope_basse", "slope_vrai_basse")
        }
        alerts = {
            a["date"]: _alerts_set(a["alerts"])
            for a in Alert.objects.filter(symbol=sym, scenario_id=backtest.scenario_id, date__gte=fetch_start_d, date__lte=end_d)
            .values("date", "alerts")
        }

        data_by_ticker[ticker] = {
            "symbol_id": sym.id,
            "price_by_date": price_by_date,
            "metrics": metrics,
            "alerts": alerts,
        }

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

    def _snapshot_portfolio(d: date):
        """Compute end-of-day portfolio snapshot.

        Portfolio is the aggregation of all allocated (ticker,line) cash + market value,
        plus remaining global cash when CP is limited.
        """
        nonlocal peak_equity, max_drawdown, invested_total

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
        if dd < max_drawdown:
            max_drawdown = dd

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
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)

            # tradable status computed for NB_JOUR_OUVRES before actions
            tradable, ratio_pct, ratio_raw = _ratio_tradable(tdata["metrics"].get(d))
            if tradable and not st["position_open"]:
                st["nb_jours_ouvres"] += 1

            G_today = None
            forced_close = False

            def _do_sell(reason: str):
                nonlocal G_today
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
                if entry != 0:
                    G_today = (close_d - entry) / entry

                # Count holding days ONLY for completed (buy->sell) trades
                if st.get("entry_date") is not None:
                    try:
                        st["buy_days_closed"] += int((d - st["entry_date"]).days) + 1
                    except Exception:
                        pass
                st["entry_date"] = None
                st["trade_count"] += 1
                st["sum_g"] += (G_today or Decimal("0"))
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

            elif sell_codes and _match_codes_with_memory(day_alerts, latched_alerts, sell_codes, st["sell_logic"]) and st["position_open"]:
                _do_sell(f"signal {_codes_label(sell_codes, st['sell_logic'])}")

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
                "buy_code": _codes_label(st["buy_codes"], st["buy_logic"]),
                "sell_code": _codes_label(st["sell_codes"], st["sell_logic"]),
                "buy_codes": st["buy_codes"],
                "sell_codes": st["sell_codes"],
                "buy_logic": st["buy_logic"],
                "sell_logic": st["sell_logic"],
                "action": "SELL" if G_today is not None else None,
                "action_G": None if G_today is None else str(G_today),
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
            if not buy_codes:
                continue
            day_alerts_raw = tdata["alerts"].get(d, set())
            event_alerts = {a.upper() for a in day_alerts_raw}
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_codes_with_memory(day_alerts, latched_alerts, buy_codes, st["buy_logic"]):
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
            if not buy_codes:
                continue
            day_alerts_raw = tdata["alerts"].get(d, set())
            event_alerts = {a.upper() for a in day_alerts_raw}
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_codes_with_memory(day_alerts, latched_alerts, buy_codes, st["buy_logic"]):
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

            logs.append(f"{ticker}[L{li+1}] BUY signal {_codes_label(buy_codes, st['buy_logic'])} on {d} close={close_d} shares={shares} cash_left={st['cash_ticker']}")

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
            if entry != 0:
                G_today = (close_d - entry) / entry
            st["trade_count"] += 1
            st["sum_g"] += (G_today or Decimal("0"))
            st["position_open"] = False
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
                    "buy_code": _codes_label(st["buy_codes"], st["buy_logic"]),
                    "sell_code": _codes_label(st["sell_codes"], st["sell_logic"]),
                    "buy_codes": st["buy_codes"],
                    "sell_codes": st["sell_codes"],
                    "buy_logic": st["buy_logic"],
                    "sell_logic": st["sell_logic"],
                    "action": "FORCED_SELL",
                    "action_G": None if G_today is None else str(G_today),
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
            tentry["lines"].append({
                "line_index": li + 1,
                "buy": st["buy_codes"],
                "sell": st["sell_codes"],
                "buy_logic": st["buy_logic"],
                "sell_logic": st["sell_logic"],
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

    # Portfolio-level synthesis across played tickers only.
    # A ticker is considered "played" if at least one line has N > 0.
    # If several signal lines exist for the same ticker, we first average
    # the played-line metrics ticker by ticker, then aggregate globally.
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
        for line in (tentry.get("lines") or []):
            final = line.get("final") or {}
            try:
                n_trades = int(final.get("N") or 0)
            except Exception:
                n_trades = 0
            if n_trades <= 0:
                continue

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

        if not ticker_ratios and not ticker_bmds:
            continue

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

    bt_return = None
    bmj_return = None
    if invested_end > 0:
        bt_return = (equity_end - invested_end) / invested_end
        if nb_days_invested > 0:
            bmj_return = bt_return / Decimal(nb_days_invested)

    results["portfolio"] = {
        "kpi": {
            "capital_total": str(CP_raw if not CP_infinite else invested_total),
            "invested_end": str(invested_end),
            "equity_end": str(equity_end),
            "BT": None if bt_return is None else str(bt_return),
            "BMJ": None if bmj_return is None else str(bmj_return),
            "NB_DAYS": nb_days_invested,
            "AVG_RATIO_IN_POSITION_PLAYED": None if avg_ratio_in_position_played is None else str(avg_ratio_in_position_played),
            "NB_PLAYED_TICKERS": len(played_ticker_ratios),
            "POSITIVE_BMD_TICKERS": positive_ticker_count,
            "POSITIVE_BMD_AVG_GAIN": None if avg_bmd_positive is None else str(avg_bmd_positive),
            "POSITIVE_BMD_AVG_RATIO_IN_POSITION": None if avg_ratio_positive is None else str(avg_ratio_positive),
            "NON_POSITIVE_BMD_TICKERS": non_positive_ticker_count,
            "NON_POSITIVE_BMD_AVG_GAIN": None if avg_bmd_non_positive is None else str(avg_bmd_non_positive),
            "NON_POSITIVE_BMD_AVG_RATIO_IN_POSITION": None if avg_ratio_non_positive is None else str(avg_ratio_non_positive),
            "max_drawdown": str(max_drawdown),
        },
        "daily": portfolio_daily,
    }

    return BacktestEngineResult(results=results, logs=logs)


def run_backtest_kpi_only(backtest: Backtest, *, max_days: int | None = None) -> dict[str, dict[str, Any]]:
    """Compute ONLY per-ticker KPI finals (no per-day rows, no portfolio).

    Additive helper used by "GameScenario" to avoid huge memory usage.

    Returns: {"TICKER": {"lines": [{"line_index":1, "BMD":"...", ...}, ...], "best_bmd":"..."}}
    """
    logs: list[str] = []

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

    data_by_ticker: dict[str, dict[str, Any]] = {}
    all_dates: set[date] = set()

    for ticker in tickers:
        sym = sym_by_ticker.get(ticker)
        if not sym:
            continue

        bars = list(
            DailyBar.objects.filter(symbol=sym, date__gte=fetch_start_d, date__lte=end_d)
            .order_by("date")
            .values("date", "close")
        )
        if not bars:
            continue

        price_by_date: dict[date, Any] = {}
        for b in bars:
            d = b["date"]
            all_dates.add(d)
            price_by_date[d] = b.get("close")

        metrics: dict[date, tuple[Any, Any, Any, Any, Any, Any, Any, Any]] = {
            m["date"]: (
                m.get("ratio_P"),
                m.get("K1"),
                m.get("K1f"),
                m.get("K2f"),
                m.get("K2"),
                m.get("K3"),
                m.get("K4"),
                m.get("P"),
            )
            for m in DailyMetric.objects.filter(
                symbol=sym,
                scenario_id=backtest.scenario_id,
                date__gte=fetch_start_d,
                date__lte=end_d,
            ).values("date", "ratio_P", "K1", "K1f", "K2f", "K2", "K3", "K4", "P")
        }
        alerts = {
            a["date"]: _alerts_set(a["alerts"])
            for a in Alert.objects.filter(symbol=sym, scenario_id=backtest.scenario_id, date__gte=fetch_start_d, date__lte=end_d)
            .values("date", "alerts")
        }

        data_by_ticker[ticker] = {"price_by_date": price_by_date, "metrics": metrics, "alerts": alerts}

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
                "allocated": False,
                "cash_ticker": Decimal("0"),
                "bank": Decimal("0"),
                "shares": 0,
                "position_open": False,
                "entry_price": None,
                "entry_date": None,
                "trade_count": 0,
                "sum_g": Decimal("0"),
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

            elif sell_codes and _match_codes_with_memory(day_alerts, latched_alerts, sell_codes, st["sell_logic"]) and st["position_open"]:
                _do_sell(f"signal {_codes_label(sell_codes, st['sell_logic'])}")

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
            if not buy_codes:
                continue
            event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_codes_with_memory(day_alerts, latched_alerts, buy_codes, st["buy_logic"]):
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
            if not buy_codes:
                continue
            event_alerts = {a.upper() for a in tdata["alerts"].get(d, set())}
            gm_code = global_momentum_regime_by_date.get(d)
            if gm_code:
                event_alerts.add(gm_code)
            day_alerts = _apply_signal_state_transitions(st["active_signal_states"], event_alerts)
            latched_alerts = _update_and_latched_states(st["and_latched_states"], event_alerts)
            if not _match_codes_with_memory(day_alerts, latched_alerts, buy_codes, st["buy_logic"]):
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
