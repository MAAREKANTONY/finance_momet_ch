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
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.db import transaction

from core.models import Alert, Backtest, DailyBar, DailyMetric, Symbol

# Pseudo signal used by UI to activate the special sell rule.
# IMPORTANT: additive only; legacy backtests ignore it unless explicitly chosen.
SPECIAL_SELL_K1F_UPPER_DOWN_B1F = "AUTO_K1F_UPPER_DOWN_B1F"


@dataclass
class BacktestEngineResult:
    results: dict[str, Any]
    logs: list[str]


def _alerts_set(alerts_str: str) -> set[str]:
    if not alerts_str:
        return set()
    return {a.strip() for a in alerts_str.split(",") if a.strip()}


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
    X = Decimal(str(backtest.ratio_threshold or 0))  # percent threshold
    include_all = bool(getattr(backtest, "include_all_tickers", False))

    signal_lines = backtest.signal_lines or []
    if not isinstance(signal_lines, list) or not signal_lines:
        # Default: A1/B1
        signal_lines = [{"buy": "A1", "sell": "B1"}]

    # Resolve symbols in one query
    symbols = list(Symbol.objects.filter(ticker__in=tickers))
    sym_by_ticker = {s.ticker: s for s in symbols}

    # Preload all data per ticker for date range
    start_d = backtest.start_date
    end_d = backtest.end_date

    data_by_ticker: dict[str, dict[str, Any]] = {}
    all_dates: set = set()

    for ticker in tickers:
        sym = sym_by_ticker.get(ticker)
        if not sym:
            logs.append(f"Ticker {ticker} not found/active; skipped.")
            continue

        bars_qs = (
            DailyBar.objects.filter(symbol=sym, date__gte=start_d, date__lte=end_d)
            .order_by("date")
            .values("date", "close")
        )
        bars = list(bars_qs)
        if not bars:
            logs.append(f"No DailyBar data for {ticker} in range; skipped.")
            continue

        price_by_date = {}
        for b in bars:
            d = b["date"]
            all_dates.add(d)
            try:
                price_by_date[d] = Decimal(str(b["close"]))
            except Exception:
                # skip bad price rows
                continue

        metrics_full = {
            m["date"]: {
                "ratio_P": m.get("ratio_P"),
                "K1": m.get("K1"),
                "K1f": m.get("K1f"),
                "K2": m.get("K2"),
                "K3": m.get("K3"),
                "K4": m.get("K4"),
            }
            for m in DailyMetric.objects.filter(
                symbol=sym,
                scenario_id=backtest.scenario_id,
                date__gte=start_d,
                date__lte=end_d,
            ).values("date", "ratio_P", "K1", "K1f", "K2", "K3", "K4")
        }
        alerts = {
            a["date"]: _alerts_set(a["alerts"])
            for a in Alert.objects.filter(symbol=sym, scenario_id=backtest.scenario_id, date__gte=start_d, date__lte=end_d)
            .values("date", "alerts")
        }

        data_by_ticker[ticker] = {
            "symbol_id": sym.id,
            "price_by_date": price_by_date,
            "metrics": metrics_full,
            "alerts": alerts,
        }

    if not data_by_ticker:
        return BacktestEngineResult(results={"error": "No usable tickers with data in range."}, logs=logs)

    dates_sorted = sorted(all_dates)
    if not dates_sorted:
        return BacktestEngineResult(results={"error": "No market dates found in range."}, logs=logs)

    # Per (ticker, line_index) state
    state: dict[tuple[str, int], dict[str, Any]] = {}

    def _ratio_tradable(ratio_p_val) -> tuple[bool, Decimal | None, Decimal | None]:
        """Return (tradable, ratio_percent, ratio_raw).

        If include_all is enabled, tradable is always True (eligibility bypass),
        while ratio values are kept for ranking/display when available.
        """
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
                "buy_code": str(line.get("buy") or "").strip().upper(),
                "sell_code": str(line.get("sell") or "").strip().upper(),
                "allocated": False,
                "cash_ticker": Decimal("0"),
                "position_open": False,
                "entry_price": None,
                "shares": 0,
                "trade_count": 0,
                "sum_g": Decimal("0"),
                "nb_jours_ouvres": 0,
                "buy_days_closed": 0,
                "entry_date": None,
                "prev_k": None,  # previous day's K-values dict (K1,K1f,K2,K3,K4)
                "daily_rows": [],
            }

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

        for (tk, _li), st in state.items():
            if not st.get("allocated"):
                continue
            cash_allocated += Decimal(st.get("cash_ticker") or 0)
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

        equity = global_cash_val + cash_allocated + positions_value

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
                "positions_value": str(positions_value),
                "equity": str(equity),
                "invested": str(invested),
                "drawdown": str(dd),
            }
        )

    def _to_dec(x) -> Decimal | None:
        if x is None:
            return None
        try:
            return Decimal(str(x))
        except Exception:
            return None

    def _cross_down(prev_a: Decimal | None, a: Decimal | None, prev_b: Decimal | None, b: Decimal | None) -> bool:
        """Return True when series A crosses series B from above between prev and current."""
        if prev_a is None or a is None or prev_b is None or b is None:
            return False
        return (prev_a > prev_b) and (a <= b)

    # Daily loop
    for d in dates_sorted:

        # 1) SELL phase (sell before buy)
        for (ticker, li), st in state.items():
            tdata = data_by_ticker.get(ticker)
            if not tdata:
                continue
            price_by_date = tdata["price_by_date"]
            if d not in price_by_date:
                continue  # no market data for this ticker that day
            close_d = price_by_date[d]
            day_alerts_raw = tdata["alerts"].get(d, set())
            day_alerts = {a.upper() for a in day_alerts_raw}

            # tradable status computed for NB_JOUR_OUVRES before actions
            tradable, ratio_pct, ratio_raw = _ratio_tradable((tdata["metrics"].get(d) or {}).get("ratio_P"))
            if tradable and not st["position_open"]:
                st["nb_jours_ouvres"] += 1

            G_today = None
            forced_close = False

            def _do_sell(reason: str):
                nonlocal G_today
                if not st["position_open"] or st["entry_price"] is None or st["shares"] <= 0:
                    return
                proceeds = Decimal(st["shares"]) * close_d
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

            sell_code = st["sell_code"]

            # Special sell mode: K1f crosses down either (1) 0 (B1f) or (2) the closest
            # "line above" among K1/K2/K3/K4 as of t-1.
            if st["position_open"] and sell_code == SPECIAL_SELL_K1F_UPPER_DOWN_B1F:
                k_today = (tdata["metrics"].get(d) or {})
                k_prev = st.get("prev_k") or {}
                k1f_prev = _to_dec(k_prev.get("K1f"))
                k1f_today = _to_dec(k_today.get("K1f"))

                # 1) B1f fallback: K1f cross 0 down
                if _cross_down(k1f_prev, k1f_today, Decimal("0"), Decimal("0")):
                    _do_sell("AUTO (B1f: K1f cross 0 down)")
                else:
                    # 2) Find the closest line above K1f at t-1 among K1/K2/K3/K4
                    candidates_prev: list[tuple[str, Decimal]] = []
                    for key in ("K1", "K2", "K3", "K4"):
                        v = _to_dec(k_prev.get(key))
                        if v is None or k1f_prev is None:
                            continue
                        if v > k1f_prev:
                            candidates_prev.append((key, v))

                    if candidates_prev and k1f_prev is not None and k1f_today is not None:
                        # pick closest above => minimal value
                        target_key, _target_prev = min(candidates_prev, key=lambda x: x[1])
                        target_prev = _to_dec(k_prev.get(target_key))
                        target_today = _to_dec(k_today.get(target_key))
                        if _cross_down(k1f_prev, k1f_today, target_prev, target_today):
                            _do_sell(f"AUTO ({target_key}: K1f cross down)")

            elif sell_code and sell_code in day_alerts and st["position_open"]:
                _do_sell(f"signal {sell_code}")

            # record daily row (we may update with buy action later, but keep as dict to mutate)
            N = st["trade_count"]
            S_G_N = None if N == 0 else (st["sum_g"] / Decimal(N))
            BT = st["sum_g"]  # == S_G_N*N
            nb = st["nb_jours_ouvres"]
            BMJ = None if nb == 0 else (BT / Decimal(nb))
            bmd_days = st.get("buy_days_closed") or 0
            BMD = None if bmd_days == 0 else (BT / Decimal(bmd_days))

            st["daily_rows"].append({
                "date": str(d),
                "price_close": str(close_d),
                "ratio_P": None if ratio_raw is None else str(ratio_raw),
                "ratio_P_pct": None if ratio_pct is None else str(ratio_pct),
                "tradable": tradable,
                "alerts": sorted(list(day_alerts_raw)),
                "buy_code": st["buy_code"],
                "sell_code": st["sell_code"],
                "action": "SELL" if G_today is not None else None,
                "action_G": None if G_today is None else str(G_today),
                "forced_close": forced_close,
                "allocated": st["allocated"],
                "cash_ticker": str(st["cash_ticker"]),
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
            buy_code = st["buy_code"]
            if not buy_code:
                continue
            day_alerts_raw = tdata["alerts"].get(d, set())
            day_alerts = {a.upper() for a in day_alerts_raw}
            if buy_code not in day_alerts:
                continue

            tradable, ratio_pct, _ = _ratio_tradable((tdata["metrics"].get(d) or {}).get("ratio_P"))
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

            buy_code = st["buy_code"]
            if not buy_code:
                continue
            day_alerts_raw = tdata["alerts"].get(d, set())
            day_alerts = {a.upper() for a in day_alerts_raw}
            if buy_code not in day_alerts:
                continue

            tradable, _, _ = _ratio_tradable((tdata["metrics"].get(d) or {}).get("ratio_P"))
            if not tradable:
                continue

            if not st["allocated"]:
                # no allocation available (limited CP)
                continue

            close_d = price_by_date[d]
            if close_d <= 0:
                continue

            cash = st["cash_ticker"]
            shares = int((cash / close_d).to_integral_value(rounding="ROUND_FLOOR"))
            if shares <= 0:
                continue

            st["shares"] = shares
            st["cash_ticker"] = cash - (Decimal(shares) * close_d)
            st["position_open"] = True
            st["entry_price"] = str(close_d)
            st["entry_date"] = d

            logs.append(f"{ticker}[L{li+1}] BUY signal {buy_code} on {d} close={close_d} shares={shares} cash_left={st['cash_ticker']}")

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

        # 4) Portfolio daily snapshot (end-of-day)
        _snapshot_portfolio(d)

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
            close_d = price_by_date[last_date]
            proceeds = Decimal(st["shares"]) * close_d
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
                    "buy_code": st["buy_code"],
                    "sell_code": st["sell_code"],
                    "action": "FORCED_SELL",
                    "action_G": None if G_today is None else str(G_today),
                    "forced_close": True,
                    "allocated": st["allocated"],
                    "cash_ticker": str(st["cash_ticker"]),
                    "shares": 0,
                    "N": N,
                    "S_G_N": None if S_G_N is None else str(S_G_N),
                    "BT": str(BT),
                    "NB_JOUR_OUVRES": nb,
                    "BMJ": None if BMJ is None else str(BMJ),
                    "BMD": None if BMD is None else str(BMD),
                    "BUY_DAYS_CLOSED": bmd_days,
                })

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
            "X": str(X),
            "signal_lines": signal_lines,
            "global_cash_end": None if global_cash is None else str(global_cash),
            "engine_version": "5.2.1",
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
            nb = st["nb_jours_ouvres"]
            BMJ = None if nb == 0 else (BT / Decimal(nb))
            bmd_days = st.get("buy_days_closed") or 0
            BMD = None if bmd_days == 0 else (BT / Decimal(bmd_days))
            tentry["lines"].append({
                "line_index": li + 1,
                "buy": st["buy_code"],
                "sell": st["sell_code"],
                "allocated": st["allocated"],
                "final": {
                    "N": N,
                    "S_G_N": None if S_G_N is None else str(S_G_N),
                    "BT": str(BT),
                    "NB_JOUR_OUVRES": nb,
                    "BMJ": None if BMJ is None else str(BMJ),
                    "BMD": None if BMD is None else str(BMD),
                    "BUY_DAYS_CLOSED": bmd_days,
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
            "max_drawdown": str(max_drawdown),
        },
        "daily": portfolio_daily,
    }

    return BacktestEngineResult(results=results, logs=logs)
