from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

from django.db import transaction
from django.utils import timezone

from core.models import (
    Alert,
    BacktestCapitalOverride,
    BacktestDailyStat,
    BacktestResult,
    BacktestRun,
    BacktestTrade,
    DailyBar,
    DailyMetric,
    Scenario,
    StrategyRule,
    Symbol,
)

D0 = Decimal("0")


def _d(x) -> Decimal:
    if x is None:
        return D0
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _round_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _parse_alerts(s: str) -> List[str]:
    if not s:
        return []
    return [p.strip() for p in (s or "").split(",") if p.strip()]


def _next_trading_day_open(symbol_id: int, date) -> Optional[Tuple]:
    """Return (date, open, close) for the first bar strictly after `date`."""
    bar = (
        DailyBar.objects.filter(symbol_id=symbol_id, date__gt=date)
        .order_by("date")
        .only("date", "open", "close")
        .first()
    )
    if not bar:
        return None
    return (bar.date, bar.open, bar.close)


@dataclass
class _State:
    # wallet cash when flat (invested all-in when buying)
    wallet: Decimal
    # if holding
    shares: Decimal = D0
    cash_remainder: Decimal = D0
    in_pos: bool = False

    # pending orders (executed at bar.open on exec_date)
    pending_buy_date: Optional[timezone.datetime.date] = None
    pending_buy_reserved: Decimal = D0  # reserved cash (only when CP is limited)
    pending_sell_date: Optional[timezone.datetime.date] = None

    # trade tracking
    open_trade: Optional[BacktestTrade] = None

    # stats
    trades_count: int = 0
    sum_g: Decimal = D0
    tradable_days: int = 0


@transaction.atomic
def run_backtest(run_id: int) -> str:
    """Run an advanced backtest.

    - Signals come from Alerts (per scenario+symbol).
    - Buy/Sell execution is at NEXT trading day's OPEN (Open J+1).
    - Entry is allowed only if ratio_p (DailyMetric.RATIO_P) >= X (run.min_ratio_p).
    - If capital_total (CP) is non-zero, it's a shared portfolio pool.
      If too many entry signals on the same day, we select by highest ratio_p.
    - Per symbol initial wallet is run.capital_per_symbol, unless overridden by BacktestCapitalOverride.
    """
    run = BacktestRun.objects.select_related("scenario", "strategy").get(pk=run_id)
    scenario: Scenario = run.scenario
    run.started_at = timezone.now()
    run.status = "RUNNING"
    run.error_message = ""
    run.save(update_fields=["started_at", "status", "error_message"])

    # Resolve strategy rules
    rules = list(StrategyRule.objects.filter(strategy=run.strategy, active=True))
    buy_codes = [r.signal_value for r in rules if r.action == StrategyRule.ACTION_BUY and r.signal_type == StrategyRule.SIGNAL_ALERT]
    sell_codes = [r.signal_value for r in rules if r.action == StrategyRule.ACTION_SELL and r.signal_type == StrategyRule.SIGNAL_ALERT]

    symbols = list(scenario.symbols.filter(active=True).all())

    # Prefetch bars, alerts, metrics
    bars_by_sym: Dict[int, Dict] = {}
    all_dates_set = set()

    for sym in symbols:
        bq = DailyBar.objects.filter(symbol=sym).order_by("date").only("date", "open", "close", "high", "low")
        dmap = {}
        for b in bq:
            dmap[b.date] = (b.open, b.close, b.high, b.low)
            all_dates_set.add(b.date)
        bars_by_sym[sym.id] = dmap

    all_dates = sorted(all_dates_set)

    alerts_by_sym: Dict[int, Dict] = {}
    for sym in symbols:
        amap = {}
        for a in Alert.objects.filter(symbol=sym, scenario=scenario).only("date", "alerts"):
            amap[a.date] = set(_parse_alerts(a.alerts))
        alerts_by_sym[sym.id] = amap

    ratio_by_sym: Dict[int, Dict] = {}
    for sym in symbols:
        rmap = {}
        for dm in DailyMetric.objects.filter(symbol=sym, scenario=scenario).only("date", "RATIO_P"):
            if dm.RATIO_P is not None:
                rmap[dm.date] = _d(dm.RATIO_P)
        ratio_by_sym[sym.id] = rmap

    # Initialize states
    states: Dict[int, _State] = {}
    for sym in symbols:
        override = BacktestCapitalOverride.objects.filter(scenario=scenario, symbol=sym).first()
        wallet = _d(override.initial_capital if override else run.capital_per_symbol)
        states[sym.id] = _State(wallet=wallet)

    # Capital pool
    infinite = _d(run.capital_total) == D0
    pool_cash = _d(run.capital_total) if not infinite else D0

    # Cleanup previous outputs for this run (if rerun)
    BacktestTrade.objects.filter(run=run).delete()
    BacktestResult.objects.filter(run=run).delete()
    BacktestDailyStat.objects.filter(run=run).delete()

    # Helpers
    def _has_bar(sym_id: int, date) -> bool:
        return date in bars_by_sym.get(sym_id, {})

    def _open_price(sym_id: int, date) -> Optional[Decimal]:
        tup = bars_by_sym.get(sym_id, {}).get(date)
        return _d(tup[0]) if tup and tup[0] is not None else None

    def _close_price(sym_id: int, date) -> Optional[Decimal]:
        tup = bars_by_sym.get(sym_id, {}).get(date)
        return _d(tup[1]) if tup and tup[1] is not None else None

    # Main loop
    for date in all_dates:
        # 1) Execute pending orders (SELL before BUY, if ever both)
        for sym in symbols:
            st = states[sym.id]
            if st.pending_sell_date == date and st.in_pos:
                price = _open_price(sym.id, date)
                if price is None or price == D0:
                    # can't execute; keep pending
                    continue
                proceeds = (st.shares * price) + st.cash_remainder
                st.wallet = proceeds
                st.shares = D0
                st.cash_remainder = D0
                st.in_pos = False

                # close trade
                if st.open_trade:
                    st.open_trade.sell_signal_date = st.open_trade.sell_signal_date or st.open_trade.buy_signal_date
                    st.open_trade.sell_exec_date = date
                    st.open_trade.sell_price = price
                    # pnl
                    if st.open_trade.buy_price and st.open_trade.buy_price != D0:
                        st.open_trade.pnl_pct = ((price - st.open_trade.buy_price) * Decimal("100")) / st.open_trade.buy_price
                    st.open_trade.pnl_amount = proceeds - (st.open_trade.shares * st.open_trade.buy_price) if (st.open_trade.shares and st.open_trade.buy_price) else None
                    st.open_trade.save()
                    # stats
                    if st.open_trade.pnl_pct is not None:
                        st.trades_count += 1
                        st.sum_g += _d(st.open_trade.pnl_pct)
                st.open_trade = None

                # return to pool
                if not infinite:
                    pool_cash += proceeds
                st.pending_sell_date = None

            # execute buy
            if st.pending_buy_date == date and (not st.in_pos):
                price = _open_price(sym.id, date)
                if price is None or price == D0:
                    # refund reservation and cancel
                    if not infinite:
                        pool_cash += st.pending_buy_reserved
                    st.pending_buy_date = None
                    st.pending_buy_reserved = D0
                    continue

                budget = st.pending_buy_reserved if (not infinite) else st.wallet
                if not infinite:
                    st.pending_buy_reserved = D0
                # buy integer shares
                shares = (budget / price).to_integral_value(rounding=ROUND_FLOOR)
                cost = shares * price
                remainder = budget - cost

                # If no share purchasable, refund and cancel
                if shares <= 0:
                    if not infinite:
                        pool_cash += budget
                    st.pending_buy_date = None
                    continue

                st.shares = shares
                st.cash_remainder = remainder
                st.in_pos = True
                # wallet becomes 0 while in position; value is tracked via shares + cash_remainder
                st.wallet = D0
                st.pending_buy_date = None

                # create trade record
                trade = BacktestTrade.objects.create(
                    run=run,
                    symbol_id=sym.id,
                    buy_signal_date=date,  # execution date is known; we store signal date separately later
                    buy_exec_date=date,
                    buy_price=price,
                    shares=shares,
                )
                st.open_trade = trade

        # 2) For each symbol, compute tradable day count (ratio_p >= X, flat, no pending buy)
        for sym in symbols:
            st = states[sym.id]
            if not _has_bar(sym.id, date):
                continue
            ratio_p = ratio_by_sym.get(sym.id, {}).get(date)
            if ratio_p is not None and ratio_p >= _d(run.min_ratio_p) and (not st.in_pos) and (st.pending_buy_date is None):
                st.tradable_days += 1

        # 3) Read signals at 'date' and schedule orders for next open
        buy_candidates: List[Tuple[Decimal, int, timezone.datetime.date]] = []  # (ratio_p, sym_id, next_date)

        for sym in symbols:
            st = states[sym.id]
            codes = alerts_by_sym.get(sym.id, {}).get(date, set())
            if not codes:
                continue

            # Sell signal
            if any(c in codes for c in sell_codes) and st.in_pos and st.pending_sell_date is None:
                nxt = _next_trading_day_open(sym.id, date)
                if nxt:
                    st.pending_sell_date = nxt[0]
                    if st.open_trade:
                        st.open_trade.sell_signal_date = date
                        st.open_trade.save(update_fields=["sell_signal_date"])

            # Buy signal (candidate)
            if any(c in codes for c in buy_codes) and (not st.in_pos) and st.pending_buy_date is None:
                ratio_p = ratio_by_sym.get(sym.id, {}).get(date)
                if ratio_p is None or ratio_p < _d(run.min_ratio_p):
                    continue
                nxt = _next_trading_day_open(sym.id, date)
                if not nxt:
                    continue
                buy_candidates.append((ratio_p, sym.id, nxt[0]))

        # Sort by ratio_p desc for capital arbitration
        buy_candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

        for ratio_p, sym_id, next_date in buy_candidates:
            st = states[sym_id]
            # determine needed capital
            needed = st.wallet if infinite else st.wallet
            if needed <= D0:
                continue
            if not infinite:
                if pool_cash < needed:
                    continue
                # reserve now
                pool_cash -= needed
                st.pending_buy_reserved = needed
            st.pending_buy_date = next_date
            # also record signal date on trade at execution; we stored exec date; patch later in post-processing
            # We'll store signal date in pending field by reusing open_trade after creation; ok.

        # 4) Write daily stats per symbol for this date if bar exists
        for sym in symbols:
            if not _has_bar(sym.id, date):
                continue
            st = states[sym.id]
            N = st.trades_count
            S_G_N = (st.sum_g / Decimal(N)) if N > 0 else None
            BT = (S_G_N * Decimal(N)) if (S_G_N is not None) else None
            BMJ = (BT / Decimal(st.tradable_days)) if (BT is not None and st.tradable_days > 0) else None

            BacktestDailyStat.objects.create(
                run=run,
                symbol_id=sym.id,
                date=date,
                ratio_p=ratio_by_sym.get(sym.id, {}).get(date),
                N=N,
                G=None,  # filled below if a trade closed today
                S_G_N=S_G_N,
                BT=BT,
                tradable_days=st.tradable_days,
                BMJ=BMJ,
            )

    # Post-process: fill daily G where trade closed on that date
    # We'll map sell_exec_date -> pnl_pct
    for t in BacktestTrade.objects.filter(run=run).only("symbol_id", "sell_exec_date", "pnl_pct"):
        if t.sell_exec_date and t.pnl_pct is not None:
            BacktestDailyStat.objects.filter(run=run, symbol_id=t.symbol_id, date=t.sell_exec_date).update(G=t.pnl_pct)

    # Final results per symbol
    for sym in symbols:
        st = states[sym.id]
        initial = _d(BacktestCapitalOverride.objects.filter(scenario=scenario, symbol=sym).values_list("initial_capital", flat=True).first() or run.capital_per_symbol)

        # Determine final value
        last_date = max(bars_by_sym.get(sym.id, {}).keys()) if bars_by_sym.get(sym.id) else None
        last_close = _close_price(sym.id, last_date) if last_date else None
        if st.in_pos and last_close is not None:
            final_value = (st.shares * last_close) + st.cash_remainder
        else:
            # if buy reserved but not executed, refund to wallet
            if st.pending_buy_reserved and not infinite:
                st.wallet += st.pending_buy_reserved
                pool_cash += st.pending_buy_reserved
                st.pending_buy_reserved = D0
            final_value = st.wallet

        initial_m = _round_money(initial)
        final_m = _round_money(_d(final_value))
        ret_pct = ((final_m - initial_m) * Decimal("100")) / initial_m if initial_m != D0 else None

        BacktestResult.objects.create(
            run=run,
            symbol=sym,
            initial_capital=initial_m,
            final_capital=final_m,
            return_pct=ret_pct,
            trades_count=st.trades_count,
            last_close=last_close,
        )

    run.status = "DONE"
    run.finished_at = timezone.now()
    run.save(update_fields=["status", "finished_at"])

    return "ok"