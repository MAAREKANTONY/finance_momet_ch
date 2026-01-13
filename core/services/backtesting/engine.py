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


@dataclass
class BacktestEngineResult:
    results: dict[str, Any]
    logs: list[str]


def _alerts_set(alerts_str: str) -> set[str]:
    if not alerts_str:
        return set()
    return {a.strip() for a in alerts_str.split(",") if a.strip()}


def run_backtest(backtest: Backtest) -> BacktestEngineResult:
    logs: list[str] = []

    # Universe
    tickers = backtest.universe_snapshot or list(backtest.scenario.symbols.values_list("ticker", flat=True))
    symbols = list(Symbol.objects.filter(ticker__in=tickers, active=True).all())
    sym_by_ticker = {s.ticker: s for s in symbols}

    # Params
    X = Decimal(str(backtest.ratio_threshold or 0))  # percent
    CT = Decimal(str(backtest.capital_per_ticker or 0))

    # Signal lines
    lines = backtest.signal_lines or []
    if not lines:
        # default fallback: A1/B1
        lines = [{"buy": "A1", "sell": "B1"}]
        logs.append("No signal_lines configured; defaulted to A1/B1.")

    results: dict[str, Any] = {
        "engine_version": "3.0",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "params": {
            "scenario_id": backtest.scenario_id,
            "start_date": str(backtest.start_date),
            "end_date": str(backtest.end_date),
            "capital_total": str(backtest.capital_total),
            "capital_per_ticker": str(backtest.capital_per_ticker),
            "ratio_threshold": str(backtest.ratio_threshold),
            "close_positions_at_end": bool(backtest.close_positions_at_end),
            "signal_lines": lines,
        },
        "tickers": {},
        "logs": [],
    }

    # Iterate each ticker independently (global CP allocation comes in a later feature)
    for ticker in tickers:
        sym = sym_by_ticker.get(ticker)
        if not sym:
            logs.append(f"Ticker {ticker} not found/active; skipped.")
            continue

        # Preload daily bars for date range
        bars = list(
            DailyBar.objects.filter(symbol=sym, date__gte=backtest.start_date, date__lte=backtest.end_date)
            .order_by("date")
            .values("date", "close")
        )
        if not bars:
            logs.append(f"No DailyBar data for {ticker} in range; skipped.")
            continue

        # Preload metrics and alerts keyed by date
        metrics = {
            m["date"]: m["ratio_P"]
            for m in DailyMetric.objects.filter(symbol=sym, scenario_id=backtest.scenario_id, date__gte=backtest.start_date, date__lte=backtest.end_date)
            .values("date", "ratio_P")
        }
        alerts = {
            a["date"]: _alerts_set(a["alerts"])
            for a in Alert.objects.filter(symbol=sym, scenario_id=backtest.scenario_id, date__gte=backtest.start_date, date__lte=backtest.end_date)
            .values("date", "alerts")
        }

        ticker_obj: dict[str, Any] = {"lines": []}

        for line in lines:
            buy_code = (line.get("buy") or "").strip()
            sell_code = (line.get("sell") or "").strip()
            if not buy_code or not sell_code:
                logs.append(f"{ticker}: invalid signal line {line}; skipped.")
                continue

            position_open = False
            entry_price: Decimal | None = None
            shares = 0
            cash_ticker = CT  # initial cash per ticker
            trade_count = 0
            sum_g = Decimal("0")

            nb_jours_ouvres = 0  # as defined in spec
            daily_rows: list[dict[str, Any]] = []

            last_day = bars[-1]["date"]

            for row in bars:
                d: date = row["date"]
                close = row["close"]
                if close is None:
                    # no tradable price
                    daily_rows.append({
                        "date": str(d),
                        "price_close": None,
                        "tradable": False,
                        "N": trade_count,
                        "G": None,
                        "S_G_N": None if trade_count == 0 else str(sum_g / Decimal(trade_count)),
                        "BT": str(sum_g),
                        "NB_JOUR_OUVRES": nb_jours_ouvres,
                        "BMJ": None if nb_jours_ouvres == 0 else str(sum_g / Decimal(nb_jours_ouvres)),
                        "position_open": position_open,
                        "shares": shares,
                        "cash_ticker": str(cash_ticker),
                    })
                    continue

                close_d = Decimal(str(close))
                ratio_p = metrics.get(d)  # Decimal in [0,1] typically
                tradable = False
                if ratio_p is not None:
                    try:
                        tradable = (Decimal(str(ratio_p)) * Decimal("100")) >= X
                    except Exception:
                        tradable = False

                # Count NB_JOUR_OUVRES at start of day before actions
                if tradable and not position_open:
                    nb_jours_ouvres += 1

                day_alerts = alerts.get(d, set())

                # Sell first
                G_today = None
                forced_close = False

                def _do_sell(reason: str):
                    nonlocal position_open, entry_price, shares, cash_ticker, trade_count, sum_g, G_today
                    if not position_open or entry_price is None or shares <= 0:
                        return
                    proceeds = Decimal(shares) * close_d
                    cash_ticker = cash_ticker + proceeds
                    g = (close_d - entry_price) / entry_price if entry_price != 0 else Decimal("0")
                    sum_g += g
                    trade_count += 1
                    G_today = g
                    position_open = False
                    entry_price = None
                    shares = 0
                    logs.append(f"{ticker} {buy_code}/{sell_code}: SELL on {d} ({reason}) G={g}")

                if position_open and (sell_code in day_alerts):
                    _do_sell("signal")

                # Forced close at end
                if backtest.close_positions_at_end and d == last_day and position_open:
                    forced_close = True
                    _do_sell("forced_end")

                # Buy after sell
                if (not position_open) and tradable and (buy_code in day_alerts):
                    # invest all cash_ticker
                    max_shares = int((cash_ticker // close_d) if close_d != 0 else 0)
                    if max_shares > 0:
                        shares = max_shares
                        cash_ticker = cash_ticker - (Decimal(shares) * close_d)
                        position_open = True
                        entry_price = close_d
                        logs.append(f"{ticker} {buy_code}/{sell_code}: BUY on {d} shares={shares} price={close_d}")

                S_G_N = None if trade_count == 0 else (sum_g / Decimal(trade_count))
                BT = sum_g  # sum of % gains per trade (as per current definition)
                BMJ = None if nb_jours_ouvres == 0 else (BT / Decimal(nb_jours_ouvres))

                daily_rows.append({
                    "date": str(d),
                    "price_close": str(close_d),
                    "ratio_P": None if ratio_p is None else str(ratio_p),
                    "tradable": tradable,
                    "alerts": sorted(list(day_alerts)),
                    "buy_code": buy_code,
                    "sell_code": sell_code,
                    "action_G": None if G_today is None else str(G_today),
                    "forced_close": forced_close,
                    "N": trade_count,
                    "G": None if G_today is None else str(G_today),
                    "S_G_N": None if S_G_N is None else str(S_G_N),
                    "BT": str(BT),
                    "NB_JOUR_OUVRES": nb_jours_ouvres,
                    "BMJ": None if BMJ is None else str(BMJ),
                    "position_open": position_open,
                    "shares": shares,
                    "cash_ticker": str(cash_ticker),
                })

            # Summary
            final = daily_rows[-1] if daily_rows else {}
            summary = {
                "ticker": ticker,
                "buy": buy_code,
                "sell": sell_code,
                "trades_N": trade_count,
                "S_G_N": final.get("S_G_N"),
                "BT": final.get("BT"),
                "NB_JOUR_OUVRES": final.get("NB_JOUR_OUVRES"),
                "BMJ": final.get("BMJ"),
            }

            ticker_obj["lines"].append({
                "buy": buy_code,
                "sell": sell_code,
                "daily": daily_rows,
                "summary": summary,
            })

        results["tickers"][ticker] = ticker_obj

    results["logs"] = logs[-500:]  # keep last 500 lines
    return BacktestEngineResult(results=results, logs=logs)
