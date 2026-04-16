from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping


ZERO = Decimal("0")
HUNDRED = Decimal("100")


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def compute_total_return(initial_capital: Any, final_capital: Any) -> Decimal | None:
    start = to_decimal(initial_capital)
    end = to_decimal(final_capital)
    if start is None or end is None or start == 0:
        return None
    return (end - start) / start


def compute_daily_average(total_return: Any, nb_days: int | Any) -> Decimal | None:
    total = to_decimal(total_return)
    try:
        days = int(nb_days or 0)
    except Exception:
        days = 0
    if total is None or days <= 0:
        return None
    return total / Decimal(days)


def compute_presence_ratio(in_position_days: Any, tradable_days: Any) -> Decimal | None:
    in_pos = to_decimal(in_position_days)
    tradable = to_decimal(tradable_days)
    if in_pos is None or tradable is None or tradable <= 0:
        return None
    return (in_pos / tradable) * HUNDRED


def aggregate_played_ticker_stats(ticker_entries: Mapping[str, Mapping[str, Any]] | None) -> dict[str, Any]:
    entries = ticker_entries or {}
    played_ticker_ratios: list[Decimal] = []
    positive_ticker_count = 0
    positive_ticker_bmds: list[Decimal] = []
    positive_ticker_ratios: list[Decimal] = []
    non_positive_ticker_count = 0
    non_positive_ticker_bmds: list[Decimal] = []
    non_positive_ticker_ratios: list[Decimal] = []

    for _ticker, tentry in entries.items():
        ticker_ratios: list[Decimal] = []
        ticker_bmds: list[Decimal] = []
        for line in (tentry.get("lines") or []):
            final = (line or {}).get("final") or {}
            try:
                n_trades = int(final.get("N") or 0)
            except Exception:
                n_trades = 0
            if n_trades <= 0:
                continue

            ratio_val = to_decimal(final.get("RATIO_IN_POSITION"))
            if ratio_val is not None:
                ticker_ratios.append(ratio_val)

            bmd_val = to_decimal(final.get("BMD"))
            if bmd_val is not None:
                ticker_bmds.append(bmd_val)

        if not ticker_ratios and not ticker_bmds:
            continue

        ticker_avg_ratio = None
        if ticker_ratios:
            ticker_avg_ratio = sum(ticker_ratios) / Decimal(len(ticker_ratios))
            played_ticker_ratios.append(ticker_avg_ratio)

        ticker_avg_bmd = None
        if ticker_bmds:
            ticker_avg_bmd = sum(ticker_bmds) / Decimal(len(ticker_bmds))

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

    def _avg(values: list[Decimal]) -> Decimal | None:
        if not values:
            return None
        return sum(values) / Decimal(len(values))

    avg_ratio_in_position_played = _avg(played_ticker_ratios)
    avg_bmd_positive = _avg(positive_ticker_bmds)
    avg_ratio_positive = _avg(positive_ticker_ratios)
    avg_bmd_non_positive = _avg(non_positive_ticker_bmds)
    avg_ratio_non_positive = _avg(non_positive_ticker_ratios)

    return {
        "AVG_RATIO_IN_POSITION_PLAYED": None if avg_ratio_in_position_played is None else str(avg_ratio_in_position_played),
        "NB_PLAYED_TICKERS": len(played_ticker_ratios),
        "POSITIVE_BMD_TICKERS": positive_ticker_count,
        "POSITIVE_BMD_AVG_GAIN": None if avg_bmd_positive is None else str(avg_bmd_positive),
        "POSITIVE_BMD_AVG_RATIO_IN_POSITION": None if avg_ratio_positive is None else str(avg_ratio_positive),
        "NON_POSITIVE_BMD_TICKERS": non_positive_ticker_count,
        "NON_POSITIVE_BMD_AVG_GAIN": None if avg_bmd_non_positive is None else str(avg_bmd_non_positive),
        "NON_POSITIVE_BMD_AVG_RATIO_IN_POSITION": None if avg_ratio_non_positive is None else str(avg_ratio_non_positive),
    }


def aggregate_amount_stats_from_ticker_entries(ticker_entries: Mapping[str, Mapping[str, Any]] | None) -> dict[str, Any]:
    entries = ticker_entries or {}
    total_gain_amount = ZERO
    total_loss_amount = ZERO
    total_trades_amount = 0
    win_trades_amount = 0
    loss_trades_amount = 0
    max_gain_amount: Decimal | None = None
    max_loss_amount: Decimal | None = None

    for _ticker, tentry in entries.items():
        for line in (tentry.get("lines") or []):
            final = (line or {}).get("final") or {}
            line_gain = to_decimal(final.get("TOTAL_GAIN_AMOUNT")) or ZERO
            line_loss = to_decimal(final.get("TOTAL_LOSS_AMOUNT")) or ZERO
            try:
                line_win = int(final.get("WIN_TRADES") or 0)
            except Exception:
                line_win = 0
            try:
                line_loss_n = int(final.get("LOSS_TRADES") or 0)
            except Exception:
                line_loss_n = 0
            line_max_gain = to_decimal(final.get("MAX_GAIN_AMOUNT"))
            line_max_loss = to_decimal(final.get("MAX_LOSS_AMOUNT"))

            total_gain_amount += line_gain
            total_loss_amount += line_loss
            win_trades_amount += line_win
            loss_trades_amount += line_loss_n
            total_trades_amount += (line_win + line_loss_n)

            if line_max_gain is not None and (max_gain_amount is None or line_max_gain > max_gain_amount):
                max_gain_amount = line_max_gain
            if line_max_loss is not None and (max_loss_amount is None or line_max_loss < max_loss_amount):
                max_loss_amount = line_max_loss

    profit_factor_amount = None
    if total_loss_amount < 0:
        profit_factor_amount = total_gain_amount / abs(total_loss_amount)
    win_rate_amount = None
    if total_trades_amount > 0:
        win_rate_amount = (Decimal(win_trades_amount) / Decimal(total_trades_amount)) * HUNDRED

    return {
        "TOTAL_GAIN_AMOUNT": str(total_gain_amount),
        "TOTAL_LOSS_AMOUNT": str(total_loss_amount),
        "TOTAL_TRADES": total_trades_amount,
        "WIN_TRADES": win_trades_amount,
        "LOSS_TRADES": loss_trades_amount,
        "WIN_RATE_AMOUNT": None if win_rate_amount is None else str(win_rate_amount),
        "PROFIT_FACTOR_AMOUNT": None if profit_factor_amount is None else str(profit_factor_amount),
        "MAX_GAIN_AMOUNT": None if max_gain_amount is None else str(max_gain_amount),
        "MAX_LOSS_AMOUNT": None if max_loss_amount is None else str(max_loss_amount),
    }
