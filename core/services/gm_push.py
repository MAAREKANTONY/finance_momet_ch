from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Callable

GM_PUSH_POS_ACTIVE = "POS_ACTIVE"
GM_PUSH_NEG_ACTIVE = "NEG_ACTIVE"
GM_PUSH_UNKNOWN = "UNKNOWN"


def _to_dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def compute_push_values_for_series(
    dated_values: dict[date, Any] | list[tuple[date, Any]],
    *,
    nglobal: int,
) -> dict[date, Decimal | None]:
    """SPa-style impulse: sum of daily returns over the last N observations."""
    nglobal = int(nglobal or 0)
    if nglobal <= 0:
        return {}
    ordered = sorted(
        dated_values.items() if isinstance(dated_values, dict) else dated_values,
        key=lambda item: item[0],
    )
    values = [_to_dec(value) for _, value in ordered]
    daily_returns: list[Decimal | None] = [None]
    for idx in range(1, len(values)):
        prev_value = values[idx - 1]
        current_value = values[idx]
        if prev_value in (None, Decimal("0")) or current_value is None:
            daily_returns.append(None)
            continue
        daily_returns.append((current_value - prev_value) / prev_value)

    out: dict[date, Decimal | None] = {}
    for idx in range(nglobal, len(ordered)):
        window = daily_returns[idx - nglobal + 1 : idx + 1]
        if len(window) != nglobal or any(value is None for value in window):
            out[ordered[idx][0]] = None
        else:
            out[ordered[idx][0]] = sum(value for value in window if value is not None)
    return out


def compute_current_push_values_by_date(
    metrics_by_ticker: dict[Any, dict[date, Any]],
    *,
    nglobal: int,
    p_getter: Callable[[Any], Any] | None = None,
) -> dict[date, Decimal | None]:
    if p_getter is None:
        p_getter = lambda value: value
    acc: dict[date, list[Decimal]] = defaultdict(list)
    for _ticker, series in (metrics_by_ticker or {}).items():
        push_by_date = compute_push_values_for_series(
            {day: p_getter(value) for day, value in (series or {}).items()},
            nglobal=nglobal,
        )
        for day, push_value in push_by_date.items():
            if push_value is not None:
                acc[day].append(push_value)
    return {
        day: (sum(values) / Decimal(len(values))) if values else None
        for day, values in acc.items()
    }


def compute_push_state_by_date(
    push_values_by_date: dict[date, Any],
    *,
    buy_threshold: Any,
    sell_threshold: Any,
) -> dict[date, str]:
    buy_threshold_dec = _to_dec(buy_threshold)
    sell_threshold_dec = _to_dec(sell_threshold)
    if buy_threshold_dec is None or sell_threshold_dec is None:
        return {}

    state = GM_PUSH_UNKNOWN
    previous_value: Decimal | None = None
    out: dict[date, str] = {}
    for day in sorted((push_values_by_date or {}).keys()):
        current_value = _to_dec(push_values_by_date.get(day))
        if previous_value is not None and current_value is not None:
            if previous_value < buy_threshold_dec and current_value > buy_threshold_dec:
                state = GM_PUSH_POS_ACTIVE
            elif previous_value > sell_threshold_dec and current_value < sell_threshold_dec:
                state = GM_PUSH_NEG_ACTIVE
        out[day] = state
        if current_value is not None:
            previous_value = current_value
    return out

