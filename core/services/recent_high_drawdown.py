from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_recent_high_drawdown_params(obj: Any) -> tuple[int | None, Decimal | None]:
    lookback = getattr(obj, "recent_high_drawdown_lookback_days", None)
    max_drop = getattr(obj, "recent_high_drawdown_max_drop_pct", None)
    try:
        lookback_value = int(lookback) if lookback not in (None, "") else None
    except (TypeError, ValueError):
        lookback_value = None
    max_drop_value = to_decimal(max_drop)
    if lookback_value is None or lookback_value <= 0 or max_drop_value is None:
        return None, None
    return lookback_value, max_drop_value


def recent_high_drawdown_enabled(obj: Any) -> bool:
    lookback, max_drop = normalize_recent_high_drawdown_params(obj)
    return lookback is not None and max_drop is not None


def compute_recent_high_drawdown_condition(
    *,
    previous_prices: Iterable[Any],
    current_price: Any,
    lookback_days: int | None,
    max_drop_pct: Decimal | None,
) -> dict[str, Any]:
    current = to_decimal(current_price)
    if lookback_days is None or lookback_days <= 0 or max_drop_pct is None:
        return {
            "enabled": False,
            "passed": False,
            "sufficient_history": False,
            "recent_high": None,
            "threshold_price": None,
        }

    previous = [to_decimal(value) for value in previous_prices]
    previous = [value for value in previous if value is not None]
    if current is None or len(previous) < lookback_days:
        return {
            "enabled": True,
            "passed": False,
            "sufficient_history": False,
            "recent_high": None if len(previous) < lookback_days else max(previous[-lookback_days:]),
            "threshold_price": None,
        }

    window = previous[-lookback_days:]
    recent_high = max(window)
    threshold_price = recent_high * (Decimal("1") + max_drop_pct)
    return {
        "enabled": True,
        "passed": current >= threshold_price,
        "sufficient_history": True,
        "recent_high": recent_high,
        "threshold_price": threshold_price,
    }

