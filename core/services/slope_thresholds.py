from __future__ import annotations

from decimal import Decimal


def effective_sell_threshold(
    buy_threshold: Decimal | None,
    sell_threshold: Decimal | None,
) -> Decimal | None:
    return sell_threshold if sell_threshold is not None else buy_threshold


def cross_up(prev_value: Decimal | None, current_value: Decimal | None, threshold: Decimal | None) -> bool:
    return (
        prev_value is not None
        and current_value is not None
        and threshold is not None
        and prev_value < threshold
        and current_value > threshold
    )


def cross_down(prev_value: Decimal | None, current_value: Decimal | None, threshold: Decimal | None) -> bool:
    return (
        prev_value is not None
        and current_value is not None
        and threshold is not None
        and prev_value > threshold
        and current_value < threshold
    )
