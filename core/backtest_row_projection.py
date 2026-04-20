from __future__ import annotations

from typing import Any


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def augment_tradable_projection_row(row: dict[str, Any]) -> dict[str, Any]:
    """Project legacy tradable counters into the read-only UI/debug shape.

    This preserves the existing consumer behavior exactly:
    - values are derived from NB_JOUR_OUVRES / BUY_DAYS_CLOSED
    - invalid or missing counters fall back to 0
    - ratio fields fall back to 0.0 when total tradable days is 0
    """
    if not isinstance(row, dict):
        return row
    projected = dict(row)
    not_in_position = _int_or_zero(projected.get("NB_JOUR_OUVRES"))
    in_position = _int_or_zero(projected.get("BUY_DAYS_CLOSED"))
    tradable_days = not_in_position + in_position
    projected["TRADABLE_DAYS_NOT_IN_POSITION"] = not_in_position
    projected["TRADABLE_DAYS_IN_POSITION_CLOSED"] = in_position
    projected["TRADABLE_DAYS"] = tradable_days
    projected["RATIO_NOT_IN_POSITION"] = (not_in_position / tradable_days * 100.0) if tradable_days > 0 else 0.0
    projected["RATIO_IN_POSITION"] = (in_position / tradable_days * 100.0) if tradable_days > 0 else 0.0
    return projected
