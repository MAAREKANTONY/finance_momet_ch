from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django import template


register = template.Library()


def _to_decimal(value) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


@register.filter
def pct(value, decimals: int = 2) -> str:
    """Format a ratio (e.g. 0.0123) as a percentage string '1.23%'.

    The backtest engine stores G/S_G_N/BT/BMJ as ratios (1.0 == 100%).
    """
    d = _to_decimal(value)
    if d is None:
        return ""
    try:
        q = d * Decimal("100")
        fmt = f"{{0:.{int(decimals)}f}}%"
        return fmt.format(float(q))
    except Exception:
        return ""


@register.filter
def num(value, decimals: int = 6) -> str:
    """Format a number safely with a fixed number of decimals."""
    d = _to_decimal(value)
    if d is None:
        return ""
    try:
        fmt = f"{{0:.{int(decimals)}f}}"
        return fmt.format(float(d))
    except Exception:
        return ""
