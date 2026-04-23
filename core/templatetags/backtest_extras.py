from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django import template

from core.trading_model_config import (
    TRADING_MODEL_LATCH_STATEFUL,
    TRADING_MODEL_LEGACY_DAILY,
    resolve_trading_model,
)


register = template.Library()


TRADING_MODEL_BUSINESS_LABELS = {
    TRADING_MODEL_LEGACY_DAILY: "Déclenchement classique (conditions simultanées)",
    TRADING_MODEL_LATCH_STATEFUL: "Déclenchement progressif (conditions validées dans le temps)",
}


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


@register.filter
def trading_model_business_label(line) -> str:
    try:
        trading_model = line.get("trading_model") if isinstance(line, dict) else getattr(line, "trading_model", None)
        buy = line.get("buy") if isinstance(line, dict) else getattr(line, "buy", None)
        resolved_model, explicit = resolve_trading_model(trading_model, buy)
    except Exception:
        return "Mode automatique"

    label = TRADING_MODEL_BUSINESS_LABELS.get(resolved_model, "Déclenchement classique (conditions simultanées)")
    if explicit:
        return label
    if resolved_model == TRADING_MODEL_LATCH_STATEFUL:
        return "Mode automatique → Déclenchement progressif (recommandé)"
    if resolved_model == TRADING_MODEL_LEGACY_DAILY:
        return "Mode automatique → Déclenchement classique"
    return f"Mode automatique → {label}"
