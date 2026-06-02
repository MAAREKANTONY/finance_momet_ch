from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django import template

from core.trading_model_config import (
    TRADING_MODEL_PROGRESSIVE_AUTO_SELL,
    TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL,
    TRADING_MODEL_LATCH_STATEFUL,
    TRADING_MODEL_LEGACY_DAILY,
    resolve_trading_model,
)


register = template.Library()


TRADING_MODEL_BUSINESS_LABELS = {
    TRADING_MODEL_LEGACY_DAILY: "Mode legacy classique",
    TRADING_MODEL_LATCH_STATEFUL: "Progressif avec vente automatique",
    TRADING_MODEL_PROGRESSIVE_AUTO_SELL: "Progressif avec vente automatique",
    TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL: "Progressif avec vente explicite",
}

GM_FILTER_BUSINESS_LABELS = {
    "IGNORE": "Ignoré",
    "GM_POS": "GM positif",
    "GM_NEG": "GM négatif",
    "GM_NEU": "GM neutre",
    "GM_POS_OR_NEU": "GM positif ou neutre",
    "GM_NEG_OR_NEU": "GM négatif ou neutre",
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

    label = TRADING_MODEL_BUSINESS_LABELS.get(resolved_model, "Mode legacy classique")
    if explicit:
        return label
    if resolved_model in {TRADING_MODEL_LATCH_STATEFUL, TRADING_MODEL_PROGRESSIVE_AUTO_SELL}:
        return "Mode automatique → Progressif avec vente automatique"
    if resolved_model == TRADING_MODEL_LEGACY_DAILY:
        return "Mode automatique → Mode legacy classique"
    return f"Mode automatique → {label}"


@register.filter
def gm_filter_business_label(value) -> str:
    code = str(value or "IGNORE").upper()
    return GM_FILTER_BUSINESS_LABELS.get(code, code)


@register.simple_tag
def line_gm_filter_display(line, side: str = "buy") -> str:
    try:
        trading_model = line.get("trading_model") if isinstance(line, dict) else getattr(line, "trading_model", None)
        buy = line.get("buy") if isinstance(line, dict) else getattr(line, "buy", None)
        resolved_model, _explicit = resolve_trading_model(trading_model, buy)
    except Exception:
        resolved_model = TRADING_MODEL_LEGACY_DAILY

    key = f"{side}_gm_filter"
    code = ""
    if isinstance(line, dict):
        code = line.get(key) or "IGNORE"
    else:
        code = getattr(line, key, None) or "IGNORE"
    code = str(code).upper()

    if side == "sell":
        if resolved_model in {TRADING_MODEL_LATCH_STATEFUL, TRADING_MODEL_PROGRESSIVE_AUTO_SELL} or code == "IGNORE":
            return ""
    return GM_FILTER_BUSINESS_LABELS.get(code, code)


@register.simple_tag
def line_market_conditions_display(line) -> str:
    if isinstance(line, dict):
        current = line.get("buy_market_gm_current", line.get("buy_gm_filter")) or "IGNORE"
        market = line.get("buy_market_gm_market") or "IGNORE"
        sector = line.get("buy_market_gm_sector") or "IGNORE"
        operator = line.get("buy_market_operator") or "AND"
    else:
        current = getattr(line, "buy_market_gm_current", getattr(line, "buy_gm_filter", "IGNORE")) or "IGNORE"
        market = getattr(line, "buy_market_gm_market", "IGNORE") or "IGNORE"
        sector = getattr(line, "buy_market_gm_sector", "IGNORE") or "IGNORE"
        operator = getattr(line, "buy_market_operator", "AND") or "AND"
    parts = []
    if str(current).upper() != "IGNORE":
        parts.append(f"GM actuel: {gm_filter_business_label(current)}")
    if str(market).upper() != "IGNORE":
        parts.append(f"GM marché: {gm_filter_business_label(market)}")
    if str(sector).upper() != "IGNORE":
        parts.append(f"GM secteur: {gm_filter_business_label(sector)}")
    if not parts:
        return "Aucune"
    op_txt = " ET " if str(operator).upper() == "AND" else " OU "
    return op_txt.join(parts)
