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

GM_CONDITION_MODE_LABELS = {
    "IGNORE": "Ignorer",
    "GM_POS": "GM positif",
    "GM_NEG": "GM négatif",
    "GM_NEU": "GM neutre",
    "GM_POS_OR_NEU": "GM positif ou neutre",
    "GM_NEG_OR_NEU": "GM négatif ou neutre",
    "POS": "GM positif",
    "NEG": "GM négatif",
    "NEU": "GM neutre",
    "POS_OR_NEU": "GM positif ou neutre",
    "NEG_OR_NEU": "GM négatif ou neutre",
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


def _gm_condition_label(entry) -> str:
    entry = entry if isinstance(entry, dict) else {}
    mode = str(entry.get("mode") or "IGNORE").upper()
    label = GM_CONDITION_MODE_LABELS.get(mode, mode)
    threshold = entry.get("threshold")
    if entry.get("explicit_threshold") and threshold not in (None, ""):
        buy_max = entry.get("buy_max_threshold") if mode in {"GM_POS", "POS"} else None
        if mode in {"GM_POS", "POS"}:
            if buy_max not in (None, ""):
                return f"{label} > {threshold}, achat bloqué > {buy_max}"
            return f"{label} > {threshold}"
        if mode in {"GM_NEG", "NEG"}:
            return f"{label} < {threshold}"
        return f"{label} seuil {threshold}"
    return label


def _gm_conditions_display(config) -> str:
    config = config if isinstance(config, dict) else {}
    parts = []
    for key, label in (("current", "GM actuel"), ("market", "GM marché"), ("sector", "GM secteur")):
        entry = config.get(key) if isinstance(config.get(key), dict) else {}
        mode = str((entry or {}).get("mode") or "IGNORE").upper()
        if mode != "IGNORE":
            parts.append(f"{label}: {_gm_condition_label(entry)}")
    if not parts:
        return "Aucune"
    op_txt = " ET " if str(config.get("operator") or "AND").upper() == "AND" else " OU "
    return op_txt.join(parts)


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


@register.simple_tag
def line_gm_sell_market_exit_display(line) -> str:
    config = line.get("gm_sell_market_exit_conditions") if isinstance(line, dict) else getattr(line, "gm_sell_market_exit_conditions", None)
    return _gm_conditions_display(config)


def _gm_push_conditions_display(config) -> str:
    if not isinstance(config, dict):
        return "Aucune"
    parts = []
    for key, label in (("current", "GM_push actuel"), ("market", "GM_push marché"), ("sector", "GM_push secteur")):
        entry = config.get(key) if isinstance(config.get(key), dict) else {}
        mode = str((entry or {}).get("mode") or "IGNORE").upper()
        if mode == "IGNORE":
            continue
        mode_label = {"POS": "impulsion positive", "NEG": "impulsion négative"}.get(mode, mode)
        threshold = entry.get("buy_threshold") if mode == "POS" else entry.get("sell_threshold")
        if threshold not in (None, ""):
            op = ">" if mode == "POS" else "<"
            buy_max = entry.get("buy_max_threshold") if mode == "POS" else None
            if buy_max not in (None, ""):
                mode_label = f"{mode_label} {op} {threshold}, achat bloqué > {buy_max}"
            else:
                mode_label = f"{mode_label} {op} {threshold}"
        parts.append(f"{label}: {mode_label}")
    if not parts:
        return "Aucune"
    op_txt = " ET " if str(config.get("operator") or "AND").upper() == "AND" else " OU "
    return op_txt.join(parts)


@register.simple_tag
def line_gm_push_buy_display(line) -> str:
    config = line.get("gm_push_buy_conditions") if isinstance(line, dict) else getattr(line, "gm_push_buy_conditions", None)
    return _gm_push_conditions_display(config)


@register.simple_tag
def line_gm_push_sell_market_exit_display(line) -> str:
    config = line.get("gm_push_sell_market_exit_conditions") if isinstance(line, dict) else getattr(line, "gm_push_sell_market_exit_conditions", None)
    return _gm_push_conditions_display(config)
