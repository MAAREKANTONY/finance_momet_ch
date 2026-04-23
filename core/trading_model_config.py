from __future__ import annotations

from typing import Any


TRADING_MODEL_LEGACY_DAILY = "LEGACY_DAILY"
TRADING_MODEL_LATCH_STATEFUL = "LATCH_STATEFUL"
TRADING_MODEL_CHOICES = {TRADING_MODEL_LEGACY_DAILY, TRADING_MODEL_LATCH_STATEFUL}

SPECIAL_SELL_K1F_UPPER_DOWN_B1F = "AUTO_K1F_UPPER_DOWN_B1F"

SIGNAL_LATCH_STATE_PAIRS: tuple[tuple[str, str], ...] = (
    ("A1", "B1"),
    ("C1", "D1"),
    ("E1", "F1"),
    ("G1", "H1"),
    ("AF", "BF"),
    ("SPA", "SPV"),
    ("SPVA", "SPVV"),
    ("SPA_BASSE", "SPV_BASSE"),
    ("SPVA_BASSE", "SPVV_BASSE"),
)

SIGNAL_LATCH_INVALIDATORS: dict[str, str] = {
    positive: negative for positive, negative in SIGNAL_LATCH_STATE_PAIRS
}


def normalize_trading_model(value: Any) -> str | None:
    if value in (None, ""):
        return None
    model = str(value).strip().upper()
    if model not in TRADING_MODEL_CHOICES:
        raise ValueError(f"Unsupported trading_model: {value}")
    return model


def normalize_model_codes(codes: Any) -> list[str]:
    if codes in (None, ""):
        return []
    if isinstance(codes, str):
        raw_items = codes.split(",")
    elif isinstance(codes, (list, tuple, set)):
        raw_items = []
        for item in codes:
            if isinstance(item, str) and "," in item:
                raw_items.extend(item.split(","))
            else:
                raw_items.append(item)
    else:
        raw_items = [codes]

    out: list[str] = []
    for raw in raw_items:
        if raw in (None, ""):
            continue
        code = str(raw).strip().upper()
        if code and code not in out:
            out.append(code)
    return out


def can_use_latch_model(buy_codes: Any) -> bool:
    normalized = normalize_model_codes(buy_codes)
    return bool(normalized) and all(code in SIGNAL_LATCH_INVALIDATORS for code in normalized)


def infer_trading_model(buy_codes: Any) -> str:
    if can_use_latch_model(buy_codes):
        return TRADING_MODEL_LATCH_STATEFUL
    return TRADING_MODEL_LEGACY_DAILY


def resolve_trading_model(value: Any, buy_codes: Any) -> tuple[str, bool]:
    explicit = value not in (None, "")
    if explicit:
        model = normalize_trading_model(value)
        if model is None:
            raise ValueError("trading_model must not be blank when provided")
        return model, True
    return infer_trading_model(buy_codes), False


def validate_explicit_latch_config(
    *,
    buy_codes: Any,
    buy_logic: str,
    sell_codes: Any,
    sell_gm_filter: str,
) -> None:
    normalized_buy = normalize_model_codes(buy_codes)
    unsupported = [code for code in normalized_buy if code not in SIGNAL_LATCH_INVALIDATORS]
    if not normalized_buy:
        raise ValueError("LATCH_STATEFUL requires at least one buy signal")
    if unsupported:
        raise ValueError(f"LATCH_STATEFUL unsupported buy signal(s): {', '.join(unsupported)}")
    if str(buy_logic or "AND").strip().upper() == "OR":
        raise ValueError("LATCH_STATEFUL does not support buy_logic=OR")

    normalized_sell = normalize_model_codes(sell_codes)
    allowed_special = [SPECIAL_SELL_K1F_UPPER_DOWN_B1F]
    if normalized_sell and normalized_sell != allowed_special:
        raise ValueError("LATCH_STATEFUL does not support explicit sell signals")
    if str(sell_gm_filter or "IGNORE").strip().upper() != "IGNORE":
        raise ValueError("LATCH_STATEFUL does not support sell_gm_filter")
