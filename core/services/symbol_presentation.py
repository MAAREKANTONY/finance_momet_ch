from __future__ import annotations


def _clean(value) -> str:
    return str(value or "").strip()


def symbol_display_name(*, ticker="", name="", name_en="") -> str:
    """Return the preferred human-readable name without changing technical keys."""
    return _clean(name_en) or _clean(name) or _clean(ticker)


def symbol_display_code(*, ticker="", exchange="") -> str:
    ticker_value = _clean(ticker)
    exchange_value = _clean(exchange)
    if ticker_value and exchange_value:
        return f"{ticker_value}.{exchange_value}"
    return ticker_value


def symbol_display_label(*, ticker="", exchange="", name="", name_en="") -> str:
    """Return ``CODE — NAME`` while avoiding a redundant ticker-only name."""
    code = symbol_display_code(ticker=ticker, exchange=exchange)
    preferred_name = _clean(name_en) or _clean(name)
    if not preferred_name or preferred_name.casefold() in {
        code.casefold(),
        _clean(ticker).casefold(),
    }:
        return code
    return f"{code} — {preferred_name}" if code else preferred_name


def symbol_front_payload(symbol) -> dict:
    """Serialize a Symbol for front-end pickers while retaining technical fields."""
    return {
        "id": symbol.id,
        "ticker": symbol.ticker,
        "name": symbol.name or "",
        "name_en": symbol.name_en or "",
        "display_name": symbol.display_name,
        "display_code": symbol.display_code,
        "display_label": symbol.display_label,
        "exchange": symbol.exchange or "",
        "sector": getattr(symbol, "sector", "") or "",
        "country": getattr(symbol, "country", "") or "",
    }
