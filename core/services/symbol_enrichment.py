from __future__ import annotations

import logging
from typing import Iterable

from core.models import Symbol
from core.services.provider_twelvedata import TwelveDataClient


logger = logging.getLogger(__name__)


ENRICHABLE_SYMBOL_FIELDS = (
    "name",
    "exchange",
    "country",
    "currency",
    "sector",
    "instrument_type",
)


def _clean_metadata_value(value) -> str:
    return str(value or "").strip()


def _symbol_label(symbol: Symbol) -> str:
    exchange = _clean_metadata_value(getattr(symbol, "exchange", ""))
    return f"{symbol.ticker}:{exchange}" if exchange else str(symbol.ticker)


def _should_update_field(*, field: str, current_value: str, incoming_value: str, only_missing: bool) -> bool:
    incoming_value = _clean_metadata_value(incoming_value)
    current_value = _clean_metadata_value(current_value)
    if not incoming_value:
        return False
    if only_missing:
        return not current_value
    if field == "exchange" and current_value and current_value.upper() != incoming_value.upper():
        return False
    return current_value != incoming_value


def _normalize_provider_metadata(payload) -> dict[str, str]:
    payload = payload if isinstance(payload, dict) else {}
    return {field: _clean_metadata_value(payload.get(field)) for field in ENRICHABLE_SYMBOL_FIELDS}


def enrich_symbols_metadata(
    symbols: Iterable[Symbol],
    *,
    only_missing: bool = True,
    dry_run: bool = False,
    provider: str = "twelvedata",
    progress_callback=None,
) -> dict:
    provider = str(provider or "twelvedata").strip().lower()
    if provider != "twelvedata":
        raise ValueError(f"Unsupported symbol metadata provider: {provider}")

    client = TwelveDataClient()
    symbol_list = list(symbols)
    totals = {
        "processed": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "errors": 0,
        "per_symbol": [],
    }

    for index, symbol in enumerate(symbol_list, start=1):
        totals["processed"] += 1
        label = _symbol_label(symbol)
        if progress_callback:
            progress_callback(index, len(symbol_list), symbol)

        detail = {
            "symbol": label,
            "updated_fields": [],
            "error": "",
            "skipped": False,
            "dry_run": bool(dry_run),
        }

        if not getattr(symbol, "ticker", ""):
            detail["skipped"] = True
            detail["error"] = "missing ticker"
            totals["skipped"] += 1
            totals["per_symbol"].append(detail)
            logger.info("[symbol-enrichment] skipped symbol=%s reason=%s", label, detail["error"])
            continue

        try:
            raw_metadata = client.fetch_symbol_metadata(symbol.ticker, exchange=symbol.exchange or "")
        except Exception as exc:
            detail["error"] = str(exc)
            totals["errors"] += 1
            totals["per_symbol"].append(detail)
            logger.warning("[symbol-enrichment] provider error symbol=%s error=%s", label, detail["error"])
            continue

        metadata = _normalize_provider_metadata(raw_metadata)
        updates = {}
        for field in ENRICHABLE_SYMBOL_FIELDS:
            current_value = _clean_metadata_value(getattr(symbol, field, ""))
            incoming_value = metadata[field]
            if _should_update_field(
                field=field,
                current_value=current_value,
                incoming_value=incoming_value,
                only_missing=only_missing,
            ):
                updates[field] = incoming_value

        if not updates:
            totals["unchanged"] += 1
            totals["per_symbol"].append(detail)
            logger.info("[symbol-enrichment] unchanged symbol=%s", label)
            continue

        detail["updated_fields"] = sorted(updates.keys())
        if dry_run:
            totals["updated"] += 1
            totals["per_symbol"].append(detail)
            logger.info(
                "[symbol-enrichment] dry-run symbol=%s updated_fields=%s",
                label,
                ",".join(detail["updated_fields"]),
            )
            continue

        try:
            for field, value in updates.items():
                setattr(symbol, field, value)
            symbol.save(update_fields=detail["updated_fields"])
        except Exception as exc:
            detail["updated_fields"] = []
            detail["error"] = str(exc)
            totals["errors"] += 1
            totals["per_symbol"].append(detail)
            logger.warning("[symbol-enrichment] save error symbol=%s error=%s", label, detail["error"])
            continue
        totals["updated"] += 1
        totals["per_symbol"].append(detail)
        logger.info(
            "[symbol-enrichment] updated symbol=%s updated_fields=%s",
            label,
            ",".join(detail["updated_fields"]),
        )

    logger.info(
        "[symbol-enrichment] summary processed=%s updated=%s unchanged=%s skipped=%s errors=%s dry_run=%s",
        totals["processed"],
        totals["updated"],
        totals["unchanged"],
        totals["skipped"],
        totals["errors"],
        int(bool(dry_run)),
    )
    return totals
