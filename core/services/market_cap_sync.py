from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from django.conf import settings

from core.models import HistoricalMarketCap, Symbol
from core.services.provider_eodhd import EODHDClient, UnsupportedEODHDSymbolError, to_eodhd_symbol


@dataclass
class MarketCapSyncSymbolResult:
    symbol: str
    provider_symbol: str
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    existing: int = 0
    skipped: bool = False
    error: str = ""


def sync_market_caps_for_symbols(
    symbols,
    from_date: date,
    to_date: date,
    provider: str = "eodhd",
    dry_run: bool = False,
    request_delay_seconds: float | None = None,
    progress_callback=None,
):
    symbol_list = _normalize_symbols(symbols)
    stats = {
        "fetched": 0,
        "inserted": 0,
        "updated": 0,
        "existing": 0,
        "skipped": 0,
        "errors": 0,
        "per_symbol": [],
    }
    if provider != "eodhd":
        raise ValueError(f"Unsupported market cap provider: {provider}")

    client = EODHDClient()
    delay_seconds = float(
        request_delay_seconds
        if request_delay_seconds is not None
        else getattr(settings, "EODHD_REQUEST_DELAY_SECONDS", 0)
    )

    for index, symbol in enumerate(symbol_list):
        if index > 0 and delay_seconds > 0:
            time.sleep(delay_seconds)

        result = MarketCapSyncSymbolResult(symbol=str(symbol), provider_symbol="")
        try:
            provider_symbol = to_eodhd_symbol(symbol)
            result.provider_symbol = provider_symbol
            _emit_progress(progress_callback, f"{symbol}: fetch {provider_symbol} {from_date}..{to_date}")
            rows = client.fetch_historical_market_cap(provider_symbol, from_date, to_date)
            result.fetched = len(rows)
            stats["fetched"] += result.fetched
            if not dry_run:
                inserted, updated, existing = _upsert_rows(symbol, provider, rows)
                result.inserted = inserted
                result.updated = updated
                result.existing = existing
                stats["inserted"] += inserted
                stats["updated"] += updated
                stats["existing"] += existing
        except UnsupportedEODHDSymbolError as exc:
            result.skipped = True
            result.error = str(exc)
            stats["skipped"] += 1
            _emit_progress(progress_callback, f"{symbol}: skipped {exc}")
        except Exception as exc:
            result.error = str(exc)
            stats["errors"] += 1
            _emit_progress(progress_callback, f"{symbol}: error {exc}")
        stats["per_symbol"].append({
            "symbol": result.symbol,
            "provider_symbol": result.provider_symbol,
            "fetched": result.fetched,
            "inserted": result.inserted,
            "updated": result.updated,
            "existing": result.existing,
            "skipped": result.skipped,
            "error": result.error,
            "dry_run": bool(dry_run),
        })

    return stats


def _normalize_symbols(symbols) -> list[Symbol]:
    if symbols is None:
        return []
    if hasattr(symbols, "all"):
        return list(symbols.all())
    if isinstance(symbols, Iterable) and not isinstance(symbols, (str, bytes)):
        return list(symbols)
    return [symbols]


def _upsert_rows(symbol: Symbol, provider: str, rows: list[dict]) -> tuple[int, int, int]:
    inserted = 0
    updated = 0
    existing = 0
    for row in rows:
        defaults = {
            "market_cap": row["market_cap"],
            "currency": row.get("currency", ""),
            "provider_symbol": row.get("provider_symbol", ""),
            "source_payload": row.get("source_payload"),
        }
        obj, created = HistoricalMarketCap.objects.get_or_create(
            provider=provider,
            symbol=symbol,
            date=row["date"],
            defaults=defaults,
        )
        if created:
            inserted += 1
        elif any(getattr(obj, field) != value for field, value in defaults.items()):
            for field, value in defaults.items():
                setattr(obj, field, value)
            obj.save(update_fields=[*defaults.keys(), "updated_at"])
            updated += 1
        else:
            existing += 1
    return inserted, updated, existing


def _emit_progress(progress_callback, message: str) -> None:
    if progress_callback is not None:
        progress_callback(message)
