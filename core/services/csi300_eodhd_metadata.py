from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from django.conf import settings
from core.models import Symbol, UniverseDefinition, UniverseMembership
from core.services.provider_eodhd import (
    EODHDClient,
    UnsupportedEODHDSymbolError,
    sanitize_provider_error_message,
    to_eodhd_symbol_from_parts,
)
from core.services.universe_resolver import CSI300_UNIVERSE_CODE


GENERIC_METADATA_VALUES = {"", "-", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "UNSPECIFIED"}
GENERIC_SECTOR_VALUES = {*GENERIC_METADATA_VALUES, "OTHER"}
APPLICABLE_FIELDS = ("name", "country", "currency", "sector")


@dataclass(frozen=True)
class CSI300MetadataCandidate:
    symbol: Symbol
    provider_symbol: str
    membership_id: int | None = None


@dataclass
class CSI300EODHDMetadataReport:
    dry_run: bool
    processed: int = 0
    fetched: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    errors: int = 0
    missing_sector: int = 0
    generic_sector: int = 0
    industries_present: int = 0
    field_updates: dict[str, int] = field(default_factory=dict)
    raw_sector_counts: dict[str, int] = field(default_factory=dict)
    applied_sector_counts: dict[str, int] = field(default_factory=dict)
    per_symbol: list[dict[str, Any]] = field(default_factory=list)


def normalize_eodhd_general_for_symbol(payload: dict[str, Any] | None) -> dict[str, str]:
    payload = payload if isinstance(payload, dict) else {}
    sector = _clean_value(payload.get("sector"))
    return {
        "name": _clean_value(payload.get("name")),
        "country": _clean_value(payload.get("country")),
        "currency": _clean_value(payload.get("currency")),
        "exchange": _clean_value(payload.get("exchange")),
        "sector": "" if _is_generic_sector(sector) else sector,
        "raw_sector": sector,
        "industry": _clean_value(payload.get("industry")),
    }


def enrich_csi300_symbols_from_eodhd_metadata(
    *,
    tickers: Iterable[str] | None = None,
    limit: int | None = None,
    dry_run: bool = True,
    client: EODHDClient | None = None,
    request_delay_seconds: float | None = None,
) -> CSI300EODHDMetadataReport:
    candidates = _metadata_candidates(tickers=tickers, limit=limit)
    report = CSI300EODHDMetadataReport(dry_run=bool(dry_run))
    client = client or EODHDClient()
    delay_seconds = float(
        request_delay_seconds
        if request_delay_seconds is not None
        else getattr(settings, "EODHD_REQUEST_DELAY_SECONDS", 0)
    )

    for index, candidate in enumerate(candidates):
        if index > 0 and delay_seconds > 0:
            time.sleep(delay_seconds)
        _process_candidate(candidate, report, client=client, dry_run=bool(dry_run))
    return report


def _metadata_candidates(*, tickers: Iterable[str] | None, limit: int | None) -> list[CSI300MetadataCandidate]:
    normalized_tickers = {str(item or "").strip().upper() for item in (tickers or []) if str(item or "").strip()}
    limit_value = int(limit or 0)
    try:
        universe = UniverseDefinition.objects.get(code=CSI300_UNIVERSE_CODE, active=True)
    except UniverseDefinition.DoesNotExist:
        return []

    qs = (
        UniverseMembership.objects.filter(universe=universe)
        .select_related("symbol")
        .exclude(symbol__isnull=True)
        .order_by("ticker", "exchange", "valid_from", "id")
    )
    if normalized_tickers:
        qs = qs.filter(ticker__in=normalized_tickers)

    candidates_by_symbol_id: dict[int, CSI300MetadataCandidate] = {}
    for membership in qs:
        if membership.symbol_id in candidates_by_symbol_id:
            continue
        provider_symbol = _provider_symbol_for_membership(membership)
        if not provider_symbol:
            continue
        candidates_by_symbol_id[membership.symbol_id] = CSI300MetadataCandidate(
            symbol=membership.symbol,
            provider_symbol=provider_symbol,
            membership_id=membership.id,
        )
        if limit_value > 0 and len(candidates_by_symbol_id) >= limit_value:
            break
    return list(candidates_by_symbol_id.values())


def _provider_symbol_for_membership(membership: UniverseMembership) -> str:
    explicit = str(membership.provider_symbol or "").strip().upper()
    if explicit:
        return explicit
    try:
        return to_eodhd_symbol_from_parts(ticker=membership.ticker, exchange=membership.exchange)
    except UnsupportedEODHDSymbolError:
        return ""


def _process_candidate(
    candidate: CSI300MetadataCandidate,
    report: CSI300EODHDMetadataReport,
    *,
    client: EODHDClient,
    dry_run: bool,
) -> None:
    symbol = candidate.symbol
    label = f"{symbol.ticker}:{symbol.exchange}" if symbol.exchange else symbol.ticker
    detail = {
        "symbol": label,
        "provider_symbol": candidate.provider_symbol,
        "updated_fields": [],
        "error": "",
        "sector": "",
        "raw_sector": "",
        "industry_present": False,
        "dry_run": bool(dry_run),
    }
    report.processed += 1

    try:
        payload = client.fetch_symbol_general_metadata(candidate.provider_symbol)
        report.fetched += 1
    except Exception as exc:
        detail["error"] = sanitize_provider_error_message(exc)
        report.errors += 1
        report.per_symbol.append(detail)
        return

    metadata = normalize_eodhd_general_for_symbol(payload)
    detail["sector"] = metadata["sector"]
    detail["raw_sector"] = metadata["raw_sector"]
    detail["industry_present"] = bool(metadata["industry"])
    if metadata["industry"]:
        report.industries_present += 1
    if metadata["raw_sector"]:
        report.raw_sector_counts[metadata["raw_sector"]] = report.raw_sector_counts.get(metadata["raw_sector"], 0) + 1
    if metadata["raw_sector"] and not metadata["sector"]:
        report.generic_sector += 1
    if not metadata["raw_sector"]:
        report.missing_sector += 1

    updates = _symbol_updates(symbol, metadata)
    if not updates:
        report.unchanged += 1
        report.per_symbol.append(detail)
        return

    report.updated += 1
    detail["updated_fields"] = sorted(updates)
    for field in updates:
        report.field_updates[field] = report.field_updates.get(field, 0) + 1
    if "sector" in updates:
        sector = updates["sector"]
        report.applied_sector_counts[sector] = report.applied_sector_counts.get(sector, 0) + 1
    if not dry_run:
        for field, value in updates.items():
            setattr(symbol, field, value)
        symbol.save(update_fields=detail["updated_fields"])
    report.per_symbol.append(detail)


def _symbol_updates(symbol: Symbol, metadata: dict[str, str]) -> dict[str, str]:
    updates: dict[str, str] = {}
    for field in APPLICABLE_FIELDS:
        incoming = _clean_value(metadata.get(field))
        if not incoming:
            continue
        current = _clean_value(getattr(symbol, field, ""))
        if not current or current.upper() in GENERIC_METADATA_VALUES:
            updates[field] = incoming
    return updates


def _clean_value(value: Any) -> str:
    return str(value or "").strip()


def _is_generic_sector(value: str) -> bool:
    return _clean_value(value).upper() in GENERIC_SECTOR_VALUES


def format_csi300_eodhd_metadata_summary(report: CSI300EODHDMetadataReport) -> str:
    mode = "dry-run" if report.dry_run else "apply"
    return (
        f"EODHD CSI300 metadata ({mode}) — "
        f"processed={report.processed}, "
        f"fetched={report.fetched}, "
        f"updated={report.updated}, "
        f"unchanged={report.unchanged}, "
        f"skipped={report.skipped}, "
        f"errors={report.errors}, "
        f"missing_sector={report.missing_sector}, "
        f"generic_sector={report.generic_sector}, "
        f"industries_present={report.industries_present}."
    )
