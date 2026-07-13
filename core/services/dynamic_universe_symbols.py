from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from core.models import (
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.provider_eodhd import UnsupportedEODHDSymbolError, to_eodhd_symbol_from_parts
from core.services.universe_resolver import CSI300_UNIVERSE_CODE


class UniverseSymbolMappingError(RuntimeError):
    pass


@dataclass
class UniverseSymbolMappingReport:
    universe_code: str
    dry_run: bool = False
    memberships_total: int = 0
    distinct_symbols: int = 0
    already_mapped: int = 0
    linked_existing_symbols: int = 0
    created_symbols: int = 0
    still_unmapped: int = 0
    provider_symbols_created: int = 0
    metadata_symbols_analyzed: int = 0
    metadata_symbols_updated: int = 0
    metadata_symbols_unchanged: int = 0
    metadata_no_reliable_source: int = 0
    metadata_industries_available: int = 0
    metadata_fields_updated: dict[str, int] = field(default_factory=dict)
    sector_counts: dict[str, int] = field(default_factory=dict)
    unsupported_exchanges: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    provider_symbol_conflicts: list[str] = field(default_factory=list)
    metadata_conflicts: list[str] = field(default_factory=list)
    coverage_batches_updated: int = 0
    coverage_snapshots_updated: int = 0


def format_universe_symbol_mapping_summary(result: UniverseSymbolMappingReport) -> str:
    mode = "dry-run" if result.dry_run else "apply"
    unsupported = len(result.unsupported_exchanges)
    return (
        f"Mapping symbols univers historique ({mode}) — "
        f"univers={result.universe_code}, "
        f"memberships={result.memberships_total}, "
        f"déjà mappés={result.already_mapped}, "
        f"liés existants={result.linked_existing_symbols}, "
        f"symbols créés={result.created_symbols}, "
        f"encore non mappés={result.still_unmapped}, "
        f"provider_symbols créés={result.provider_symbols_created}, "
        f"symbols metadata analysés={result.metadata_symbols_analyzed}, "
        f"symbols metadata mis à jour={result.metadata_symbols_updated}, "
        f"symbols metadata inchangés={result.metadata_symbols_unchanged}, "
        f"sans source fiable={result.metadata_no_reliable_source}, "
        f"industries source disponibles={result.metadata_industries_available}, "
        f"exchanges non supportés={unsupported}, "
        f"batches recalculés={result.coverage_batches_updated}, "
        f"snapshots recalculés={result.coverage_snapshots_updated}."
    )


SUPPORTED_MEMBERSHIP_EXCHANGES_BY_UNIVERSE = {
    CSI300_UNIVERSE_CODE: {"SHG", "SHE", "XSHG", "XSHE"},
}
SYMBOL_METADATA_FIELDS_FROM_MEMBERSHIP = ("name", "country", "currency", "sector")
GENERIC_METADATA_VALUES = {"", "-", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "UNSPECIFIED"}


def ensure_universe_membership_symbols(
    universe_code: str,
    *,
    create_missing: bool = True,
    dry_run: bool = False,
    enrich_metadata: bool = False,
) -> UniverseSymbolMappingReport:
    code = str(universe_code or "").strip().upper()
    if not code:
        raise UniverseSymbolMappingError("universe_code is required.")
    try:
        universe = UniverseDefinition.objects.get(code=code, active=True)
    except UniverseDefinition.DoesNotExist as exc:
        raise UniverseSymbolMappingError(f"UniverseDefinition {code} is missing or inactive.") from exc

    report = UniverseSymbolMappingReport(universe_code=code, dry_run=bool(dry_run))
    memberships = list(universe.memberships.select_related("symbol").order_by("ticker", "exchange", "valid_from", "id"))
    report.memberships_total = len(memberships)
    report.distinct_symbols = len({(membership.ticker, membership.exchange) for membership in memberships})
    _record_membership_conflicts(report, memberships)

    with transaction.atomic():
        for membership in memberships:
            _ensure_membership_provider_symbol(code, membership, report, dry_run=dry_run)
            if membership.symbol_id:
                report.already_mapped += 1
                if enrich_metadata:
                    _enrich_symbol_from_membership(
                        code,
                        membership.symbol,
                        membership,
                        report,
                        dry_run=dry_run,
                    )
                continue
            if not _membership_exchange_supported(code, membership.exchange):
                value = f"{membership.ticker}:{membership.exchange or '—'}"
                report.unsupported_exchanges.append(value)
                report.warnings.append(f"exchange non supporté pour {value}")
                report.still_unmapped += 1
                continue

            symbol, warning = _find_existing_symbol(membership)
            if warning:
                report.warnings.append(warning)
                report.still_unmapped += 1
                continue
            if symbol is not None:
                report.linked_existing_symbols += 1
                if enrich_metadata:
                    _enrich_symbol_from_membership(code, symbol, membership, report, dry_run=dry_run)
                if not dry_run:
                    membership.symbol = symbol
                    membership.save(update_fields=["symbol", "updated_at"])
                continue

            if not create_missing:
                report.still_unmapped += 1
                continue

            report.created_symbols += 1
            if enrich_metadata:
                _record_created_symbol_metadata(code, membership, report)
            if not dry_run:
                symbol = _create_symbol_from_membership(code, membership)
                membership.symbol = symbol
                membership.save(update_fields=["symbol", "updated_at"])

        if not dry_run:
            coverage = _refresh_universe_coverage(universe)
            report.coverage_batches_updated = coverage["batches_updated"]
            report.coverage_snapshots_updated = coverage["snapshots_updated"]

    return report


def _membership_exchange_supported(universe_code: str, exchange: str) -> bool:
    supported = SUPPORTED_MEMBERSHIP_EXCHANGES_BY_UNIVERSE.get(universe_code)
    if not supported:
        return True
    return str(exchange or "").strip().upper() in supported


def _find_existing_symbol(membership: UniverseMembership) -> tuple[Symbol | None, str]:
    qs = Symbol.objects.filter(ticker=membership.ticker)
    if membership.exchange:
        qs = qs.filter(exchange__iexact=membership.exchange)
    count = qs.count()
    if count == 1:
        return qs.get(), ""
    if count == 0:
        return None, ""
    return None, f"symbol ambigu pour {membership.ticker}:{membership.exchange or '—'}"


def _create_symbol_from_membership(universe_code: str, membership: UniverseMembership) -> Symbol:
    metadata = _metadata_from_membership(universe_code, membership)
    return Symbol.objects.create(
        ticker=str(membership.ticker or ""),
        exchange=str(membership.exchange or ""),
        name=metadata["name"],
        country=metadata["country"],
        currency=metadata["currency"],
        sector=metadata["sector"],
        active=True,
    )


def _ensure_membership_provider_symbol(
    universe_code: str,
    membership: UniverseMembership,
    report: UniverseSymbolMappingReport,
    *,
    dry_run: bool,
) -> None:
    if universe_code != CSI300_UNIVERSE_CODE:
        return
    if not _membership_exchange_supported(universe_code, membership.exchange):
        return
    try:
        expected = to_eodhd_symbol_from_parts(ticker=membership.ticker, exchange=membership.exchange)
    except UnsupportedEODHDSymbolError as exc:
        report.warnings.append(f"provider_symbol non calculable pour {membership.ticker}:{membership.exchange}: {exc}")
        return
    current = str(membership.provider_symbol or "").strip().upper()
    if current:
        if current != expected:
            message = f"{membership.ticker}:{membership.exchange} provider_symbol={membership.provider_symbol} attendu={expected}"
            if message not in report.provider_symbol_conflicts:
                report.provider_symbol_conflicts.append(message)
        return
    report.provider_symbols_created += 1
    if not dry_run:
        membership.provider_symbol = expected
        membership.save(update_fields=["provider_symbol", "updated_at"])


def _metadata_from_membership(universe_code: str, membership: UniverseMembership) -> dict[str, str]:
    payload = membership.source_payload or {}
    metadata = {
        "name": _clean_reliable_metadata_value(_source_payload_value(payload, "company_name", "name")),
        "country": _clean_reliable_metadata_value(_source_payload_value(payload, "country")),
        "currency": _clean_reliable_metadata_value(_source_payload_value(payload, "currency")),
        "sector": _clean_reliable_metadata_value(_source_payload_value(payload, "sector")),
        "industry": _clean_reliable_metadata_value(_source_payload_value(payload, "industry")),
    }
    if universe_code == CSI300_UNIVERSE_CODE and _membership_exchange_supported(universe_code, membership.exchange):
        if not metadata["country"]:
            metadata["country"] = "CN"
        if not metadata["currency"]:
            metadata["currency"] = "CNY"
    return metadata


def _record_created_symbol_metadata(
    universe_code: str,
    membership: UniverseMembership,
    report: UniverseSymbolMappingReport,
) -> None:
    metadata = _metadata_from_membership(universe_code, membership)
    reliable_fields = {
        field: value
        for field, value in metadata.items()
        if field in SYMBOL_METADATA_FIELDS_FROM_MEMBERSHIP and value
    }
    if metadata.get("industry"):
        report.metadata_industries_available += 1
    if metadata.get("sector"):
        report.sector_counts[metadata["sector"]] = report.sector_counts.get(metadata["sector"], 0) + 1
    if not reliable_fields:
        report.metadata_no_reliable_source += 1
        return
    report.metadata_symbols_analyzed += 1
    report.metadata_symbols_updated += 1
    for field in reliable_fields:
        report.metadata_fields_updated[field] = report.metadata_fields_updated.get(field, 0) + 1


def _enrich_symbol_from_membership(
    universe_code: str,
    symbol: Symbol,
    membership: UniverseMembership,
    report: UniverseSymbolMappingReport,
    *,
    dry_run: bool,
) -> None:
    metadata = _metadata_from_membership(universe_code, membership)
    reliable_fields = {
        field: value
        for field, value in metadata.items()
        if field in SYMBOL_METADATA_FIELDS_FROM_MEMBERSHIP and value
    }
    if metadata.get("industry"):
        report.metadata_industries_available += 1
    if metadata.get("sector"):
        report.sector_counts[metadata["sector"]] = report.sector_counts.get(metadata["sector"], 0) + 1
    if not reliable_fields:
        report.metadata_no_reliable_source += 1
        return

    report.metadata_symbols_analyzed += 1
    updates = {}
    for field, incoming in reliable_fields.items():
        current = str(getattr(symbol, field, "") or "").strip()
        if _should_update_symbol_metadata(current, incoming):
            updates[field] = incoming

    if not updates:
        report.metadata_symbols_unchanged += 1
        return

    report.metadata_symbols_updated += 1
    for field in updates:
        report.metadata_fields_updated[field] = report.metadata_fields_updated.get(field, 0) + 1
    if dry_run:
        return
    for field, value in updates.items():
        setattr(symbol, field, value)
    symbol.save(update_fields=sorted(updates))


def _record_membership_conflicts(
    report: UniverseSymbolMappingReport,
    memberships: list[UniverseMembership],
) -> None:
    provider_symbols_by_key: dict[tuple[str, str], set[str]] = {}
    symbol_ids_by_key: dict[tuple[str, str], set[int]] = {}
    for membership in memberships:
        key = (membership.ticker, membership.exchange)
        provider_symbol = str(membership.provider_symbol or "").strip().upper()
        if provider_symbol:
            provider_symbols_by_key.setdefault(key, set()).add(provider_symbol)
        if membership.symbol_id:
            symbol_ids_by_key.setdefault(key, set()).add(membership.symbol_id)
    for key, provider_symbols in sorted(provider_symbols_by_key.items()):
        if len(provider_symbols) > 1:
            report.provider_symbol_conflicts.append(
                f"{key[0]}:{key[1] or '—'} provider_symbols multiples={','.join(sorted(provider_symbols))}"
            )
    for key, symbol_ids in sorted(symbol_ids_by_key.items()):
        if len(symbol_ids) > 1:
            report.metadata_conflicts.append(
                f"{key[0]}:{key[1] or '—'} symbol_ids multiples={','.join(str(item) for item in sorted(symbol_ids))}"
            )


def _clean_reliable_metadata_value(value: str) -> str:
    value = str(value or "").strip()
    return "" if value.upper() in GENERIC_METADATA_VALUES else value


def _should_update_symbol_metadata(current_value: str, incoming_value: str) -> bool:
    incoming = _clean_reliable_metadata_value(incoming_value)
    if not incoming:
        return False
    current = str(current_value or "").strip()
    return not current or current.upper() in GENERIC_METADATA_VALUES


def _source_payload_value(payload: dict[str, Any], *keys: str) -> str:
    extras = payload.get("extras") if isinstance(payload.get("extras"), dict) else {}
    row = payload.get("row") if isinstance(payload.get("row"), dict) else {}
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value).strip()
        value = extras.get(key)
        if value not in (None, ""):
            return str(value).strip()
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _refresh_universe_coverage(universe: UniverseDefinition) -> dict[str, int]:
    batches_updated = 0
    snapshots_updated = 0
    memberships = UniverseMembership.objects.filter(universe=universe)
    for batch in UniverseImportBatch.objects.filter(universe=universe).order_by("id"):
        batch_summary = _refresh_batch_coverage(batch, memberships)
        snapshots_updated += batch_summary["snapshots_updated"]
        batches_updated += 1
    return {"batches_updated": batches_updated, "snapshots_updated": snapshots_updated}


def _refresh_batch_coverage(batch: UniverseImportBatch, memberships_qs) -> dict[str, int]:
    max_actual = max_mapped = max_unmapped = 0
    batch_status = UniverseCoverageStatus.VALIDATED
    snapshots_updated = 0
    current = batch.period_start
    while current <= batch.period_end:
        active = memberships_qs.filter(
            valid_from__lte=current,
        ).filter(Q(valid_to__isnull=True) | Q(valid_to__gte=current))
        actual = active.count()
        mapped = active.exclude(symbol__isnull=True).count()
        unmapped = actual - mapped
        status = (
            UniverseCoverageStatus.VALIDATED
            if actual >= batch.expected_member_count and mapped >= actual and unmapped == 0
            else UniverseCoverageStatus.PARTIAL
        )
        if status != UniverseCoverageStatus.VALIDATED:
            batch_status = UniverseCoverageStatus.PARTIAL
        max_actual = max(max_actual, actual)
        max_mapped = max(max_mapped, mapped)
        max_unmapped = max(max_unmapped, unmapped)
        UniverseCoverageSnapshot.objects.update_or_create(
            universe=batch.universe,
            coverage_date=current,
            defaults={
                "import_batch": batch,
                "expected_member_count": batch.expected_member_count,
                "actual_member_count": actual,
                "mapped_member_count": mapped,
                "unmapped_member_count": unmapped,
                "status": status,
                "metadata": {
                    **(batch.metadata or {}),
                    "symbol_mapping_refreshed": True,
                },
            },
        )
        snapshots_updated += 1
        current += timedelta(days=1)

    batch.imported_member_count = max_actual
    batch.mapped_member_count = max_mapped
    batch.unmapped_member_count = max_unmapped
    batch.status = batch_status
    batch.validated_at = timezone.now() if batch_status == UniverseCoverageStatus.VALIDATED else None
    metadata = dict(batch.metadata or {})
    metadata["symbol_mapping_refreshed"] = True
    batch.metadata = metadata
    batch.save(update_fields=[
        "imported_member_count",
        "mapped_member_count",
        "unmapped_member_count",
        "status",
        "validated_at",
        "metadata",
        "updated_at",
    ])
    return {"snapshots_updated": snapshots_updated}
