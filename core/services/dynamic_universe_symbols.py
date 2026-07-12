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
from core.services.universe_resolver import CSI300_UNIVERSE_CODE


class UniverseSymbolMappingError(RuntimeError):
    pass


@dataclass
class UniverseSymbolMappingReport:
    universe_code: str
    dry_run: bool = False
    memberships_total: int = 0
    already_mapped: int = 0
    linked_existing_symbols: int = 0
    created_symbols: int = 0
    still_unmapped: int = 0
    unsupported_exchanges: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
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
        f"exchanges non supportés={unsupported}, "
        f"batches recalculés={result.coverage_batches_updated}, "
        f"snapshots recalculés={result.coverage_snapshots_updated}."
    )


SUPPORTED_MEMBERSHIP_EXCHANGES_BY_UNIVERSE = {
    CSI300_UNIVERSE_CODE: {"SHG", "SHE", "XSHG", "XSHE"},
}


def ensure_universe_membership_symbols(
    universe_code: str,
    *,
    create_missing: bool = True,
    dry_run: bool = False,
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

    with transaction.atomic():
        for membership in memberships:
            if membership.symbol_id:
                report.already_mapped += 1
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
                if not dry_run:
                    membership.symbol = symbol
                    membership.save(update_fields=["symbol", "updated_at"])
                continue

            if not create_missing:
                report.still_unmapped += 1
                continue

            report.created_symbols += 1
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
    payload = membership.source_payload or {}
    return Symbol.objects.create(
        ticker=str(membership.ticker or ""),
        exchange=str(membership.exchange or ""),
        name=_source_payload_value(payload, "company_name", "name"),
        country=_source_payload_value(payload, "country"),
        currency=_source_payload_value(payload, "currency"),
        sector=_source_payload_value(payload, "sector"),
        active=True,
    )


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
