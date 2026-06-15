from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from django.db import transaction
from django.utils import timezone

from core.models import (
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.provider_eodhd import EODHDClient, EODHDError
from core.services.universe_import import SUPPORTED_UNIVERSE_CODE, UniverseImportError

SOURCE_NAME = "eodhd_fundamentals"
PROVIDER = "eodhd"


@dataclass(frozen=True)
class EODHDMembershipRow:
    row_number: int
    provider_code: str
    ticker: str
    exchange: str
    provider_symbol: str
    valid_from: date
    valid_to: date | None
    company_name: str
    source_payload: dict[str, Any]


@dataclass
class EODHDSyncResult:
    dry_run: bool
    universe_code: str
    period_start: date
    period_end: date
    provider_records: int = 0
    records_retained: int = 0
    records_skipped: int = 0
    memberships_created: int = 0
    memberships_updated: int = 0
    imported_member_count: int = 0
    mapped_member_count: int = 0
    unmapped_member_count: int = 0
    coverage_days: int = 0
    status: str = UniverseCoverageStatus.IMPORTED
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    batch_id: int | None = None


def sync_sp500_historical_memberships_from_eodhd(
    *,
    coverage_start: date | str,
    coverage_end: date | str,
    expected_member_count: int = 500,
    dry_run: bool = True,
    client: EODHDClient | None = None,
) -> EODHDSyncResult:
    start = _parse_required_date(coverage_start, "coverage_start")
    end = _parse_required_date(coverage_end, "coverage_end")
    if end < start:
        raise UniverseImportError("coverage_end must be greater than or equal to coverage_start.")
    if expected_member_count < 1:
        raise UniverseImportError("expected_member_count must be greater than zero.")

    client = client or EODHDClient()
    try:
        provider_records = client.fetch_sp500_historical_components()
    except EODHDError:
        raise
    except Exception as exc:
        raise EODHDError(str(exc)) from exc

    result = EODHDSyncResult(
        dry_run=bool(dry_run),
        universe_code=SUPPORTED_UNIVERSE_CODE,
        period_start=start,
        period_end=end,
        provider_records=len(provider_records),
    )
    rows = _build_membership_rows(provider_records, start, end, result)
    result.records_retained = len(rows)
    result.records_skipped = max(result.provider_records - result.records_retained, 0)

    if not rows:
        result.status = UniverseCoverageStatus.FAILED
        result.errors.append("EODHD returned no S&P500 membership rows intersecting the requested period.")
        if dry_run:
            return result
        raise UniverseImportError(result.errors[-1])

    universe = UniverseDefinition.objects.filter(code=SUPPORTED_UNIVERSE_CODE).first()
    if universe is None and dry_run:
        result.warnings.append(f"UniverseDefinition {SUPPORTED_UNIVERSE_CODE} would be created.")

    mapped_by_row, mapping_errors = _resolve_symbols(rows)
    for row_number, message in mapping_errors.items():
        result.warnings.append(f"row {row_number}: {message}")

    summary = _coverage_summary(
        rows=rows,
        symbols_by_row=mapped_by_row,
        coverage_start=start,
        coverage_end=end,
        expected_member_count=expected_member_count,
        force_partial=bool(mapping_errors),
    )
    result.imported_member_count = summary["max_actual"]
    result.mapped_member_count = summary["max_mapped"]
    result.unmapped_member_count = summary["max_unmapped"]
    result.coverage_days = len(summary["snapshots"])
    result.status = summary["batch_status"]

    if dry_run:
        return result

    with transaction.atomic():
        if universe is None:
            universe = UniverseDefinition.objects.create(
                code=SUPPORTED_UNIVERSE_CODE,
                name="S&P 500",
                source=SOURCE_NAME,
                active=True,
                metadata={"provider": PROVIDER, "source": SOURCE_NAME},
            )
        elif universe.source != SOURCE_NAME:
            universe.source = SOURCE_NAME
            universe.metadata = {**(universe.metadata or {}), "provider": PROVIDER, "source": SOURCE_NAME}
            universe.save(update_fields=["source", "metadata", "updated_at"])

        created, updated = _upsert_memberships(universe, rows, mapped_by_row, mapping_errors)
        result.memberships_created = created
        result.memberships_updated = updated

        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider=PROVIDER,
            source_name=SOURCE_NAME,
            source_reference="fundamentals/GSPC.INDX?filter=HistoricalTickerComponents",
            period_start=start,
            period_end=end,
            expected_member_count=expected_member_count,
            imported_member_count=result.imported_member_count,
            mapped_member_count=result.mapped_member_count,
            unmapped_member_count=result.unmapped_member_count,
            status=result.status,
            validated_at=timezone.now() if result.status == UniverseCoverageStatus.VALIDATED else None,
            metadata={
                "provider_records": result.provider_records,
                "records_retained": result.records_retained,
                "records_skipped": result.records_skipped,
                "memberships_created": created,
                "memberships_updated": updated,
                "warnings": result.warnings,
                "mapping_errors": mapping_errors,
            },
        )
        result.batch_id = batch.id

        for item in summary["snapshots"]:
            snapshot_status = item["status"]
            if batch.status != UniverseCoverageStatus.VALIDATED:
                snapshot_status = UniverseCoverageStatus.PARTIAL
            UniverseCoverageSnapshot.objects.update_or_create(
                universe=universe,
                coverage_date=item["date"],
                defaults={
                    "import_batch": batch,
                    "expected_member_count": expected_member_count,
                    "actual_member_count": item["actual"],
                    "mapped_member_count": item["mapped"],
                    "unmapped_member_count": item["unmapped"],
                    "status": snapshot_status,
                    "metadata": {"source_name": SOURCE_NAME, "provider": PROVIDER},
                },
            )

    return result


def _build_membership_rows(
    provider_records: list[dict[str, Any]],
    coverage_start: date,
    coverage_end: date,
    result: EODHDSyncResult,
) -> list[EODHDMembershipRow]:
    rows: list[EODHDMembershipRow] = []
    for index, record in enumerate(provider_records, start=1):
        code = str(record.get("Code") or "").strip().upper()
        if not code:
            raise UniverseImportError(f"record {index}: Code is required.")

        start = _parse_optional_date(record.get("StartDate"), f"record {index} StartDate")
        end = _parse_optional_date(record.get("EndDate"), f"record {index} EndDate")
        if start and end and end < start:
            raise UniverseImportError(f"record {index}: EndDate must be greater than or equal to StartDate.")
        if end and end < coverage_start:
            continue
        if start and start > coverage_end:
            result.warnings.append(
                f"record {index} {code}: StartDate {start.isoformat()} is after coverage_end; skipped."
            )
            continue
        if _is_historical_provider_suffix(code) and not _symbol_exists_for_provider_code(code):
            result.warnings.append(f"record {index} {code}: historical _OLD provider suffix skipped.")
            continue
        if start is None:
            start = coverage_start
            result.warnings.append(
                f"record {index} {code}: missing StartDate; using coverage_start {coverage_start.isoformat()}."
            )
            assumed_start = True
        else:
            assumed_start = False
        capped_end = end
        if capped_end and capped_end > coverage_end:
            result.warnings.append(
                f"record {index} {code}: EndDate {capped_end.isoformat()} is after coverage_end; capped to {coverage_end.isoformat()}."
            )
            capped_end = coverage_end

        source_payload = {
            "provider_record": record.get("source_payload") or record,
            "provider_code": code,
            "assumed_valid_from_coverage_start": assumed_start,
            "original_start_date": record.get("StartDate"),
            "original_end_date": record.get("EndDate"),
        }
        rows.append(
            EODHDMembershipRow(
                row_number=index,
                provider_code=code,
                ticker=code,
                exchange="",
                provider_symbol=f"{code}.US",
                valid_from=start,
                valid_to=capped_end,
                company_name=str(record.get("Name") or "").strip(),
                source_payload=source_payload,
            )
        )
    return rows


def _resolve_symbols(rows: list[EODHDMembershipRow]) -> tuple[dict[int, Symbol | None], dict[int, str]]:
    mapped_by_row: dict[int, Symbol | None] = {}
    mapping_errors: dict[int, str] = {}
    for row in rows:
        candidates = _ticker_candidates(row.provider_code)
        qs = Symbol.objects.filter(ticker__in=candidates)
        count = qs.count()
        if count == 1:
            symbol = qs.get()
            mapped_by_row[row.row_number] = symbol
            continue
        mapped_by_row[row.row_number] = None
        if count == 0:
            mapping_errors[row.row_number] = f"unmapped symbol {row.provider_code}"
        else:
            mapping_errors[row.row_number] = f"ambiguous symbol {row.provider_code}; provide local symbol mapping"
    return mapped_by_row, mapping_errors


def _ticker_candidates(provider_code: str) -> list[str]:
    code = str(provider_code or "").strip().upper()
    candidates = [code]
    if "-" in code:
        candidates.append(code.replace("-", "."))
    if "." in code:
        candidates.append(code.replace(".", "-"))
    return list(dict.fromkeys(candidates))


def _symbol_exists_for_provider_code(provider_code: str) -> bool:
    return Symbol.objects.filter(ticker__in=_ticker_candidates(provider_code)).exists()


def _is_historical_provider_suffix(provider_code: str) -> bool:
    return "_OLD" in str(provider_code or "").strip().upper()


def _upsert_memberships(
    universe: UniverseDefinition,
    rows: list[EODHDMembershipRow],
    mapped_by_row: dict[int, Symbol | None],
    mapping_errors: dict[int, str],
) -> tuple[int, int]:
    created = 0
    updated = 0
    for row in rows:
        symbol = mapped_by_row.get(row.row_number)
        ticker = symbol.ticker if symbol is not None else row.ticker
        exchange = symbol.exchange if symbol is not None else ""
        source_payload = {
            **row.source_payload,
            "company_name": row.company_name,
            "source": SOURCE_NAME,
            "mapping_error": mapping_errors.get(row.row_number, ""),
            "stored_ticker": ticker,
            "stored_exchange": exchange,
        }
        defaults = {
            "symbol": symbol,
            "provider_symbol": row.provider_symbol,
            "valid_to": row.valid_to,
            "source": SOURCE_NAME,
            "source_payload": source_payload,
        }
        membership, was_created = UniverseMembership.objects.get_or_create(
            universe=universe,
            ticker=ticker,
            exchange=exchange,
            valid_from=row.valid_from,
            defaults=defaults,
        )
        if was_created:
            created += 1
            continue
        changed_fields = []
        for field, value in defaults.items():
            if getattr(membership, field) != value:
                setattr(membership, field, value)
                changed_fields.append(field)
        if changed_fields:
            membership.save(update_fields=[*changed_fields, "updated_at"])
            updated += 1
    return created, updated


def _coverage_summary(
    rows: list[EODHDMembershipRow],
    symbols_by_row: dict[int, Symbol | None],
    coverage_start: date,
    coverage_end: date,
    expected_member_count: int,
    force_partial: bool,
) -> dict[str, Any]:
    snapshots = []
    max_actual = max_mapped = max_unmapped = 0
    batch_status = UniverseCoverageStatus.VALIDATED
    current = coverage_start
    while current <= coverage_end:
        active = [
            row
            for row in rows
            if row.valid_from <= current and (row.valid_to is None or current <= row.valid_to)
        ]
        actual = len(active)
        mapped = sum(1 for row in active if symbols_by_row.get(row.row_number) is not None)
        unmapped = actual - mapped
        status = (
            UniverseCoverageStatus.VALIDATED
            if actual >= expected_member_count and mapped >= actual and unmapped == 0 and not force_partial
            else UniverseCoverageStatus.PARTIAL
        )
        if status != UniverseCoverageStatus.VALIDATED:
            batch_status = UniverseCoverageStatus.PARTIAL
        max_actual = max(max_actual, actual)
        max_mapped = max(max_mapped, mapped)
        max_unmapped = max(max_unmapped, unmapped)
        snapshots.append({
            "date": current,
            "actual": actual,
            "mapped": mapped,
            "unmapped": unmapped,
            "status": status,
        })
        current += timedelta(days=1)
    return {
        "batch_status": batch_status,
        "snapshots": snapshots,
        "max_actual": max_actual,
        "max_mapped": max_mapped,
        "max_unmapped": max_unmapped,
    }


def _parse_required_date(value, label: str) -> date:
    parsed = _parse_optional_date(value, label)
    if parsed is None:
        raise UniverseImportError(f"{label} is required.")
    return parsed


def _parse_optional_date(value, label: str) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise UniverseImportError(f"{label} must be YYYY-MM-DD.") from exc
