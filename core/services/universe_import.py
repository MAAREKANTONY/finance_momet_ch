from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
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

DEFAULT_UNIVERSE_CODE = "SP500"
# Backward-compatible name used by the existing S&P500 EODHD sync code.
SUPPORTED_UNIVERSE_CODE = DEFAULT_UNIVERSE_CODE
REQUIRED_COLUMN_GROUPS = (
    ("universe_code",),
    ("ticker", "symbol"),
    ("valid_from", "start_date"),
)
OPTIONAL_SOURCE_PAYLOAD_COLUMNS = (
    "weight",
    "mic",
    "name",
    "company_name",
    "country",
    "currency",
    "sector",
    "industry",
)


class UniverseImportError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedMembershipRow:
    row_number: int
    universe_code: str
    ticker: str
    exchange: str
    provider_symbol: str
    valid_from: date
    valid_to: date | None
    company_name: str
    mic: str
    source: str
    raw: dict[str, str]


@dataclass
class UniverseImportResult:
    dry_run: bool
    universe_code: str
    period_start: date | None
    period_end: date | None
    rows_read: int = 0
    memberships_created: int = 0
    memberships_updated: int = 0
    imported_member_count: int = 0
    mapped_member_count: int = 0
    unmapped_member_count: int = 0
    coverage_days: int = 0
    status: str = UniverseCoverageStatus.IMPORTED
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    batch_id: int | None = None
    rows_valid: int = 0
    rows_rejected: int = 0
    distinct_tickers: int = 0
    exchanges: list[str] = field(default_factory=list)
    valid_from_min: date | None = None
    valid_to_max: date | None = None
    open_memberships: int = 0
    universe_name: str = ""


def import_universe_memberships_from_csv(
    csv_path,
    universe_code: str = DEFAULT_UNIVERSE_CODE,
    universe_name: str = "",
    coverage_start: date | str | None = None,
    coverage_end: date | str | None = None,
    provider: str = "manual_csv",
    source_name: str = "manual_csv",
    source_reference: str = "",
    expected_member_count: int = 500,
    dry_run: bool = True,
) -> UniverseImportResult:
    requested_code = _normalize_code(universe_code)
    if not requested_code:
        raise UniverseImportError("universe_code is required.")

    coverage_start = _parse_optional_date(coverage_start, "coverage_start")
    coverage_end = _parse_optional_date(coverage_end, "coverage_end")
    if coverage_start and coverage_end and coverage_end < coverage_start:
        raise UniverseImportError("coverage_end must be greater than or equal to coverage_start.")
    if expected_member_count < 1:
        raise UniverseImportError("expected_member_count must be greater than zero.")

    rows = _read_csv_rows(csv_path, requested_code)
    period_start = coverage_start or min((row.valid_from for row in rows), default=None)
    period_end = coverage_end or _max_row_end(rows)
    if not dry_run and (period_start is None or period_end is None):
        raise UniverseImportError("coverage_start/coverage_end or CSV membership dates are required when dry_run=False.")

    result = UniverseImportResult(
        dry_run=bool(dry_run),
        universe_code=requested_code,
        period_start=period_start,
        period_end=period_end,
        rows_read=len(rows),
        rows_valid=len(rows),
        rows_rejected=0,
        distinct_tickers=len({row.ticker for row in rows}),
        exchanges=sorted({row.exchange for row in rows if row.exchange}),
        valid_from_min=min((row.valid_from for row in rows), default=None),
        valid_to_max=max((row.valid_to for row in rows if row.valid_to is not None), default=None),
        open_memberships=sum(1 for row in rows if row.valid_to is None),
        universe_name=(str(universe_name or "").strip() or _default_universe_name(requested_code)),
    )

    if not rows:
        result.status = UniverseCoverageStatus.FAILED
        result.errors.append("CSV contains no membership rows.")
        if dry_run:
            return result
        raise UniverseImportError("CSV contains no membership rows.")

    universe = UniverseDefinition.objects.filter(code=requested_code).first()
    if universe is None and dry_run:
        result.warnings.append(f"UniverseDefinition {requested_code} would be created.")

    mapped_by_row: dict[int, Symbol | None] = {}
    mapping_errors: dict[int, str] = {}
    for row in rows:
        symbol, error = _resolve_symbol(row)
        mapped_by_row[row.row_number] = symbol
        if error:
            mapping_errors[row.row_number] = error
            result.warnings.append(f"row {row.row_number}: {error}")

    if dry_run:
        result.imported_member_count = _count_active_rows(rows, period_start)
        result.mapped_member_count = _count_active_rows(
            [row for row in rows if mapped_by_row.get(row.row_number) is not None],
            period_start,
        )
        result.unmapped_member_count = max(result.imported_member_count - result.mapped_member_count, 0)
        if period_start and period_end:
            result.coverage_days = _count_calendar_days(period_start, period_end)
            status, _summary = _preview_coverage_status(
                rows=rows,
                symbols_by_row=mapped_by_row,
                coverage_start=period_start,
                coverage_end=period_end,
                expected_member_count=expected_member_count,
                has_mapping_errors=bool(mapping_errors),
            )
            result.status = status
        else:
            result.status = UniverseCoverageStatus.PARTIAL if mapping_errors else UniverseCoverageStatus.IMPORTED
        return result

    with transaction.atomic():
        if universe is None:
            universe = UniverseDefinition.objects.create(
                code=requested_code,
                name=result.universe_name,
                source=source_name or provider,
                active=True,
            )
        else:
            changed_fields = []
            requested_name = str(universe_name or "").strip()
            if requested_name and universe.name != requested_name:
                universe.name = requested_name
                changed_fields.append("name")
            if not universe.source and (source_name or provider):
                universe.source = source_name or provider
                changed_fields.append("source")
            if not universe.active:
                universe.active = True
                changed_fields.append("active")
            if changed_fields:
                universe.save(update_fields=[*changed_fields, "updated_at"])

        created, updated = _upsert_memberships(universe, rows, mapped_by_row, mapping_errors)
        result.memberships_created = created
        result.memberships_updated = updated

        active_summary = _imported_rows_coverage_summary(
            rows=rows,
            symbols_by_row=mapped_by_row,
            coverage_start=period_start,
            coverage_end=period_end,
            expected_member_count=expected_member_count,
            force_partial=bool(mapping_errors),
        )
        result.imported_member_count = active_summary["max_actual"]
        result.mapped_member_count = active_summary["max_mapped"]
        result.unmapped_member_count = active_summary["max_unmapped"]
        result.coverage_days = len(active_summary["snapshots"])
        result.status = active_summary["batch_status"]

        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider=provider,
            source_name=source_name,
            source_reference=source_reference,
            period_start=period_start,
            period_end=period_end,
            expected_member_count=expected_member_count,
            imported_member_count=result.imported_member_count,
            mapped_member_count=result.mapped_member_count,
            unmapped_member_count=result.unmapped_member_count,
            status=result.status,
            validated_at=timezone.now() if result.status == UniverseCoverageStatus.VALIDATED else None,
            metadata={
                "rows_read": result.rows_read,
                "rows_valid": result.rows_valid,
                "rows_rejected": result.rows_rejected,
                "distinct_tickers": result.distinct_tickers,
                "exchanges": result.exchanges,
                "valid_from_min": result.valid_from_min.isoformat() if result.valid_from_min else None,
                "valid_to_max": result.valid_to_max.isoformat() if result.valid_to_max else None,
                "open_memberships": result.open_memberships,
                "memberships_created": created,
                "memberships_updated": updated,
                "mapping_errors": mapping_errors,
            },
        )
        result.batch_id = batch.id

        for item in active_summary["snapshots"]:
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
                    "metadata": {"source_name": source_name, "source_reference": source_reference},
                },
            )

    return result


def _read_csv_rows(csv_path, requested_code: str) -> list[ParsedMembershipRow]:
    path = Path(csv_path)
    if not path.exists():
        raise UniverseImportError(f"CSV file not found: {path}")

    rows: list[ParsedMembershipRow] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = {str(name or "").strip() for name in (reader.fieldnames or [])}
        missing = [
            "/".join(group)
            for group in REQUIRED_COLUMN_GROUPS
            if not any(name in fieldnames for name in group)
        ]
        if missing:
            raise UniverseImportError(f"CSV is missing required columns: {', '.join(missing)}")
        for row_number, raw in enumerate(reader, start=2):
            row = _parse_row(raw, row_number, requested_code)
            rows.append(row)
    return rows


def _parse_row(raw: dict[str, Any], row_number: int, requested_code: str) -> ParsedMembershipRow:
    normalized = {key: str(value or "").strip() for key, value in raw.items()}
    row_code = _normalize_code(normalized.get("universe_code", ""))
    if not row_code:
        raise UniverseImportError(f"row {row_number}: universe_code is required.")
    if row_code != requested_code:
        raise UniverseImportError(f"row {row_number}: universe_code={row_code} does not match requested universe_code={requested_code}.")
    ticker = _first_value(normalized, "ticker", "symbol").upper()
    if not ticker:
        raise UniverseImportError(f"row {row_number}: ticker/symbol is required.")
    valid_from = _parse_required_date(_first_value(normalized, "valid_from", "start_date"), f"row {row_number} valid_from")
    valid_to = _parse_optional_date(_first_value(normalized, "valid_to", "end_date"), f"row {row_number} valid_to")
    if valid_to and valid_to < valid_from:
        raise UniverseImportError(f"row {row_number}: valid_to must be greater than or equal to valid_from.")
    exchange = _first_value(normalized, "exchange", "mic").upper()
    company_name = _first_value(normalized, "company_name", "name")
    return ParsedMembershipRow(
        row_number=row_number,
        universe_code=row_code,
        ticker=ticker,
        exchange=exchange,
        provider_symbol=normalized.get("provider_symbol", ""),
        valid_from=valid_from,
        valid_to=valid_to,
        company_name=company_name,
        mic=normalized.get("mic", "").upper(),
        source=normalized.get("source", "") or "manual_csv",
        raw=normalized,
    )


def _resolve_symbol(row: ParsedMembershipRow) -> tuple[Symbol | None, str]:
    qs = Symbol.objects.filter(ticker=row.ticker)
    if row.exchange:
        qs = qs.filter(exchange__iexact=row.exchange)
    count = qs.count()
    if count == 1:
        return qs.get(), ""
    if count == 0:
        suffix = f":{row.exchange}" if row.exchange else ""
        return None, f"unmapped symbol {row.ticker}{suffix}"
    return None, f"ambiguous symbol {row.ticker}; provide exchange"


def _upsert_memberships(
    universe: UniverseDefinition,
    rows: list[ParsedMembershipRow],
    mapped_by_row: dict[int, Symbol | None],
    mapping_errors: dict[int, str],
) -> tuple[int, int]:
    created = 0
    updated = 0
    for row in rows:
        source_payload = {
            "company_name": row.company_name,
            "mic": row.mic,
            "source": row.source,
            "extras": {key: row.raw.get(key, "") for key in OPTIONAL_SOURCE_PAYLOAD_COLUMNS if row.raw.get(key, "")},
            "row": row.raw,
            "mapping_error": mapping_errors.get(row.row_number, ""),
        }
        defaults = {
            "symbol": mapped_by_row.get(row.row_number),
            "provider_symbol": row.provider_symbol,
            "valid_to": row.valid_to,
            "source": row.source,
            "source_payload": source_payload,
        }
        membership, was_created = UniverseMembership.objects.get_or_create(
            universe=universe,
            ticker=row.ticker,
            exchange=row.exchange,
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


def _imported_rows_coverage_summary(
    rows: list[ParsedMembershipRow],
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


def _preview_coverage_status(
    rows: list[ParsedMembershipRow],
    symbols_by_row: dict[int, Symbol | None],
    coverage_start: date,
    coverage_end: date,
    expected_member_count: int,
    has_mapping_errors: bool,
) -> tuple[str, dict[str, int]]:
    status = UniverseCoverageStatus.VALIDATED
    max_actual = max_mapped = max_unmapped = 0
    current = coverage_start
    while current <= coverage_end:
        active = [row for row in rows if row.valid_from <= current and (row.valid_to is None or current <= row.valid_to)]
        actual = len(active)
        mapped = sum(1 for row in active if symbols_by_row.get(row.row_number) is not None)
        unmapped = actual - mapped
        max_actual = max(max_actual, actual)
        max_mapped = max(max_mapped, mapped)
        max_unmapped = max(max_unmapped, unmapped)
        if actual < expected_member_count or mapped < actual or unmapped != 0 or has_mapping_errors:
            status = UniverseCoverageStatus.PARTIAL
        current += timedelta(days=1)
    return status, {"actual": max_actual, "mapped": max_mapped, "unmapped": max_unmapped}


def _normalize_code(value: str) -> str:
    return str(value or "").strip().upper()


def _first_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _default_universe_name(universe_code: str) -> str:
    code = _normalize_code(universe_code)
    if code == DEFAULT_UNIVERSE_CODE:
        return "S&P 500"
    return code


def _parse_required_date(value: str, label: str) -> date:
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


def _max_row_end(rows: list[ParsedMembershipRow]) -> date | None:
    closed_dates = [row.valid_to for row in rows if row.valid_to is not None]
    if closed_dates:
        return max(closed_dates)
    return max((row.valid_from for row in rows), default=None)


def _count_active_rows(rows: list[ParsedMembershipRow], as_of: date | None) -> int:
    if as_of is None:
        return len(rows)
    return sum(1 for row in rows if row.valid_from <= as_of and (row.valid_to is None or as_of <= row.valid_to))


def _count_calendar_days(start: date, end: date) -> int:
    return (end - start).days + 1
