from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from django.db import transaction

from core.models import Symbol
from core.services.provider_eodhd import EODHDClient, EODHDError
from core.services.universe_import import UniverseImportError

DEFAULT_EXCHANGE = "US"
DEFAULT_COUNTRY = "US"
DEFAULT_CURRENCY = "USD"
DEFAULT_INSTRUMENT_TYPE = "Common Stock"


@dataclass(frozen=True)
class SymbolBootstrapCandidate:
    row_number: int
    provider_code: str
    ticker: str
    name: str
    provider_record: dict[str, Any]


@dataclass
class SymbolBootstrapResult:
    dry_run: bool
    period_start: date
    period_end: date
    provider_records: int = 0
    records_retained: int = 0
    existing: int = 0
    to_create: int = 0
    created: int = 0
    skipped: int = 0
    warnings: list[str] = field(default_factory=list)
    create_examples: list[str] = field(default_factory=list)


def bootstrap_sp500_symbols_from_eodhd(
    *,
    coverage_start: date | str,
    coverage_end: date | str,
    dry_run: bool = True,
    client: EODHDClient | None = None,
) -> SymbolBootstrapResult:
    start = _parse_required_date(coverage_start, "coverage_start")
    end = _parse_required_date(coverage_end, "coverage_end")
    if end < start:
        raise UniverseImportError("coverage_end must be greater than or equal to coverage_start.")

    client = client or EODHDClient()
    try:
        provider_records = client.fetch_sp500_historical_components()
    except EODHDError:
        raise
    except Exception as exc:
        raise EODHDError(str(exc)) from exc

    result = SymbolBootstrapResult(
        dry_run=bool(dry_run),
        period_start=start,
        period_end=end,
        provider_records=len(provider_records),
    )
    candidates = _build_candidates(provider_records, start, end, result)
    result.records_retained = len(candidates)

    unique_candidates: dict[str, SymbolBootstrapCandidate] = {}
    for candidate in candidates:
        unique_candidates.setdefault(candidate.ticker, candidate)

    create_candidates: list[SymbolBootstrapCandidate] = []
    for candidate in unique_candidates.values():
        if _symbol_exists_for_provider_code(candidate.provider_code):
            result.existing += 1
            continue
        create_candidates.append(candidate)

    result.to_create = len(create_candidates)
    result.create_examples = [candidate.ticker for candidate in create_candidates[:10]]

    if dry_run:
        return result

    with transaction.atomic():
        for candidate in create_candidates:
            _obj, created = Symbol.objects.get_or_create(
                ticker=candidate.ticker,
                exchange=DEFAULT_EXCHANGE,
                defaults={
                    "name": candidate.name,
                    "instrument_type": DEFAULT_INSTRUMENT_TYPE,
                    "country": DEFAULT_COUNTRY,
                    "currency": DEFAULT_CURRENCY,
                    "active": True,
                },
            )
            if created:
                result.created += 1
            else:
                result.existing += 1

    return result


def _build_candidates(
    provider_records: list[dict[str, Any]],
    coverage_start: date,
    coverage_end: date,
    result: SymbolBootstrapResult,
) -> list[SymbolBootstrapCandidate]:
    candidates: list[SymbolBootstrapCandidate] = []
    for index, record in enumerate(provider_records, start=1):
        code = str(record.get("Code") or "").strip().upper()
        if not code:
            raise UniverseImportError(f"record {index}: Code is required.")

        start = _parse_optional_date(record.get("StartDate"), f"record {index} StartDate")
        end = _parse_optional_date(record.get("EndDate"), f"record {index} EndDate")
        if start and end and end < start:
            raise UniverseImportError(f"record {index}: EndDate must be greater than or equal to StartDate.")
        if end and end < coverage_start:
            result.skipped += 1
            continue
        if start and start > coverage_end:
            result.skipped += 1
            continue
        if start is None:
            result.warnings.append(
                f"record {index} {code}: missing StartDate; treating as intersecting from coverage_start {coverage_start.isoformat()}."
            )

        if _is_historical_provider_suffix(code):
            if _symbol_exists_for_provider_code(code):
                result.existing += 1
            else:
                result.skipped += 1
                result.warnings.append(f"record {index} {code}: historical _OLD provider suffix skipped.")
            continue

        candidates.append(
            SymbolBootstrapCandidate(
                row_number=index,
                provider_code=code,
                ticker=_creation_ticker_for_provider_code(code),
                name=str(record.get("Name") or "").strip(),
                provider_record=record,
            )
        )
    return candidates


def _symbol_exists_for_provider_code(provider_code: str) -> bool:
    return Symbol.objects.filter(ticker__in=_ticker_candidates(provider_code)).exists()


def _ticker_candidates(provider_code: str) -> list[str]:
    code = str(provider_code or "").strip().upper()
    candidates = [code]
    if "-" in code:
        candidates.append(code.replace("-", "."))
    if "." in code:
        candidates.append(code.replace(".", "-"))
    return list(dict.fromkeys(candidates))


def _creation_ticker_for_provider_code(provider_code: str) -> str:
    code = str(provider_code or "").strip().upper()
    if "-" in code:
        return code.replace("-", ".")
    return code


def _is_historical_provider_suffix(provider_code: str) -> bool:
    return "_OLD" in str(provider_code or "").strip().upper()


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
