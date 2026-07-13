from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Callable

from django.db import transaction
from django.db.models import Min, Max
from django.utils import timezone

from core.models import DailyBar, Symbol
from core.services.china_benchmark_registry import (
    CSI300_MARKET_BENCHMARK,
    CSI300_MARKET_FALLBACK,
    CSI300_SECTOR_BENCHMARKS,
    ChinaBenchmarkDefinition,
    expected_primary_benchmarks,
    unsupported_sector_benchmarks,
)
from core.services.provider_eodhd import EODHDClient, EODHDError, sanitize_provider_error_message


DEFAULT_START_DATE = date(2021, 8, 20)


class CSI300BenchmarkPreparationError(RuntimeError):
    pass


@dataclass
class CSI300BenchmarkPreparationReport:
    dry_run: bool
    start_date: date
    end_date: date
    expected: int = 0
    supported: int = 0
    unsupported_sectors: list[str] = field(default_factory=list)
    existing_symbols: int = 0
    created_symbols: int = 0
    updated_symbol_metadata: int = 0
    conflicts: int = 0
    skipped_conflicts: int = 0
    provider_successes: int = 0
    errors: int = 0
    inserted_bars: int = 0
    updated_bars: int = 0
    unchanged_bars: int = 0
    no_data: int = 0
    first_ohlc: dict[str, str | None] = field(default_factory=dict)
    last_ohlc: dict[str, str | None] = field(default_factory=dict)
    per_benchmark: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        return payload


def _parse_date(value: date | str | None, *, default: date, field_name: str) -> date:
    if value is None or value == "":
        return default
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise CSI300BenchmarkPreparationError(f"Invalid {field_name}: {value}") from exc


def _split_provider_symbol(provider_symbol: str) -> tuple[str, str]:
    ticker, sep, exchange = str(provider_symbol or "").strip().upper().partition(".")
    if not ticker or not sep or not exchange:
        raise CSI300BenchmarkPreparationError(f"Invalid EODHD provider symbol: {provider_symbol}")
    return ticker, exchange


def _definition_detail(definition: ChinaBenchmarkDefinition) -> dict[str, Any]:
    payload = definition.as_dict()
    payload["ticker"] = _split_provider_symbol(definition.provider_symbol)[0] if definition.provider_symbol else ""
    return payload


def _daily_bar_defaults(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": int(row["volume"]),
        "source": "eodhd",
    }


def _values_equal(current: Any, incoming: Any) -> bool:
    if isinstance(incoming, Decimal):
        return Decimal(str(current)) == incoming
    return current == incoming


def _upsert_daily_bars(symbol: Symbol, rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    inserted = updated = unchanged = 0
    for row in rows:
        defaults = _daily_bar_defaults(row)
        bar, created = DailyBar.objects.get_or_create(symbol=symbol, date=row["date"], defaults=defaults)
        if created:
            inserted += 1
            continue
        changed_fields = [
            field_name
            for field_name, incoming_value in defaults.items()
            if not _values_equal(getattr(bar, field_name), incoming_value)
        ]
        if not changed_fields:
            unchanged += 1
            continue
        for field_name in changed_fields:
            setattr(bar, field_name, defaults[field_name])
        bar.save(update_fields=[*changed_fields, "ingested_at"])
        updated += 1
    return inserted, updated, unchanged


def _existing_ohlc_bounds(symbol: Symbol) -> tuple[date | None, date | None]:
    bounds = DailyBar.objects.filter(symbol=symbol).aggregate(first=Min("date"), last=Max("date"))
    return bounds.get("first"), bounds.get("last")


def _ensure_benchmark_symbol(
    definition: ChinaBenchmarkDefinition,
    *,
    dry_run: bool,
    report: CSI300BenchmarkPreparationReport,
) -> tuple[Symbol | None, dict[str, Any]]:
    ticker, exchange = _split_provider_symbol(definition.provider_symbol or "")
    detail = _definition_detail(definition)
    detail.update({"status": "", "symbol_id": None, "error": ""})
    symbol = Symbol.objects.filter(ticker=ticker, exchange=exchange).first()
    if symbol:
        report.existing_symbols += 1
        existing_type = str(symbol.instrument_type or "").strip()
        expected_type = definition.instrument_type
        detail.update({
            "status": "existing",
            "symbol_id": symbol.id,
            "expected_instrument_type": expected_type,
            "existing_instrument_type": existing_type,
        })
        if not existing_type:
            report.updated_symbol_metadata += 1
            detail["status"] = "dry_run_update_type" if dry_run else "updated_type"
            if not dry_run:
                symbol.instrument_type = expected_type
                symbol.save(update_fields=["instrument_type"])
            return symbol, detail
        if existing_type.upper() != expected_type:
            report.conflicts += 1
            report.skipped_conflicts += 1
            detail.update({
                "status": "conflict",
                "error": "instrument_type differs",
                "conflict_reason": "instrument_type differs",
            })
            return None, detail
        return symbol, detail

    report.created_symbols += 1
    detail["status"] = "dry_run_create" if dry_run else "created"
    if dry_run:
        return Symbol(
            ticker=ticker,
            exchange=exchange,
            name=definition.name,
            instrument_type=definition.instrument_type,
            country="China",
            currency="CNY",
            active=True,
        ), detail

    symbol = Symbol.objects.create(
        ticker=ticker,
        exchange=exchange,
        name=definition.name,
        instrument_type=definition.instrument_type,
        country="China",
        currency="CNY",
        active=True,
    )
    detail["symbol_id"] = symbol.id
    return symbol, detail


def _record_ohlc_bounds(report: CSI300BenchmarkPreparationReport, provider_symbol: str, symbol: Symbol) -> None:
    first, last = _existing_ohlc_bounds(symbol)
    report.first_ohlc[provider_symbol] = first.isoformat() if first else None
    report.last_ohlc[provider_symbol] = last.isoformat() if last else None


def prepare_csi300_benchmarks(
    *,
    dry_run: bool = True,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    verify_provider: bool = False,
    client: EODHDClient | None = None,
    progress_callback: Callable[[CSI300BenchmarkPreparationReport, int, int], None] | None = None,
) -> CSI300BenchmarkPreparationReport:
    resolved_start = _parse_date(start_date, default=DEFAULT_START_DATE, field_name="start_date")
    resolved_end = _parse_date(end_date, default=timezone.now().date(), field_name="end_date")
    if resolved_end < resolved_start:
        raise CSI300BenchmarkPreparationError("end_date must be greater than or equal to start_date.")

    definitions = expected_primary_benchmarks()
    unsupported = unsupported_sector_benchmarks()
    report = CSI300BenchmarkPreparationReport(
        dry_run=bool(dry_run),
        start_date=resolved_start,
        end_date=resolved_end,
        expected=len(definitions) + len(unsupported),
        supported=len(definitions),
        unsupported_sectors=[definition.canonical_sector for definition in unsupported],
    )
    for definition in unsupported:
        report.per_benchmark.append({**definition.as_dict(), "status": "unsupported", "error": ""})

    eodhd_client = client if client is not None else (EODHDClient() if (not dry_run or verify_provider) else None)
    total = len(definitions)
    for index, definition in enumerate(definitions, start=1):
        symbol, detail = _ensure_benchmark_symbol(definition, dry_run=dry_run, report=report)
        provider_symbol = definition.provider_symbol or ""
        if symbol and getattr(symbol, "id", None):
            _record_ohlc_bounds(report, provider_symbol, symbol)

        if detail.get("status") == "conflict":
            report.per_benchmark.append(detail)
            if progress_callback:
                progress_callback(report, index, total)
            continue

        if dry_run and not verify_provider:
            report.per_benchmark.append(detail)
            if progress_callback:
                progress_callback(report, index, total)
            continue

        try:
            rows = eodhd_client.fetch_historical_ohlc(provider_symbol, resolved_start, resolved_end) if eodhd_client else []
        except EODHDError as exc:
            report.errors += 1
            detail["error"] = sanitize_provider_error_message(exc)
            report.per_benchmark.append(detail)
            if progress_callback:
                progress_callback(report, index, total)
            continue

        if not rows:
            report.no_data += 1
            detail["status"] = f"{detail['status']}_no_data" if detail["status"] else "no_data"
            report.per_benchmark.append(detail)
            if progress_callback:
                progress_callback(report, index, total)
            continue

        report.provider_successes += 1
        if dry_run:
            detail["provider_rows"] = len(rows)
            report.per_benchmark.append(detail)
            if progress_callback:
                progress_callback(report, index, total)
            continue

        with transaction.atomic():
            inserted, updated, unchanged = _upsert_daily_bars(symbol, rows)
        report.inserted_bars += inserted
        report.updated_bars += updated
        report.unchanged_bars += unchanged
        _record_ohlc_bounds(report, provider_symbol, symbol)
        detail.update({"provider_rows": len(rows), "inserted_bars": inserted, "updated_bars": updated, "unchanged_bars": unchanged})
        report.per_benchmark.append(detail)
        if progress_callback:
            progress_callback(report, index, total)

    return report


def format_csi300_benchmark_report_summary(report: CSI300BenchmarkPreparationReport) -> str:
    mode = "dry-run" if report.dry_run else "apply"
    return (
        f"Benchmarks CSI300 ({mode}) — expected={report.expected} supported={report.supported} "
        f"unsupported={len(report.unsupported_sectors)} existing={report.existing_symbols} "
        f"created={report.created_symbols} conflicts={report.conflicts} "
        f"provider_successes={report.provider_successes} errors={report.errors} "
        f"inserted_bars={report.inserted_bars} updated_bars={report.updated_bars} "
        f"unchanged_bars={report.unchanged_bars} no_data={report.no_data}"
    )


def registry_summary() -> dict[str, Any]:
    return {
        "market": CSI300_MARKET_BENCHMARK.as_dict(),
        "market_fallback": CSI300_MARKET_FALLBACK.as_dict(),
        "sectors": {sector: definition.as_dict() for sector, definition in CSI300_SECTOR_BENCHMARKS.items()},
    }
