from __future__ import annotations

import json
from collections import defaultdict
from datetime import timedelta
from typing import Any

from django.db.models import Exists, Max, OuterRef

from core.models import (
    DailyBar,
    ProcessingJob,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseDefinition,
    UniverseMembership,
)
from core.services.china_benchmark_registry import expected_primary_benchmarks, provider_symbol_parts
from core.services.csi300_csv_generation import latest_valid_csi300_generation
from core.services.csi300_eodhd_metadata import GENERIC_SECTOR_VALUES
from core.services.csi300_sector_gm import build_csi300_sector_gm_coverage
from core.services.universe_resolver import CSI300_UNIVERSE_CODE


def _report_json(message: str) -> dict[str, Any]:
    for line in str(message or "").splitlines():
        if line.startswith("report_json="):
            try:
                payload = json.loads(line.split("=", 1)[1])
                return payload if isinstance(payload, dict) else {}
            except json.JSONDecodeError:
                return {}
    return {}


def _job_summary(job: ProcessingJob | None) -> dict[str, Any] | None:
    if job is None:
        return None
    payload = _report_json(job.message)
    return {
        "id": job.id,
        "status": job.status,
        "operational_status": payload.get("operational_status") or job.status,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "warnings": payload.get("warnings") or [],
        "error": job.error,
        "report": payload,
    }


def _coverage_inconsistencies(universe: UniverseDefinition, memberships: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    snapshots = list(
        UniverseCoverageSnapshot.objects.filter(universe=universe)
        .order_by("coverage_date")
        .values(
            "coverage_date",
            "actual_member_count",
            "mapped_member_count",
            "unmapped_member_count",
            "status",
            "import_batch_id",
        )
    )
    issues: list[str] = []
    if not snapshots:
        issues.append("Aucun snapshot de couverture.")
        return {
            "count": 0,
            "coverage_start": None,
            "coverage_end": None,
            "missing_days": 0,
            "batch_ids": [],
            "current_batch_id": None,
        }, issues

    start = snapshots[0]["coverage_date"]
    end = snapshots[-1]["coverage_date"]
    expected_days = (end - start).days + 1
    if len(snapshots) != expected_days:
        issues.append(f"Couverture discontinue : {expected_days - len(snapshots)} jour(s) manquant(s).")
    invalid_statuses = {"FAILED", "PARTIAL", "STALE"}
    if any(row["status"] in invalid_statuses for row in snapshots):
        issues.append("Un ou plusieurs snapshots ont un statut non validé.")

    starts: dict[Any, int] = defaultdict(int)
    ends: dict[Any, int] = defaultdict(int)
    mapping_starts: dict[Any, int] = defaultdict(int)
    mapping_ends: dict[Any, int] = defaultdict(int)
    for membership in memberships:
        valid_from = max(membership["valid_from"], start)
        valid_to = min(membership["valid_to"] or end, end)
        if valid_to < start or valid_from > end:
            continue
        starts[valid_from] += 1
        ends[valid_to + timedelta(days=1)] += 1
        if membership["symbol_id"]:
            mapping_starts[valid_from] += 1
            mapping_ends[valid_to + timedelta(days=1)] += 1

    active = mapped = 0
    mismatches = 0
    for row in snapshots:
        day = row["coverage_date"]
        active += starts[day] - ends[day]
        mapped += mapping_starts[day] - mapping_ends[day]
        if (
            row["actual_member_count"] != active
            or row["mapped_member_count"] != mapped
            or row["unmapped_member_count"] != active - mapped
        ):
            mismatches += 1
    if mismatches:
        issues.append(f"{mismatches} snapshot(s) ont des compteurs incohérents avec les memberships.")

    batch_ids = sorted({row["import_batch_id"] for row in snapshots})
    return {
        "count": len(snapshots),
        "coverage_start": start,
        "coverage_end": end,
        "missing_days": max(expected_days - len(snapshots), 0),
        "batch_ids": batch_ids,
        "current_batch_id": snapshots[-1]["import_batch_id"],
        "counter_mismatches": mismatches,
    }, issues


def build_csi300_operations_status() -> dict[str, Any]:
    universe = UniverseDefinition.objects.filter(code=CSI300_UNIVERSE_CODE, active=True).first()
    if universe is None:
        return {
            "status": "NOT_READY",
            "referential": {"status": "NOT_READY", "issues": ["Univers CSI300 actif introuvable."]},
            "metadata": {},
            "ohlc": {},
            "jobs": {},
            "generation": latest_valid_csi300_generation(),
        }

    memberships = list(
        UniverseMembership.objects.filter(universe=universe)
        .order_by("ticker", "exchange", "valid_from")
        .values("ticker", "exchange", "valid_from", "valid_to", "symbol_id")
    )
    overlap_count = 0
    previous: dict[tuple[str, str], Any] = {}
    for row in memberships:
        key = (row["ticker"], row["exchange"])
        previous_end = previous.get(key)
        if previous_end is None and key in previous:
            overlap_count += 1
        elif previous_end is not None and row["valid_from"] <= previous_end:
            overlap_count += 1
        previous[key] = row["valid_to"]

    snapshot_summary, referential_issues = _coverage_inconsistencies(universe, memberships)
    if not memberships:
        referential_issues.append("Aucun membership CSI300.")
    if overlap_count:
        referential_issues.append(f"{overlap_count} chevauchement(s) de memberships.")
    historical_tickers = len({(row["ticker"], row["exchange"]) for row in memberships})
    symbol_ids = {row["symbol_id"] for row in memberships if row["symbol_id"]}
    unmapped = len(memberships) - sum(1 for row in memberships if row["symbol_id"])
    if unmapped:
        referential_issues.append(f"{unmapped} membership(s) non mappé(s).")

    symbols = list(Symbol.objects.filter(id__in=symbol_ids).order_by("ticker", "exchange", "id"))
    generic_values = {value.upper() for value in GENERIC_SECTOR_VALUES}
    names_present = sum(1 for symbol in symbols if str(symbol.name_en or "").strip())
    sector_missing = sum(1 for symbol in symbols if not str(symbol.sector or "").strip())
    sector_generic = sum(
        1
        for symbol in symbols
        if str(symbol.sector or "").strip() and str(symbol.sector or "").strip().upper() in generic_values
    )
    sector_useful = len(symbols) - sector_missing - sector_generic

    bar_exists = DailyBar.objects.filter(symbol_id=OuterRef("pk"))
    symbols_with_bar = Symbol.objects.filter(id__in=symbol_ids).annotate(has_bar=Exists(bar_exists)).filter(has_bar=True).count()
    last_bar_date = DailyBar.objects.filter(symbol_id__in=symbol_ids).aggregate(last=Max("date")).get("last")

    benchmark_rows = []
    for definition in expected_primary_benchmarks():
        ticker, exchange = provider_symbol_parts(definition.provider_symbol)
        symbol = Symbol.objects.filter(ticker=ticker, exchange=exchange).first()
        available = bool(symbol and DailyBar.objects.filter(symbol=symbol).exists())
        last_date = DailyBar.objects.filter(symbol=symbol).aggregate(last=Max("date")).get("last") if symbol else None
        benchmark_rows.append(
            {
                "provider_symbol": definition.provider_symbol,
                "canonical_sector": definition.canonical_sector,
                "is_market": definition.is_market,
                "available": available,
                "last_date": last_date,
            }
        )

    metadata_job = ProcessingJob.objects.filter(job_type=ProcessingJob.JobType.ENRICH_METADATA).order_by("-id").first()
    generation_job = ProcessingJob.objects.filter(job_type=ProcessingJob.JobType.GENERATE_CSI300_CSV).order_by("-id").first()
    refresh_job = ProcessingJob.objects.filter(job_type=ProcessingJob.JobType.REFRESH_CSI300_DATA).order_by("-id").first()
    active_jobs = ProcessingJob.objects.filter(
        job_type__in=[ProcessingJob.JobType.GENERATE_CSI300_CSV, ProcessingJob.JobType.REFRESH_CSI300_DATA],
        status__in=[ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING],
    ).count()
    last_failed = ProcessingJob.objects.filter(
        job_type__in=[ProcessingJob.JobType.GENERATE_CSI300_CSV, ProcessingJob.JobType.REFRESH_CSI300_DATA],
        status=ProcessingJob.Status.FAILED,
    ).order_by("-id").first()
    last_success = ProcessingJob.objects.filter(
        job_type__in=[ProcessingJob.JobType.GENERATE_CSI300_CSV, ProcessingJob.JobType.REFRESH_CSI300_DATA],
        status=ProcessingJob.Status.DONE,
    ).order_by("-id").first()

    warnings: list[str] = []
    names_missing = len(symbols) - names_present
    actions_without_bars = len(symbols) - symbols_with_bar
    if names_missing:
        warnings.append(f"{names_missing} nom(s) anglais absent(s).")
    if sector_missing or sector_generic:
        warnings.append(f"Secteurs partiels : absents={sector_missing}, génériques={sector_generic}.")
    if actions_without_bars:
        warnings.append(f"{actions_without_bars} action(s) CSI300 sans OHLC.")
    missing_benchmarks = [row["provider_symbol"] for row in benchmark_rows if not row["available"]]
    if missing_benchmarks:
        warnings.append(f"Benchmarks sans OHLC : {', '.join(missing_benchmarks)}.")

    sector_gm = {}
    if snapshot_summary.get("coverage_start") and snapshot_summary.get("coverage_end"):
        active_members_expected = int(
            UniverseCoverageSnapshot.objects.filter(universe=universe).aggregate(value=Max("actual_member_count")).get("value")
            or 0
        )
        sector_gm = build_csi300_sector_gm_coverage(
            symbols=symbols,
            coverage_start=snapshot_summary["coverage_start"],
            coverage_end=snapshot_summary["coverage_end"],
            active_members_expected=active_members_expected,
        )

    overall_status = "NOT_READY" if referential_issues else ("READY_WITH_WARNINGS" if warnings else "READY")
    return {
        "status": overall_status,
        "referential": {
            "status": "NOT_READY" if referential_issues else "READY",
            "memberships": len(memberships),
            "historical_tickers": historical_tickers,
            "mapped_symbols": len(symbol_ids),
            "unmapped_memberships": unmapped,
            "overlaps": overlap_count,
            **snapshot_summary,
            "issues": referential_issues,
        },
        "metadata": {
            "name_en_present": names_present,
            "name_en_missing": names_missing,
            "sectors_useful": sector_useful,
            "sectors_generic": sector_generic,
            "sectors_missing": sector_missing,
            "last_sync": _job_summary(metadata_job),
        },
        "ohlc": {
            "symbols_expected": len(symbols),
            "symbols_with_bars": symbols_with_bar,
            "symbols_without_bars": actions_without_bars,
            "last_date": last_bar_date,
            "market_benchmark_available": next((row["available"] for row in benchmark_rows if row["is_market"]), False),
            "sector_benchmarks_available": sum(1 for row in benchmark_rows if not row["is_market"] and row["available"]),
            "sector_benchmarks_missing": [
                row["provider_symbol"] for row in benchmark_rows if not row["is_market"] and not row["available"]
            ],
            "benchmarks": benchmark_rows,
        },
        "sector_gm": sector_gm,
        "jobs": {
            "active": active_jobs,
            "last_generation": _job_summary(generation_job),
            "last_refresh": _job_summary(refresh_job),
            "last_success": _job_summary(last_success),
            "last_failure": _job_summary(last_failed),
        },
        "generation": latest_valid_csi300_generation(),
        "warnings": warnings,
    }
