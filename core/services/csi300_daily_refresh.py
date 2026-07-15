from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any, Callable

from django.db import transaction
from django.db.models import Max, Q
from django.utils import timezone

from core.models import DailyBar, Symbol, UniverseCoverageSnapshot, UniverseDefinition, UniverseMembership
from core.services.china_benchmark_registry import expected_primary_benchmarks
from core.services.csi300_benchmark_preparation import prepare_csi300_benchmarks
from core.services.provider_eodhd import (
    EODHDClient,
    EODHDError,
    UnsupportedEODHDSymbolError,
    sanitize_provider_error_message,
    to_eodhd_symbol_from_parts,
)
from core.services.universe_resolver import CSI300_UNIVERSE_CODE


class CSI300DailyRefreshError(RuntimeError):
    pass


@dataclass
class CSI300DailyRefreshReport:
    status: str = "READY"
    refresh_date: str = ""
    membership_as_of: str = ""
    actions_expected: int = 0
    actions_processed: int = 0
    actions_up_to_date: int = 0
    actions_fetched: int = 0
    actions_no_data: int = 0
    actions_errors: int = 0
    unmapped_memberships: int = 0
    benchmarks_expected: int = 0
    benchmark_tickers: list[str] = field(default_factory=list)
    benchmarks_fetched: int = 0
    benchmarks_no_data: int = 0
    benchmark_errors: int = 0
    inserted_bars: int = 0
    updated_bars: int = 0
    unchanged_bars: int = 0
    warnings: list[str] = field(default_factory=list)
    per_action: list[dict[str, Any]] = field(default_factory=list)
    per_benchmark: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _daily_bar_defaults(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": int(row["volume"]),
        "source": "eodhd",
    }


def _upsert_daily_bars_precise(symbol: Symbol, rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    inserted = updated = unchanged = 0
    for row in rows:
        defaults = _daily_bar_defaults(row)
        bar = DailyBar.objects.filter(symbol=symbol, date=row["date"]).first()
        if bar is None:
            DailyBar.objects.create(symbol=symbol, date=row["date"], **defaults)
            inserted += 1
            continue
        changed = [name for name, value in defaults.items() if getattr(bar, name) != value]
        if not changed:
            unchanged += 1
            continue
        for name in changed:
            setattr(bar, name, defaults[name])
        bar.save(update_fields=[*changed, "ingested_at"])
        updated += 1
    return inserted, updated, unchanged


def _action_scope() -> tuple[date, date, list[UniverseMembership], int]:
    universe = UniverseDefinition.objects.filter(code=CSI300_UNIVERSE_CODE, active=True).first()
    if universe is None:
        raise CSI300DailyRefreshError("L'univers CSI300 actif est introuvable.")
    bounds = UniverseCoverageSnapshot.objects.filter(universe=universe).aggregate(
        first=Max("coverage_date"),
    )
    membership_as_of = bounds.get("first")
    if membership_as_of is None:
        raise CSI300DailyRefreshError("Aucun snapshot autoritatif CSI300 n'est disponible.")
    coverage_start = (
        UniverseCoverageSnapshot.objects.filter(universe=universe)
        .order_by("coverage_date")
        .values_list("coverage_date", flat=True)
        .first()
    )
    memberships = list(
        UniverseMembership.objects.filter(
            universe=universe,
            valid_from__lte=membership_as_of,
        )
        .filter(Q(valid_to__isnull=True) | Q(valid_to__gte=membership_as_of))
        .select_related("symbol")
        .order_by("ticker", "exchange", "valid_from")
    )
    unique_by_symbol: dict[int, UniverseMembership] = {}
    unmapped = 0
    for membership in memberships:
        if membership.symbol_id is None:
            unmapped += 1
            continue
        unique_by_symbol.setdefault(membership.symbol_id, membership)
    return coverage_start or membership_as_of, membership_as_of, list(unique_by_symbol.values()), unmapped


def refresh_csi300_daily_data(
    *,
    refresh_date: date | None = None,
    client: EODHDClient | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> CSI300DailyRefreshReport:
    today = refresh_date or timezone.now().date()
    coverage_start, membership_as_of, memberships, unmapped = _action_scope()
    definitions = expected_primary_benchmarks()
    report = CSI300DailyRefreshReport(
        refresh_date=today.isoformat(),
        membership_as_of=membership_as_of.isoformat(),
        actions_expected=len(memberships) + unmapped,
        unmapped_memberships=unmapped,
        benchmarks_expected=len(definitions),
        benchmark_tickers=[str(item.provider_symbol) for item in definitions],
    )
    eodhd_client = client or EODHDClient()

    for index, membership in enumerate(memberships, start=1):
        symbol = membership.symbol
        detail = {
            "ticker": membership.ticker,
            "exchange": membership.exchange,
            "provider_symbol": membership.provider_symbol,
            "status": "",
            "error": "",
        }
        if progress_callback:
            progress_callback(f"actions {index}/{len(memberships)} {membership.provider_symbol}")
        last_date = DailyBar.objects.filter(symbol=symbol).aggregate(last=Max("date")).get("last")
        fetch_start = (last_date + timedelta(days=1)) if last_date else coverage_start
        if fetch_start > today:
            report.actions_up_to_date += 1
            detail["status"] = "up_to_date"
            report.per_action.append(detail)
            continue
        try:
            provider_symbol = to_eodhd_symbol_from_parts(
                ticker=membership.ticker,
                exchange=membership.exchange,
                provider_symbol=membership.provider_symbol,
            )
            rows = eodhd_client.fetch_historical_ohlc(provider_symbol, fetch_start, today)
        except (EODHDError, UnsupportedEODHDSymbolError) as exc:
            report.actions_errors += 1
            detail["status"] = "error"
            detail["error"] = sanitize_provider_error_message(exc)
            report.per_action.append(detail)
            continue
        report.actions_processed += 1
        if not rows:
            report.actions_no_data += 1
            detail["status"] = "no_data"
            report.per_action.append(detail)
            continue
        with transaction.atomic():
            inserted, updated, unchanged = _upsert_daily_bars_precise(symbol, rows)
        report.actions_fetched += 1
        report.inserted_bars += inserted
        report.updated_bars += updated
        report.unchanged_bars += unchanged
        detail.update(
            status="fetched",
            rows=len(rows),
            inserted=inserted,
            updated=updated,
            unchanged=unchanged,
        )
        report.per_action.append(detail)

    if progress_callback:
        progress_callback(f"benchmarks 0/{len(definitions)}")
    benchmark_report = prepare_csi300_benchmarks(
        dry_run=False,
        start_date=today - timedelta(days=7),
        end_date=today,
        client=eodhd_client,
        progress_callback=(
            (lambda _report, processed, total: progress_callback(f"benchmarks {processed}/{total}"))
            if progress_callback
            else None
        ),
    )
    report.benchmarks_fetched = int(benchmark_report.provider_successes or 0)
    report.benchmarks_no_data = int(benchmark_report.no_data or 0)
    report.benchmark_errors = int(benchmark_report.errors or 0) + int(benchmark_report.conflicts or 0)
    report.inserted_bars += int(benchmark_report.inserted_bars or 0)
    report.updated_bars += int(benchmark_report.updated_bars or 0)
    report.unchanged_bars += int(benchmark_report.unchanged_bars or 0)
    report.per_benchmark = list(benchmark_report.per_benchmark or [])

    action_requests = len(memberships) - report.actions_up_to_date
    provider_requests = action_requests + len(definitions)
    provider_errors = report.actions_errors + int(benchmark_report.errors or 0)
    if provider_requests and provider_errors >= provider_requests:
        raise CSI300DailyRefreshError(
            "Le provider EODHD n'a répondu pour aucune action ni aucun benchmark CSI300."
        )

    if report.unmapped_memberships:
        report.warnings.append(f"{report.unmapped_memberships} membership(s) CSI300 non mappé(s).")
    if report.actions_errors:
        report.warnings.append(f"{report.actions_errors} action(s) en erreur provider.")
    if report.actions_no_data:
        report.warnings.append(f"{report.actions_no_data} action(s) sans nouvelle donnée.")
    if report.benchmark_errors:
        report.warnings.append(f"{report.benchmark_errors} benchmark(s) en erreur.")
    if report.benchmarks_no_data:
        report.warnings.append(f"{report.benchmarks_no_data} benchmark(s) sans nouvelle donnée.")
    if report.warnings:
        report.status = "READY_WITH_WARNINGS"
    return report
