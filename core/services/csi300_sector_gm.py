from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Iterable

from django.db.models import Max, Min, Q

from core.models import DailyBar, Symbol
from core.services.china_benchmark_registry import (
    CSI300_MARKET_BENCHMARK,
    CSI300_SECTOR_BENCHMARKS,
    STATUS_UNSUPPORTED,
    canonical_sector_for_raw,
    provider_symbol_parts,
    supported_sector_benchmarks,
)


SECTOR_GM_READY = "READY"
SECTOR_GM_READY_WITH_WARNINGS = "READY_WITH_WARNINGS"
SECTOR_GM_NOT_READY = "NOT_READY"

SECTOR_REASON_MISSING = "SECTOR_MISSING"
SECTOR_REASON_GENERIC = "SECTOR_GENERIC"
SECTOR_REASON_UNSUPPORTED = "SECTOR_UNSUPPORTED"
SECTOR_REASON_BENCHMARK_MISSING = "BENCHMARK_MISSING"
SECTOR_REASON_BENCHMARK_OHLC_MISSING = "BENCHMARK_OHLC_MISSING"

GENERIC_SECTOR_VALUES = {"-", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "UNSPECIFIED", "OTHER"}
_BOUNDARY_TOLERANCE_DAYS = 3
_EXAMPLE_LIMIT = 10


@dataclass(frozen=True)
class CSI300SectorBenchmarkResolution:
    raw_sector: str
    canonical_sector: str
    provider_symbol: str
    benchmark_ticker: str
    benchmark_exchange: str
    reason: str

    @property
    def supported(self) -> bool:
        return bool(self.provider_symbol and not self.reason)

    def as_dict(self) -> dict:
        return asdict(self)


def resolve_csi300_sector_benchmark(symbol: Symbol | None) -> CSI300SectorBenchmarkResolution:
    raw_sector = str(getattr(symbol, "sector", "") or "").strip()
    if not raw_sector:
        return CSI300SectorBenchmarkResolution("", "", "", "", "", SECTOR_REASON_MISSING)
    if raw_sector.upper() in GENERIC_SECTOR_VALUES:
        return CSI300SectorBenchmarkResolution(raw_sector, "", "", "", "", SECTOR_REASON_GENERIC)

    canonical = canonical_sector_for_raw(raw_sector)
    definition = CSI300_SECTOR_BENCHMARKS.get(canonical) if canonical else None
    if definition is None or definition.status == STATUS_UNSUPPORTED or not definition.provider_symbol:
        return CSI300SectorBenchmarkResolution(raw_sector, canonical, "", "", "", SECTOR_REASON_UNSUPPORTED)

    ticker, exchange = provider_symbol_parts(definition.provider_symbol)
    return CSI300SectorBenchmarkResolution(
        raw_sector,
        canonical,
        str(definition.provider_symbol),
        ticker,
        exchange,
        "",
    )


def csi300_benchmark_exchange_for_ticker(ticker: str | None) -> str:
    ticker_value = str(ticker or "").strip().upper()
    for definition in (CSI300_MARKET_BENCHMARK, *supported_sector_benchmarks()):
        candidate_ticker, exchange = provider_symbol_parts(definition.provider_symbol)
        if candidate_ticker.upper() == ticker_value:
            return exchange.upper()
    return ""


def _registry_issues() -> list[str]:
    issues: list[str] = []
    provider_symbols: list[str] = []
    for definition in supported_sector_benchmarks():
        ticker, exchange = provider_symbol_parts(definition.provider_symbol)
        if not ticker or not exchange or definition.is_fallback:
            issues.append(f"Entrée secteur invalide: {definition.canonical_sector}.")
        provider_symbols.append(str(definition.provider_symbol or ""))
    if len(provider_symbols) != len(set(provider_symbols)):
        issues.append("Le registre des benchmarks secteur contient des doublons.")
    return issues


def _symbol_label(symbol: Symbol) -> str:
    return f"{symbol.ticker}.{symbol.exchange}" if symbol.exchange else symbol.ticker


def _append_example(examples: dict[str, list[dict]], reason: str, symbol: Symbol, resolution, benchmark: str = "") -> None:
    bucket = examples.setdefault(reason, [])
    if len(bucket) >= _EXAMPLE_LIMIT:
        return
    bucket.append({
        "ticker": _symbol_label(symbol),
        "sector": resolution.raw_sector,
        "canonical_sector": resolution.canonical_sector,
        "expected_benchmark": benchmark or resolution.provider_symbol,
        "reason": reason,
    })


def build_csi300_sector_gm_coverage(
    *,
    symbols: Iterable[Symbol],
    coverage_start: date,
    coverage_end: date,
    active_members_expected: int | None = None,
) -> dict:
    unique_symbols = {int(symbol.id): symbol for symbol in symbols if getattr(symbol, "id", None)}
    ordered_symbols = sorted(unique_symbols.values(), key=lambda item: (item.ticker, item.exchange, item.id))
    registry_issues = _registry_issues()

    supported_definitions = supported_sector_benchmarks()
    benchmark_pairs = [provider_symbol_parts(item.provider_symbol) for item in supported_definitions]
    benchmark_query = Q(pk__in=[])
    for ticker, exchange in benchmark_pairs:
        benchmark_query |= Q(ticker=ticker, exchange=exchange)
    benchmark_symbols = list(Symbol.objects.filter(benchmark_query)) if benchmark_pairs else []
    benchmark_by_provider_symbol = {
        f"{symbol.ticker}.{symbol.exchange}": symbol
        for symbol in benchmark_symbols
    }
    bar_ranges = {
        row["symbol_id"]: row
        for row in DailyBar.objects.filter(
            symbol_id__in=[symbol.id for symbol in benchmark_symbols],
            date__gte=coverage_start,
            date__lte=coverage_end,
        )
        .values("symbol_id")
        .annotate(first=Min("date"), last=Max("date"))
    }

    benchmark_status: dict[str, dict] = {}
    latest_start = min(coverage_end, coverage_start + timedelta(days=_BOUNDARY_TOLERANCE_DAYS))
    earliest_end = max(coverage_start, coverage_end - timedelta(days=_BOUNDARY_TOLERANCE_DAYS))
    for definition in supported_definitions:
        provider_symbol = str(definition.provider_symbol)
        benchmark_symbol = benchmark_by_provider_symbol.get(provider_symbol)
        bar_range = bar_ranges.get(benchmark_symbol.id) if benchmark_symbol else None
        ohlc_available = bool(
            bar_range
            and bar_range["first"] <= latest_start
            and bar_range["last"] >= earliest_end
        )
        benchmark_status[provider_symbol] = {
            "canonical_sector": definition.canonical_sector,
            "provider_symbol": provider_symbol,
            "symbol_id": benchmark_symbol.id if benchmark_symbol else None,
            "symbol_available": benchmark_symbol is not None,
            "ohlc_available": ohlc_available,
            "first_date": bar_range["first"].isoformat() if bar_range else None,
            "last_date": bar_range["last"].isoformat() if bar_range else None,
        }

    counts = {
        "symbols_considered": len(ordered_symbols),
        "members_with_sector_useful": 0,
        "members_with_supported_benchmark": 0,
        "members_with_usable_sector_gm": 0,
        "members_without_sector": 0,
        "members_with_generic_sector": 0,
        "members_with_unsupported_sector": 0,
        "members_with_missing_benchmark": 0,
        "members_with_missing_benchmark_ohlc": 0,
    }
    examples: dict[str, list[dict]] = {}
    used_benchmarks: set[str] = set()
    sectors_supported: set[str] = set()
    sectors_unsupported: set[str] = set()

    for symbol in ordered_symbols:
        resolution = resolve_csi300_sector_benchmark(symbol)
        if resolution.reason == SECTOR_REASON_MISSING:
            counts["members_without_sector"] += 1
            _append_example(examples, resolution.reason, symbol, resolution)
            continue
        if resolution.reason == SECTOR_REASON_GENERIC:
            counts["members_with_generic_sector"] += 1
            _append_example(examples, resolution.reason, symbol, resolution)
            continue

        counts["members_with_sector_useful"] += 1
        if resolution.reason == SECTOR_REASON_UNSUPPORTED:
            counts["members_with_unsupported_sector"] += 1
            if resolution.canonical_sector:
                sectors_unsupported.add(resolution.canonical_sector)
            _append_example(examples, resolution.reason, symbol, resolution)
            continue

        counts["members_with_supported_benchmark"] += 1
        sectors_supported.add(resolution.canonical_sector)
        status = benchmark_status.get(resolution.provider_symbol) or {}
        if not status.get("symbol_available"):
            counts["members_with_missing_benchmark"] += 1
            _append_example(examples, SECTOR_REASON_BENCHMARK_MISSING, symbol, resolution)
            continue
        if not status.get("ohlc_available"):
            counts["members_with_missing_benchmark_ohlc"] += 1
            _append_example(examples, SECTOR_REASON_BENCHMARK_OHLC_MISSING, symbol, resolution)
            continue
        counts["members_with_usable_sector_gm"] += 1
        used_benchmarks.add(resolution.provider_symbol)

    uncovered = counts["symbols_considered"] - counts["members_with_usable_sector_gm"]
    if registry_issues or counts["members_with_usable_sector_gm"] == 0:
        status = SECTOR_GM_NOT_READY
    elif uncovered:
        status = SECTOR_GM_READY_WITH_WARNINGS
    else:
        status = SECTOR_GM_READY

    missing_benchmarks = [
        provider_symbol for provider_symbol, item in benchmark_status.items() if not item["symbol_available"]
    ]
    benchmarks_without_ohlc = [
        provider_symbol
        for provider_symbol, item in benchmark_status.items()
        if item["symbol_available"] and not item["ohlc_available"]
    ]
    return {
        "status": status,
        "active_members_expected": int(active_members_expected or 0),
        **counts,
        "members_without_usable_sector_gm": uncovered,
        "coverage_ratio": (
            counts["members_with_usable_sector_gm"] / counts["symbols_considered"]
            if counts["symbols_considered"]
            else 0
        ),
        "supported_sector_count": len(supported_definitions),
        "supported_sectors_used": sorted(sectors_supported),
        "unsupported_sectors_present": sorted(sectors_unsupported),
        "benchmarks_available": sorted(
            provider_symbol for provider_symbol, item in benchmark_status.items() if item["ohlc_available"]
        ),
        "benchmarks_used": sorted(used_benchmarks),
        "benchmarks_missing": missing_benchmarks,
        "benchmarks_without_ohlc": benchmarks_without_ohlc,
        "benchmark_details": [benchmark_status[key] for key in sorted(benchmark_status)],
        "registry_issues": registry_issues,
        "examples": examples,
        "requires_confirmation": status == SECTOR_GM_READY_WITH_WARNINGS,
        "no_fallback": True,
    }
