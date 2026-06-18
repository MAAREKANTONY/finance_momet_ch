from __future__ import annotations

from dataclasses import dataclass

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count

from core.models import (
    DailyBar,
    HistoricalMarketCap,
    ProcessingJob,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseDefinition,
    UniverseMembership,
)
from core.services.universe_resolver import SP500_UNIVERSE_CODE


SP500_DEFAULTS = {
    "name": "S&P 500",
    "description": "Historical S&P 500 universe definition. Memberships are imported separately.",
    "source": "reference_data",
    "active": True,
    "metadata": {"provider": "eodhd", "scope": "dynamic_universe_v1"},
}

REFERENCE_ETF_EXCHANGE = "NYSE ARCA"
REFERENCE_ETF_DEFAULTS = {
    "exchange": REFERENCE_ETF_EXCHANGE,
    "instrument_type": "ETF",
    "country": "US",
    "currency": "USD",
    "active": True,
}


@dataclass(frozen=True)
class ReferenceSymbol:
    ticker: str
    name: str
    group: str

    def defaults(self) -> dict[str, str | bool]:
        return {**REFERENCE_ETF_DEFAULTS, "name": self.name}


US_MARKET_ETFS = [
    ReferenceSymbol("SPY", "SPDR S&P 500 ETF Trust", "US market ETFs"),
    ReferenceSymbol("QQQ", "Invesco QQQ Trust", "US market ETFs"),
    ReferenceSymbol("DIA", "SPDR Dow Jones Industrial Average ETF Trust", "US market ETFs"),
    ReferenceSymbol("IWM", "iShares Russell 2000 ETF", "US market ETFs"),
]

US_SECTOR_ETFS = [
    ReferenceSymbol("XLK", "Technology Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLF", "Financial Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLE", "Energy Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLV", "Health Care Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLY", "Consumer Discretionary Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLP", "Consumer Staples Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLI", "Industrial Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLB", "Materials Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLU", "Utilities Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLRE", "Real Estate Select Sector SPDR Fund", "US sector ETFs"),
    ReferenceSymbol("XLC", "Communication Services Select Sector SPDR Fund", "US sector ETFs"),
]

EUROPE_ETFS = [
    ReferenceSymbol("VGK", "Vanguard FTSE Europe ETF", "Europe ETFs"),
    ReferenceSymbol("FEZ", "SPDR EURO STOXX 50 ETF", "Europe ETFs"),
    ReferenceSymbol("EZU", "iShares MSCI Eurozone ETF", "Europe ETFs"),
    ReferenceSymbol("EWU", "iShares MSCI United Kingdom ETF", "Europe ETFs"),
    ReferenceSymbol("EWQ", "iShares MSCI France ETF", "Europe ETFs"),
    ReferenceSymbol("EWG", "iShares MSCI Germany ETF", "Europe ETFs"),
    ReferenceSymbol("EWI", "iShares MSCI Italy ETF", "Europe ETFs"),
    ReferenceSymbol("EWP", "iShares MSCI Spain ETF", "Europe ETFs"),
    ReferenceSymbol("EWN", "iShares MSCI Netherlands ETF", "Europe ETFs"),
    ReferenceSymbol("EWD", "iShares MSCI Sweden ETF", "Europe ETFs"),
    ReferenceSymbol("EWL", "iShares MSCI Switzerland ETF", "Europe ETFs"),
]

REFERENCE_SYMBOL_GROUPS = [
    ("US market ETFs", US_MARKET_ETFS),
    ("US sector ETFs", US_SECTOR_ETFS),
    ("Europe ETFs", EUROPE_ETFS),
]


def _ensure_symbol(reference: ReferenceSymbol, *, dry_run: bool) -> str:
    defaults = reference.defaults()
    symbol = Symbol.objects.filter(ticker=reference.ticker, exchange=defaults["exchange"]).first()
    if symbol is None:
        if not dry_run:
            Symbol.objects.create(ticker=reference.ticker, **defaults)
            return "created"
        return "would_create"

    update_fields: list[str] = []
    for field in ("name", "instrument_type", "country", "currency"):
        if not str(getattr(symbol, field) or "").strip() and defaults.get(field):
            setattr(symbol, field, defaults[field])
            update_fields.append(field)
    if not symbol.active:
        symbol.active = True
        update_fields.append("active")

    if update_fields and not dry_run:
        symbol.save(update_fields=update_fields)
        return "updated"
    if update_fields and dry_run:
        return "would_update"
    return "existing"


def _status_counts() -> dict[str, int]:
    return {
        "memberships": UniverseMembership.objects.count(),
        "dailybars": DailyBar.objects.count(),
        "marketcaps": HistoricalMarketCap.objects.count(),
        "jobs": ProcessingJob.objects.count(),
    }


class Command(BaseCommand):
    help = "Initialize minimal idempotent reference data for a fresh StockAlert database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show the reference data changes that would be applied without writing to the database.",
        )

    def handle(self, *args, **options):
        dry_run = bool(options.get("dry_run"))
        symbol_stats: dict[str, dict[str, int]] = {}

        with transaction.atomic():
            universe = UniverseDefinition.objects.filter(code=SP500_UNIVERSE_CODE).first()
            if universe is None:
                if dry_run:
                    self.stdout.write("SP500 UniverseDefinition would be created active=True")
                    universe_id = None
                else:
                    universe = UniverseDefinition.objects.create(
                        code=SP500_UNIVERSE_CODE,
                        **SP500_DEFAULTS,
                    )
                    universe_id = universe.id
                    self.stdout.write(self.style.SUCCESS("SP500 UniverseDefinition created active=True"))
            elif not universe.active:
                universe_id = universe.id
                if dry_run:
                    self.stdout.write("SP500 UniverseDefinition would be reactivated")
                else:
                    universe.active = True
                    universe.save(update_fields=["active", "updated_at"])
                    self.stdout.write(self.style.SUCCESS("SP500 UniverseDefinition reactivated"))
            else:
                universe_id = universe.id
                self.stdout.write("SP500 UniverseDefinition already exists active=True")

            for group_name, references in REFERENCE_SYMBOL_GROUPS:
                counts = {"created": 0, "existing": 0, "updated": 0, "would_create": 0, "would_update": 0}
                for reference in references:
                    status = _ensure_symbol(reference, dry_run=dry_run)
                    counts[status] = counts.get(status, 0) + 1
                symbol_stats[group_name] = counts
                self.stdout.write(
                    f"{group_name}: "
                    f"created={counts.get('created', 0)} "
                    f"existing={counts.get('existing', 0)} "
                    f"updated={counts.get('updated', 0)} "
                    f"would_create={counts.get('would_create', 0)} "
                    f"would_update={counts.get('would_update', 0)}"
                )

            if dry_run:
                transaction.set_rollback(True)

        persisted_universe = UniverseDefinition.objects.filter(code=SP500_UNIVERSE_CODE).first()
        membership_count = (
            UniverseMembership.objects.filter(universe=persisted_universe).count()
            if persisted_universe
            else 0
        )
        coverage_counts = {}
        if persisted_universe:
            coverage_counts = {
                row["status"]: row["count"]
                for row in UniverseCoverageSnapshot.objects.filter(universe=persisted_universe)
                .values("status")
                .annotate(count=Count("id"))
                .order_by("status")
            }

        total_reference_symbols = sum(len(references) for _, references in REFERENCE_SYMBOL_GROUPS)
        heavy_counts = _status_counts()
        self.stdout.write(
            "summary "
            f"mode={'dry-run' if dry_run else 'apply'} "
            f"universe={SP500_UNIVERSE_CODE} "
            f"universe_id={universe_id or ''} "
            f"reference_symbols={total_reference_symbols} "
            f"memberships={membership_count} "
            f"coverage={coverage_counts or '{}'} "
            f"dailybars={heavy_counts['dailybars']} "
            f"marketcaps={heavy_counts['marketcaps']} "
            f"jobs={heavy_counts['jobs']}"
        )
        self.stdout.write(
            "note init_reference_data creates only minimal reference definitions and common market/sector ETF symbols; "
            "it does not import S&P500 memberships, validate coverage, fetch OHLC, create market caps, run jobs, or call providers."
        )
