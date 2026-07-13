from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from core.services.dynamic_universe_symbols import (
    UniverseSymbolMappingError,
    ensure_universe_membership_symbols,
    format_universe_symbol_mapping_summary,
)
from core.services.universe_resolver import CSI300_UNIVERSE_CODE


class Command(BaseCommand):
    help = "Enrich CSI300 Symbol metadata from imported UniverseMembership CSV payloads. Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Report intended updates without writing. This is the default.")
        parser.add_argument("--apply", action="store_true", help="Persist provider symbols, symbol mappings, and missing Symbol metadata.")
        parser.add_argument(
            "--no-create-missing-symbols",
            action="store_true",
            help="Do not create missing local Symbol rows from CSI300 memberships.",
        )

    def handle(self, *args, **options):
        if options["dry_run"] and options["apply"]:
            raise CommandError("--dry-run and --apply cannot be used together")

        dry_run = not bool(options["apply"])
        create_missing = not bool(options["no_create_missing_symbols"])
        try:
            result = ensure_universe_membership_symbols(
                CSI300_UNIVERSE_CODE,
                create_missing=create_missing,
                dry_run=dry_run,
                enrich_metadata=True,
            )
        except UniverseSymbolMappingError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(format_universe_symbol_mapping_summary(result))
        self.stdout.write(
            "coverage "
            f"tickers_analyzed={result.distinct_symbols} "
            f"names_enriched={result.metadata_fields_updated.get('name', 0)} "
            f"countries_enriched={result.metadata_fields_updated.get('country', 0)} "
            f"currencies_enriched={result.metadata_fields_updated.get('currency', 0)} "
            f"sectors_enriched={result.metadata_fields_updated.get('sector', 0)} "
            f"industries_available={result.metadata_industries_available} "
            f"provider_symbols_created={result.provider_symbols_created} "
            f"unchanged={result.metadata_symbols_unchanged} "
            f"errors=0 "
            f"conflicts={len(result.provider_symbol_conflicts) + len(result.metadata_conflicts)} "
            f"dry_run={int(result.dry_run)}"
        )
        for sector, count in sorted(result.sector_counts.items()):
            self.stdout.write(f"sector {sector} symbols={count} source=UniverseMembership.source_payload")
        for warning in result.warnings:
            self.stdout.write(self.style.WARNING(f"warning: {warning}"))
        for conflict in result.provider_symbol_conflicts:
            self.stdout.write(self.style.WARNING(f"conflict: {conflict}"))
        for conflict in result.metadata_conflicts:
            self.stdout.write(self.style.WARNING(f"conflict: {conflict}"))
