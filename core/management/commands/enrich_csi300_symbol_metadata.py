from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from core.services.csi300_eodhd_metadata import (
    GENERIC_SECTOR_VALUES,
    enrich_csi300_symbols_from_eodhd_metadata,
    format_csi300_eodhd_metadata_summary,
)
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
            "--source",
            choices=("csv", "eodhd"),
            default="csv",
            help="Metadata source. csv uses imported membership payloads; eodhd fetches Fundamentals General.",
        )
        parser.add_argument(
            "--ticker",
            action="append",
            default=[],
            help="Limit EODHD mode to one local ticker. Can be repeated or comma-separated.",
        )
        parser.add_argument("--limit", type=int, default=0, help="Limit the number of CSI300 symbols processed in EODHD mode.")
        parser.add_argument(
            "--no-create-missing-symbols",
            action="store_true",
            help="Do not create missing local Symbol rows from CSI300 memberships in csv mode.",
        )

    def handle(self, *args, **options):
        if options["dry_run"] and options["apply"]:
            raise CommandError("--dry-run and --apply cannot be used together")

        dry_run = not bool(options["apply"])
        source = options["source"]
        if source == "eodhd":
            self._handle_eodhd(dry_run=dry_run, options=options)
            return

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

    def _handle_eodhd(self, *, dry_run: bool, options):
        if options["no_create_missing_symbols"]:
            raise CommandError("--no-create-missing-symbols is only supported with --source csv")
        tickers = _parse_tickers(options.get("ticker") or [])
        result = enrich_csi300_symbols_from_eodhd_metadata(
            tickers=tickers,
            limit=options.get("limit") or None,
            dry_run=dry_run,
        )
        self.stdout.write(format_csi300_eodhd_metadata_summary(result))
        self.stdout.write(
            "coverage "
            f"tickers_analyzed={result.processed} "
            f"names_enriched={result.field_updates.get('name_en', 0)} "
            f"english_names_present={result.english_names_present} "
            f"english_names_useful={result.english_names_useful} "
            f"english_names_to_create={result.english_names_to_create} "
            f"english_names_created={result.english_names_created} "
            f"english_names_unchanged={result.english_names_unchanged} "
            f"english_names_preserved={result.english_names_preserved} "
            f"english_names_missing={result.english_names_missing} "
            f"english_names_rejected={result.english_names_rejected} "
            f"countries_enriched={result.field_updates.get('country', 0)} "
            f"currencies_enriched={result.field_updates.get('currency', 0)} "
            f"sectors_enriched={result.field_updates.get('sector', 0)} "
            f"industries_available={result.industries_present} "
            f"missing_sector={result.missing_sector} "
            f"generic_sector={result.generic_sector} "
            f"unchanged={result.unchanged} "
            f"errors={result.errors} "
            f"dry_run={int(result.dry_run)}"
        )
        for sector, count in sorted(result.raw_sector_counts.items()):
            decision = "ignored_generic" if sector.strip().upper() in GENERIC_SECTOR_VALUES else "usable_raw"
            self.stdout.write(f"raw_sector {sector} symbols={count} decision={decision}")
        for sector, count in sorted(result.applied_sector_counts.items()):
            self.stdout.write(f"applied_sector {sector} symbols={count} source=eodhd_fundamentals_general")
        for detail in result.per_symbol:
            if detail.get("error"):
                self.stdout.write(self.style.WARNING(f"{detail['symbol']}: {detail['error']}"))
            elif detail.get("updated_fields"):
                self.stdout.write(
                    f"{detail['symbol']}: fields={','.join(detail['updated_fields'])} "
                    f"provider_symbol={detail['provider_symbol']} "
                    f"name_en_candidate={detail.get('english_name_candidate', '')}"
                )


def _parse_tickers(values) -> list[str]:
    tickers: list[str] = []
    for value in values:
        for item in str(value or "").split(","):
            item = item.strip().upper()
            if item:
                tickers.append(item)
    return list(dict.fromkeys(tickers))
