from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from core.services.provider_eodhd import EODHDError
from core.services.sp500_symbol_bootstrap import bootstrap_sp500_symbols_from_eodhd
from core.services.universe_import import UniverseImportError


class Command(BaseCommand):
    help = "Bootstrap missing local Symbols from EODHD S&P500 historical components. Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--coverage-start", required=True, help="Coverage start date, YYYY-MM-DD.")
        parser.add_argument("--coverage-end", required=True, help="Coverage end date, YYYY-MM-DD.")
        parser.add_argument("--apply", action="store_true", help="Create missing Symbols.")

    def handle(self, *args, **options):
        try:
            result = bootstrap_sp500_symbols_from_eodhd(
                coverage_start=date.fromisoformat(options["coverage_start"]),
                coverage_end=date.fromisoformat(options["coverage_end"]),
                dry_run=not bool(options["apply"]),
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        except (UniverseImportError, EODHDError) as exc:
            raise CommandError(str(exc)) from exc

        mode = "apply" if not result.dry_run else "dry-run"
        self.stdout.write(
            "summary "
            f"mode={mode} "
            f"period={result.period_start}..{result.period_end} "
            f"provider_records={result.provider_records} "
            f"retained={result.records_retained} "
            f"existing={result.existing} "
            f"to_create={result.to_create} "
            f"created={result.created} "
            f"skipped={result.skipped} "
            f"warnings={len(result.warnings)}"
        )
        if result.create_examples:
            self.stdout.write(f"examples_to_create={','.join(result.create_examples)}")
        for warning in result.warnings:
            self.stdout.write(self.style.WARNING(f"warning: {warning}"))
