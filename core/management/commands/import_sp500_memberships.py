from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from core.management.commands.import_universe_memberships import write_import_summary
from core.services.universe_import import UniverseImportError, import_universe_memberships_from_csv


class Command(BaseCommand):
    help = "Import historical S&P500 memberships from a local CSV file. Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Path to the CSV file to import.")
        parser.add_argument("--coverage-start", required=True, help="Coverage start date, YYYY-MM-DD.")
        parser.add_argument("--coverage-end", required=True, help="Coverage end date, YYYY-MM-DD.")
        parser.add_argument("--expected-member-count", type=int, default=500, help="Expected active members per date.")
        parser.add_argument("--provider", default="manual_csv", help="Provider label stored on the import batch.")
        parser.add_argument("--source-name", default="manual_csv", help="Source name stored on the import batch.")
        parser.add_argument("--source-reference", default="", help="Optional source reference stored on the import batch.")
        parser.add_argument("--apply", action="store_true", help="Persist memberships, batch, and coverage snapshots.")

    def handle(self, *args, **options):
        try:
            result = import_universe_memberships_from_csv(
                csv_path=options["file"],
                universe_code="SP500",
                universe_name="S&P 500",
                coverage_start=date.fromisoformat(options["coverage_start"]),
                coverage_end=date.fromisoformat(options["coverage_end"]),
                provider=options["provider"],
                source_name=options["source_name"],
                source_reference=options["source_reference"],
                expected_member_count=options["expected_member_count"],
                dry_run=not bool(options["apply"]),
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        except UniverseImportError as exc:
            raise CommandError(str(exc)) from exc

        write_import_summary(self, result)
