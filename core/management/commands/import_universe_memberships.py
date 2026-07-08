from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from core.services.universe_import import UniverseImportError, import_universe_memberships_from_csv


def _optional_date(value: str | None) -> date | None:
    value = str(value or "").strip()
    if not value:
        return None
    return date.fromisoformat(value)


def write_import_summary(command: BaseCommand, result) -> None:
    mode = "apply" if not result.dry_run else "dry-run"
    command.stdout.write(
        "summary "
        f"mode={mode} "
        f"universe={result.universe_code} "
        f"universe_name={result.universe_name} "
        f"period={result.period_start}..{result.period_end} "
        f"rows={result.rows_read} "
        f"valid={result.rows_valid} "
        f"rejected={result.rows_rejected} "
        f"distinct_tickers={result.distinct_tickers} "
        f"exchanges={','.join(result.exchanges)} "
        f"valid_from_min={result.valid_from_min or ''} "
        f"valid_to_max={result.valid_to_max or ''} "
        f"open_memberships={result.open_memberships} "
        f"created={result.memberships_created} "
        f"updated={result.memberships_updated} "
        f"imported={result.imported_member_count} "
        f"mapped={result.mapped_member_count} "
        f"unmapped={result.unmapped_member_count} "
        f"coverage_days={result.coverage_days} "
        f"status={result.status} "
        f"batch_id={result.batch_id or ''}"
    )
    for warning in result.warnings:
        command.stdout.write(command.style.WARNING(f"warning: {warning}"))
    for error in result.errors:
        command.stdout.write(command.style.ERROR(f"error: {error}"))


class Command(BaseCommand):
    help = "Import historical universe memberships from a local CSV file. Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--csv", required=True, help="Path to the CSV file to import.")
        parser.add_argument("--universe-code", required=True, help="Universe/index code, e.g. CSI300.")
        parser.add_argument("--universe-name", default="", help="Display name for the UniverseDefinition.")
        parser.add_argument("--coverage-start", default="", help="Optional coverage start date, YYYY-MM-DD.")
        parser.add_argument("--coverage-end", default="", help="Optional coverage end date, YYYY-MM-DD.")
        parser.add_argument("--expected-member-count", type=int, default=1, help="Expected active members per date.")
        parser.add_argument("--provider", default="manual_csv", help="Provider label stored on the import batch.")
        parser.add_argument("--source-name", default="manual_csv", help="Source name stored on the import batch.")
        parser.add_argument("--source-reference", default="", help="Optional source reference stored on the import batch.")
        parser.add_argument("--dry-run", action="store_true", help="Validate without writing. This is the default.")
        parser.add_argument("--apply", action="store_true", help="Persist memberships, batch, and coverage snapshots.")

    def handle(self, *args, **options):
        if options["dry_run"] and options["apply"]:
            raise CommandError("--dry-run and --apply cannot be used together")

        try:
            result = import_universe_memberships_from_csv(
                csv_path=options["csv"],
                universe_code=options["universe_code"],
                universe_name=options["universe_name"],
                coverage_start=_optional_date(options["coverage_start"]),
                coverage_end=_optional_date(options["coverage_end"]),
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
