from __future__ import annotations

import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from core.services.dynamic_universe_readiness import (
    CHECK_ERROR,
    CHECK_OK,
    CHECK_SKIPPED,
    CHECK_WARNING,
    REPORT_NOT_READY,
    check_dynamic_universe_readiness,
)


class Command(BaseCommand):
    help = "Check Dynamic Universe readiness without provider calls or database writes."

    def add_arguments(self, parser):
        parser.add_argument("--universe", default="SP500", help="Universe code. P0 supports SP500.")
        parser.add_argument("--start", dest="start_date", help="Start date, YYYY-MM-DD.")
        parser.add_argument("--end", dest="end_date", help="End date, YYYY-MM-DD.")
        parser.add_argument("--require-gm-market", action="store_true", help="Check GM_market benchmark DailyBars.")
        parser.add_argument("--require-gm-sector", action="store_true", help="Check GM_sector ETF DailyBars.")
        parser.add_argument("--warmup-days", type=int, default=0, help="Calendar warmup days before --start.")
        parser.add_argument("--scenario-id", type=int, default=None, help="Optional scenario id used to infer GM requirements.")
        parser.add_argument("--backtest-id", type=int, default=None, help="Optional backtest id used to infer dates, warmup and GM requirements.")
        parser.add_argument("--json", action="store_true", help="Print a stable JSON payload.")
        parser.add_argument("--strict-exit-code", action="store_true", help="Exit with code 1 when readiness is NOT_READY.")

    def handle(self, *args, **options):
        try:
            start = _parse_optional_date(options.get("start_date"), "--start")
            end = _parse_optional_date(options.get("end_date"), "--end")
            if options.get("backtest_id") is None and (start is None or end is None):
                raise CommandError("--start and --end are required unless --backtest-id supplies them.")
            if start is None:
                start = date.today()
            if end is None:
                end = start

            report = check_dynamic_universe_readiness(
                universe=options["universe"],
                start=start,
                end=end,
                warmup_days=options["warmup_days"],
                require_gm_market=options["require_gm_market"],
                require_gm_sector=options["require_gm_sector"],
                scenario_id=options.get("scenario_id"),
                backtest_id=options.get("backtest_id"),
            )
        except CommandError:
            raise
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        if options["json"]:
            self.stdout.write(json.dumps(report.as_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        else:
            self._write_human_report(report)

        if options["strict_exit_code"] and report.status == REPORT_NOT_READY:
            raise CommandError("Dynamic Universe readiness is NOT_READY.")

    def _write_human_report(self, report):
        self.stdout.write(
            f"Dynamic Universe readiness — {report.universe} — "
            f"{report.start.isoformat()} → {report.end.isoformat()}"
        )
        self.stdout.write("")
        for check in report.checks:
            self.stdout.write(f"[{_status_label(check.status)}] {check.label} : {check.message}")
        self.stdout.write("")
        self.stdout.write(f"Statut global : {report.status}")
        if report.suggested_actions:
            self.stdout.write("")
            self.stdout.write("Actions recommandées :")
            for action in report.suggested_actions:
                command = action.command
                if "{start}" in command or "{end}" in command:
                    coverage_start = report.metadata.get("coverage_start") or report.start.isoformat()
                    command = command.format(start=coverage_start, end=report.end.isoformat())
                self.stdout.write(f"- {command or action.label}")


def _parse_optional_date(value, label: str) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise CommandError(f"{label} must be YYYY-MM-DD.") from exc


def _status_label(status: str) -> str:
    return {
        CHECK_OK: "OK",
        CHECK_WARNING: "WARNING",
        CHECK_ERROR: "ERROR",
        CHECK_SKIPPED: "SKIPPED",
    }.get(status, status)
