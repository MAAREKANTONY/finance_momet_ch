from __future__ import annotations

from django.core.management.base import BaseCommand

from core.job_recovery import recover_jobs


class Command(BaseCommand):
    help = "Recover stale/zombie ProcessingJob rows and resync related business objects."

    def add_arguments(self, parser):
        parser.add_argument("--ids", help="Comma-separated ProcessingJob ids to recover.")
        parser.add_argument("--running-heartbeat-minutes", type=int, default=20)
        parser.add_argument("--running-started-minutes", type=int, default=45)
        parser.add_argument("--pending-minutes", type=int, default=90)
        parser.add_argument("--no-pending", action="store_true", help="Do not recover old PENDING jobs.")
        parser.add_argument(
            "--no-requested-pending",
            action="store_true",
            help="Do not auto-recover PENDING jobs already marked cancel_requested/kill_requested.",
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--no-sync-terminal", action="store_true")

    def handle(self, *args, **opts):
        ids_raw = (opts.get("ids") or "").strip()
        ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip()] if ids_raw else None

        decisions, stats = recover_jobs(
            ids=ids,
            running_heartbeat_minutes=int(opts.get("running_heartbeat_minutes") or 20),
            running_started_minutes=int(opts.get("running_started_minutes") or 45),
            pending_minutes=int(opts.get("pending_minutes") or 90),
            include_pending=not bool(opts.get("no_pending")),
            include_requested_pending=not bool(opts.get("no_requested_pending")),
            dry_run=bool(opts.get("dry_run")),
            sync_recent_terminal=not bool(opts.get("no_sync_terminal")),
        )

        self.stdout.write(f"Matched jobs: {stats.matched}")
        for job, decision in decisions[:200]:
            self.stdout.write(
                f" - #{job.id} {job.job_type} {job.status} -> {decision.status} | {decision.reason}"
            )

        if opts.get("dry_run"):
            self.stdout.write(self.style.WARNING("Dry-run only: no database row was changed."))
            if stats.synced_terminal:
                self.stdout.write(f"Related objects re-synced: {stats.synced_terminal}")
            return

        self.stdout.write(
            self.style.SUCCESS(
                "Recovered jobs: "
                f"updated={stats.updated} failed={stats.failed} cancelled={stats.cancelled} killed={stats.killed}"
            )
        )
        self.stdout.write(f"Related objects re-synced: {stats.synced_terminal}")
