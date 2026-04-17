from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from core.job_broker import broker_queue_snapshot, purge_broker_queue
from core.job_recovery import recover_jobs


class Command(BaseCommand):
    help = "Inspect/purge the Celery Redis queue and recover stale tracked jobs in one explicit operator command."

    def add_arguments(self, parser):
        parser.add_argument("--queue", default=getattr(settings, "CELERY_TASK_DEFAULT_QUEUE", "celery"))
        parser.add_argument("--sample-limit", type=int, default=5)
        parser.add_argument("--purge-broker", action="store_true")
        parser.add_argument("--force", action="store_true", help="Required with --purge-broker to actually delete Redis queue messages.")
        parser.add_argument("--recover", action="store_true", help="Run tracked-job recovery after inspection/purge.")
        parser.add_argument("--running-heartbeat-minutes", type=int, default=20)
        parser.add_argument("--running-started-minutes", type=int, default=45)
        parser.add_argument("--pending-minutes", type=int, default=90)
        parser.add_argument("--requested-stop-minutes", type=int, default=3)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **opts):
        queue_name = (opts.get("queue") or "celery").strip() or "celery"
        sample_limit = max(0, int(opts.get("sample_limit") or 0))

        snapshot = broker_queue_snapshot(queue_name=queue_name, sample_limit=sample_limit)
        self.stdout.write(f"Queue: {snapshot.queue_name}")
        if snapshot.error:
            self.stdout.write(self.style.WARNING(f"Broker inspection error: {snapshot.error}"))
        else:
            self.stdout.write(f"Queue length before: {snapshot.length}")
            for sample in snapshot.samples:
                self.stdout.write(f" - {sample}")

        if opts.get("purge_broker"):
            if not opts.get("force"):
                raise CommandError("--purge-broker is destructive; rerun with --force.")
            if opts.get("dry_run"):
                self.stdout.write(self.style.WARNING("Dry-run: broker queue not purged."))
            else:
                removed = purge_broker_queue(queue_name=queue_name)
                self.stdout.write(self.style.SUCCESS(f"Broker queue purged: removed {removed} queued message(s)."))

        if opts.get("recover"):
            decisions, stats = recover_jobs(
                running_heartbeat_minutes=int(opts.get("running_heartbeat_minutes") or 20),
                running_started_minutes=int(opts.get("running_started_minutes") or 45),
                pending_minutes=int(opts.get("pending_minutes") or 90),
                requested_stop_minutes=int(opts.get("requested_stop_minutes") or 3),
                include_pending=True,
                include_requested_pending=True,
                dry_run=bool(opts.get("dry_run")),
                sync_recent_terminal=True,
            )
            self.stdout.write(f"Tracked jobs matched: {stats.matched}")
            for job, decision in decisions[:50]:
                self.stdout.write(f" - #{job.id} {job.job_type} {job.status} -> {decision.status} | {decision.reason}")
            if opts.get("dry_run"):
                self.stdout.write(self.style.WARNING("Dry-run: tracked jobs not modified."))
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Tracked jobs recovered: updated={stats.updated} failed={stats.failed} cancelled={stats.cancelled} killed={stats.killed}"
                    )
                )
                self.stdout.write(f"Related objects re-synced: {stats.synced_terminal}")

        final_snapshot = broker_queue_snapshot(queue_name=queue_name, sample_limit=sample_limit)
        if final_snapshot.error:
            self.stdout.write(self.style.WARNING(f"Queue length after: unavailable ({final_snapshot.error})"))
        else:
            self.stdout.write(f"Queue length after: {final_snapshot.length}")
