from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from core.models import ProcessingJob


class Command(BaseCommand):
    help = "Mark stuck ProcessingJob rows as FAILED/CANCELLED/KILLED for manual cleanup."

    def add_arguments(self, parser):
        parser.add_argument("--ids", help="Comma-separated ProcessingJob ids to update.")
        parser.add_argument("--status", default="FAILED", choices=["FAILED", "CANCELLED", "KILLED"])
        parser.add_argument("--older-than-minutes", type=int, default=0)
        parser.add_argument("--include-pending", action="store_true")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--reason", default="Manual cleanup command")

    def handle(self, *args, **opts):
        qs = ProcessingJob.objects.all()
        ids_raw = (opts.get("ids") or "").strip()
        if ids_raw:
            ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip()]
            qs = qs.filter(id__in=ids)
        else:
            statuses = [ProcessingJob.Status.RUNNING]
            if opts.get("include_pending"):
                statuses.append(ProcessingJob.Status.PENDING)
            qs = qs.filter(status__in=statuses)
            older = int(opts.get("older_than_minutes") or 0)
            if older > 0:
                cutoff = timezone.now() - timedelta(minutes=older)
                qs = qs.filter(created_at__lt=cutoff)

        count = qs.count()
        self.stdout.write(f"Matched jobs: {count}")
        if opts.get("dry_run"):
            for job in qs.only("id", "job_type", "status", "task_id")[:100]:
                self.stdout.write(f" - #{job.id} {job.job_type} {job.status} task_id={job.task_id}")
            return

        updated = qs.update(
            status=opts["status"],
            finished_at=timezone.now(),
            error=opts.get("reason") or "Manual cleanup command",
            cancel_requested=False,
            kill_requested=False,
        )
        self.stdout.write(self.style.SUCCESS(f"Updated jobs: {updated}"))
