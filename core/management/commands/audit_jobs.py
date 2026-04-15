from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from core.job_recovery import audit_jobs


class Command(BaseCommand):
    help = "Audit active ProcessingJob rows and report suspicious/stale jobs without modifying the database."

    def add_arguments(self, parser):
        parser.add_argument("--ids", help="Comma-separated ProcessingJob ids to audit.")
        parser.add_argument(
            "--stale-minutes",
            type=int,
            help="Shortcut threshold applied to running heartbeat and pending age. Running jobs without heartbeat use max(stale, stale*2).",
        )
        parser.add_argument("--running-heartbeat-minutes", type=int, default=20)
        parser.add_argument("--running-started-minutes", type=int, default=45)
        parser.add_argument("--pending-minutes", type=int, default=90)
        parser.add_argument("--no-pending", action="store_true", help="Exclude PENDING jobs from the audit.")
        parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")

    def handle(self, *args, **opts):
        ids_raw = (opts.get("ids") or "").strip()
        ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip()] if ids_raw else None

        stale_minutes = opts.get("stale_minutes")
        running_heartbeat_minutes = int(opts.get("running_heartbeat_minutes") or 20)
        running_started_minutes = int(opts.get("running_started_minutes") or 45)
        pending_minutes = int(opts.get("pending_minutes") or 90)
        if stale_minutes:
            stale_minutes = max(1, int(stale_minutes))
            running_heartbeat_minutes = stale_minutes
            running_started_minutes = max(stale_minutes * 2, stale_minutes)
            pending_minutes = stale_minutes

        findings, stats = audit_jobs(
            ids=ids,
            running_heartbeat_minutes=running_heartbeat_minutes,
            running_started_minutes=running_started_minutes,
            pending_minutes=pending_minutes,
            include_pending=not bool(opts.get("no_pending")),
        )

        if opts.get("json"):
            payload = {
                "audited": stats.audited,
                "healthy": stats.healthy,
                "suspect": stats.suspect,
                "critical": stats.critical,
                "thresholds": {
                    "running_heartbeat_minutes": running_heartbeat_minutes,
                    "running_started_minutes": running_started_minutes,
                    "pending_minutes": pending_minutes,
                    "include_pending": not bool(opts.get("no_pending")),
                },
                "findings": [
                    {
                        "job_id": f.job_id,
                        "job_type": f.job_type,
                        "status": f.status,
                        "severity": f.severity,
                        "category": f.category,
                        "summary": f.summary,
                        "stale_seconds": f.stale_seconds,
                        "task_id": f.task_id,
                        "worker_hostname": f.worker_hostname,
                        "checkpoint": f.checkpoint,
                    }
                    for f in findings
                ],
            }
            self.stdout.write(json.dumps(payload, indent=2, sort_keys=True))
            return

        self.stdout.write(
            "Audit summary: "
            f"audited={stats.audited} healthy={stats.healthy} suspect={stats.suspect} critical={stats.critical}"
        )
        self.stdout.write(
            "Thresholds: "
            f"running-heartbeat={running_heartbeat_minutes}m "
            f"running-started={running_started_minutes}m "
            f"pending={pending_minutes}m"
        )
        if not findings:
            self.stdout.write(self.style.SUCCESS("No suspicious active jobs found."))
            return

        for finding in findings[:500]:
            checkpoint = finding.checkpoint or "-"
            worker = finding.worker_hostname or "-"
            task_id = finding.task_id or "-"
            self.stdout.write(
                f" - #{finding.job_id} [{finding.severity.upper()}] {finding.job_type} {finding.status} "
                f"category={finding.category} stale={finding.stale_seconds}s checkpoint={checkpoint} "
                f"worker={worker} task={task_id} | {finding.summary}"
            )
