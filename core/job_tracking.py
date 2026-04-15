from __future__ import annotations

from typing import Any
import time

from django.utils import timezone

from .models import ProcessingJob


class JobCancelled(Exception):
    """Raised when a ProcessingJob has cancel_requested=True."""


class JobKilled(Exception):
    """Raised when a ProcessingJob has kill_requested=True."""


def extract_worker_hostname(task_request: Any | None = None) -> str:
    """Best-effort extraction of the Celery worker hostname."""
    if task_request is None:
        return ""
    for attr in ("hostname", "origin"):
        value = getattr(task_request, attr, "") or ""
        value = str(value).strip()
        if value:
            return value[:255]
    delivery_info = getattr(task_request, "delivery_info", None) or {}
    for key in ("hostname", "consumer_tag"):
        value = delivery_info.get(key, "")
        value = str(value).strip()
        if value:
            return value[:255]
    return ""


def mark_job_running(job: ProcessingJob | None, *, task_request: Any | None = None, message: str | None = None) -> ProcessingJob | None:
    """Mark a tracked job as RUNNING and stamp first visibility metadata."""
    if job is None or not getattr(job, "id", None):
        return job
    now = timezone.now()
    hostname = extract_worker_hostname(task_request)
    checkpoint = (message or "started").strip()[:255]
    ProcessingJob.objects.filter(id=job.id).update(
        status=ProcessingJob.Status.RUNNING,
        task_id=(getattr(task_request, "id", "") or getattr(job, "task_id", "") or "")[:64],
        started_at=now,
        heartbeat_at=now,
        last_checkpoint=checkpoint,
        worker_hostname=hostname,
        **({"message": message} if message is not None else {}),
    )
    job.refresh_from_db(fields=[
        "status", "task_id", "started_at", "heartbeat_at", "last_checkpoint", "worker_hostname", "message"
    ])
    return job


def job_checkpoint(
    job: ProcessingJob | None,
    *,
    checkpoint: str | None = None,
    task_request: Any | None = None,
    refresh_flags: bool = True,
    heartbeat: bool = True,
) -> None:
    """Cooperative checkpoint used by long-running Celery jobs.

    Effects:
    - refresh cancel/kill flags from DB
    - raise JobCancelled / JobKilled when requested
    - stamp heartbeat_at, last_checkpoint and worker_hostname for operational visibility
    """
    if job is None or not getattr(job, "id", None):
        return

    if refresh_flags:
        job.refresh_from_db(fields=["cancel_requested", "kill_requested", "status"])
    if getattr(job, "kill_requested", False):
        raise JobKilled("kill requested")
    if getattr(job, "cancel_requested", False):
        raise JobCancelled("cancel requested")

    updates: dict[str, Any] = {}
    if heartbeat:
        updates["heartbeat_at"] = timezone.now()
    if checkpoint:
        updates["last_checkpoint"] = str(checkpoint).strip()[:255]
    hostname = extract_worker_hostname(task_request)
    if hostname:
        updates["worker_hostname"] = hostname
    if updates:
        ProcessingJob.objects.filter(id=job.id).update(**updates)
        if "heartbeat_at" in updates:
            job.heartbeat_at = updates["heartbeat_at"]
        if "last_checkpoint" in updates:
            job.last_checkpoint = updates["last_checkpoint"]
        if "worker_hostname" in updates:
            job.worker_hostname = updates["worker_hostname"]


class JobCheckpointPulse:
    """Throttle cooperative checkpoints by iterations and elapsed seconds.

    Goal:
    - keep cancel/kill reactive enough for long loops
    - avoid hammering PostgreSQL with a heartbeat on every row
    """

    def __init__(
        self,
        job: ProcessingJob | None,
        *,
        every_n: int = 100,
        every_seconds: float = 15.0,
        task_request: Any | None = None,
        base_label: str = "",
    ) -> None:
        self.job = job
        self.every_n = max(1, int(every_n or 1))
        self.every_seconds = max(0.0, float(every_seconds or 0.0))
        self.task_request = task_request
        self.base_label = (base_label or "").strip()
        self._count = 0
        self._first_ts: float | None = None
        self._last_ts: float | None = None

    def hit(self, *, checkpoint: str | None = None, force: bool = False, heartbeat: bool = True) -> bool:
        if self.job is None or not getattr(self.job, "id", None):
            return False
        self._count += 1
        now_ts = time.monotonic()
        if self._first_ts is None:
            self._first_ts = now_ts
        should_emit = bool(force)
        if not should_emit and self.every_n > 0 and (self._count % self.every_n == 0):
            should_emit = True
        time_anchor = self._last_ts if self._last_ts is not None else self._first_ts
        if not should_emit and self.every_seconds > 0 and time_anchor is not None and (now_ts - time_anchor) >= self.every_seconds:
            should_emit = True
        if not should_emit:
            return False
        label = (checkpoint or "").strip()
        if self.base_label and label:
            label = f"{self.base_label}:{label}"
        elif self.base_label:
            label = self.base_label
        job_checkpoint(
            self.job,
            checkpoint=label or None,
            task_request=self.task_request,
            heartbeat=heartbeat,
        )
        self._last_ts = now_ts
        return True
