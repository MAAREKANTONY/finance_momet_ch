from __future__ import annotations

from typing import Any

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
