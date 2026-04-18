from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

import logging

from .job_broker import broker_queue_snapshot
from .models import ProcessingJob

logger = logging.getLogger(__name__)


def _build_conflict_outcome(*, job_type: str, message: str, created_by=None, backtest=None, scenario=None, game_scenario=None, active_job: ProcessingJob) -> JobLaunchOutcome:
    err = ActiveJobConflictError(
        f"Another job is already active (job #{active_job.id}, {active_job.job_type}, {active_job.status})."
    )
    job = ProcessingJob.objects.create(
        job_type=job_type,
        status=ProcessingJob.Status.FAILED,
        backtest=backtest,
        scenario=scenario,
        game_scenario=game_scenario,
        created_by=created_by,
        message=message,
        error=str(err),
        finished_at=timezone.now(),
    )
    return JobLaunchOutcome(job=job, dispatch_error=err)


def launch_processing_job(
    *,
    task: Any,
    job_type: str,
    task_kwargs: Mapping[str, Any] | None = None,
    created_by=None,
    backtest=None,
    scenario=None,
    game_scenario=None,
    message: str = "En attente d'exécution",
) -> JobLaunchOutcome:
    """Create a tracked ProcessingJob then enqueue its Celery task after DB commit.

    Sprint P0.1 goals:
    - eliminate scattered create + delay + save(task_id) patterns
    - ensure the task is published only after the ProcessingJob row is committed
    - fail the job explicitly if broker publication raises synchronously
    """
    payload = dict(task_kwargs or {})
    outcome: dict[str, Any] = {}

    active_job = find_active_processing_job()
    if active_job is not None:
        from .job_recovery import recover_jobs

        recover_jobs(
            ids=[active_job.id],
            running_heartbeat_minutes=int(getattr(settings, "JOB_STALE_HEARTBEAT_MINUTES", 15)),
            running_started_minutes=int(getattr(settings, "JOB_STALE_STARTED_MINUTES", 30)),
            pending_minutes=int(getattr(settings, "JOB_STALE_PENDING_MINUTES", 60)),
            requested_stop_minutes=int(getattr(settings, "JOB_REQUESTED_STOP_STALE_MINUTES", 3)),
            include_pending=True,
            include_requested_pending=True,
            dry_run=False,
            sync_recent_terminal=True,
        )
        active_job = find_active_processing_job()

    if active_job is not None:
        return _build_conflict_outcome(
            job_type=job_type,
            message=message,
            created_by=created_by,
            backtest=backtest,
            scenario=scenario,
            game_scenario=game_scenario,
            active_job=active_job,
        )

    job = ProcessingJob.objects.create(
        job_type=job_type,
        status=ProcessingJob.Status.PENDING,
        backtest=backtest,
        scenario=scenario,
        game_scenario=game_scenario,
        created_by=created_by,
        message=message,
    )

    def _enqueue() -> None:
        before_snapshot = broker_queue_snapshot(sample_limit=2)
        logger.warning(
            "[job-launch] enqueue start job_id=%s job_type=%s queue=%s queue_len_before=%s samples=%s",
            job.id,
            job_type,
            before_snapshot.queue_name,
            before_snapshot.length,
            before_snapshot.samples,
        )
        try:
            async_result = task.apply_async(kwargs={**payload, "job_id": job.id})
        except Exception as exc:  # pragma: no cover - exercised via tests with mock side effects
            ProcessingJob.objects.filter(id=job.id).update(
                status=ProcessingJob.Status.FAILED,
                error=f"Task dispatch failed: {exc}",
                finished_at=timezone.now(),
            )
            logger.exception("[job-launch] enqueue failed job_id=%s job_type=%s", job.id, job_type)
            outcome["dispatch_error"] = exc
            return

        task_id = (getattr(async_result, "id", "") or "")[:64]
        ProcessingJob.objects.filter(id=job.id).update(task_id=task_id)
        after_snapshot = broker_queue_snapshot(sample_limit=2)
        logger.warning(
            "[job-launch] enqueue success job_id=%s job_type=%s task_id=%s queue=%s queue_len_after=%s samples=%s",
            job.id,
            job_type,
            task_id,
            after_snapshot.queue_name,
            after_snapshot.length,
            after_snapshot.samples,
        )
        outcome["task_id"] = task_id

    transaction.on_commit(_enqueue)

    job.refresh_from_db()
    return JobLaunchOutcome(job=job, dispatch_error=outcome.get("dispatch_error"))


def dispatch_task_after_commit(
    *,
    task: Any,
    task_args: list[Any] | tuple[Any, ...] | None = None,
    task_kwargs: Mapping[str, Any] | None = None,
) -> TaskDispatchOutcome:
    """Publish an untracked Celery task only after the surrounding DB transaction commits."""
    args = list(task_args or [])
    kwargs = dict(task_kwargs or {})
    outcome: dict[str, Any] = {}

    def _enqueue() -> None:
        try:
            async_result = task.apply_async(args=args, kwargs=kwargs)
        except Exception as exc:  # pragma: no cover - exercised through mocks/tests
            outcome["dispatch_error"] = exc
            return

        outcome["task_id"] = (getattr(async_result, "id", "") or "")[:64]

    transaction.on_commit(_enqueue)
    return TaskDispatchOutcome(
        task_id=str(outcome.get("task_id", "")),
        dispatch_error=outcome.get("dispatch_error"),
    )
