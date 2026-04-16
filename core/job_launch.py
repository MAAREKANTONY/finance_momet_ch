from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .models import ProcessingJob


def find_active_processing_job(*, job_type: str | None = None, game_scenario=None, backtest=None) -> ProcessingJob | None:
    """Return the most recent active tracked job matching the provided owner filters."""
    qs = ProcessingJob.objects.filter(status__in=[ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING])
    if job_type:
        qs = qs.filter(job_type=job_type)
    if game_scenario is not None:
        game_id = getattr(game_scenario, "id", game_scenario)
        qs = qs.filter(game_scenario_id=game_id)
    if backtest is not None:
        backtest_id = getattr(backtest, "id", backtest)
        qs = qs.filter(backtest_id=backtest_id)
    return qs.order_by("-id").only("id", "status", "job_type", "game_scenario_id", "backtest_id", "created_at").first()



@dataclass(slots=True)
class TaskDispatchOutcome:
    task_id: str = ""
    dispatch_error: Exception | None = None

    @property
    def launched(self) -> bool:
        return self.dispatch_error is None and bool((self.task_id or "").strip())


@dataclass(slots=True)
class JobLaunchOutcome:
    job: ProcessingJob
    dispatch_error: Exception | None = None

    @property
    def launched(self) -> bool:
        return self.dispatch_error is None and bool((self.job.task_id or "").strip())


class ActiveJobConflictError(RuntimeError):
    """Raised when a new tracked job is requested while another one is still active."""


def _recover_stale_active_jobs() -> None:
    """Best-effort inline recovery before rejecting a new launch.

    Goal: avoid keeping the whole queue blocked by a zombie PENDING/RUNNING row
    until the periodic cleanup task runs.
    """
    from .job_recovery import recover_jobs

    active_ids = list(
        ProcessingJob.objects.filter(status__in=[ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING])
        .order_by("id")
        .values_list("id", flat=True)
    )
    if not active_ids:
        return
    recover_jobs(
        ids=active_ids,
        running_heartbeat_minutes=int(getattr(settings, "JOB_STALE_HEARTBEAT_MINUTES", 2)),
        running_started_minutes=int(getattr(settings, "JOB_STALE_STARTED_MINUTES", 3)),
        pending_minutes=int(getattr(settings, "JOB_STALE_PENDING_MINUTES", 10)),
        requested_stop_minutes=int(getattr(settings, "JOB_REQUESTED_STOP_STALE_MINUTES", 1)),
        include_pending=True,
        include_requested_pending=True,
        dry_run=False,
        sync_recent_terminal=True,
    )


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

    _recover_stale_active_jobs()
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
        try:
            async_result = task.apply_async(kwargs={**payload, "job_id": job.id})
        except Exception as exc:  # pragma: no cover - exercised via tests with mock side effects
            ProcessingJob.objects.filter(id=job.id).update(
                status=ProcessingJob.Status.FAILED,
                error=f"Task dispatch failed: {exc}",
                finished_at=timezone.now(),
            )
            outcome["dispatch_error"] = exc
            return

        task_id = (getattr(async_result, "id", "") or "")[:64]
        ProcessingJob.objects.filter(id=job.id).update(task_id=task_id)
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
