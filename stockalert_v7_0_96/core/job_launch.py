from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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
