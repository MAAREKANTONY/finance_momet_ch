from __future__ import annotations

from .models import Backtest, GameScenario, ProcessingJob


def _truncate_message(value: str, limit: int = 2000) -> str:
    value = (value or '').strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1] + '…'


def _terminal_message(job: ProcessingJob) -> str:
    terminal = job.status
    if terminal == ProcessingJob.Status.CANCELLED:
        return _truncate_message(job.error or job.message or 'Cancelled by user.')
    if terminal == ProcessingJob.Status.KILLED:
        return _truncate_message(job.error or job.message or 'Killed by user.')
    return _truncate_message(job.error or job.message or 'Job failed.')


def _sync_backtest(job: ProcessingJob) -> None:
    if not getattr(job, 'backtest_id', None):
        return
    Backtest.objects.filter(
        id=job.backtest_id,
        status__in=[Backtest.Status.PENDING, Backtest.Status.RUNNING],
    ).update(
        status=Backtest.Status.FAILED,
        error_message=_terminal_message(job),
    )


def _sync_game_scenario(job: ProcessingJob) -> None:
    if not getattr(job, 'game_scenario_id', None):
        return
    GameScenario.objects.filter(id=job.game_scenario_id).update(
        last_run_status=(job.status or '').lower(),
        last_run_message=_terminal_message(job),
    )


def sync_related_state_for_terminal_job(job: ProcessingJob | None) -> None:
    """Keep owner objects aligned when a tracked job reaches a terminal state.

    Supported owners:
    - Backtest: normalize CANCELLED/KILLED/FAILED to Backtest.FAILED with error_message.
    - GameScenario: propagate terminal job state to last_run_status/last_run_message.

    The sync is intentionally additive and idempotent to remain safe during recovery.
    """
    if not job:
        return

    if job.status not in {
        ProcessingJob.Status.CANCELLED,
        ProcessingJob.Status.KILLED,
        ProcessingJob.Status.FAILED,
    }:
        return

    _sync_backtest(job)
    _sync_game_scenario(job)
