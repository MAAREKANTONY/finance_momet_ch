from __future__ import annotations

from .models import Backtest, ProcessingJob


def _truncate_message(value: str, limit: int = 2000) -> str:
    value = (value or '').strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1] + '…'


def sync_related_state_for_terminal_job(job: ProcessingJob | None) -> None:
    """Keep owner objects aligned when a tracked job reaches a terminal state.

    For now, the only owner with an explicit lifecycle we must keep in sync is Backtest.
    Backtest has no CANCELLED/KILLED states, so terminal stop states are normalized to FAILED
    with a meaningful error_message instead of leaving the backtest stuck in PENDING/RUNNING.
    """
    if not job or not getattr(job, 'backtest_id', None):
        return

    terminal = job.status
    if terminal not in {
        ProcessingJob.Status.CANCELLED,
        ProcessingJob.Status.KILLED,
        ProcessingJob.Status.FAILED,
    }:
        return

    if terminal == ProcessingJob.Status.CANCELLED:
        message = job.error or job.message or 'Cancelled by user.'
    elif terminal == ProcessingJob.Status.KILLED:
        message = job.error or job.message or 'Killed by user.'
    else:
        message = job.error or job.message or 'Job failed.'

    Backtest.objects.filter(
        id=job.backtest_id,
        status__in=[Backtest.Status.PENDING, Backtest.Status.RUNNING],
    ).update(
        status=Backtest.Status.FAILED,
        error_message=_truncate_message(message),
    )
