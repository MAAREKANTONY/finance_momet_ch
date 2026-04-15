from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from django.db.models import Q, QuerySet
from django.utils import timezone

from .job_status_sync import sync_related_state_for_terminal_job
from .models import ProcessingJob


@dataclass
class RecoveryDecision:
    status: str
    reason: str


@dataclass
class RecoveryStats:
    matched: int = 0
    updated: int = 0
    failed: int = 0
    cancelled: int = 0
    killed: int = 0
    synced_terminal: int = 0

    def bump_terminal(self, status: str) -> None:
        self.updated += 1
        if status == ProcessingJob.Status.FAILED:
            self.failed += 1
        elif status == ProcessingJob.Status.CANCELLED:
            self.cancelled += 1
        elif status == ProcessingJob.Status.KILLED:
            self.killed += 1




@dataclass
class AuditFinding:
    job_id: int
    job_type: str
    status: str
    severity: str
    category: str
    summary: str
    stale_seconds: int = 0
    task_id: str = ""
    worker_hostname: str = ""
    checkpoint: str = ""


@dataclass
class AuditStats:
    audited: int = 0
    healthy: int = 0
    suspect: int = 0
    critical: int = 0


def _stale_seconds(job: ProcessingJob, now=None) -> int:
    now = now or timezone.now()
    age_ref = job.heartbeat_at or job.started_at or job.created_at
    if not age_ref:
        return 0
    return max(0, int((now - age_ref).total_seconds()))


def audit_job(job: ProcessingJob, *, now=None, running_heartbeat_minutes: int = 20, running_started_minutes: int = 45, pending_minutes: int = 90) -> AuditFinding | None:
    now = now or timezone.now()
    stale_s = _stale_seconds(job, now=now)
    hb_limit_s = max(60, int(running_heartbeat_minutes or 20) * 60)
    started_limit_s = max(60, int(running_started_minutes or 45) * 60)
    pending_limit_s = max(60, int(pending_minutes or 90) * 60)

    if job.status == ProcessingJob.Status.RUNNING:
        if job.kill_requested:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="critical",
                category="running_kill_requested",
                summary="RUNNING job still active despite kill request.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
        if job.cancel_requested:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="suspect",
                category="running_cancel_requested",
                summary="RUNNING job still active despite cancel request.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
        if job.heartbeat_at and stale_s > hb_limit_s:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="critical",
                category="running_stale_heartbeat",
                summary="RUNNING job heartbeat is older than the configured threshold.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
        if not job.heartbeat_at and stale_s > started_limit_s:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="critical",
                category="running_no_heartbeat",
                summary="RUNNING job has no heartbeat and appears stale.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
        return None

    if job.status == ProcessingJob.Status.PENDING:
        if job.kill_requested:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="critical",
                category="pending_kill_requested",
                summary="PENDING job is still queued despite kill request.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
        if job.cancel_requested:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="suspect",
                category="pending_cancel_requested",
                summary="PENDING job is still queued despite cancel request.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
        if stale_s > pending_limit_s:
            return AuditFinding(
                job_id=job.id, job_type=job.job_type, status=job.status, severity="suspect",
                category="pending_too_old",
                summary="PENDING job is older than the configured threshold.",
                stale_seconds=stale_s, task_id=job.task_id or "", worker_hostname=job.worker_hostname or "", checkpoint=job.last_checkpoint or "",
            )
    return None


def audit_jobs(*, ids: list[int] | None = None, running_heartbeat_minutes: int = 20, running_started_minutes: int = 45, pending_minutes: int = 90, include_pending: bool = True):
    statuses = [ProcessingJob.Status.RUNNING]
    if include_pending:
        statuses.append(ProcessingJob.Status.PENDING)
    qs = ProcessingJob.objects.filter(status__in=statuses).order_by('id')
    if ids:
        qs = qs.filter(id__in=list(ids))

    findings: list[AuditFinding] = []
    stats = AuditStats(audited=qs.count())
    now = timezone.now()
    for job in qs.only('id','job_type','status','task_id','created_at','started_at','heartbeat_at','cancel_requested','kill_requested','last_checkpoint','worker_hostname'):
        finding = audit_job(
            job, now=now, running_heartbeat_minutes=running_heartbeat_minutes,
            running_started_minutes=running_started_minutes, pending_minutes=pending_minutes,
        )
        if finding is None:
            stats.healthy += 1
            continue
        findings.append(finding)
        if finding.severity == 'critical':
            stats.critical += 1
        else:
            stats.suspect += 1
    return findings, stats


def stale_recovery_queryset(
    *,
    running_heartbeat_minutes: int = 20,
    running_started_minutes: int = 45,
    pending_minutes: int = 90,
    include_pending: bool = True,
    include_requested_pending: bool = True,
    ids: list[int] | None = None,
) -> QuerySet[ProcessingJob]:
    now = timezone.now()
    hb_cutoff = now - timedelta(minutes=max(1, int(running_heartbeat_minutes or 1)))
    started_cutoff = now - timedelta(minutes=max(1, int(running_started_minutes or 1)))
    pending_cutoff = now - timedelta(minutes=max(1, int(pending_minutes or 1)))

    q_running_hb = Q(status=ProcessingJob.Status.RUNNING) & Q(heartbeat_at__isnull=False) & Q(heartbeat_at__lt=hb_cutoff)
    q_running_nohb = Q(status=ProcessingJob.Status.RUNNING) & Q(heartbeat_at__isnull=True) & Q(started_at__isnull=False) & Q(started_at__lt=started_cutoff)

    q = q_running_hb | q_running_nohb
    if include_pending:
        q_pending_old = Q(status=ProcessingJob.Status.PENDING) & Q(created_at__lt=pending_cutoff)
        q |= q_pending_old
    if include_requested_pending:
        q_pending_requested = Q(status=ProcessingJob.Status.PENDING) & (Q(cancel_requested=True) | Q(kill_requested=True))
        q |= q_pending_requested

    qs = ProcessingJob.objects.filter(q).order_by("id")
    if ids:
        qs = qs.filter(id__in=list(ids))
    return qs


def decide_recovery(job: ProcessingJob, *, now=None) -> RecoveryDecision | None:
    now = now or timezone.now()
    if job.status not in {ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING}:
        return None

    age_ref = job.heartbeat_at or job.started_at or job.created_at
    age_s = int((now - age_ref).total_seconds()) if age_ref else 0

    if job.kill_requested:
        return RecoveryDecision(
            status=ProcessingJob.Status.KILLED,
            reason=f"Recovered stale job after kill request (stale_for={age_s}s).",
        )
    if job.cancel_requested:
        return RecoveryDecision(
            status=ProcessingJob.Status.CANCELLED,
            reason=f"Recovered stale job after cancel request (stale_for={age_s}s).",
        )

    if job.status == ProcessingJob.Status.PENDING:
        return RecoveryDecision(
            status=ProcessingJob.Status.FAILED,
            reason=f"Recovered stale pending job (never started, age={age_s}s).",
        )

    if job.heartbeat_at:
        return RecoveryDecision(
            status=ProcessingJob.Status.FAILED,
            reason=f"Recovered stale running job (heartbeat too old, stale_for={age_s}s).",
        )
    return RecoveryDecision(
        status=ProcessingJob.Status.FAILED,
        reason=f"Recovered stale running job (no heartbeat, age={age_s}s).",
    )


def apply_recovery(job: ProcessingJob, decision: RecoveryDecision, *, dry_run: bool = False, now=None) -> bool:
    now = now or timezone.now()
    if dry_run:
        return False

    job.status = decision.status
    job.finished_at = now
    if decision.status == ProcessingJob.Status.FAILED:
        job.error = decision.reason
        if not job.message:
            job.message = decision.reason
    else:
        suffix = decision.reason
        job.message = ((job.message or "").rstrip() + ("\n" if (job.message or "").strip() else "") + suffix)[:4000]
    job.save(update_fields=["status", "finished_at", "error", "message"])
    sync_related_state_for_terminal_job(job)
    return True


def sync_terminal_jobs(*, queryset: QuerySet[ProcessingJob] | None = None, limit: int | None = None) -> int:
    qs = queryset or ProcessingJob.objects.filter(
        status__in=[
            ProcessingJob.Status.FAILED,
            ProcessingJob.Status.CANCELLED,
            ProcessingJob.Status.KILLED,
        ]
    )
    if limit:
        qs = qs.order_by("-id")[:limit]
    count = 0
    for job in qs.only("id", "status", "backtest_id", "message", "error"):
        sync_related_state_for_terminal_job(job)
        count += 1
    return count


def recover_jobs(
    *,
    ids: list[int] | None = None,
    running_heartbeat_minutes: int = 20,
    running_started_minutes: int = 45,
    pending_minutes: int = 90,
    include_pending: bool = True,
    include_requested_pending: bool = True,
    dry_run: bool = False,
    sync_recent_terminal: bool = True,
) -> tuple[list[tuple[ProcessingJob, RecoveryDecision]], RecoveryStats]:
    qs = stale_recovery_queryset(
        running_heartbeat_minutes=running_heartbeat_minutes,
        running_started_minutes=running_started_minutes,
        pending_minutes=pending_minutes,
        include_pending=include_pending,
        include_requested_pending=include_requested_pending,
        ids=ids,
    )
    stats = RecoveryStats(matched=qs.count())
    decisions: list[tuple[ProcessingJob, RecoveryDecision]] = []
    now = timezone.now()
    for job in qs.only(
        "id", "job_type", "status", "task_id", "created_at", "started_at", "heartbeat_at",
        "cancel_requested", "kill_requested", "message", "error", "backtest_id",
    ):
        decision = decide_recovery(job, now=now)
        if not decision:
            continue
        decisions.append((job, decision))
        if apply_recovery(job, decision, dry_run=dry_run, now=now):
            stats.bump_terminal(decision.status)

    if sync_recent_terminal:
        stats.synced_terminal = sync_terminal_jobs(limit=200)
    return decisions, stats
