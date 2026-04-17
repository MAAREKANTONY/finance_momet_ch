from __future__ import annotations

from pathlib import Path

from celery import current_app
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import QuerySet, Count
from django.http import FileResponse, Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST
from django.utils import timezone

from .job_broker import broker_queue_snapshot, purge_broker_queue
from .job_recovery import recover_jobs
from .job_status_sync import sync_related_state_for_terminal_job
from .models import ProcessingJob

_ALLOWED_JOB_STATUSES = {
    ProcessingJob.Status.PENDING,
    ProcessingJob.Status.RUNNING,
    ProcessingJob.Status.DONE,
    ProcessingJob.Status.FAILED,
    ProcessingJob.Status.CANCELLED,
    ProcessingJob.Status.KILLED,
}
_ALLOWED_PAGE_SIZES = [25, 50, 100, 200]
_DEFAULT_PAGE_SIZE = 50


def _job_maintenance_allowed(request: HttpRequest) -> bool:
    return bool(request.user.is_staff or request.user.is_superuser)


def _active_job_counts() -> dict[str, int]:
    counts = {"PENDING": 0, "RUNNING": 0}
    for row in ProcessingJob.objects.filter(status__in=[ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING]).values("status").annotate(c=Count("id")):
        if row["status"] in counts:
            counts[row["status"]] = int(row["c"] or 0)
    return counts


def _base_jobs_queryset() -> QuerySet[ProcessingJob]:
    """Lean queryset for the jobs list.

    Important: keep /jobs lightweight. We avoid large text fields and only load the
    columns that the list template actually renders.
    """
    return (
        ProcessingJob.objects.select_related("backtest", "scenario")
        .defer("message", "error", "output_file", "output_name")
        .only(
            "id",
            "job_type",
            "status",
            "task_id",
            "backtest_id",
            "scenario_id",
            "cancel_requested",
            "kill_requested",
            "heartbeat_at",
            "last_checkpoint",
            "worker_hostname",
            "created_at",
            "started_at",
            "finished_at",
            "backtest__name",
            "scenario__name",
        )
    )


def _normalize_status(raw: str) -> str:
    value = (raw or "").strip().upper()
    return value if value in _ALLOWED_JOB_STATUSES else ""


def _normalize_job_type(raw: str) -> str:
    value = (raw or "").strip().upper()
    allowed = {choice for choice, _label in ProcessingJob.JobType.choices}
    return value if value in allowed else ""


def _normalize_page_size(raw: str) -> int:
    try:
        value = int(raw or _DEFAULT_PAGE_SIZE)
    except (TypeError, ValueError):
        value = _DEFAULT_PAGE_SIZE
    return max(min(value, max(_ALLOWED_PAGE_SIZES)), min(_ALLOWED_PAGE_SIZES))


def _normalize_cursor(raw: str) -> int:
    try:
        value = int(raw or 0)
    except (TypeError, ValueError):
        value = 0
    return max(value, 0)


@login_required
def jobs_page(request: HttpRequest) -> HttpResponse:
    """List background jobs with a deliberately lean query/rendering path."""
    status = _normalize_status(request.GET.get("status") or "")
    job_type = _normalize_job_type(request.GET.get("type") or "")
    page_size = _normalize_page_size(request.GET.get("page_size") or _DEFAULT_PAGE_SIZE)
    cursor = _normalize_cursor(request.GET.get("cursor") or 0)

    qs = _base_jobs_queryset()
    if status:
        qs = qs.filter(status=status)
    if job_type:
        qs = qs.filter(job_type=job_type)

    qs = qs.order_by("-id")
    if cursor > 0:
        qs = qs.filter(id__lt=cursor)

    rows = list(qs[: page_size + 1])
    has_next = len(rows) > page_size
    jobs = rows[:page_size]
    next_cursor = jobs[-1].id if has_next and jobs else None

    q = request.GET.copy()
    q.pop("cursor", None)
    q.pop("show_counts", None)
    query_string = q.urlencode()

    broker_snapshot = broker_queue_snapshot(sample_limit=5)
    active_counts = _active_job_counts()

    return render(
        request,
        "jobs.html",
        {
            "jobs": jobs,
            "status": status,
            "job_type": job_type,
            "counts": None,
            "job_types": ProcessingJob.JobType.choices,
            "page_size": page_size,
            "query_string": query_string,
            "show_counts": False,
            "page_sizes": _ALLOWED_PAGE_SIZES,
            "has_next": has_next,
            "next_cursor": next_cursor,
            "broker_snapshot": broker_snapshot,
            "active_counts": active_counts,
            "maintenance_allowed": _job_maintenance_allowed(request),
        },
    )


@login_required
def job_detail(request: HttpRequest, pk: int) -> HttpResponse:
    job = get_object_or_404(
        ProcessingJob.objects.select_related("backtest", "scenario", "created_by").defer(
            "backtest__results", "backtest__settings", "backtest__universe_snapshot", "backtest__signal_lines",
            "scenario__description",
        ),
        pk=pk,
    )
    return render(request, "job_detail.html", {"job": job})


@login_required
def job_download(request: HttpRequest, pk: int) -> HttpResponse:
    job = get_object_or_404(
        ProcessingJob.objects.select_related("created_by", "backtest", "scenario").defer(
            "backtest__results", "backtest__settings", "backtest__universe_snapshot", "backtest__signal_lines",
            "scenario__description",
        ),
        pk=pk,
    )
    if job.created_by and job.created_by != request.user and not request.user.is_staff:
        raise Http404("Not allowed")

    p = (job.output_file or "").strip()
    if not p:
        raise Http404("No file")

    path = Path(p)
    try:
        path_resolved = path.resolve()
        exports_root = Path("/data/exports").resolve()
        if not str(path_resolved).startswith(str(exports_root)):
            raise Http404("Invalid path")
    except Exception as exc:
        if isinstance(exc, Http404):
            raise
        raise Http404("Invalid path")

    if not path_resolved.exists():
        raise Http404("Missing file")

    filename = job.output_name or path_resolved.name
    return FileResponse(open(path_resolved, "rb"), as_attachment=True, filename=filename)


@login_required
@require_POST
def job_cancel(request: HttpRequest, pk: int) -> HttpResponse:
    job = ProcessingJob.objects.filter(id=pk).only("id", "status", "backtest_id", "message", "error").first()
    if not job:
        messages.error(request, "Job introuvable.")
        return redirect("jobs_page")

    if job.status not in {ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING}:
        messages.warning(request, f"Le job #{job.id} n'est plus annulable.")
        return redirect("jobs_page")

    updates = {"cancel_requested": True}
    if job.status == ProcessingJob.Status.RUNNING:
        updates.update({
            "message": ((job.message or "").rstrip() + ("\n" if (job.message or "").strip() else "") + "Cancel requested by user.")[:4000],
        })
    if job.status == ProcessingJob.Status.PENDING:
        updates.update({
            "status": ProcessingJob.Status.CANCELLED,
            "finished_at": timezone.now(),
        })
    ProcessingJob.objects.filter(id=job.id).update(**updates)
    if job.status == ProcessingJob.Status.PENDING:
        for field, value in updates.items():
            setattr(job, field, value)
        sync_related_state_for_terminal_job(job)
    messages.success(request, f"Annulation demandée pour le job #{job.id}.")
    return redirect("jobs_page")


@login_required
@require_POST
def job_kill(request: HttpRequest, pk: int) -> HttpResponse:
    job = ProcessingJob.objects.filter(id=pk).only("id", "task_id", "status", "backtest_id", "message", "error").first()
    if not job:
        messages.error(request, "Job introuvable.")
        return redirect("jobs_page")

    if job.status not in {ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING}:
        messages.warning(request, f"Le job #{job.id} n'est plus killable.")
        return redirect("jobs_page")

    updates = {
        "kill_requested": True,
        "cancel_requested": True,
    }
    if job.status == ProcessingJob.Status.RUNNING:
        updates.update({
            "message": ((job.message or "").rstrip() + ("\n" if (job.message or "").strip() else "") + "Kill requested by user.")[:4000],
        })
    if job.status == ProcessingJob.Status.PENDING:
        updates.update({
            "status": ProcessingJob.Status.KILLED,
            "finished_at": timezone.now(),
        })
    ProcessingJob.objects.filter(id=job.id).update(**updates)
    if job.status == ProcessingJob.Status.PENDING:
        for field, value in updates.items():
            setattr(job, field, value)
        sync_related_state_for_terminal_job(job)

    task_id = (job.task_id or "").strip()
    if task_id:
        try:
            current_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            try:
                current_app.control.revoke(task_id, terminate=True, signal="SIGKILL")
            except Exception:
                pass
            messages.success(request, f"Kill demandé pour le job #{job.id}.")
        except Exception as exc:
            messages.warning(request, f"Kill demandé mais revoke a échoué: {exc}")
    else:
        messages.warning(request, f"Kill demandé, mais aucun task_id n'est associé à ce job.")
    return redirect("jobs_page")


@login_required
@require_POST
def jobs_recover_stale(request: HttpRequest) -> HttpResponse:
    if not _job_maintenance_allowed(request):
        messages.error(request, "Action réservée aux administrateurs.")
        return redirect("jobs_page")
    dry_run = request.POST.get("dry_run") == "1"
    decisions, stats = recover_jobs(
        running_heartbeat_minutes=20,
        running_started_minutes=45,
        pending_minutes=90,
        requested_stop_minutes=3,
        include_pending=True,
        include_requested_pending=True,
        dry_run=dry_run,
        sync_recent_terminal=True,
    )
    if dry_run:
        messages.info(request, f"Analyse recovery: {stats.matched} job(s) stale détecté(s), {len(decisions)} décision(s) proposées.")
    else:
        messages.success(request, f"Recovery terminé: matched={stats.matched}, updated={stats.updated}, failed={stats.failed}, cancelled={stats.cancelled}, killed={stats.killed}.")
    return redirect("jobs_page")


@login_required
@require_POST
def jobs_purge_broker(request: HttpRequest) -> HttpResponse:
    if not _job_maintenance_allowed(request):
        messages.error(request, "Action réservée aux administrateurs.")
        return redirect("jobs_page")
    if request.POST.get("confirm") != "PURGE":
        messages.error(request, "Confirmation invalide. Saisir PURGE pour vider la queue broker.")
        return redirect("jobs_page")
    removed = purge_broker_queue()
    messages.success(request, f"Queue broker purgée: {removed} message(s) supprimé(s).")
    return redirect("jobs_page")


@login_required
@require_POST
def jobs_recover_and_purge(request: HttpRequest) -> HttpResponse:
    if not _job_maintenance_allowed(request):
        messages.error(request, "Action réservée aux administrateurs.")
        return redirect("jobs_page")
    if request.POST.get("confirm") != "PURGE":
        messages.error(request, "Confirmation invalide. Saisir PURGE pour lancer recover + purge.")
        return redirect("jobs_page")
    removed = purge_broker_queue()
    _decisions, stats = recover_jobs(
        running_heartbeat_minutes=20,
        running_started_minutes=45,
        pending_minutes=0,
        requested_stop_minutes=3,
        include_pending=True,
        include_requested_pending=True,
        dry_run=False,
        sync_recent_terminal=True,
    )
    messages.success(request, f"Recover + purge terminé: broker={removed} message(s) supprimé(s), jobs mis à jour={stats.updated}.")
    return redirect("jobs_page")
