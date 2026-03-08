from __future__ import annotations

from pathlib import Path

from celery import current_app
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import QuerySet
from django.http import FileResponse, Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST

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
    job = ProcessingJob.objects.filter(id=pk).only("id", "status", "cancel_requested").first()
    if not job:
        messages.error(request, "Job introuvable.")
        return redirect("jobs_page")

    if job.status not in {ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING}:
        messages.warning(request, f"Le job #{job.id} n'est plus annulable.")
        return redirect("jobs_page")

    ProcessingJob.objects.filter(id=job.id).update(cancel_requested=True)
    messages.success(request, f"Annulation demandée pour le job #{job.id}.")
    return redirect("jobs_page")


@login_required
@require_POST
def job_kill(request: HttpRequest, pk: int) -> HttpResponse:
    job = ProcessingJob.objects.filter(id=pk).only("id", "task_id", "status").first()
    if not job:
        messages.error(request, "Job introuvable.")
        return redirect("jobs_page")

    if job.status not in {ProcessingJob.Status.PENDING, ProcessingJob.Status.RUNNING}:
        messages.warning(request, f"Le job #{job.id} n'est plus killable.")
        return redirect("jobs_page")

    ProcessingJob.objects.filter(id=job.id).update(kill_requested=True)

    if job.task_id:
        current_app.control.revoke(job.task_id, terminate=True, signal="SIGTERM")
        messages.success(request, f"Kill demandé pour le job #{job.id}.")
    else:
        messages.warning(request, f"Le job #{job.id} n'a pas de task_id Celery à tuer.")
    return redirect("jobs_page")
