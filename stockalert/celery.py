import logging
import os

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stockalert.settings")
app = Celery("stockalert")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

logger = logging.getLogger(__name__)


# Daily system refresh schedule (local time zone defined by APP_TIMEZONE)
DAILY_REFRESH_HOUR = int(os.getenv('DAILY_REFRESH_HOUR', '8'))
DAILY_REFRESH_MINUTE = int(os.getenv('DAILY_REFRESH_MINUTE', '0'))
app.conf.beat_schedule = {
    'check-scheduled-alerts': {'task': 'core.tasks.check_and_send_scheduled_alerts_task', 'schedule': crontab(minute=0)},
    'cleanup-stale-processing-jobs': {'task': 'core.tasks.cleanup_stale_processing_jobs_task', 'schedule': crontab(minute='*/1')},
    # Daily end-to-end refresh: bars fetch + scenarios compute + games tables
    'daily-system-refresh': {'task': 'core.tasks.daily_system_refresh_job_task', 'schedule': crontab(hour=DAILY_REFRESH_HOUR, minute=DAILY_REFRESH_MINUTE)},
}


@worker_ready.connect
def recover_orphan_jobs_after_worker_ready(sender=None, **kwargs):
    """Reconcile orphan active ProcessingJob rows right after worker startup.

    This is intentionally aggressive because the current operating mode is:
    - one Celery worker for tracked business jobs
    - no business parallelism
    - no hidden replay of a killed tracked task
    """
    from django.conf import settings

    if not bool(getattr(settings, "JOB_RECOVER_ORPHANED_ON_WORKER_READY", True)):
        logger.info("worker_ready orphan recovery disabled by settings")
        return

    try:
        from core.job_recovery import recover_active_jobs_after_worker_restart

        hostname = ""
        if sender is not None:
            hostname = str(getattr(sender, "hostname", "") or "").strip()
        stats = recover_active_jobs_after_worker_restart(worker_hostname=hostname)
        if stats.updated:
            logger.warning(
                "Recovered %s orphan active jobs on worker startup (hostname=%s)",
                stats.updated,
                hostname or "?",
            )
        else:
            logger.info("No orphan active jobs found on worker startup (hostname=%s)", hostname or "?")
    except Exception:
        logger.exception("Failed to recover orphan active jobs on worker startup")
