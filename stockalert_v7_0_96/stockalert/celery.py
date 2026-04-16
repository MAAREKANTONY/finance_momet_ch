import os
from celery import Celery
from celery.schedules import crontab
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stockalert.settings")
app = Celery("stockalert")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


# Daily system refresh schedule (local time zone defined by APP_TIMEZONE)
DAILY_REFRESH_HOUR = int(os.getenv('DAILY_REFRESH_HOUR', '8'))
DAILY_REFRESH_MINUTE = int(os.getenv('DAILY_REFRESH_MINUTE', '0'))
app.conf.beat_schedule = {
    'check-scheduled-alerts': {'task':'core.tasks.check_and_send_scheduled_alerts_task','schedule': crontab(minute=0)},
    'cleanup-stale-processing-jobs': {'task':'core.tasks.cleanup_stale_processing_jobs_task','schedule': crontab(minute='*/1')},
    # Daily end-to-end refresh: bars fetch + scenarios compute + games tables
    'daily-system-refresh': {'task': 'core.tasks.daily_system_refresh_job_task', 'schedule': crontab(hour=DAILY_REFRESH_HOUR, minute=DAILY_REFRESH_MINUTE)},
}