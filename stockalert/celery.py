import os
from celery import Celery
from celery.schedules import crontab
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stockalert.settings")
app = Celery("stockalert")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

app.conf.beat_schedule = {
    'check-scheduled-alerts': {'task':'core.tasks.check_and_send_scheduled_alerts_task','schedule': crontab(minute='*')},
}
