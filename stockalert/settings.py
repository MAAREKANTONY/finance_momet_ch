import os
from pathlib import Path
from dotenv import load_dotenv
import dj_database_url

BASE_DIR = Path(__file__).resolve().parent.parent

# Application version (shown in UI footer)
APP_VERSION = "V5.2.7"
load_dotenv(BASE_DIR / ".env", override=False)

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-secret-key")
DEBUG = os.getenv("DJANGO_DEBUG", "0") == "1"
ALLOWED_HOSTS = [h.strip() for h in os.getenv("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",") if h.strip()]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_celery_beat",
    "core",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "stockalert.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "core.context_processors.app_version",
            ],
        },
    },
]

WSGI_APPLICATION = "stockalert.wsgi.application"

#DATABASES = {
#    "default": {
#        "ENGINE": "django.db.backends.postgresql",
#        "NAME": os.getenv("POSTGRES_DB", "stockalert"),
#        "USER": os.getenv("POSTGRES_USER", "stockalert"),
#        "PASSWORD": os.getenv("POSTGRES_PASSWORD", "stockalert"),
#        "HOST": os.getenv("POSTGRES_HOST", "db"),
#        "PORT": os.getenv("POSTGRES_PORT", "5432"),
#    }
#}


DATABASES = {
    "default": dj_database_url.config(
        default=os.getenv("DATABASE_URL"),
        conn_max_age=600,
    )
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "fr-fr"
TIME_ZONE = os.getenv("APP_TIMEZONE", "UTC")
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "/accounts/login/"
LOGIN_REDIRECT_URL = "/"

EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
EMAIL_HOST = os.getenv("EMAIL_HOST", "")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "")
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "1") == "1"
DEFAULT_FROM_EMAIL = os.getenv("EMAIL_FROM", EMAIL_HOST_USER or "alerts@example.com")

CELERY_BROKER_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = CELERY_BROKER_URL
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
DEFAULT_EXCHANGE = os.getenv("DEFAULT_EXCHANGE", "")

from celery.schedules import crontab
FETCH_BARS_HOUR = int(os.getenv("FETCH_BARS_HOUR", "23"))
COMPUTE_HOUR = int(os.getenv("COMPUTE_HOUR", "23"))
EMAIL_HOUR = int(os.getenv("EMAIL_HOUR", "23"))

CELERY_BEAT_SCHEDULE = {
    'check-scheduled-alerts': {
        'task': 'core.tasks.check_and_send_scheduled_alerts_task',
        'schedule': crontab(minute='*'),
    },
    "fetch-daily-bars": {"task": "core.tasks.fetch_daily_bars_task", "schedule": crontab(hour=FETCH_BARS_HOUR, minute=5)},
    "compute-metrics": {"task": "core.tasks.compute_metrics_task", "schedule": crontab(hour=COMPUTE_HOUR, minute=15)},
    "send-daily-alerts": {"task": "core.tasks.send_daily_alerts_task", "schedule": crontab(hour=EMAIL_HOUR, minute=25)},
}
