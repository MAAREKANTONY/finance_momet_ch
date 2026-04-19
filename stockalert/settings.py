import os
import sys
from pathlib import Path

from celery.schedules import crontab
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

# Application version (shown in UI footer)
# NOTE: can be overridden via environment variable APP_VERSION
APP_VERSION = os.getenv("APP_VERSION", "V7.0.72")

# Load .env if present (does not override real env vars by default)
load_dotenv(BASE_DIR / ".env", override=False)

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-secret-key")
DEBUG = os.getenv("DJANGO_DEBUG", "0") == "1"

def _csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [v.strip() for v in raw.split(",") if v.strip()]

# Hosts & CSRF (prod friendly)
ALLOWED_HOSTS = _csv_env(
    "DJANGO_ALLOWED_HOSTS",
    "localhost,127.0.0.1",
)

CSRF_TRUSTED_ORIGINS = _csv_env(
    "DJANGO_CSRF_TRUSTED_ORIGINS",
    "https://momet.lifesev.info,https://www.momet.lifesev.info",
)

# If behind a proxy (Fly.io / nginx), Django must trust X-Forwarded-Proto
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

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
    "core.middleware.RequestMemoryLogMiddleware",
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

TESTING = any(arg in sys.argv for arg in ["test", "pytest"])
USE_SQLITE_FOR_TESTS = os.getenv("DJANGO_TEST_USE_SQLITE", "1") == "1"

if TESTING and USE_SQLITE_FOR_TESTS:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "test_db.sqlite3",
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.getenv("POSTGRES_DB", "stockalert"),
            "USER": os.getenv("POSTGRES_USER", "stockalert"),
            "PASSWORD": os.getenv("POSTGRES_PASSWORD", "stockalert"),
            "HOST": os.getenv("POSTGRES_HOST", "db"),
            "PORT": os.getenv("POSTGRES_PORT", "5432"),
        }
    }

if TESTING and USE_SQLITE_FOR_TESTS:
    MIGRATION_MODULES = {"core": None}

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

# Twelve Data safety throttling
# Keep the default below the provider hard limit to preserve a margin and avoid
# bursty minute-overflows on full-market refreshes.
TWELVEDATA_RATE_LIMIT_ENABLED = os.getenv("TWELVEDATA_RATE_LIMIT_ENABLED", "1") == "1"
TWELVEDATA_MAX_CALLS_PER_MINUTE = int(os.getenv("TWELVEDATA_MAX_CALLS_PER_MINUTE", "340"))
TWELVEDATA_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("TWELVEDATA_RATE_LIMIT_WINDOW_SECONDS", "60"))
TWELVEDATA_RATE_LIMIT_SLEEP_BUFFER_SECONDS = float(os.getenv("TWELVEDATA_RATE_LIMIT_SLEEP_BUFFER_SECONDS", "0.25"))
TWELVEDATA_RATE_LIMIT_KEY_PREFIX = os.getenv("TWELVEDATA_RATE_LIMIT_KEY_PREFIX", "ratelimit:twelvedata")
TWELVEDATA_BACKOFF_SECONDS = int(os.getenv("TWELVEDATA_BACKOFF_SECONDS", "65"))
TWELVEDATA_MAX_RETRIES = int(os.getenv("TWELVEDATA_MAX_RETRIES", "3"))

# Legacy standalone batch hours are intentionally kept only as deprecated envs
# for backward compatibility with existing .env files. They are no longer used in
# the scheduler because fetch/compute/send are orchestrated through a smaller set
# of tracked jobs to avoid non-tracked tasks blocking the single worker.
FETCH_BARS_HOUR = int(os.getenv("FETCH_BARS_HOUR", "23"))
COMPUTE_HOUR = int(os.getenv("COMPUTE_HOUR", "23"))
EMAIL_HOUR = int(os.getenv("EMAIL_HOUR", "23"))

# Daily batch (bars fetch + metrics compute + games tables)
DAILY_REFRESH_HOUR = int(os.getenv("DAILY_REFRESH_HOUR", "8"))
DAILY_REFRESH_MINUTE = int(os.getenv("DAILY_REFRESH_MINUTE", "0"))

# IMPORTANT: do not reintroduce legacy non-tracked periodic tasks here.
# In production we want exactly one automatic heavy orchestration path:
# daily_system_refresh_job_task. This prevents a background fetch/compute run from
# silently occupying the solo worker and leaving user-triggered tracked jobs in
# PENDING for a long time.
CELERY_BEAT_SCHEDULE = {
    "check-scheduled-alerts": {
        "task": "core.tasks.check_and_send_scheduled_alerts_task",
        "schedule": crontab(minute=0),
    },
    "cleanup-stale-processing-jobs": {
        "task": "core.tasks.cleanup_stale_processing_jobs_task",
        "schedule": crontab(minute="*/1"),
    },
    "daily-system-refresh": {
        "task": "core.tasks.daily_system_refresh_job_task",
        "schedule": crontab(hour=DAILY_REFRESH_HOUR, minute=DAILY_REFRESH_MINUTE),
    },
}

# Cookie security (recommended in prod; keep enabled even if DEBUG=0)
CSRF_COOKIE_SECURE = os.getenv("CSRF_COOKIE_SECURE", "1") == "1"
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "1") == "1"
CSRF_COOKIE_SAMESITE = os.getenv("CSRF_COOKIE_SAMESITE", "Lax")
SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")

# ProcessingJob cleanup thresholds (minutes)
# Tests should exercise the canonical recovery defaults, not container-specific
# .env overrides that are meant for long-lived production workers.
JOB_DB_RETRY_ATTEMPTS = int(os.getenv('JOB_DB_RETRY_ATTEMPTS', '5'))
JOB_DB_RETRY_DELAY_SECONDS = int(os.getenv('JOB_DB_RETRY_DELAY_SECONDS', '2'))
JOB_DB_RETRY_BACKOFF_SECONDS = int(os.getenv('JOB_DB_RETRY_BACKOFF_SECONDS', '2'))
JOB_TASK_RETRY_COUNTDOWN_SECONDS = int(os.getenv('JOB_TASK_RETRY_COUNTDOWN_SECONDS', '15'))
JOB_TASK_MAX_RETRIES = int(os.getenv('JOB_TASK_MAX_RETRIES', '5'))
if TESTING:
    JOB_STALE_HEARTBEAT_MINUTES = 2
    JOB_STALE_STARTED_MINUTES = 3
    JOB_STALE_PENDING_MINUTES = 10
    JOB_REQUESTED_STOP_STALE_MINUTES = 1
else:
    JOB_STALE_HEARTBEAT_MINUTES = int(os.getenv('JOB_STALE_HEARTBEAT_MINUTES', '2'))
    JOB_STALE_STARTED_MINUTES = int(os.getenv('JOB_STALE_STARTED_MINUTES', '3'))
    JOB_STALE_PENDING_MINUTES = int(os.getenv('JOB_STALE_PENDING_MINUTES', '10'))
    JOB_REQUESTED_STOP_STALE_MINUTES = int(os.getenv('JOB_REQUESTED_STOP_STALE_MINUTES', '1'))

# GameScenario scheduler defaults (used by core.tasks)
GAME_SCENARIO_RUN_HOUR = int(os.getenv('GAME_SCENARIO_RUN_HOUR', '3'))
GAME_SCENARIO_RUN_MINUTE = int(os.getenv('GAME_SCENARIO_RUN_MINUTE', '5'))


# Lightweight request/memory diagnostics (console only; no behavior change)
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "memory": {
            "handlers": ["console"],
            "level": os.getenv("REQUEST_MEMORY_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
    },
}
