import multiprocessing
import os


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


bind = os.getenv("GUNICORN_BIND", "0.0.0.0:8000")
workers = _env_int("WEB_CONCURRENCY", 1)
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "sync")
timeout = _env_int("GUNICORN_TIMEOUT", 120)
graceful_timeout = _env_int("GUNICORN_GRACEFUL_TIMEOUT", 30)
keepalive = _env_int("GUNICORN_KEEPALIVE", 5)
max_requests = _env_int("GUNICORN_MAX_REQUESTS", 20)
max_requests_jitter = _env_int("GUNICORN_MAX_REQUESTS_JITTER", 10)
preload_app = False
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
capture_output = True
worker_tmp_dir = os.getenv("GUNICORN_WORKER_TMP_DIR", "/dev/shm")
