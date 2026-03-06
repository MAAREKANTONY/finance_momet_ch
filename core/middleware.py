import logging
import re
import time
from typing import Optional

from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger("memory")


def _read_status_value(key: str) -> Optional[int]:
    try:
        with open("/proc/self/status", "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                if line.startswith(key):
                    m = re.search(r"(\d+)", line)
                    if m:
                        return int(m.group(1))
                    return None
    except OSError:
        return None
    return None


def get_process_memory_snapshot() -> dict[str, float | int | None]:
    rss_kb = _read_status_value("VmRSS:")
    hwm_kb = _read_status_value("VmHWM:")
    size_kb = _read_status_value("VmSize:")
    return {
        "rss_kb": rss_kb,
        "rss_mib": round(rss_kb / 1024.0, 1) if rss_kb is not None else None,
        "hwm_kb": hwm_kb,
        "hwm_mib": round(hwm_kb / 1024.0, 1) if hwm_kb is not None else None,
        "vmsize_kb": size_kb,
        "vmsize_mib": round(size_kb / 1024.0, 1) if size_kb is not None else None,
    }


class RequestMemoryLogMiddleware(MiddlewareMixin):
    """Low-risk request instrumentation for production diagnostics.

    Logs the request path, response status, elapsed time and the current process
    memory (RSS/HWM). The middleware does not change business behavior.
    """

    def process_request(self, request):
        request._request_memlog_started_at = time.perf_counter()
        return None

    def process_response(self, request, response):
        started_at = getattr(request, "_request_memlog_started_at", None)
        duration_ms = ((time.perf_counter() - started_at) * 1000.0) if started_at else None
        mem = get_process_memory_snapshot()
        logger.info(
            "request path=%s method=%s status=%s duration_ms=%s rss_mib=%s hwm_mib=%s vmsize_mib=%s",
            getattr(request, "path", "?"),
            getattr(request, "method", "?"),
            getattr(response, "status_code", "?"),
            f"{duration_ms:.1f}" if duration_ms is not None else "-",
            mem["rss_mib"],
            mem["hwm_mib"],
            mem["vmsize_mib"],
        )
        return response

    def process_exception(self, request, exception):
        started_at = getattr(request, "_request_memlog_started_at", None)
        duration_ms = ((time.perf_counter() - started_at) * 1000.0) if started_at else None
        mem = get_process_memory_snapshot()
        logger.exception(
            "request_exception path=%s method=%s duration_ms=%s rss_mib=%s hwm_mib=%s vmsize_mib=%s exc=%s",
            getattr(request, "path", "?"),
            getattr(request, "method", "?"),
            f"{duration_ms:.1f}" if duration_ms is not None else "-",
            mem["rss_mib"],
            mem["hwm_mib"],
            mem["vmsize_mib"],
            exception.__class__.__name__,
        )
        return None
