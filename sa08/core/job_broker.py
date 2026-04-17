from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import redis
from django.conf import settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BrokerQueueSnapshot:
    queue_name: str
    length: int | None
    samples: list[str]
    error: str = ""

    @property
    def available(self) -> bool:
        return self.error == ""



def get_default_queue_name() -> str:
    return str(getattr(settings, "CELERY_TASK_DEFAULT_QUEUE", "celery") or "celery")



def get_redis_client():
    return redis.Redis.from_url(settings.CELERY_BROKER_URL)



def get_broker_queue_length(queue_name: str | None = None) -> int:
    queue = (queue_name or get_default_queue_name()).strip() or "celery"
    client = get_redis_client()
    return int(client.llen(queue))



def purge_broker_queue(queue_name: str | None = None) -> int:
    queue = (queue_name or get_default_queue_name()).strip() or "celery"
    client = get_redis_client()
    before = int(client.llen(queue))
    client.delete(queue)
    return before



def _extract_task_name(raw_value: str) -> str:
    text = (raw_value or "").strip()
    if not text:
        return "<empty>"
    try:
        payload = json.loads(text)
    except Exception:
        return text[:180]

    if isinstance(payload, dict):
        headers = payload.get("headers") or {}
        if isinstance(headers, dict):
            task = (headers.get("task") or "").strip()
            task_id = (headers.get("id") or "").strip()
            if task:
                return f"{task}#{task_id}" if task_id else task
        body = payload.get("body")
        if isinstance(body, dict):
            task = (body.get("task") or "").strip()
            if task:
                return task
    return text[:180]



def sample_broker_queue(queue_name: str | None = None, *, limit: int = 5) -> list[str]:
    queue = (queue_name or get_default_queue_name()).strip() or "celery"
    limit = max(0, int(limit or 0))
    if limit <= 0:
        return []
    client = get_redis_client()
    raw_items = client.lrange(queue, 0, max(0, limit - 1))
    samples: list[str] = []
    for item in raw_items:
        if isinstance(item, bytes):
            text = item.decode("utf-8", errors="replace")
        else:
            text = str(item)
        samples.append(_extract_task_name(text))
    return samples



def broker_queue_snapshot(*, queue_name: str | None = None, sample_limit: int = 3) -> BrokerQueueSnapshot:
    queue = (queue_name or get_default_queue_name()).strip() or "celery"
    try:
        return BrokerQueueSnapshot(
            queue_name=queue,
            length=get_broker_queue_length(queue),
            samples=sample_broker_queue(queue, limit=sample_limit),
        )
    except Exception as exc:  # pragma: no cover - resilience path
        logger.warning("[broker] unable to inspect queue %s: %s", queue, exc)
        return BrokerQueueSnapshot(queue_name=queue, length=None, samples=[], error=str(exc))
