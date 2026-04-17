import logging
import time
from dataclasses import dataclass

import redis
from django.conf import settings

logger = logging.getLogger(__name__)


@dataclass
class RateLimitAcquireResult:
    allowed: bool
    count: int
    sleep_seconds: float


class TwelveDataMinuteRateLimiter:
    """Global per-minute rate limiter backed by Redis.

    Design goals:
    - safe with multiple Celery workers / tasks
    - low-risk / easy to reason about
    - no DB schema change

    Strategy:
    - use one Redis counter per fixed time window (default: 60 seconds)
    - INCR is atomic, so the Nth caller can decide whether it is allowed
    - when the budget is exceeded, callers wait for the next window

    Notes:
    - We intentionally keep the configurable limit below the provider hard limit
      to preserve a safety margin.
    - If Redis is unavailable, callers proceed without global throttling to avoid
      breaking existing behavior.
    """

    def __init__(self):
        self.enabled = bool(getattr(settings, "TWELVEDATA_RATE_LIMIT_ENABLED", True))
        self.limit = max(1, int(getattr(settings, "TWELVEDATA_MAX_CALLS_PER_MINUTE", 340)))
        self.window_seconds = max(1, int(getattr(settings, "TWELVEDATA_RATE_LIMIT_WINDOW_SECONDS", 60)))
        self.key_prefix = str(getattr(settings, "TWELVEDATA_RATE_LIMIT_KEY_PREFIX", "ratelimit:twelvedata"))
        self.sleep_buffer_seconds = float(getattr(settings, "TWELVEDATA_RATE_LIMIT_SLEEP_BUFFER_SECONDS", 0.25))
        self._client = None

    def _redis(self):
        if self._client is None:
            self._client = redis.Redis.from_url(settings.CELERY_BROKER_URL)
        return self._client

    def _bucket_key(self, ts: float | None = None) -> str:
        ts = ts if ts is not None else time.time()
        bucket = int(ts // self.window_seconds)
        return f"{self.key_prefix}:{bucket}"

    def try_acquire(self) -> RateLimitAcquireResult:
        if not self.enabled:
            return RateLimitAcquireResult(True, 0, 0.0)

        now = time.time()
        try:
            r = self._redis()
            key = self._bucket_key(now)
            current = int(r.incr(key))
            if current == 1:
                r.expire(key, self.window_seconds + 5)
            if current <= self.limit:
                return RateLimitAcquireResult(True, current, 0.0)

            # Wait until the next bucket opens.
            elapsed = now % self.window_seconds
            sleep_for = max(0.05, (self.window_seconds - elapsed) + self.sleep_buffer_seconds)
            return RateLimitAcquireResult(False, current, sleep_for)
        except Exception as e:
            logger.warning("[twelvedata-rate-limit] redis unavailable, proceeding without throttle: %s", e)
            return RateLimitAcquireResult(True, 0, 0.0)

    def wait_for_slot(self) -> None:
        while True:
            res = self.try_acquire()
            if res.allowed:
                return
            logger.info(
                "[twelvedata-rate-limit] minute budget reached (%s/%s). sleeping %.2fs",
                res.count,
                self.limit,
                res.sleep_seconds,
            )
            time.sleep(res.sleep_seconds)


_rate_limiter_singleton: TwelveDataMinuteRateLimiter | None = None


def get_twelvedata_rate_limiter() -> TwelveDataMinuteRateLimiter:
    global _rate_limiter_singleton
    if _rate_limiter_singleton is None:
        _rate_limiter_singleton = TwelveDataMinuteRateLimiter()
    return _rate_limiter_singleton
