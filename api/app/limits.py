"""Quota + rate-limit enforcement via Redis.

Limits come from the user's plan (rate_limit_per_min, monthly_quota); NULL/0 means
no limit. Counters live in Redis (own DB number). Redis errors fail OPEN — a Redis
outage must never block the API. Disabled entirely when no redis_url (dev).

Note: the Redis monthly counter is the enforcement counter, not the billing truth
(api.audit_log is). If Redis is flushed mid-month, the quota counter resets.
"""

from __future__ import annotations

import datetime as dt
import logging
import time

from .config import settings

logger = logging.getLogger("api.limits")
_client = None


class RateLimited(Exception):
    def __init__(self, retry_after: int, detail: str):
        self.retry_after = retry_after
        self.detail = detail
        super().__init__(detail)


async def _redis():
    global _client
    if not settings.redis_url:
        return None
    if _client is None:
        import redis.asyncio as aioredis  # lazy

        _client = aioredis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def enforce(user: dict) -> None:
    """Increment counters and raise RateLimited if the plan's limits are exceeded."""
    plan = user.get("plan") or {}
    uid = user.get("user_id")
    rpm = plan.get("rate_limit_per_min")
    quota = plan.get("monthly_quota")
    if uid is None or (not rpm and not quota):
        return
    r = await _redis()
    if r is None:
        return
    try:
        if rpm:
            key = f"rl:{uid}:{int(time.time() // 60)}"
            n = await r.incr(key)
            if n == 1:
                await r.expire(key, 60)
            if n > rpm:
                raise RateLimited(60, f"Rate limit {rpm}/min exceeded")
        if quota:
            key = f"q:{uid}:{dt.datetime.now(dt.UTC):%Y-%m}"
            cur = int(await r.get(key) or 0)
            if cur >= quota:
                raise RateLimited(3600, f"Monthly quota {quota} requests exceeded")
            n = await r.incr(key)
            if n == 1:
                await r.expire(key, 45 * 86400)  # outlives the month
    except RateLimited:
        raise
    except Exception:  # pragma: no cover - never block on a limiter backend error
        logger.warning("limits backend error; failing open", exc_info=True)


async def close() -> None:
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        finally:
            _client = None
