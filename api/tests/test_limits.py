"""Quota / rate-limit logic (with an in-memory fake Redis)."""

import asyncio

import pytest

from app import limits


class FakeRedis:
    def __init__(self):
        self.d = {}

    async def incr(self, k):
        self.d[k] = self.d.get(k, 0) + 1
        return self.d[k]

    async def expire(self, k, s):
        return True

    async def get(self, k):
        return self.d.get(k)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _patch(monkeypatch, obj):
    async def fake():
        return obj
    monkeypatch.setattr(limits, "_redis", fake)


def test_no_limits_is_noop(monkeypatch):
    _patch(monkeypatch, FakeRedis())
    _run(limits.enforce({"user_id": 1, "plan": {}}))  # nothing to enforce


def test_rate_limit_trips_on_third(monkeypatch):
    _patch(monkeypatch, FakeRedis())
    user = {"user_id": 1, "plan": {"rate_limit_per_min": 2}}
    _run(limits.enforce(user))
    _run(limits.enforce(user))
    with pytest.raises(limits.RateLimited):
        _run(limits.enforce(user))


def test_monthly_quota_trips_on_third(monkeypatch):
    _patch(monkeypatch, FakeRedis())
    user = {"user_id": 2, "plan": {"monthly_quota": 2}}
    _run(limits.enforce(user))
    _run(limits.enforce(user))
    with pytest.raises(limits.RateLimited):
        _run(limits.enforce(user))


def test_disabled_without_redis(monkeypatch):
    async def none():
        return None
    monkeypatch.setattr(limits, "_redis", none)
    # limits configured but no Redis -> fail open (no raise)
    _run(limits.enforce({"user_id": 1, "plan": {"rate_limit_per_min": 1, "monthly_quota": 1}}))
