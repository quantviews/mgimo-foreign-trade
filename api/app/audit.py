"""Audit middleware — one record per request (the metering / monitoring truth).

Dev mode: records are logged to stdout. Prod mode: insert into Postgres api_audit_log
(TODO). Writing is best-effort and must never break the response.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import settings

logger = logging.getLogger("api.audit")


def _client_ip(request: Request) -> str | None:
    """Return the client host only if it is a valid IP (audit_log.ip is INET).

    request.client.host can be a non-IP string (a hostname, or 'testclient' under
    the test client). Behind nginx, wiring X-Forwarded-For is a later refinement.
    """
    host = request.client.host if request.client else None
    if not host:
        return None
    try:
        ipaddress.ip_address(host)
        return host
    except ValueError:
        return None


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        latency_ms = int((time.perf_counter() - started) * 1000)

        # `user`/`rows_returned`/`cost_units` are set by the auth dependency and route.
        user = getattr(request.state, "user", None) or {}
        record = {
            "user_id": user.get("user_id"),
            "token_id": user.get("token_id"),
            "endpoint": request.url.path,
            "method": request.method,
            "params": dict(request.query_params),  # query params never carry the token
            "status": response.status_code,
            "rows_returned": getattr(request.state, "rows_returned", None),
            "latency_ms": latency_ms,
            "cost_units": getattr(request.state, "cost_units", 1),
            "ip": _client_ip(request),
        }
        await _emit(record)
        return response


async def _emit(record: dict) -> None:
    try:
        if settings.postgres_dsn:
            # Fire-and-forget so the audit insert never adds latency to the response.
            asyncio.create_task(_safe_insert(record))
        else:
            logger.info("audit %s", json.dumps(record, ensure_ascii=False))
    except Exception:  # pragma: no cover - audit must never break the response
        logger.exception("audit scheduling failed")


async def _safe_insert(record: dict) -> None:
    try:
        from . import store  # lazy

        await store.insert_audit(record)
    except Exception:  # pragma: no cover
        logger.exception("audit pg insert failed")
