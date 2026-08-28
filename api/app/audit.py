"""Audit middleware — one record per request (the metering / monitoring truth).

Dev mode: records are logged to stdout. Prod mode: insert into Postgres api_audit_log
(TODO). Writing is best-effort and must never break the response.
"""

from __future__ import annotations

import json
import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import settings

logger = logging.getLogger("api.audit")

# Never log the token itself.
_SENSITIVE_HEADERS = {"authorization", "cookie"}


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        latency_ms = int((time.perf_counter() - started) * 1000)

        # `user` and `rows_returned` are set by the auth dependency / route handler.
        user = getattr(request.state, "user", None)
        record = {
            "user_id": (user or {}).get("user_id"),
            "endpoint": request.url.path,
            "method": request.method,
            "params": dict(request.query_params),
            "status": response.status_code,
            "rows_returned": getattr(request.state, "rows_returned", None),
            "latency_ms": latency_ms,
            "cost_units": getattr(request.state, "cost_units", 1),
            "ip": request.client.host if request.client else None,
        }
        _emit(record)
        return response


def _emit(record: dict) -> None:
    try:
        if settings.postgres_dsn:
            _write_postgres(record)  # TODO: async batched insert into api_audit_log
        else:
            logger.info("audit %s", json.dumps(record, ensure_ascii=False))
    except Exception:  # pragma: no cover - audit must never break the response
        logger.exception("audit write failed")


def _write_postgres(record: dict) -> None:  # pragma: no cover
    """Placeholder — Phase 1 wiring writes to Postgres api_audit_log."""
    logger.info("audit(pg-pending) %s", json.dumps(record, ensure_ascii=False))
