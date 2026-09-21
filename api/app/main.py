"""FastAPI application entrypoint."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from . import db, limits, store
from .audit import AuditMiddleware
from .config import settings
from .routers import fizob, health, meta, odata_feed, reference, trade

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Open the read-only DuckDB connection eagerly so startup fails fast if the
    # serving file is missing.
    db.get_connection()
    logging.getLogger("api").info("DuckDB opened: %s", settings.resolved_duckdb_path())
    # Postgres pool for tokens/audit (no-op in dev mode / no DSN).
    await store.init_pool()
    yield
    await limits.close()
    await store.close_pool()
    db.close_connection()


class ForwardedPrefixMiddleware:
    """Honour ``X-Forwarded-Prefix`` from the reverse proxy.

    Behind nginx the API is served under a path prefix (``/api``) that nginx
    strips before proxying. We set ``root_path`` from the header so every
    generated URL carries the external prefix: OData ``@odata.context`` /
    ``@odata.nextLink`` and the Swagger ``openapi.json`` link. Direct access on
    :8000 sends no such header, so ``root_path`` stays empty and those URLs are
    correct there too.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            for key, value in scope.get("headers", []):
                if key == b"x-forwarded-prefix":
                    prefix = value.decode("latin-1").rstrip("/")
                    if prefix:
                        scope = dict(scope)
                        scope["root_path"] = prefix
                    break
        await self.app(scope, receive, send)


app = FastAPI(title=settings.api_title, version=settings.api_version, lifespan=lifespan)
app.add_middleware(AuditMiddleware)
# Outermost: set root_path before routing/URL generation (see class docstring).
app.add_middleware(ForwardedPrefixMiddleware)

app.include_router(health.router)
app.include_router(meta.router)
app.include_router(reference.router)
app.include_router(trade.router)
app.include_router(fizob.router)
app.include_router(odata_feed.router)


@app.exception_handler(HTTPException)
async def problem_json_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Render errors as application/problem+json (RFC 7807)."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "type": "about:blank",
            "title": exc.detail if isinstance(exc.detail, str) else "Error",
            "status": exc.status_code,
            "detail": exc.detail,
        },
        media_type="application/problem+json",
        headers=getattr(exc, "headers", None),
    )
