"""FastAPI application entrypoint."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from . import db, store
from .audit import AuditMiddleware
from .config import settings
from .routers import health, meta, reference, trade

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
    await store.close_pool()
    db.close_connection()


app = FastAPI(title=settings.api_title, version=settings.api_version, lifespan=lifespan)
app.add_middleware(AuditMiddleware)

app.include_router(health.router)
app.include_router(meta.router)
app.include_router(reference.router)
app.include_router(trade.router)


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
