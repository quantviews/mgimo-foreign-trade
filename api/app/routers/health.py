"""Liveness + data freshness. No authentication."""

from fastapi import APIRouter

from .. import db

router = APIRouter()


@router.get("/health")
def health() -> dict:
    info = db.data_version()
    return {"status": "ok", **info}
