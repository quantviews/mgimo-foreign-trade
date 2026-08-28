"""Discovery: available dimensions/metrics/filters + data version + your plan."""

from fastapi import APIRouter, Depends

from .. import db, query, store
from ..auth import authenticate
from ..config import settings

router = APIRouter(prefix="/v1")


@router.get("/meta")
async def meta(user: dict = Depends(authenticate)) -> dict:
    usage = None
    if settings.postgres_dsn and user.get("user_id"):
        try:
            usage = {"requests_this_month": await store.monthly_usage(user["user_id"])}
        except Exception:
            usage = None
    return {
        "data": db.data_version(),
        "filters": sorted(query._FILTER_IN.keys()) + ["period_from", "period_to"],
        "group_by": sorted(query._GROUP_DIMS.keys()),
        "metrics": sorted(query._METRICS.keys()),
        "default_metrics": list(query._DEFAULT_METRICS),
        "include": sorted(query._LABELS.keys()),
        "plan": user.get("plan"),
        "usage": usage,
    }
