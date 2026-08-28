"""Discovery: available dimensions/metrics/filters + data version + your plan."""

from fastapi import APIRouter, Depends

from .. import db, query
from ..auth import authenticate

router = APIRouter(prefix="/v1")


@router.get("/meta")
def meta(user: dict = Depends(authenticate)) -> dict:
    return {
        "data": db.data_version(),
        "filters": sorted(query._FILTER_IN.keys()) + ["period_from", "period_to"],
        "group_by": sorted(query._GROUP_DIMS.keys()),
        "metrics": sorted(query._METRICS.keys()),
        "default_metrics": list(query._DEFAULT_METRICS),
        "include": sorted(query._LABELS.keys()),
        "plan": user.get("plan"),
    }
