"""Reference lookups for dropdowns (countries, TNVED names)."""

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import db
from ..auth import authenticate

router = APIRouter(prefix="/v1/reference")

_TNVED_LEVELS = {2, 4, 6, 8, 10}


@router.get("/countries")
def countries(user: dict = Depends(authenticate)) -> list[dict]:
    cols, rows = db.run_query(
        "SELECT STRANA AS strana, STRANA_NAME AS country_name "
        "FROM country_reference WHERE STRANA IS NOT NULL ORDER BY STRANA"
    )
    return [dict(zip(cols, r)) for r in rows]


@router.get("/tnved")
def tnved(
    level: int = Query(..., description="TNVED level: 2, 4, 6, 8 or 10"),
    strana: str | None = Query(None, pattern="^[A-Za-z]{2}$", description="Country-specific chapter heading (ISO2); omit for the general reference"),
    user: dict = Depends(authenticate),
) -> list[dict]:
    if level not in _TNVED_LEVELS:
        raise HTTPException(400, f"level must be one of {sorted(_TNVED_LEVELS)}")
    cols, rows = db.run_query(
        "SELECT r.TNVED_CODE AS code, COALESCE(n.TNVED_NAME,r.TNVED_NAME) AS name, "
        "COALESCE(n.TNVED_NAME_EN,r.TNVED_NAME_EN) AS name_en, "
        "r.TNVED_UNIT AS unit, CASE WHEN n.TNVED_CODE IS NOT NULL THEN 'national_manual' ELSE r.NAME_SOURCE END AS name_source "
        "FROM tnved_reference r LEFT JOIN tnved_country_chapters n "
        "ON n.STRANA=? AND r.TNVED_LEVEL=2 AND n.TNVED_CODE=r.TNVED_CODE "
        "WHERE r.TNVED_LEVEL = ? ORDER BY r.TNVED_CODE",
        [strana.upper() if strana else None, level],
    )
    return [dict(zip(cols, r)) for r in rows]
