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
    user: dict = Depends(authenticate),
) -> list[dict]:
    if level not in _TNVED_LEVELS:
        raise HTTPException(400, f"level must be one of {sorted(_TNVED_LEVELS)}")
    cols, rows = db.run_query(
        "SELECT TNVED_CODE AS code, TNVED_NAME AS name "
        "FROM tnved_reference WHERE TNVED_LEVEL = ? ORDER BY TNVED_CODE",
        [level],
    )
    return [dict(zip(cols, r)) for r in rows]
