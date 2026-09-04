"""GET /v1/fizob — physical-volume index (fizob) rows.

fizob is a base-normalised index computed per (STRANA, NAPR, tn_level, tn_code, PERIOD);
no aggregation here — filter and page. Safe: filter columns are allowlisted, values are
parameterized. Source: fizob_enriched (levels 0/2/4/6; names for 2/4).
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .. import db
from ..auth import authenticate
from ..config import settings
from .trade import _csv_response, _jsonable

router = APIRouter(prefix="/v1")

_NAPR_ALIAS = {"im": "ИМ", "ex": "ЭК", "им": "ИМ", "эк": "ЭК"}
_ORDER = {
    "period": "PERIOD", "strana": "STRANA", "napr": "NAPR",
    "tn_level": "tn_level", "tn_code": "tn_code", "idx": "idx",
}
_SELECT = (
    "STRANA, NAPR, PERIOD, tn_level, tn_code, "
    "CASE WHEN tn_level = 2 THEN TNVED2_NAME "
    "WHEN tn_level = 4 THEN TNVED4_NAME END AS tn_name, "
    "fizob, fizob_bp, idx"
)
_PERIOD_YM = re.compile(r"^\d{4}-\d{2}$")
_PERIOD_YMD = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _norm(v):
    return [x.strip() for x in (v or []) if x is not None and str(x).strip()]


def _period(v: str) -> str:
    v = v.strip()
    if _PERIOD_YM.match(v):
        return v + "-01"
    if _PERIOD_YMD.match(v):
        return v
    raise HTTPException(400, f"Bad period '{v}': expected YYYY-MM or YYYY-MM-DD")


@router.get("/fizob")
def fizob(
    request: Request,
    user: dict = Depends(authenticate),
    strana: list[str] | None = Query(None),
    napr: list[str] | None = Query(None),
    tn_level: list[int] | None = Query(None, description="0 (страновой итог), 2, 4, 6"),
    tn_code: list[str] | None = Query(None),
    period_from: str | None = Query(None),
    period_to: str | None = Query(None),
    order_by: str | None = Query(None),
    format: str = Query("json", pattern="^(json|csv)$"),
    limit: int = Query(None, ge=1),
    offset: int = Query(0, ge=0),
):
    max_rows = int(user.get("plan", {}).get("max_rows", settings.max_page_rows))
    page = min(limit or settings.default_page_rows, max_rows)

    clauses: list[str] = []
    params: list = []

    svals = [s.upper() for s in _norm(strana)]
    if svals:
        clauses.append(f"STRANA IN ({', '.join(['?'] * len(svals))})")
        params += svals

    nvals = []
    for v in _norm(napr):
        mv = _NAPR_ALIAS.get(v.lower()) or (v if v in ("ИМ", "ЭК") else None)
        if mv is None:
            raise HTTPException(400, f"Bad napr '{v}' (use im/ex or ИМ/ЭК)")
        nvals.append(mv)
    if nvals:
        clauses.append(f"NAPR IN ({', '.join(['?'] * len(nvals))})")
        params += nvals

    if tn_level:
        lv = [int(x) for x in tn_level]
        clauses.append(f"tn_level IN ({', '.join(['?'] * len(lv))})")
        params += lv

    cvals = _norm(tn_code)
    if cvals:
        clauses.append(f"tn_code IN ({', '.join(['?'] * len(cvals))})")
        params += cvals

    if period_from:
        clauses.append("PERIOD >= ?")
        params.append(_period(period_from))
    if period_to:
        clauses.append("PERIOD <= ?")
        params.append(_period(period_to))

    ob = (order_by or "period").strip().lower()
    if ob not in _ORDER:
        raise HTTPException(400, f"Unknown order_by: {ob}")

    sql = f"SELECT {_SELECT} FROM fizob_enriched"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += f" ORDER BY {_ORDER[ob]} LIMIT {page + 1} OFFSET {int(offset)}"

    columns, rows = db.run_query(sql, params)
    has_more = len(rows) > page
    rows = rows[:page]
    request.state.rows_returned = len(rows)
    request.state.cost_units = 1

    meta = {
        "rows": len(rows),
        "has_more": has_more,
        "next_offset": (offset + page) if has_more else None,
        "page_rows": page,
        "max_rows": max_rows,
        "table": "fizob_enriched",
    }
    if format == "csv":
        return _csv_response(columns, rows)
    data = [{c: _jsonable(v) for c, v in zip(columns, r)} for r in rows]
    return JSONResponse({"meta": meta, "data": data})
