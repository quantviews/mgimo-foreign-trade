"""GET /v1/trade — filtered/aggregated trade data (JSON or CSV)."""

from __future__ import annotations

import csv
import io
import datetime as dt

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .. import db
from ..auth import authenticate
from ..config import settings
from ..query import QueryError, build_trade_query

router = APIRouter(prefix="/v1")


def _split(value: str | None) -> list[str]:
    return [p.strip() for p in (value or "").split(",") if p.strip()]


def _jsonable(v):
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10]
    return v


@router.get("/trade")
def trade(
    request: Request,
    user: dict = Depends(authenticate),
    strana: list[str] | None = Query(None),
    napr: list[str] | None = Query(None),
    type: list[str] | None = Query(None),
    source: list[str] | None = Query(None),
    tnved2: list[str] | None = Query(None),
    tnved4: list[str] | None = Query(None),
    tnved6: list[str] | None = Query(None),
    tnved: list[str] | None = Query(None),
    edizm_iso: list[str] | None = Query(None),
    edizm: list[str] | None = Query(None),
    period_from: str | None = Query(None),
    period_to: str | None = Query(None),
    group_by: str | None = Query(None, description="comma list, e.g. strana,tnved2,period"),
    metrics: str | None = Query(None, description="comma list; default stoim,netto"),
    include: str | None = Query(None, description="comma list of name fields"),
    order_by: str | None = Query(None),
    format: str = Query("json", pattern="^(json|csv)$"),
    limit: int = Query(None, ge=1),
    offset: int = Query(0, ge=0),
):
    max_rows = int(user.get("plan", {}).get("max_rows", settings.max_page_rows))
    requested = limit or settings.default_page_rows
    page = min(requested, max_rows)

    filters = {
        "strana": strana, "napr": napr, "type": type, "source": source,
        "tnved2": tnved2, "tnved4": tnved4, "tnved6": tnved6, "tnved": tnved,
        "edizm_iso": edizm_iso, "edizm": edizm,
    }
    try:
        sql, params, qmeta = build_trade_query(
            filters=filters,
            group_by=_split(group_by),
            metrics=_split(metrics),
            include=_split(include),
            period_from=period_from,
            period_to=period_to,
            order_by=order_by,
            limit=page,
            offset=offset,
        )
        columns, rows = db.run_query(sql, params)
    except QueryError as e:
        raise HTTPException(400, str(e))

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
        "table": qmeta["table"],
    }

    if format == "csv":
        return _csv_response(columns, rows)
    data = [{c: _jsonable(v) for c, v in zip(columns, r)} for r in rows]
    return JSONResponse({"meta": meta, "data": data})


def _csv_response(columns: list[str], rows: list[tuple]) -> StreamingResponse:
    # ru-locale friendly: UTF-8 BOM + ';' delimiter. Numbers use '.' (documented);
    # for typed data prefer JSON + Power Query.
    buf = io.StringIO()
    buf.write("﻿")
    writer = csv.writer(buf, delimiter=";", lineterminator="\n")
    writer.writerow(columns)
    for r in rows:
        writer.writerow([_jsonable(v) for v in r])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=trade.csv"},
    )
