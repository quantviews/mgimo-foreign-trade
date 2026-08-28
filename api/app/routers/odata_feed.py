"""OData v4 endpoints: service document, $metadata, and the `trade` entity set."""

from __future__ import annotations

import datetime as dt
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from .. import db, odata
from ..auth import authenticate
from ..config import settings

router = APIRouter()
_ODATA_HEADERS = {"OData-Version": "4.0"}


def _base(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _jsonable(v):
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10]
    return v


@router.get("/odata/")
def service_root(request: Request, user: dict = Depends(authenticate)) -> JSONResponse:
    return JSONResponse(odata.service_document(_base(request)), headers=_ODATA_HEADERS)


@router.get("/odata/$metadata")
def metadata(user: dict = Depends(authenticate)) -> Response:
    return Response(
        content=odata.metadata_xml(), media_type="application/xml", headers=_ODATA_HEADERS
    )


@router.get("/odata/trade")
def trade_entityset(
    request: Request,
    user: dict = Depends(authenticate),
    filter_: str | None = Query(None, alias="$filter"),
    select: str | None = Query(None, alias="$select"),
    orderby: str | None = Query(None, alias="$orderby"),
    top: int | None = Query(None, alias="$top", ge=1),
    skip: int = Query(0, alias="$skip", ge=0),
    count: str | None = Query(None, alias="$count"),
) -> JSONResponse:
    max_rows = int(user.get("plan", {}).get("max_rows", settings.max_page_rows))
    page = min(top or odata.DEFAULT_PAGE, max_rows)

    try:
        sql, params, cols = odata.build_trade_query(
            filter_expr=filter_, select=select, orderby=orderby, top=page, skip=skip
        )
        columns, rows = db.run_query(sql, params)
    except odata.ODataError as e:
        raise HTTPException(400, str(e))

    has_more = len(rows) > page
    rows = rows[:page]
    request.state.rows_returned = len(rows)

    value = []
    for idx, r in enumerate(rows):
        rec = {"Id": skip + idx + 1}
        rec.update({c: _jsonable(v) for c, v in zip(columns, r)})
        value.append(rec)

    base = _base(request)
    body: dict = {"@odata.context": f"{base}/odata/$metadata#trade", "value": value}

    if (count or "").lower() == "true":
        csql, cparams = odata.count_query(filter_)
        _, crows = db.run_query(csql, cparams)
        body["@odata.count"] = crows[0][0]

    if has_more:
        q = dict(request.query_params)
        q["$skip"] = str(skip + page)
        body["@odata.nextLink"] = f"{base}/odata/trade?{urlencode(q)}"

    return JSONResponse(body, headers=_ODATA_HEADERS)
