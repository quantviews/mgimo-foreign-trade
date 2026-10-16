"""Complete, bounded monthly exports with owner-authenticated downloads."""
import base64
from datetime import date
from typing import Literal
from uuid import UUID, uuid4
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool
from .. import db, store
from ..auth import authenticate
from ..query import build_trade_query, QueryError
from ..export_utils import build_export, MIME
from ..comparison_data import export_comparison
from .trade import _jsonable, _split

router = APIRouter(prefix="/v1")


class ExportQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    strana: list[str] | None = None
    napr: list[str] | None = None
    tnved2: list[str] | None = None
    tnved4: list[str] | None = None
    tnved6: list[str] | None = None
    tnved: list[str] | None = None
    type: list[str] | None = None
    source: list[str] | None = None
    edizm: list[str] | None = None
    edizm_iso: list[str] | None = None
    period_from: str | None = None
    period_to: str | None = None
    end_mode: Literal["latest", "fixed"] = "latest"
    group_by: str = "strana,period"
    metrics: str = "stoim,netto"
    include: str | None = None
    inline: bool = False
    compare_yoy: bool = False


def prepare(body, max_rows):
    version = db.data_version()
    if not version.get("period_max"):
        raise HTTPException(503, "Latest data period is unavailable")
    if body.end_mode == "fixed" and not body.period_to:
        raise HTTPException(400, "fixed end_mode requires period_to")
    end = version["period_max"] if body.end_mode == "latest" else body.period_to
    if not any((body.strana, body.tnved2, body.tnved4, body.tnved6, body.tnved)):
        raise HTTPException(400, "Select countries or a product group for export")
    if not body.period_from:
        raise HTTPException(400, "period_from is required; exports span at most 60 months")
    try:
        start_date = date.fromisoformat(body.period_from[:7] + "-01")
        end_date = date.fromisoformat(str(end)[:7] + "-01")
    except ValueError:
        raise HTTPException(400, "Invalid export period")
    months = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1
    if months < 1 or months > 60:
        raise HTTPException(400, "Export period must span 1 to 60 months")
    windows = [(start_date.isoformat(), end_date.isoformat())]
    if body.compare_yoy:
        if months > 12:
            raise HTTPException(400, "YoY export current period must span at most 12 months")
        windows.insert(0, (start_date.replace(year=start_date.year-1).isoformat(),
                           end_date.replace(year=end_date.year-1).isoformat()))
    dims = [d for d in _split(body.group_by) if d != "year"]
    for d in ("strana", "period", "type", "source"):
        if d not in dims: dims.append(d)
    filters = body.model_dump(exclude_none=True)
    filters.update(period_to=end, group_by=",".join(dims))
    filter_names = ("strana", "napr", "tnved2", "tnved4", "tnved6", "tnved", "type", "source", "edizm", "edizm_iso")
    cap = max(1, min(1000, int(max_rows)))
    values = []
    for window_start, window_end in windows:
        try:
            sql, params, _ = build_trade_query(
                filters={k: filters[k] for k in filter_names if k in filters},
                group_by=dims, metrics=_split(body.metrics), include=_split(body.include),
                period_from=window_start, period_to=window_end, order_by="period", limit=cap, offset=0)
        except QueryError as exc:
            raise HTTPException(400, str(exc))
        columns, window_values = db.run_query(sql, params)
        values.extend(window_values)
    if len(values) > cap:
        raise HTTPException(413, "Export exceeds row limit; narrow filters. No partial file was created.")
    if not values:
        raise HTTPException(404, "No observations for the selected filters; this does not mean zero trade.")
    rows = [{k: _jsonable(v) for k, v in zip(columns, row)} for row in values]
    normalized_rows = [{k.lower():v for k,v in r.items()} for r in rows]
    if body.compare_yoy:
        filters["yoy_periods"] = {"previous": list(windows[0]), "current": list(windows[1])}
        for row in rows:
            period = str(row.get("period", row.get("PERIOD", "")))[:10]
            row["comparison_period"] = "current" if windows[1][0] <= period <= windows[1][1] else "previous"
    comparison = {} if body.compare_yoy else export_comparison(normalized_rows, version, filters)
    content, normalized, totals = build_export(rows, version, filters, comparison if not body.compare_yoy else None)
    if len(content) > 5_000_000:
        raise HTTPException(413, "Export exceeds 5 MB; narrow filters")
    summary = [{"strana": k[0], "type": k[1],
                "stoim": str(v[0]) if "stoim" in columns else None,
                "netto": str(v[1]) if "netto" in columns and all(r.get("netto") is not None for r in normalized if (r.get("strana", "ALL"), r.get("type", "unspecified")) == k) else None}
               for k, v in sorted(totals.items())]
    return content, {"meta": {**version, "rows": len(rows), "complete": True, "has_more": False},
                     "filters": filters, "summary": summary, "comparison": comparison,
                     "file_manifest": ({"periods": filters["yoy_periods"],
                         "sheets": {name: sum(1 for r in normalized if (r.get("type") == "pred" or r.get("source") == "nowcast") == estimated)
                                    for name, estimated in [("Факт", False), ("Nowcast", True)]},
                         "note": "Исходные строки двух периодов; расчеты долей и вкладов выполняются в ответе. Nowcast не входит в фактические итоги. Оценки показателей Comtrade остаются с исходными флагами."}
                         if body.compare_yoy else None),
                     "format": "xlsx", "limits": {"max_rows": cap, "max_months": 60, "max_bytes": 5_000_000},
                     "coverage_note": "Complete for selected filters, not a guarantee of source coverage. Missing observations are not zeros."}, normalized


async def export_owner(request, user, pool):
    delegated = request.headers.get("x-export-user-id")
    if delegated is None:
        return user["user_id"]
    trusted = await pool.fetchval("SELECT EXISTS(SELECT 1 FROM export_delegates WHERE token_id=$1)", user.get("token_id"))
    if not trusted:
        raise HTTPException(403, "Export delegation is not allowed for this token")
    try:
        uid = int(delegated)
        if not 0 < uid < 2147483648: raise ValueError()
    except ValueError:
        raise HTTPException(400, "Invalid export user")
    if not await pool.fetchval("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND active)", uid):
        raise HTTPException(403, "Export user is unavailable")
    return uid


@router.post("/exports/trade")
async def create_export(body: ExportQuery, request: Request, user=Depends(authenticate)):
    pool = await store.init_pool()
    if pool is None:
        raise HTTPException(503, "Persistent export storage is not configured")
    owner = await export_owner(request, user, pool)
    content, result, rows = await run_in_threadpool(prepare, body, user['plan']['max_rows'])
    eid = uuid4()
    async with pool.acquire() as conn:
        async with conn.transaction():
            retry = await conn.fetchval("SELECT api.reserve_export($1,$2,$3,$4)", eid, owner, len(rows), len(content))
            if retry:
                raise HTTPException(429, {"code": "export_quota_exceeded",
                    "message": "Export quota for rolling 24 hours exceeded: 5000 rows, 20 MB, 10 files",
                    "retry_after": retry}, headers={"Retry-After": str(retry)})
            await conn.execute("DELETE FROM trade_exports WHERE expires_at < now()")
            expiry = await conn.fetchval(
                "INSERT INTO trade_exports(id,user_id,xlsx_content) VALUES($1,$2,$3) RETURNING expires_at",
                eid, owner, content)
    request.state.rows_returned = len(rows)
    request.state.cost_units = 1
    exempt = await pool.fetchval("SELECT EXISTS(SELECT 1 FROM api.export_quota_exemptions WHERE user_id=$1)", owner)
    result.update(export_id=str(eid), expires_at=expiry.isoformat(),
                  download_path=f"/v1/exports/{eid}", authentication="Authorization header required; no API keys in URLs",
                  daily_limits={"window_hours": 24, "exempt": bool(exempt), "rows": None if exempt else 5000, "bytes": None if exempt else 20000000, "files": None if exempt else 10})
    if body.inline:
        result.update(xlsx_base64=base64.b64encode(content).decode("ascii"), data=rows)
    return result


@router.get("/exports/{export_id}")
async def download_export(export_id: UUID, user=Depends(authenticate)):
    pool = await store.init_pool()
    if pool is None: raise HTTPException(503, "Export storage unavailable")
    row = await pool.fetchrow("SELECT csv_content, xlsx_content FROM trade_exports WHERE id=$1 AND user_id=$2 AND expires_at>now()",
                              export_id, user['user_id'])
    if row is None: raise HTTPException(404, "Export not found or expired")
    is_xlsx = row['xlsx_content'] is not None
    ext = 'xlsx' if is_xlsx else 'csv'
    return Response(row['xlsx_content'] if is_xlsx else row['csv_content'], media_type=MIME if is_xlsx else "text/csv; charset=utf-8", headers={
        "Content-Disposition": f'attachment; filename="trade-{export_id}.{ext}"',
        "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"})
