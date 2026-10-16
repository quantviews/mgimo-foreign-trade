"""The common deterministic comparison service used by MCP and Excel."""
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from ..auth import authenticate
from ..comparison import compare
from ..comparison_data import database_get

router = APIRouter(prefix='/v1')


class ComparisonQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    strana: list[str] = Field(min_length=2, max_length=10)
    napr: str
    months: int = Field(default=12, ge=1, le=60)
    period_from: str | None = None
    period_to: str | None = None
    tnved2: list[str] | None = None
    tnved4: list[str] | None = None
    tnved6: list[str] | None = None
    tnved: list[str] | None = None
    include_fizob: bool = True


@router.post('/compare/trade')
def comparison(body: ComparisonQuery, request: Request, user=Depends(authenticate)):
    cap = max(1, min(1000, int(user['plan']['max_rows'])))
    try:
        result = compare(lambda path, params: database_get(path, params, cap),
                         body.strana, body.napr, body.months, body.period_from, body.period_to,
                         {k:getattr(body,k) for k in ('tnved','tnved2','tnved4','tnved6') if getattr(body,k)},
                         body.include_fizob)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    request.state.rows_returned = len(result.get('totals', []))
    request.state.cost_units = 1
    return result
