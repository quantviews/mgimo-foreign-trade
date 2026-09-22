"""Tool schemas (DeepSeek / OpenAI function-calling) and their executor.

Each tool maps to a GET on the read-only data API, called with the single shared
demo-plan key. Results are trimmed so a chat turn cannot balloon the token budget.
"""

from __future__ import annotations

import json

import httpx

from .config import settings

_STR_ARRAY = {"type": "array", "items": {"type": "string"}}

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "meta",
            "description": "Data coverage and the latest available month, plus valid "
                           "dimensions/metrics. Rarely needed: trade() already returns "
                           "meta.period_max.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "reference",
            "description": "Lookup dictionaries. name='countries' for ISO-2 codes and "
                           "Russian names; name='tnved' for HS/ТН ВЭД code names at a "
                           "level. Use only when you do not know a code.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "enum": ["countries", "tnved"]},
                    "level": {"type": "integer", "description": "for tnved: 2,4,6,8,10"},
                    "limit": {"type": "integer"},
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trade",
            "description": "Foreign-trade rows or server-side aggregates. Work general "
                           "to specific: aggregate high (tnved2/strana/year) first, then "
                           "drill down. Every response includes meta.period_max.",
            "parameters": {
                "type": "object",
                "properties": {
                    "strana": {**_STR_ARRAY, "description": "ISO-2 partner, e.g. ['CN','IN','TR']"},
                    "napr": {**_STR_ARRAY, "description": "'ЭК' export / 'ИМ' import"},
                    "tnved2": _STR_ARRAY, "tnved4": _STR_ARRAY,
                    "tnved6": _STR_ARRAY, "tnved": _STR_ARRAY,
                    "type": {**_STR_ARRAY, "description": "'fact' or 'nowcast'"},
                    "source": _STR_ARRAY,
                    "edizm": _STR_ARRAY,
                    "period_from": {"type": "string", "description": "YYYY-MM-DD (month start)"},
                    "period_to": {"type": "string", "description": "YYYY-MM-DD (month start)"},
                    "group_by": {"type": "string", "description": "comma dims: strana,napr,"
                                 "type,source,tnved2/4/6/tnved,edizm,period,year"},
                    "metrics": {"type": "string", "description": "comma: stoim,netto,kol "
                                "(kol needs edizm in group_by)"},
                    "include": {"type": "string", "description": "name cols, e.g. tnved2_name"},
                    "order_by": {"type": "string", "description": "dim or metric, '-' desc"},
                    "limit": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fizob",
            "description": "Physical-volume indices (real volumes with the price effect "
                           "removed) - a project speciality. Filter by strana, tn_level "
                           "(0/2/4/6), tn_code, period.",
            "parameters": {
                "type": "object",
                "properties": {
                    "strana": _STR_ARRAY,
                    "napr": {**_STR_ARRAY, "description": "'ЭК'/'ИМ'"},
                    "tn_level": {**_STR_ARRAY, "description": "'0','2','4','6'"},
                    "tn_code": _STR_ARRAY,
                    "period_from": {"type": "string"},
                    "period_to": {"type": "string"},
                    "order_by": {"type": "string"},
                    "limit": {"type": "integer"},
                },
            },
        },
    },
]

_LIST_FIELDS = {
    "strana", "napr", "tnved2", "tnved4", "tnved6", "tnved", "type", "source",
    "edizm", "tn_level", "tn_code",
}
_SCALAR_FIELDS = {"period_from", "period_to", "group_by", "metrics", "include",
                  "order_by", "limit"}
_MAX_RESULT_CHARS = 6000
_MAX_RESULT_ROWS = 40


def _build_params(args: dict) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = []
    for key in _LIST_FIELDS:
        val = args.get(key)
        if val is None:
            continue
        for v in (val if isinstance(val, list) else [val]):
            if v is not None and str(v) != "":
                params.append((key, str(v)))
    for key in _SCALAR_FIELDS:
        val = args.get(key)
        if val is not None and str(val) != "":
            params.append((key, str(val)))
    return params


def _trim(text: str) -> str:
    """Keep meta + a bounded number of rows so tool results stay small."""
    if len(text) <= _MAX_RESULT_CHARS:
        return text
    try:
        obj = json.loads(text)
        data = obj.get("data")
        if isinstance(data, list) and len(data) > _MAX_RESULT_ROWS:
            obj["data"] = data[:_MAX_RESULT_ROWS]
            obj.setdefault("meta", {})
            obj["_truncated"] = f"showing {_MAX_RESULT_ROWS} of {len(data)} rows"
        return json.dumps(obj, ensure_ascii=False)[:_MAX_RESULT_CHARS + 500]
    except Exception:
        return text[:_MAX_RESULT_CHARS] + ' ...","_truncated":true}'


async def execute_tool(client: httpx.AsyncClient, name: str, args: dict) -> str:
    headers = {"Authorization": f"Bearer {settings.service_api_key}",
               "Accept": "application/json"}
    if name == "meta":
        path, params = "/v1/meta", []
    elif name == "reference":
        sub = args.get("name") or "countries"
        if sub not in ("countries", "tnved"):
            return json.dumps({"error": f"unknown reference '{sub}'"})
        path = f"/v1/reference/{sub}"
        params = []
        if args.get("level") is not None:
            params.append(("level", str(args["level"])))
        if args.get("limit") is not None:
            params.append(("limit", str(args["limit"])))
    elif name in ("trade", "fizob"):
        path, params = f"/v1/{name}", _build_params(args)
    else:
        return json.dumps({"error": f"unknown tool '{name}'"})

    try:
        r = await client.get(f"{settings.api_base}{path}", params=params,
                             headers=headers, timeout=60)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": f"request failed: {e}"})
    if r.status_code >= 400:
        return json.dumps({"error": f"HTTP {r.status_code}", "detail": r.text[:400]})
    return _trim(r.text)


def summarize_tool_result(name: str, args: dict, result: str) -> str:
    """Short, human-readable line for the transparency panel ('which data')."""
    rows = None
    try:
        obj = json.loads(result)
        if isinstance(obj.get("data"), list):
            rows = len(obj["data"])
    except Exception:
        pass
    bits = []
    for k in ("strana", "tnved4", "tnved2", "tnved", "napr", "group_by",
              "period_from", "period_to", "name", "tn_level"):
        if args.get(k):
            v = args[k]
            bits.append(f"{k}={','.join(v) if isinstance(v, list) else v}")
    tail = f" -> {rows} rows" if rows is not None else ""
    return f"{name}: {' '.join(bits)}{tail}".strip()
