"""Safe SQL builder for /v1/trade.

Security model: column/dimension/metric names come ONLY from the allowlists below
(never from user input), and all values are passed as DuckDB query parameters (?),
so neither identifiers nor values can be injected.
"""

from __future__ import annotations

import re

# --- Allowlists -------------------------------------------------------------

# Filterable columns (value filters, `IN (...)`).
_FILTER_IN = {
    "strana": "STRANA",
    "napr": "NAPR",
    "type": "TYPE",
    "source": "SOURCE",
    "tnved2": "TNVED2",
    "tnved4": "TNVED4",
    "tnved6": "TNVED6",
    "tnved": "TNVED",
    "edizm_iso": "EDIZM_ISO",
    "edizm": "EDIZM",
}

# NAPR aliases — ASCII is the documented primary path; Cyrillic also accepted.
_NAPR_ALIAS = {"im": "ИМ", "ex": "ЭК", "им": "ИМ", "эк": "ЭК"}

# Aggregation dimensions (group_by / order_by).
_GROUP_DIMS = {
    "strana": "STRANA",
    "napr": "NAPR",
    "type": "TYPE",
    "source": "SOURCE",
    "tnved2": "TNVED2",
    "tnved4": "TNVED4",
    "tnved6": "TNVED6",
    "tnved": "TNVED",
    "edizm": "EDIZM",
    "edizm_iso": "EDIZM_ISO",
    "period": "PERIOD",
    "year": "EXTRACT(YEAR FROM PERIOD)::INT",
}

# Summable metrics.
_METRICS = {"stoim": "SUM(STOIM)", "netto": "SUM(NETTO)", "kol": "SUM(KOL)"}
_DEFAULT_METRICS = ["stoim", "netto"]  # kol is opt-in (needs edizm in group_by)

# Opt-in name labels — only available from the enriched view.
_LABELS = {
    "country_name": "COUNTRY_NAME",
    "tnved2_name": "TNVED2_NAME",
    "tnved4_name": "TNVED4_NAME",
    "tnved6_name": "TNVED6_NAME",
    "tnved_name": "TNVED_NAME",
    "tnved_translated": "TNVED_TRANSLATED",
}
# In aggregation mode a label needs its code grouped (1:1, no fan-out).
_LABEL_REQUIRES_DIM = {
    "country_name": "strana",
    "tnved2_name": "tnved2",
    "tnved4_name": "tnved4",
    "tnved6_name": "tnved6",
    "tnved_name": "tnved",
    "tnved_translated": "tnved",
}

# Columns returned in raw (non-aggregated) mode.
_RAW_COLUMNS = [
    "NAPR", "STRANA", "TNVED", "EDIZM", "EDIZM_ISO", "STOIM", "NETTO", "KOL",
    "TNVED2", "TNVED4", "TNVED6", "TNVED8", "SOURCE", "TYPE", "PERIOD",
]

_BASE_TABLE = "unified_trade_data"
_ENRICHED_VIEW = "unified_trade_data_enriched"

_PERIOD_YM = re.compile(r"^\d{4}-\d{2}$")
_PERIOD_YMD = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class QueryError(ValueError):
    """Bad request — maps to HTTP 400."""


def _norm_list(values: list[str] | None) -> list[str]:
    return [v.strip() for v in (values or []) if v is not None and v.strip() != ""]


def _parse_period(value: str) -> str:
    v = value.strip()
    if _PERIOD_YM.match(v):
        return v + "-01"
    if _PERIOD_YMD.match(v):
        return v
    raise QueryError(f"Bad period '{value}': expected YYYY-MM or YYYY-MM-DD")


def _build_where(filters: dict, period_from, period_to) -> tuple[str, list]:
    clauses: list[str] = []
    params: list = []
    for key, col in _FILTER_IN.items():
        vals = _norm_list(filters.get(key))
        if not vals:
            continue
        if key == "strana":
            vals = [v.upper() for v in vals]
        elif key == "napr":
            mapped = []
            for v in vals:
                mv = _NAPR_ALIAS.get(v.lower())
                if mv is None and v in ("ИМ", "ЭК"):
                    mv = v
                if mv is None:
                    raise QueryError(f"Bad napr value '{v}' (use im/ex or ИМ/ЭК)")
                mapped.append(mv)
            vals = mapped
        placeholders = ", ".join(["?"] * len(vals))
        clauses.append(f"{col} IN ({placeholders})")
        params.extend(vals)
    if period_from:
        clauses.append("PERIOD >= ?")
        params.append(_parse_period(period_from))
    if period_to:
        clauses.append("PERIOD <= ?")
        params.append(_parse_period(period_to))
    return " AND ".join(clauses), params


def build_trade_query(
    *,
    filters: dict,
    group_by: list[str] | None,
    metrics: list[str] | None,
    include: list[str] | None,
    period_from: str | None,
    period_to: str | None,
    order_by: str | None,
    limit: int,
    offset: int,
) -> tuple[str, list, dict]:
    """Return (sql, params, meta). Raises QueryError (-> 400) on invalid input."""
    group_by = _norm_list(group_by)
    metrics = _norm_list(metrics)
    include = _norm_list(include)

    for g in group_by:
        if g not in _GROUP_DIMS:
            raise QueryError(f"Unknown group_by dimension: {g}")
    for m in metrics:
        if m not in _METRICS:
            raise QueryError(f"Unknown metric: {m}")
    for i in include:
        if i not in _LABELS:
            raise QueryError(f"Unknown include field: {i}")

    where, params = _build_where(filters, period_from, period_to)
    table = _ENRICHED_VIEW if include else _BASE_TABLE

    if group_by:
        metrics = metrics or list(_DEFAULT_METRICS)
        if "kol" in metrics and "edizm" not in group_by:
            raise QueryError(
                "metric 'kol' requires 'edizm' in group_by "
                "(supplementary units are not additive)"
            )
        select_parts, group_parts = [], []
        for g in group_by:
            select_parts.append(f"{_GROUP_DIMS[g]} AS {g}")
            group_parts.append(_GROUP_DIMS[g])
        for i in include:
            dim = _LABEL_REQUIRES_DIM[i]
            if dim not in group_by:
                raise QueryError(f"include '{i}' requires '{dim}' in group_by")
            select_parts.append(f"{_LABELS[i]} AS {i}")
            group_parts.append(_LABELS[i])
        for m in metrics:
            select_parts.append(f"{_METRICS[m]} AS {m}")
        sql = f"SELECT {', '.join(select_parts)} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        sql += " GROUP BY " + ", ".join(group_parts)
    else:
        # Raw mode: full rows + any requested labels. `metrics` ignored (no aggregation).
        select_parts = list(_RAW_COLUMNS)
        for i in include:
            select_parts.append(f"{_LABELS[i]} AS {i}")
        sql = f"SELECT {', '.join(select_parts)} FROM {table}"
        if where:
            sql += f" WHERE {where}"

    # ORDER BY (default: period).
    ob = (order_by or "period").strip().lower()
    if ob in _GROUP_DIMS:
        ob_expr = _GROUP_DIMS[ob]
    elif ob in _METRICS and group_by:
        ob_expr = ob  # metric alias exists in SELECT
    else:
        raise QueryError(f"Unknown order_by: {ob}")
    sql += f" ORDER BY {ob_expr}"

    # LIMIT/OFFSET. limit/offset are validated ints (limit is already capped to the
    # plan's max_page_rows by the caller), safe to inline. We fetch page+1 rows to
    # detect whether more pages exist (keyset cursor comes in a later iteration).
    page = max(1, int(limit))
    sql += f" LIMIT {page + 1} OFFSET {int(offset)}"

    return sql, params, {"table": table, "page_rows": page, "offset": int(offset)}
