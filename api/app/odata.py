"""Minimal OData v4 feed for Excel / Power BI ("From OData Feed").

Exposes one entity set `trade` over the base unified_trade_data table with the query
options Excel actually uses: $filter, $select, $orderby, $top, $skip, $count, plus
server-driven paging (@odata.nextLink). Names/labels can be added as a second entity
later. Security: property/column names come only from the allowlist; $filter values
are parameterized.
"""

from __future__ import annotations

import re

# Entity properties -> DB columns (identical names). `Id` is a synthetic per-page key.
ODATA_COLUMNS = {
    "PERIOD": "PERIOD",
    "NAPR": "NAPR",
    "STRANA": "STRANA",
    "TNVED": "TNVED",
    "TNVED2": "TNVED2",
    "TNVED4": "TNVED4",
    "TNVED6": "TNVED6",
    "TNVED8": "TNVED8",
    "EDIZM": "EDIZM",
    "EDIZM_ISO": "EDIZM_ISO",
    "SOURCE": "SOURCE",
    "TYPE": "TYPE",
    "STOIM": "STOIM",
    "NETTO": "NETTO",
    "KOL": "KOL",
}
_EDM_TYPE = {
    "PERIOD": "Edm.Date",
    "STOIM": "Edm.Double",
    "NETTO": "Edm.Double",
    "KOL": "Edm.Double",
}  # everything else -> Edm.String
_SELECT_ORDER = list(ODATA_COLUMNS.keys())

DEFAULT_PAGE = 5000


class ODataError(ValueError):
    """Bad OData query -> 400."""


def metadata_xml() -> str:
    props = ['        <Property Name="Id" Type="Edm.Int64" Nullable="false"/>']
    for name in _SELECT_ORDER:
        props.append(f'        <Property Name="{name}" Type="{_EDM_TYPE.get(name, "Edm.String")}"/>')
    props_xml = "\n".join(props)
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">\n'
        "  <edmx:DataServices>\n"
        '    <Schema Namespace="Trade" xmlns="http://docs.oasis-open.org/odata/ns/edm">\n'
        '      <EntityType Name="TradeRow">\n'
        "        <Key><PropertyRef Name=\"Id\"/></Key>\n"
        f"{props_xml}\n"
        "      </EntityType>\n"
        '      <EntityContainer Name="Container">\n'
        '        <EntitySet Name="trade" EntityType="Trade.TradeRow"/>\n'
        "      </EntityContainer>\n"
        "    </Schema>\n"
        "  </edmx:DataServices>\n"
        "</edmx:Edmx>\n"
    )


def service_document(base: str) -> dict:
    return {
        "@odata.context": f"{base}/odata/$metadata",
        "value": [{"name": "trade", "kind": "EntitySet", "url": "trade"}],
    }


# --- $filter parser (safe subset) -------------------------------------------

_OPS = {"eq": "=", "ne": "<>", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}
_TOKEN = re.compile(r"\s*('(?:[^']|'')*'|\(|\)|[^\s()]+)")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _tokenize(text: str) -> list[str]:
    tokens, pos = [], 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            break
        tokens.append(m.group(1))
        pos = m.end()
    return tokens


class _Parser:
    def __init__(self, tokens: list[str]):
        self.t = tokens
        self.i = 0

    def _peek(self):
        return self.t[self.i] if self.i < len(self.t) else None

    def _next(self):
        tok = self._peek()
        self.i += 1
        return tok

    def parse(self):
        sql, params = self._or()
        if self.i != len(self.t):
            raise ODataError(f"Unexpected token in $filter: {self._peek()}")
        return sql, params

    def _or(self):
        sql, params = self._and()
        while (self._peek() or "").lower() == "or":
            self._next()
            r_sql, r_params = self._and()
            sql = f"({sql} OR {r_sql})"
            params += r_params
        return sql, params

    def _and(self):
        sql, params = self._cmp()
        while (self._peek() or "").lower() == "and":
            self._next()
            r_sql, r_params = self._cmp()
            sql = f"({sql} AND {r_sql})"
            params += r_params
        return sql, params

    def _cmp(self):
        tok = self._peek()
        if tok == "(":
            self._next()
            sql, params = self._or()
            if self._next() != ")":
                raise ODataError("Unbalanced parentheses in $filter")
            return f"({sql})", params
        field = self._next()
        if field not in ODATA_COLUMNS:
            raise ODataError(f"Unknown $filter field: {field}")
        op = (self._next() or "").lower()
        if op not in _OPS:
            raise ODataError(f"Unsupported operator: {op}")
        raw = self._next()
        if raw is None:
            raise ODataError("Missing value in $filter")
        value = _literal(field, raw)
        return f"{ODATA_COLUMNS[field]} {_OPS[op]} ?", [value]


def _literal(field: str, raw: str):
    if raw.startswith("'") and raw.endswith("'"):
        s = raw[1:-1].replace("''", "'")
        if field == "PERIOD":
            return s[:10]
        return s
    if _DATE.match(raw):  # unquoted date / datetime literal
        return raw[:10]
    try:
        return float(raw) if "." in raw or "e" in raw.lower() else int(raw)
    except ValueError:
        raise ODataError(f"Bad literal in $filter: {raw}")


def filter_to_sql(expr: str | None) -> tuple[str, list]:
    if not expr or not expr.strip():
        return "", []
    return _Parser(_tokenize(expr)).parse()


# --- entity set query -------------------------------------------------------

def build_trade_query(
    *, filter_expr, select, orderby, top, skip
) -> tuple[str, list, list[str]]:
    """Return (sql, params, columns). Raises ODataError -> 400."""
    cols = _SELECT_ORDER
    if select:
        req = [c.strip() for c in select.split(",") if c.strip()]
        for c in req:
            if c != "Id" and c not in ODATA_COLUMNS:
                raise ODataError(f"Unknown $select field: {c}")
        cols = [c for c in req if c in ODATA_COLUMNS] or _SELECT_ORDER

    where, params = filter_to_sql(filter_expr)

    order_sql = "PERIOD, STRANA, TNVED, NAPR"  # deterministic default (paging-stable)
    if orderby and orderby.strip():
        parts = []
        for item in orderby.split(","):
            bits = item.split()
            name = bits[0]
            if name not in ODATA_COLUMNS:
                raise ODataError(f"Unknown $orderby field: {name}")
            direction = "DESC" if len(bits) > 1 and bits[1].lower() == "desc" else "ASC"
            parts.append(f"{ODATA_COLUMNS[name]} {direction}")
        order_sql = ", ".join(parts)

    sql = f"SELECT {', '.join(cols)} FROM unified_trade_data"
    if where:
        sql += f" WHERE {where}"
    sql += f" ORDER BY {order_sql} LIMIT {int(top) + 1} OFFSET {int(skip)}"
    return sql, params, cols
