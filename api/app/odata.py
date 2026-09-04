"""Minimal OData v4 feed for Excel / Power BI ("From OData Feed").

Exposes two entity sets over the serving DuckDB with the query options Excel actually
uses: $filter, $select, $orderby, $top, $skip, $count, plus server-driven paging
(@odata.nextLink):

  * `trade` — raw trade rows over unified_trade_data_enriched;
  * `fizob` — physical-volume index rows over fizob_enriched.

Each set is described by an `Entity`: a source table, a property->SQL-expression map
(the allowlist), Edm types and a paging-stable default order. Security: property/column
names come only from the entity's allowlist; $filter values are parameterized.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_PAGE = 5000


class ODataError(ValueError):
    """Bad OData query -> 400."""


@dataclass(frozen=True)
class Entity:
    name: str  # entity set name / URL segment, e.g. "trade"
    type_name: str  # EntityType name in $metadata, e.g. "TradeRow"
    source: str  # DuckDB table/view
    select_order: tuple[str, ...]  # property display order + default $select
    columns: dict[str, str]  # property -> SQL expression (identical unless derived)
    edm: dict[str, str]  # property -> Edm type (default Edm.String)
    default_order: str  # ORDER BY SQL (deterministic -> paging-stable)
    date_fields: frozenset[str]  # properties whose $filter literals are dates


# --- entity definitions -----------------------------------------------------

_TRADE_ORDER = (
    "PERIOD", "NAPR", "STRANA",
    "TNVED", "TNVED_NAME", "TNVED_NAME_OFFICIAL", "TNVED_NAME_EN",
    "TNVED_UNIT", "TNVED_NAME_SOURCE",
    "TNVED2", "TNVED2_NAME",
    "TNVED4", "TNVED4_NAME",
    "TNVED6", "TNVED8",
    "EDIZM", "EDIZM_ISO", "SOURCE", "TYPE",
    "STOIM", "NETTO", "KOL",
)

TRADE = Entity(
    name="trade",
    type_name="TradeRow",
    source="unified_trade_data_enriched",
    select_order=_TRADE_ORDER,
    columns={name: name for name in _TRADE_ORDER},
    edm={"PERIOD": "Edm.Date", "STOIM": "Edm.Double", "NETTO": "Edm.Double", "KOL": "Edm.Double"},
    default_order="PERIOD, STRANA, TNVED, NAPR",
    date_fields=frozenset({"PERIOD"}),
)

_FIZOB_ORDER = (
    "STRANA", "NAPR", "PERIOD", "tn_level", "tn_code", "tn_name",
    "fizob", "fizob_bp", "idx",
)

FIZOB = Entity(
    name="fizob",
    type_name="FizobRow",
    source="fizob_enriched",
    select_order=_FIZOB_ORDER,
    columns={
        "STRANA": "STRANA", "NAPR": "NAPR", "PERIOD": "PERIOD",
        "tn_level": "tn_level", "tn_code": "tn_code",
        # names only exist at levels 2/4; derived to a single property.
        "tn_name": "CASE WHEN tn_level = 2 THEN TNVED2_NAME "
                   "WHEN tn_level = 4 THEN TNVED4_NAME END",
        "fizob": "fizob", "fizob_bp": "fizob_bp", "idx": "idx",
    },
    edm={
        "PERIOD": "Edm.Date", "tn_level": "Edm.Int32",
        "fizob": "Edm.Double", "fizob_bp": "Edm.Double", "idx": "Edm.Double",
    },
    default_order="STRANA, NAPR, tn_level, tn_code, PERIOD",
    date_fields=frozenset({"PERIOD"}),
)

ENTITIES: dict[str, Entity] = {TRADE.name: TRADE, FIZOB.name: FIZOB}

# Back-compat: some callers/tests still reference the trade allowlist directly.
ODATA_COLUMNS = TRADE.columns


def get_entity(name: str) -> Entity:
    try:
        return ENTITIES[name]
    except KeyError:
        raise ODataError(f"Unknown entity set: {name}")


# --- $metadata / service document -------------------------------------------

def _entity_type_xml(entity: Entity) -> str:
    props = ['        <Property Name="Id" Type="Edm.Int64" Nullable="false"/>']
    for name in entity.select_order:
        props.append(
            f'        <Property Name="{name}" Type="{entity.edm.get(name, "Edm.String")}"/>'
        )
    props_xml = "\n".join(props)
    return (
        f'      <EntityType Name="{entity.type_name}">\n'
        '        <Key><PropertyRef Name="Id"/></Key>\n'
        f"{props_xml}\n"
        "      </EntityType>\n"
    )


def metadata_xml() -> str:
    types = "".join(_entity_type_xml(e) for e in ENTITIES.values())
    sets = "".join(
        f'        <EntitySet Name="{e.name}" EntityType="Trade.{e.type_name}"/>\n'
        for e in ENTITIES.values()
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">\n'
        "  <edmx:DataServices>\n"
        '    <Schema Namespace="Trade" xmlns="http://docs.oasis-open.org/odata/ns/edm">\n'
        f"{types}"
        '      <EntityContainer Name="Container">\n'
        f"{sets}"
        "      </EntityContainer>\n"
        "    </Schema>\n"
        "  </edmx:DataServices>\n"
        "</edmx:Edmx>\n"
    )


def service_document(base: str) -> dict:
    return {
        "@odata.context": f"{base}/odata/$metadata",
        "value": [{"name": e.name, "kind": "EntitySet", "url": e.name} for e in ENTITIES.values()],
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
    def __init__(self, tokens: list[str], entity: Entity):
        self.t = tokens
        self.i = 0
        self.entity = entity

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
        if field not in self.entity.columns:
            raise ODataError(f"Unknown $filter field: {field}")
        op = (self._next() or "").lower()
        if op not in _OPS:
            raise ODataError(f"Unsupported operator: {op}")
        raw = self._next()
        if raw is None:
            raise ODataError("Missing value in $filter")
        value = _literal(self.entity, field, raw)
        return f"{self.entity.columns[field]} {_OPS[op]} ?", [value]


def _literal(entity: Entity, field: str, raw: str):
    if raw.startswith("'") and raw.endswith("'"):
        s = raw[1:-1].replace("''", "'")
        return s[:10] if field in entity.date_fields else s
    if _DATE.match(raw):  # unquoted date / datetime literal
        return raw[:10]
    try:
        return float(raw) if "." in raw or "e" in raw.lower() else int(raw)
    except ValueError:
        raise ODataError(f"Bad literal in $filter: {raw}")


def filter_to_sql(expr: str | None, entity: Entity = TRADE) -> tuple[str, list]:
    if not expr or not expr.strip():
        return "", []
    return _Parser(_tokenize(expr), entity).parse()


# --- entity set query -------------------------------------------------------

def build_query(entity: Entity, *, filter_expr, select, orderby, top, skip):
    """Return (sql, params, columns) for an entity set. Raises ODataError -> 400."""
    cols = list(entity.select_order)
    if select:
        req = [c.strip() for c in select.split(",") if c.strip()]
        for c in req:
            if c != "Id" and c not in entity.columns:
                raise ODataError(f"Unknown $select field: {c}")
        cols = [c for c in req if c in entity.columns] or list(entity.select_order)

    where, params = filter_to_sql(filter_expr, entity)

    order_sql = entity.default_order  # deterministic default (paging-stable)
    if orderby and orderby.strip():
        parts = []
        for item in orderby.split(","):
            bits = item.split()
            name = bits[0]
            if name not in entity.columns:
                raise ODataError(f"Unknown $orderby field: {name}")
            direction = "DESC" if len(bits) > 1 and bits[1].lower() == "desc" else "ASC"
            parts.append(f"{entity.columns[name]} {direction}")
        order_sql = ", ".join(parts)

    # Alias only derived properties, so plain columns keep byte-identical SQL.
    select_sql = ", ".join(
        c if entity.columns[c] == c else f"{entity.columns[c]} AS {c}" for c in cols
    )
    sql = f"SELECT {select_sql} FROM {entity.source}"
    if where:
        sql += f" WHERE {where}"
    sql += f" ORDER BY {order_sql} LIMIT {int(top) + 1} OFFSET {int(skip)}"
    return sql, params, cols


def count_query(entity: Entity, filter_expr: str | None) -> tuple[str, list]:
    """SQL for $count=true (same WHERE as the entity set)."""
    where, params = filter_to_sql(filter_expr, entity)
    sql = f"SELECT COUNT(*) FROM {entity.source}" + (f" WHERE {where}" if where else "")
    return sql, params


# --- back-compat wrappers ---------------------------------------------------

def build_trade_query(*, filter_expr, select, orderby, top, skip):
    return build_query(TRADE, filter_expr=filter_expr, select=select,
                       orderby=orderby, top=top, skip=skip)
