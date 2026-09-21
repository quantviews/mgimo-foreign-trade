#!/usr/bin/env python3
"""MGIMO foreign-trade MCP server (stdio transport).

Exposes the read-only trade API (https://nts.mgimo.ru/api) as MCP tools so any
MCP client (Claude Desktop, IDEs, agents) can query Russian foreign-trade data
in natural language. It is a thin wrapper over the HTTP API, so all
authentication, quotas and audit stay in one place (the API).

Configuration via environment variables:
  MGIMO_API_TOKEN  personal API key (required). Get one from the Superset
                   cabinet at https://nts.mgimo.ru/superset/apikey/ .
  MGIMO_API_BASE   API base URL (default https://nts.mgimo.ru/api).

The token is only ever sent in the Authorization header, never returned to the
model or logged.
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

DEFAULT_BASE = "https://nts.mgimo.ru/api"

mcp = FastMCP("mgimo-trade")


def _token() -> str:
    tok = os.environ.get("MGIMO_API_TOKEN")
    if not tok:
        # Fallback: a MGIMO_API_TOKEN= line in a .env next to this file or in cwd.
        for d in (Path.cwd(), Path(__file__).resolve().parent):
            env = d / ".env"
            if env.exists():
                for line in env.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if line.strip().startswith("MGIMO_API_TOKEN="):
                        tok = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
            if tok:
                break
    if not tok:
        raise RuntimeError(
            "No API token. Set MGIMO_API_TOKEN (env var or a line in a .env). "
            "Get a key from the Superset cabinet at https://nts.mgimo.ru/superset/apikey/ ."
        )
    return tok


def _base() -> str:
    return os.environ.get("MGIMO_API_BASE", DEFAULT_BASE).rstrip("/")


def _get(path: str, params: list[tuple[str, str]]) -> str:
    """GET a JSON endpoint and return the raw response text (JSON or an error)."""
    pairs = [(k, str(v)) for k, v in params if v is not None and str(v) != ""]
    try:
        resp = httpx.get(
            f"{_base()}{path}",
            params=pairs,
            headers={"Authorization": f"Bearer {_token()}", "Accept": "application/json"},
            timeout=90,
        )
    except RuntimeError as e:  # missing token, surfaced as a clear message
        return str(e)
    except Exception as e:  # noqa: BLE001 - surface any transport error to the model
        return f"Request failed for {path}: {e}"
    if resp.status_code >= 400:
        return f"HTTP {resp.status_code} for {path}: {resp.text[:800]}"
    return resp.text


def _multi(name: str, values: list[str] | None) -> list[tuple[str, str]]:
    return [(name, v) for v in (values or [])]


@mcp.tool()
def meta() -> str:
    """Data version and coverage, plus your API plan and remaining monthly quota.

    Call this first to learn the latest available period (period_max) and how
    much of your quota is left. No parameters. Returns JSON.
    """
    return _get("/v1/meta", [])


@mcp.tool()
def reference(name: str = "countries", level: int | None = None, limit: int | None = None) -> str:
    """Lookup dictionaries for resolving codes and names.

    name="countries": ISO-2 country codes and their Russian names.
    name="tnved": HS/ТН ВЭД commodity-code names; pass level 2, 4, 6, 8 or 10.
    Use this to turn a product/country name into a code before filtering, or a
    code into a human name. Returns JSON.
    """
    params: list[tuple[str, str]] = []
    if level is not None:
        params.append(("level", str(level)))
    if limit is not None:
        params.append(("limit", str(limit)))
    return _get(f"/v1/reference/{name}", params)


@mcp.tool()
def trade(
    strana: list[str] | None = None,
    napr: list[str] | None = None,
    tnved2: list[str] | None = None,
    tnved4: list[str] | None = None,
    tnved6: list[str] | None = None,
    tnved: list[str] | None = None,
    type: list[str] | None = None,
    source: list[str] | None = None,
    edizm: list[str] | None = None,
    period_from: str | None = None,
    period_to: str | None = None,
    group_by: str | None = None,
    metrics: str | None = None,
    include: str | None = None,
    order_by: str | None = None,
    limit: int | None = None,
) -> str:
    """Query Russian foreign-trade rows or server-side aggregates.

    Repeatable filters (pass a list of strings):
      strana  ISO-2 partner country, e.g. ["CN"], ["IN","TR"] (China/India/Turkey
              are the detailed national sources; 138 more come from UN Comtrade).
      napr    direction: "ЭК" (export from Russia) or "ИМ" (import to Russia).
      tnved2 / tnved4 / tnved6 / tnved  HS / ТН ВЭД codes at 2, 4, 6 or full digits.
      type    "fact" or "nowcast" (model estimate for the newest months).
      source  "national", "comtrade" or "nowcast".
      edizm   unit of measure.
    Period (month start, YYYY-MM-DD): period_from, period_to.
    group_by  comma-separated dimensions, e.g. "strana,tnved2,period". Omit for
              raw rows; set it for aggregates (sums over the group).
    metrics   comma list from: stoim (value, USD), netto (net weight, kg),
              kol (quantity in the extra unit). Default "stoim,netto".
    include   extra name columns, e.g. "tnved2_name".
    order_by  a grouped dimension or a selected metric; prefix "-" for descending
              (e.g. "-stoim"). limit: max rows returned.
    Returns JSON. Tip: call meta() for the latest period and reference() for codes.
    """
    params: list[tuple[str, str]] = []
    for key, vals in (
        ("strana", strana), ("napr", napr), ("type", type), ("source", source),
        ("tnved2", tnved2), ("tnved4", tnved4), ("tnved6", tnved6), ("tnved", tnved),
        ("edizm", edizm),
    ):
        params += _multi(key, vals)
    for key, val in (
        ("period_from", period_from), ("period_to", period_to), ("group_by", group_by),
        ("metrics", metrics), ("include", include), ("order_by", order_by), ("limit", limit),
    ):
        if val is not None:
            params.append((key, str(val)))
    return _get("/v1/trade", params)


@mcp.tool()
def fizob(
    strana: list[str] | None = None,
    napr: list[str] | None = None,
    tn_level: list[str] | None = None,
    tn_code: list[str] | None = None,
    period_from: str | None = None,
    period_to: str | None = None,
    order_by: str | None = None,
    limit: int | None = None,
) -> str:
    """Physical-volume indices: real trade volumes with the price effect removed.

    This indicator is a project speciality (not available in TradeMap or UN
    Comtrade). Filters (repeatable, pass a list):
      strana    ISO-2 partner country.
      napr      "ЭК" (export) or "ИМ" (import).
      tn_level  index granularity: "0" (whole country), or "2" / "4" / "6" (HS level).
      tn_code   specific HS / ТН ВЭД code at the chosen level.
    Period (YYYY-MM-DD): period_from, period_to.
    order_by  one of: period, strana, napr, tn_level, tn_code, idx (ascending).
    limit: max rows. Returns JSON (idx is the index; fact-only, no nowcast).
    """
    params: list[tuple[str, str]] = []
    for key, vals in (("strana", strana), ("napr", napr), ("tn_level", tn_level), ("tn_code", tn_code)):
        params += _multi(key, vals)
    for key, val in (("period_from", period_from), ("period_to", period_to), ("order_by", order_by), ("limit", limit)):
        if val is not None:
            params.append((key, str(val)))
    return _get("/v1/fizob", params)


def main() -> None:
    mcp.run()  # stdio transport by default


if __name__ == "__main__":
    main()
