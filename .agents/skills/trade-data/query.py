#!/usr/bin/env python3
"""Query the MGIMO foreign-trade API and print JSON.

Token comes from MGIMO_API_TOKEN (env var, else the repo-root .env). Base URL
from MGIMO_API_BASE (default: the pilot VPS). The token is only ever sent in the
Authorization header, never printed.

Subcommands:
  trade      aggregated trade rows (/v1/trade)
  fizob      physical-volume index rows (/v1/fizob)
  meta       your usage/plan and data version (/v1/meta)
  reference  code dictionaries (/v1/reference/<name>, default tnved)
  get        arbitrary GET path, e.g. an OData query (/odata/fizob?...)

stdlib only; no pip install needed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_BASE = "https://nts.mgimo.ru/api"

# Cyrillic in responses must survive a Windows console (cp1252 by default).
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass


def repo_root() -> Path:
    # Skill lives at <root>/.claude/skills/trade-data/query.py. Walk up to the
    # repo root by looking for a .env or .git marker, so the .env fallback works
    # regardless of nesting or the current working directory.
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".env").exists() or (parent / ".git").exists():
            return parent
    return here.parents[3]


def load_token() -> str:
    tok = os.environ.get("MGIMO_API_TOKEN")
    if not tok:
        env = repo_root() / ".env"
        if env.exists():
            for line in env.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.strip().startswith("MGIMO_API_TOKEN="):
                    tok = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not tok:
        sys.exit(
            "No token: set MGIMO_API_TOKEN (env var or a line in the repo-root .env). "
            "Get a key from the Superset cabinet at /apikey/."
        )
    return tok


def base_url() -> str:
    return os.environ.get("MGIMO_API_BASE", DEFAULT_BASE).rstrip("/")


def call(path: str, params: list[tuple[str, str]] | None = None) -> str:
    pairs = [(k, str(v)) for k, v in (params or []) if v is not None and str(v) != ""]
    query = urllib.parse.urlencode(pairs)
    sep = "&" if ("?" in path and query) else ("?" if query else "")
    url = f"{base_url()}{path}{sep}{query}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {load_token()}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "ignore")[:800]
        sys.exit(f"HTTP {e.code} for {path}\n{detail}")
    except Exception as e:  # noqa: BLE001 - surface any transport error clearly
        sys.exit(f"Request failed for {base_url()}{path}: {e}")


def emit(body: str) -> None:
    # Pretty-print JSON when possible; otherwise pass through (e.g. CSV).
    try:
        print(json.dumps(json.loads(body), ensure_ascii=False, indent=2))
    except ValueError:
        print(body)


def _multi(args_value: list[str] | None) -> list[tuple[str, str]]:
    return list(args_value or [])


def cmd_trade(a: argparse.Namespace) -> None:
    p: list[tuple[str, str]] = []
    for key in ("strana", "napr", "type", "source", "tnved2", "tnved4", "tnved6", "tnved", "edizm"):
        for v in getattr(a, key) or []:
            p.append((key, v))
    for key in ("period_from", "period_to", "group_by", "metrics", "include", "limit"):
        v = getattr(a, key)
        if v is not None:
            p.append((key, v))
    if a.order_by:
        p.append(("order_by", ("-" + a.order_by) if a.desc else a.order_by))
    emit(call("/v1/trade", p))


def cmd_fizob(a: argparse.Namespace) -> None:
    p: list[tuple[str, str]] = []
    for v in a.strana or []:
        p.append(("strana", v))
    for v in a.napr or []:
        p.append(("napr", v))
    for v in a.tn_level or []:
        p.append(("tn_level", v))
    for v in a.tn_code or []:
        p.append(("tn_code", v))
    for key in ("period_from", "period_to", "limit"):
        v = getattr(a, key)
        if v is not None:
            p.append((key, v))
    if a.order_by:
        p.append(("order_by", ("-" + a.order_by) if a.desc else a.order_by))
    emit(call("/v1/fizob", p))


def cmd_meta(a: argparse.Namespace) -> None:
    emit(call("/v1/meta"))


def cmd_reference(a: argparse.Namespace) -> None:
    p: list[tuple[str, str]] = []
    if a.level is not None:
        p.append(("level", a.level))
    if a.limit is not None:
        p.append(("limit", a.limit))
    emit(call(f"/v1/reference/{a.name}", p))


def cmd_get(a: argparse.Namespace) -> None:
    raw = a.path if a.path.startswith("/") else "/" + a.path
    parsed = urllib.parse.urlsplit(raw)
    # Percent-encode the query so OData $filter with spaces/quotes is valid.
    query = urllib.parse.urlencode(
        urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    )
    emit(call(parsed.path + (f"?{query}" if query else "")))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Query the MGIMO foreign-trade API.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("trade", help="aggregated trade (/v1/trade)")
    for key in ("strana", "napr", "type", "source", "tnved2", "tnved4", "tnved6", "tnved", "edizm"):
        t.add_argument(f"--{key}", action="append", help="repeatable filter")
    t.add_argument("--period-from", dest="period_from")
    t.add_argument("--period-to", dest="period_to")
    t.add_argument("--group-by", dest="group_by", help="e.g. strana,tnved2,period")
    t.add_argument("--metrics", help="stoim,netto,kol (default stoim,netto)")
    t.add_argument("--include", help="name fields, e.g. tnved2_name")
    t.add_argument("--order-by", dest="order_by", help="a grouped dimension or a selected metric")
    t.add_argument("--desc", action="store_true", help="sort --order-by descending")
    t.add_argument("--limit")
    t.set_defaults(func=cmd_trade)

    f = sub.add_parser("fizob", help="physical-volume index (/v1/fizob)")
    f.add_argument("--strana", action="append")
    f.add_argument("--napr", action="append")
    f.add_argument("--tn-level", dest="tn_level", action="append", help="0, 2, 4 or 6")
    f.add_argument("--tn-code", dest="tn_code", action="append")
    f.add_argument("--period-from", dest="period_from")
    f.add_argument("--period-to", dest="period_to")
    f.add_argument("--order-by", dest="order_by", help="period, strana, napr, tn_level, tn_code or idx")
    f.add_argument("--desc", action="store_true", help="sort --order-by descending")
    f.add_argument("--limit")
    f.set_defaults(func=cmd_fizob)

    m = sub.add_parser("meta", help="usage/plan + data version (/v1/meta)")
    m.set_defaults(func=cmd_meta)

    r = sub.add_parser("reference", help="code dictionaries: countries, or tnved --level N")
    r.add_argument("name", nargs="?", default="countries", help="countries or tnved")
    r.add_argument("--level", help="TNVED level for name=tnved: 2, 4, 6, 8 or 10")
    r.add_argument("--limit")
    r.set_defaults(func=cmd_reference)

    g = sub.add_parser("get", help="arbitrary GET path, e.g. /odata/fizob?...")
    g.add_argument("path")
    g.set_defaults(func=cmd_get)
    return ap


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
