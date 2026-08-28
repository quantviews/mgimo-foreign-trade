"""DuckDB access — one read-only connection, a fresh cursor per query (thread-safe)."""

from __future__ import annotations

import threading
from typing import Any

import duckdb

from .config import settings

_con: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Open (once) the read-only DuckDB connection to the serving file."""
    global _con
    if _con is None:
        with _lock:
            if _con is None:
                _con = duckdb.connect(
                    str(settings.resolved_duckdb_path()), read_only=True
                )
                _con.execute(f"PRAGMA threads={int(settings.duckdb_threads)}")
    return _con


def close_connection() -> None:
    global _con
    if _con is not None:
        _con.close()
        _con = None


def run_query(sql: str, params: list[Any] | None = None) -> tuple[list[str], list[tuple]]:
    """Run a read-only query on a fresh cursor. Returns (column_names, rows)."""
    cur = get_connection().cursor()
    try:
        cur.execute(sql, params or [])
        columns = [d[0] for d in cur.description]
        return columns, cur.fetchall()
    finally:
        cur.close()


def data_version() -> dict:
    """Cheap freshness/summary for /health and /meta."""
    cols, rows = run_query(
        "SELECT COUNT(*) AS rows, MIN(PERIOD) AS period_min, MAX(PERIOD) AS period_max "
        "FROM unified_trade_data"
    )
    r = rows[0]
    return {
        "rows": r[0],
        "period_min": str(r[1]) if r[1] is not None else None,
        "period_max": str(r[2]) if r[2] is not None else None,
        # "version" = latest reported month (build timestamp added by the pipeline later).
        "data_version": (str(r[2])[:7] if r[2] is not None else None),
    }
