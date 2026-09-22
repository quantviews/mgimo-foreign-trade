"""DuckDB access — one read-only connection, a fresh cursor per query (thread-safe)."""

from __future__ import annotations

import threading
import time
from typing import Any

import duckdb

from .config import settings

_con: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()

# data_version() scans the whole table (COUNT + MIN/MAX over PERIOD); the answer
# changes only when a new month is loaded, so cache it briefly. This keeps /meta
# fast and lets /trade attach period_max to every response for free.
_DV_TTL = 300.0  # seconds
_dv_cache: tuple[float, dict] | None = None


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
    """Cheap freshness/summary for /health, /meta and /trade (cached, see _DV_TTL)."""
    global _dv_cache
    now = time.monotonic()
    cached = _dv_cache
    if cached is not None and now - cached[0] < _DV_TTL:
        return cached[1]
    cols, rows = run_query(
        "SELECT COUNT(*) AS rows, MIN(PERIOD) AS period_min, MAX(PERIOD) AS period_max "
        "FROM unified_trade_data"
    )
    r = rows[0]
    result = {
        "rows": r[0],
        "period_min": str(r[1]) if r[1] is not None else None,
        "period_max": str(r[2]) if r[2] is not None else None,
        # "version" = latest reported month (build timestamp added by the pipeline later).
        "data_version": (str(r[2])[:7] if r[2] is not None else None),
    }
    _dv_cache = (now, result)
    return result
