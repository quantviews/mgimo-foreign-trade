#!/usr/bin/env python3
"""Extract a period slice from unified_trade_data.duckdb into a separate DuckDB file."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
from core.reference_tables import build_unified_trade_data_enriched_view_sql

DEFAULT_SOURCE = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"

ENRICHED_VIEW_SQL = build_unified_trade_data_enriched_view_sql()

FIZOB_VIEW_SQL = """
CREATE OR REPLACE VIEW fizob_index_v AS
SELECT *,
       CASE WHEN fizob_bp = 0 THEN NULL ELSE fizob / fizob_bp END AS idx
FROM fizob_index
"""


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/")


def slice_database(
    source_db: Path,
    output_db: Path,
    start_year: int,
    end_year: int,
    *,
    overwrite: bool = True,
) -> None:
    if not source_db.exists():
        raise FileNotFoundError(f"Source database not found: {source_db}")
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year})")

    output_db.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and output_db.exists():
        raise FileExistsError(f"Output already exists: {output_db}")
    for sidecar in output_db.parent.glob(output_db.name + "*"):
        if sidecar.is_file():
            sidecar.unlink(missing_ok=True)

    src = _sql_path(source_db)
    logger.info("Source: %s", source_db)
    logger.info("Output: %s", output_db)
    logger.info("Period filter: %s–%s (inclusive by calendar year)", start_year, end_year)

    conn = duckdb.connect(str(output_db))
    try:
        conn.execute(f"ATTACH '{src}' AS src (READ_ONLY)")

        conn.execute(
            f"""
            CREATE TABLE unified_trade_data AS
            SELECT * FROM src.unified_trade_data
            WHERE EXTRACT(YEAR FROM PERIOD) BETWEEN {start_year} AND {end_year}
            """
        )
        n_trade = conn.execute("SELECT COUNT(*) FROM unified_trade_data").fetchone()[0]
        period_range = conn.execute(
            "SELECT MIN(PERIOD), MAX(PERIOD) FROM unified_trade_data"
        ).fetchone()
        logger.info("unified_trade_data: %s rows, PERIOD %s .. %s", f"{n_trade:,}", *period_range)

        conn.execute("CREATE TABLE country_reference AS SELECT * FROM src.country_reference")
        conn.execute("CREATE TABLE tnved_reference AS SELECT * FROM src.tnved_reference")
        logger.info(
            "Reference tables: country=%s, tnved=%s",
            conn.execute("SELECT COUNT(*) FROM country_reference").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM tnved_reference").fetchone()[0],
        )

        has_fizob = "fizob_index" in {
            row[0] for row in conn.execute("SHOW TABLES FROM src").fetchall()
        }
        if has_fizob:
            conn.execute(
                f"""
                CREATE TABLE fizob_index AS
                SELECT * FROM src.fizob_index
                WHERE EXTRACT(YEAR FROM PERIOD) BETWEEN {start_year} AND {end_year}
                """
            )
            n_fizob = conn.execute("SELECT COUNT(*) FROM fizob_index").fetchone()[0]
            fizob_range = conn.execute("SELECT MIN(PERIOD), MAX(PERIOD) FROM fizob_index").fetchone()
            logger.info("fizob_index: %s rows, PERIOD %s .. %s", f"{n_fizob:,}", *fizob_range)
            conn.execute(FIZOB_VIEW_SQL)

        conn.execute(ENRICHED_VIEW_SQL)
        conn.execute("DETACH src")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    size_mb = output_db.stat().st_size / (1024 * 1024)
    logger.info("Saved slice: %s (%.1f MB)", output_db, size_mb)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "db" / "unified_trade_data_2025_2026.duckdb",
    )
    parser.add_argument("--start-year", type=int, default=2025)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--no-overwrite", action="store_true")
    args = parser.parse_args()

    slice_database(
        args.source.resolve(),
        args.output.resolve(),
        args.start_year,
        args.end_year,
        overwrite=not args.no_overwrite,
    )


if __name__ == "__main__":
    main()
