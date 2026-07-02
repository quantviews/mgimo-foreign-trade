#!/usr/bin/env python3
"""Reload hs4_reference in an existing unified_trade_data.duckdb without re-merging facts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from core.reference_tables import refresh_hs4_reference_db  # noqa: E402

DEFAULT_DB = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reload hs4_reference from hs4_labels.json into existing DuckDB."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to DuckDB file (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
        help="Project root (metadata/hs4_labels.json or site/data/hs4_labels.json)",
    )
    args = parser.parse_args()

    if not args.db.exists():
        parser.error(f"DuckDB file not found: {args.db}")

    row_count = refresh_hs4_reference_db(args.db, args.project_root)
    logger.info("Done: hs4_reference has %s rows", row_count)


if __name__ == "__main__":
    main()
