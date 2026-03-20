#!/usr/bin/env python3
"""
Export TNVED8 and TNVED codes that have no translation (empty TNVED8_NAME / TNVED_NAME)
from unified_trade_data_enriched to separate JSON files.
"""

import duckdb
import json
from pathlib import Path


def main():
    project_root = Path(__file__).resolve().parent.parent
    db_path = project_root / "db" / "unified_trade_data.duckdb"
    out_dir = project_root / "metadata" / "missing_translations"
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path), read_only=True)

    # TNVED8 codes with empty TNVED8_NAME
    tnved8_missing = conn.execute("""
        SELECT DISTINCT TNVED8 AS code
        FROM unified_trade_data_enriched
        WHERE (TNVED8_NAME IS NULL OR TRIM(COALESCE(TNVED8_NAME, '')) = '')
          AND TNVED8 IS NOT NULL
          AND TRIM(COALESCE(TNVED8, '')) != ''
        ORDER BY TNVED8
    """).fetchall()

    # TNVED codes (full) with empty TNVED_NAME
    tnved_missing = conn.execute("""
        SELECT DISTINCT TNVED AS code
        FROM unified_trade_data_enriched
        WHERE (TNVED_NAME IS NULL OR TRIM(COALESCE(TNVED_NAME, '')) = '')
          AND TNVED IS NOT NULL
          AND TRIM(COALESCE(TNVED, '')) != ''
        ORDER BY TNVED
    """).fetchall()

    conn.close()

    tnved8_codes = [row[0] for row in tnved8_missing]
    tnved_codes = [row[0] for row in tnved_missing]

    tnved8_file = out_dir / "tnved8_missing_translations.json"
    tnved_file = out_dir / "tnved_missing_translations.json"

    with open(tnved8_file, "w", encoding="utf-8") as f:
        json.dump(
            {"description": "TNVED8 codes with empty TNVED8_NAME", "count": len(tnved8_codes), "codes": tnved8_codes},
            f,
            ensure_ascii=False,
            indent=2,
        )

    with open(tnved_file, "w", encoding="utf-8") as f:
        json.dump(
            {"description": "TNVED codes with empty TNVED_NAME", "count": len(tnved_codes), "codes": tnved_codes},
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"TNVED8 without translation: {len(tnved8_codes)} -> {tnved8_file}")
    print(f"TNVED without translation:  {len(tnved_codes)} -> {tnved_file}")


if __name__ == "__main__":
    main()
