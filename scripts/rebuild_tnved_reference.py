"""Пересборка tnved_reference (и зависящего от неё представления) на месте.

Справочник ТН ВЭД меняется чаще, чем сами данные, а таблица наименований —
это ~15 МБ против 7 млн строк фактов. Скрипт обновляет только её и
пересоздаёт unified_trade_data_enriched, не трогая базовую таблицу.
Полная пересборка (src/pipelines/merge_pipeline.py) делает то же самое,
но заодно перестраивает весь набор.

Запуск: python scripts/rebuild_tnved_reference.py [--db db/unified_trade_data.duckdb]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from core.reference_tables import (  # noqa: E402
    NAME_SOURCE_FTS,
    build_unified_trade_data_enriched_view_from_base_sql,
    load_tnved_mapping,
)


def build_reference_df(project_root: Path) -> pd.DataFrame:
    mappings = load_tnved_mapping(project_root)
    refs = []
    for level_name, mapping in mappings.items():
        level = int(level_name.replace("tnved", ""))
        for code, data in mapping.items():
            name = data.get("name", "")
            if not name:
                continue
            code = str(code).strip()
            padded = code[:10] if len(code) >= 10 else code + "0" * (10 - len(code))
            refs.append(
                {
                    "TNVED_CODE": padded[:level],
                    "TNVED_LEVEL": level,
                    "TNVED_NAME": name,
                    "TNVED_UNIT": data.get("unit"),
                    "NAME_SOURCE": data.get("source", NAME_SOURCE_FTS),
                    "TRANSLATED": data.get("translated", False),
                }
            )
    df = pd.DataFrame(refs)
    # официальное наименование выигрывает у машинного перевода
    return df.sort_values("TRANSLATED").drop_duplicates(
        subset=["TNVED_CODE", "TNVED_LEVEL"], keep="first"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="db/unified_trade_data.duckdb")
    args = ap.parse_args()

    df = build_reference_df(PROJECT_ROOT)
    print(f"строк справочника: {len(df)}")
    print(df.groupby(["TNVED_LEVEL", "NAME_SOURCE"]).size().to_string())

    db_path = PROJECT_ROOT / args.db
    conn = duckdb.connect(str(db_path))
    try:
        before = conn.execute("SELECT count(*) FROM tnved_reference").fetchone()[0]
        conn.register("tnved_ref_df", df)
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DROP TABLE IF EXISTS tnved_reference")
        conn.execute(
            """
            CREATE TABLE tnved_reference AS
            SELECT DISTINCT TNVED_CODE, TNVED_LEVEL, TNVED_NAME,
                   TNVED_UNIT, NAME_SOURCE, TRANSLATED
            FROM tnved_ref_df
            ORDER BY TNVED_LEVEL, TNVED_CODE
            """
        )
        conn.execute(
            "CREATE INDEX idx_tnved_ref_code_level ON tnved_reference(TNVED_CODE, TNVED_LEVEL)"
        )
        conn.execute(build_unified_trade_data_enriched_view_from_base_sql())
        conn.execute("COMMIT")
        conn.unregister("tnved_ref_df")
        after = conn.execute("SELECT count(*) FROM tnved_reference").fetchone()[0]
        print(f"tnved_reference: {before} -> {after} строк; представление пересоздано")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
