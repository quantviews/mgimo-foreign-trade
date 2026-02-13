#!/usr/bin/env python3
"""
Практические задания к Занятию 3: DuckDB и SQL.
Запускать из корня проекта: python lessons/practices/practice_03_duckdb.py
"""
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"


def main():
    if not DB_PATH.exists():
        print(f"ОШИБКА: База данных не найдена: {DB_PATH}")
        return

    conn = duckdb.connect(str(DB_PATH))

    print("=" * 60)
    print("ПРАКТИКА 1: Структура БД")
    print("=" * 60)
    tables = conn.execute("SHOW TABLES").fetchall()
    print("Таблицы:", [t[0] for t in tables])
    print()

    print("=" * 60)
    print("ПРАКТИКА 2: Топ товарных групп по импорту (2023)")
    print("=" * 60)
    top_groups = conn.execute("""
        SELECT TNVED2_NAME, SUM(STOIM) as total_import,
               COUNT(DISTINCT STRANA) as num_countries
        FROM unified_trade_data_enriched
        WHERE PERIOD >= '2023-01-01' AND NAPR = 'ИМ'
        GROUP BY TNVED2_NAME
        ORDER BY total_import DESC
        LIMIT 10
    """).df()
    print(top_groups.to_string(index=False))
    print()

    print("=" * 60)
    print("ПРАКТИКА 3: Месячная динамика импорта и экспорта")
    print("=" * 60)
    dynamics = conn.execute("""
        SELECT PERIOD,
            SUM(CASE WHEN NAPR = 'ИМ' THEN STOIM ELSE 0 END) as import_value,
            SUM(CASE WHEN NAPR = 'ЭК' THEN STOIM ELSE 0 END) as export_value
        FROM unified_trade_data
        WHERE PERIOD >= '2023-01-01'
        GROUP BY PERIOD
        ORDER BY PERIOD
    """).df()
    print(dynamics.head(12).to_string(index=False))
    print("...")
    print()

    print("=" * 60)
    print("ПРАКТИКА 4: Товары с наибольшим весом (NETTO)")
    print("=" * 60)
    by_weight = conn.execute("""
        SELECT TNVED4_NAME, SUM(NETTO) as total_weight_kg,
               SUM(STOIM) as total_value_usd
        FROM unified_trade_data_enriched
        WHERE PERIOD >= '2023-01-01' AND NAPR = 'ИМ' AND NETTO > 0
        GROUP BY TNVED4_NAME
        HAVING SUM(NETTO) > 10000000
        ORDER BY total_weight_kg DESC
        LIMIT 10
    """).df()
    print(by_weight.to_string(index=False))
    print()

    print("=" * 60)
    print("ПРАКТИКА 5: Экспорт в CSV")
    print("=" * 60)
    result = conn.execute("""
        SELECT STRANA, TNVED2, SUM(STOIM) as total
        FROM unified_trade_data
        WHERE PERIOD >= '2023-01-01' AND NAPR = 'ИМ'
        GROUP BY STRANA, TNVED2
        ORDER BY total DESC
        LIMIT 100
    """).df()

    out_path = PROJECT_ROOT / "lessons" / "practices" / "export_analysis.csv"
    result.to_csv(out_path, index=False)
    print(f"Экспортировано {len(result)} строк в {out_path}")
    print()

    conn.close()
    print("Готово!")


if __name__ == "__main__":
    main()
