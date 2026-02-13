#!/usr/bin/env python3
"""
Практические задания к Занятию 2: Старая и новая статистика.
Запускать из корня проекта: python lessons/practices/practice_02_old_new.py
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
    print("ПРАКТИКА 1: Данные до января 2022")
    print("=" * 60)
    df_old = conn.execute("""
        SELECT * FROM unified_trade_data
        WHERE PERIOD < '2022-01-01'
        LIMIT 5000
    """).df()
    print(f"Строк: {len(df_old)}")
    print(f"Период: {df_old['PERIOD'].min()} — {df_old['PERIOD'].max()}")
    if "SOURCE" in df_old.columns:
        print(df_old["SOURCE"].value_counts())
    print()

    print("=" * 60)
    print("ПРАКТИКА 2: Топ-10 стран по импорту в 2021")
    print("=" * 60)
    top_2021 = conn.execute("""
        SELECT STRANA, SUM(STOIM) as total_import
        FROM unified_trade_data
        WHERE PERIOD >= '2021-01-01' AND PERIOD < '2022-01-01'
          AND NAPR = 'ИМ'
        GROUP BY STRANA
        ORDER BY total_import DESC
        LIMIT 10
    """).df()
    print(top_2021.to_string(index=False))
    print()

    print("=" * 60)
    print("ПРАКТИКА 3: Динамика экспорта по месяцам 2021")
    print("=" * 60)
    export_2021 = conn.execute("""
        SELECT PERIOD, SUM(STOIM) as total_export
        FROM unified_trade_data
        WHERE PERIOD >= '2021-01-01' AND PERIOD < '2022-01-01'
          AND NAPR = 'ЭК'
        GROUP BY PERIOD
        ORDER BY PERIOD
    """).df()
    print(export_2021.to_string(index=False))
    print()

    print("=" * 60)
    print("ПРАКТИКА 4: Сравнение топ-5 до и после 2022")
    print("=" * 60)
    before = conn.execute("""
        SELECT STRANA, SUM(STOIM) as total
        FROM unified_trade_data
        WHERE PERIOD >= '2021-01-01' AND PERIOD < '2022-01-01'
          AND NAPR = 'ИМ'
        GROUP BY STRANA
        ORDER BY total DESC
        LIMIT 5
    """).df()
    after = conn.execute("""
        SELECT STRANA, SUM(STOIM) as total
        FROM unified_trade_data
        WHERE PERIOD >= '2022-01-01' AND NAPR = 'ИМ'
        GROUP BY STRANA
        ORDER BY total DESC
        LIMIT 5
    """).df()
    print("Топ-5 по импорту 2021:", before["STRANA"].tolist())
    print("Топ-5 по импорту 2022+:", after["STRANA"].tolist())
    print()

    # Визуализация (если matplotlib установлен)
    try:
        import matplotlib.pyplot as plt

        print("=" * 60)
        print("ПРАКТИКА 5: Визуализация динамики")
        print("=" * 60)
        dynamics = conn.execute("""
            SELECT PERIOD,
                SUM(CASE WHEN NAPR = 'ИМ' THEN STOIM ELSE 0 END) as import_val,
                SUM(CASE WHEN NAPR = 'ЭК' THEN STOIM ELSE 0 END) as export_val
            FROM unified_trade_data
            WHERE PERIOD >= '2021-01-01'
            GROUP BY PERIOD
            ORDER BY PERIOD
        """).df()

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(dynamics["PERIOD"], dynamics["import_val"], label="Импорт")
        ax.plot(dynamics["PERIOD"], dynamics["export_val"], label="Экспорт")
        ax.legend()
        ax.set_title("Динамика импорта и экспорта (тыс. USD)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        out_path = PROJECT_ROOT / "lessons" / "practices" / "dynamics_plot.png"
        plt.savefig(out_path)
        print(f"График сохранён: {out_path}")
        plt.close()
    except ImportError:
        print("matplotlib не установлен — визуализация пропущена")

    conn.close()
    print("\nГотово!")


if __name__ == "__main__":
    main()
