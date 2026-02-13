#!/usr/bin/env python3
"""
Практические задания к Занятию 4: Визуализация (альтернатива Superset).
Используйте, если Superset недоступен во время занятия.
Запускать из корня проекта: python lessons/practices/practice_04_superset.py
"""
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"
OUT_DIR = PROJECT_ROOT / "lessons" / "practices"


def main():
    if not DB_PATH.exists():
        print(f"ОШИБКА: База данных не найдена: {DB_PATH}")
        return

    conn = duckdb.connect(str(DB_PATH))
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("ПРАКТИКА 1: Динамика импорта (для графика)")
    print("=" * 60)
    df = conn.execute("""
        SELECT PERIOD, SUM(STOIM) as total
        FROM unified_trade_data
        WHERE PERIOD >= '2022-01-01' AND NAPR = 'ИМ'
        GROUP BY PERIOD
        ORDER BY PERIOD
    """).df()
    print(df.head(10).to_string(index=False))
    print()

    try:
        import matplotlib.pyplot as plt

        print("=" * 60)
        print("ПРАКТИКА 2: Построение графика")
        print("=" * 60)
        plt.figure(figsize=(10, 5))
        plt.plot(df["PERIOD"], df["total"])
        plt.title("Динамика импорта (тыс. USD)")
        plt.xlabel("Период")
        plt.ylabel("Стоимость, тыс. USD")
        plt.xticks(rotation=45)
        plt.tight_layout()
        out_path = OUT_DIR / "import_dynamics.png"
        plt.savefig(out_path)
        print(f"График сохранён: {out_path}")
        plt.close()
    except ImportError:
        print("matplotlib не установлен — установите: pip install matplotlib")

    print("=" * 60)
    print("ПРАКТИКА 3: Сводная таблица по странам и TNVED2")
    print("=" * 60)
    pivot = conn.execute("""
        SELECT STRANA, TNVED2, SUM(STOIM) as total
        FROM unified_trade_data
        WHERE PERIOD >= '2023-01-01' AND PERIOD < '2024-01-01'
          AND NAPR = 'ИМ' AND TNVED2 IN ('27', '84', '85', '87')
        GROUP BY STRANA, TNVED2
    """).df()

    pivot_wide = pivot.pivot(index="TNVED2", columns="STRANA", values="total")
    print(pivot_wide.to_string())
    print()

    conn.close()
    print("Готово!")
    print("\nПримечание: Для работы с Superset получите доступ к инстансу от преподавателя.")


if __name__ == "__main__":
    main()
