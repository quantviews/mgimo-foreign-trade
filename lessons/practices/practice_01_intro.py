#!/usr/bin/env python3
"""
Практические задания к Занятию 1: Работа с прямыми данными ФТС.
Сначала создайте сводный датасет: python src/load_fts_csv.py
Запуск: python lessons/practices/practice_01_intro.py
"""
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FTS_PARQUET = PROJECT_ROOT / "data_processed" / "fts_2021_2022.parquet"


def main():
    if not FTS_PARQUET.exists():
        print(f"ОШИБКА: Файл не найден: {FTS_PARQUET}")
        print("Сначала выполните: python src/load_fts_csv.py")
        print("Убедитесь, что CSV-файлы ФТС лежат в data_raw/fts_data/")
        return

    df = pd.read_parquet(FTS_PARQUET)

    print("=" * 60)
    print("ПРАКТИКА 1: Загрузка данных ФТС")
    print("=" * 60)
    print(f"Загружено строк: {len(df):,}")
    print(f"Колонки: {df.columns.tolist()}")
    print()

    print("=" * 60)
    print("ПРАКТИКА 2: Структура данных")
    print("=" * 60)
    print("Направления (NAPR):", df["NAPR"].unique().tolist())
    print("Период:", df["PERIOD"].min(), "—", df["PERIOD"].max())
    print("Стран:", df["STRANA"].nunique())
    print()

    print("=" * 60)
    print("ПРАКТИКА 3: Записей по странам")
    print("=" * 60)
    by_country = df.groupby("STRANA").size().sort_values(ascending=False).head(15)
    print(by_country.to_string())
    print()

    print("=" * 60)
    print("ПРАКТИКА 4: Импорт по странам (сумма STOIM, тыс. USD)")
    print("=" * 60)
    import_by_country = (
        df[df["NAPR"] == "ИМ"]
        .groupby("STRANA")["STOIM"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    # STOIM в тыс. USD — показываем в млрд для читаемости
    for country, val in import_by_country.items():
        print(f"  {country}: {val/1e6:,.1f} млрд USD")
    print()

    print("=" * 60)
    print("ПРАКТИКА 5: Топ товарных групп (TNVED2) по сумме STOIM")
    print("=" * 60)
    df_valid = df[
        df["TNVED2"].notna()
        & (df["TNVED2"].astype(str).str.strip() != "")
        & (df["TNVED2"].astype(str).str.len() >= 2)
    ]
    top_tnved2 = (
        df_valid.groupby("TNVED2")
        .agg(total_stoim=("STOIM", "sum"), cnt=("TNVED", "count"))
        .sort_values("total_stoim", ascending=False)
        .head(15)
    )
    top_tnved2["млрд USD"] = (top_tnved2["total_stoim"] / 1e6).round(1)
    print(top_tnved2[["млрд USD", "cnt"]].to_string())
    print()

    print("Готово!")


if __name__ == "__main__":
    main()
