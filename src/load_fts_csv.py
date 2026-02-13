#!/usr/bin/env python3
"""
Загрузка и объединение CSV-файлов ФТС из data_raw/fts_data/.

Ожидаемая структура папки:
  data_raw/fts_data/
    2021-01.csv
    2021-02.csv
    ...
    2022-01.csv

Файлы могут иметь разные форматы. Поддерживаются:
- Разделитель: ; или ,
- Кодировка: utf-8, cp1251
- Колонки: маппинг через FTS_COLUMN_MAP (см. ниже)

Запуск: python src/load_fts_csv.py
"""
from pathlib import Path
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FTS_DIR = PROJECT_ROOT / "data_raw" / "fts_data"
OUTPUT_PARQUET = PROJECT_ROOT / "data_processed" / "fts_2021_2022.parquet"

# Маппинг колонок ФТС -> наша схема
# Добавьте свои варианты названий, если файлы отличаются
FTS_COLUMN_MAP = {
    # Направление: ИМ/ЭК или 1/2 или Импорт/Экспорт
    "NAPR": ["NAPR", "napr", "Направление", "Тип", "Тур del", "flow", "Flow"],
    # Код страны: ISO2 или цифровой код
    "STRANA": ["STRANA", "strana", "Страна", "KOD_STR", "Kod_STR", "country", "partner"],
    # Код ТН ВЭД
    "TNVED": ["TNVED", "tnved", "ТН ВЭД", "G33", "G33_10", "commodity", "cmdCode"],
    # Стоимость (тыс. USD)
    "STOIM": ["STOIM", "stoim", "Стоимость", "G46", "value", "primaryValue"],
    # Вес нетто (кг)
    "NETTO": ["NETTO", "netto", "Вес нетто", "G38", "netWgt"],
    # Количество в доп. единице
    "KOL": ["KOL", "kol", "Количество", "G31_7", "Kolvo2", "qty", "altQty"],
    # Единица измерения
    "EDIZM": ["EDIZM", "edizm", "Единица", "ED_IZM", "qtyUnitAbbr", "altQtyUnitAbbr"],
}


def _detect_sep_encoding(path: Path) -> tuple:
    """Определяет разделитель и кодировку по первой строке."""
    for enc in ["utf-8", "cp1251", "latin1"]:
        try:
            with open(path, encoding=enc) as f:
                first = f.readline()
            if ";" in first and first.count(";") > first.count(","):
                return ";", enc
            return ",", enc
        except UnicodeDecodeError:
            continue
    return ",", "utf-8"


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Сопоставляет колонки ФТС с нашей схемой."""
    result = {}
    cols_upper = {c.upper(): c for c in df.columns}
    cols_lower = {c.lower(): c for c in df.columns}
    cols_raw = {c: c for c in df.columns}

    for our_col, variants in FTS_COLUMN_MAP.items():
        found = None
        for v in variants:
            key = v.upper() if len(v) > 2 else v
            if key in cols_upper:
                found = cols_upper[key]
                break
            if v in cols_raw:
                found = v
                break
        if found is not None:
            result[our_col] = df[found]

    return pd.DataFrame(result) if result else pd.DataFrame()


def _parse_period_from_filename(name: str):
    """Извлекает период из имени файла: 2021-01.csv, 202101.csv, 2021_01.csv."""
    m = re.search(r"(\d{4})[-_]?(\d{2})", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


def load_fts_csv_files() -> pd.DataFrame:
    """Загружает все CSV из data_raw/fts_data/ и объединяет в один DataFrame."""
    if not FTS_DIR.exists():
        logger.error(f"Папка не найдена: {FTS_DIR}")
        return pd.DataFrame()

    csv_files = sorted(FTS_DIR.glob("*.csv"))
    if not csv_files:
        logger.error(f"CSV-файлы не найдены в {FTS_DIR}")
        return pd.DataFrame()

    logger.info(f"Найдено файлов: {len(csv_files)}")

    dfs = []
    for path in csv_files:
        period = _parse_period_from_filename(path.name)
        sep, enc = _detect_sep_encoding(path)

        try:
            df = pd.read_csv(path, sep=sep, encoding=enc, low_memory=False, dtype=str)
        except Exception as e:
            logger.warning(f"Ошибка чтения {path.name}: {e}")
            continue

        if df.empty:
            continue

        mapped = _map_columns(df)
        if mapped.empty:
            logger.warning(f"Не удалось сопоставить колонки в {path.name}. Колонки: {list(df.columns)}")
            # Пробуем использовать как есть, если названия похожи
            if "STOIM" in df.columns or "G46" in df.columns or "Стоимость" in df.columns:
                mapped = df.copy()
                if period and "PERIOD" not in mapped.columns:
                    mapped["PERIOD"] = period
            else:
                continue

        if period and "PERIOD" not in mapped.columns:
            mapped["PERIOD"] = period

        # Нормализация NAPR
        if "NAPR" in mapped.columns:
            napr = mapped["NAPR"].astype(str).str.upper().str.strip()
            napr = napr.replace({"1": "ИМ", "2": "ЭК", "ИМПОРТ": "ИМ", "ЭКСПОРТ": "ЭК", "IMPORT": "ИМ", "EXPORT": "ЭК"})
            mapped["NAPR"] = napr

        # Числовые колонки
        for col in ["STOIM", "NETTO", "KOL"]:
            if col in mapped.columns:
                mapped[col] = pd.to_numeric(mapped[col].replace("", None), errors="coerce")

        # TNVED — строка, дополнение нулями справа до 10
        if "TNVED" in mapped.columns:
            tnved = mapped["TNVED"].astype(str).str.strip().str.replace(r"\D", "", regex=True)
            mapped["TNVED"] = tnved.apply(lambda x: (x + "0" * (10 - len(x)))[:10] if len(x) > 0 else "")

        # TNVED2, TNVED4, TNVED6
        if "TNVED" in mapped.columns:
            mapped["TNVED2"] = mapped["TNVED"].str[:2]
            mapped["TNVED4"] = mapped["TNVED"].str[:4]
            mapped["TNVED6"] = mapped["TNVED"].str[:6]

        # STRANA — приведение к ISO2 если нужно (цифровой код)
        if "STRANA" in mapped.columns:
            mapped["STRANA"] = mapped["STRANA"].astype(str).str.strip().str.upper()

        dfs.append(mapped)
        logger.info(f"  Загружен {path.name}: {len(mapped)} строк")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)

    # PERIOD в datetime
    if "PERIOD" in combined.columns:
        combined["PERIOD"] = pd.to_datetime(combined["PERIOD"], errors="coerce")

    # Удаление строк без ключевых полей
    required = ["NAPR", "PERIOD", "STRANA", "TNVED", "STOIM"]
    for r in required:
        if r in combined.columns:
            combined = combined.dropna(subset=[r])
        else:
            logger.warning(f"Колонка {r} отсутствует в итоговом датасете")

    return combined


def main():
    df = load_fts_csv_files()
    if df.empty:
        logger.error("Нет данных для сохранения.")
        return

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET, index=False)
    logger.info(f"Сохранено {len(df)} строк в {OUTPUT_PARQUET}")

    # Краткая сводка
    print("\n=== Сводка по данным ФТС ===")
    print(f"Строк: {len(df)}")
    print(f"Период: {df['PERIOD'].min()} — {df['PERIOD'].max()}")
    if "NAPR" in df.columns:
        print(df["NAPR"].value_counts())
    if "STRANA" in df.columns:
        print(f"Стран: {df['STRANA'].nunique()}")


if __name__ == "__main__":
    main()
