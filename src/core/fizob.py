"""Приведение fizob-parquet к схеме таблицы fizob_index.

Преобразование выражено в SQL поверх `read_parquet`, а не в pandas: файлы
физобъёмов дают 39,5 млн строк, и материализация их в Python стоила около
8 ГБ памяти (2 ГБ на одном только fizob_4), потому что pandas хранит строковые
колонки как объекты. Самой работы там на десяток секунд — это выбор колонок,
переименование и константа уровня, — так что поднимать эти строки в процесс
незачем: DuckDB читает parquet сам.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

# Схема таблицы fizob_index.
FIZOB_INDEX_COLUMNS = ("STRANA", "NAPR", "PERIOD", "tn_level", "tn_code", "fizob", "fizob_bp")

# Имя файла -> (уровень ТН ВЭД, колонка кода, колонка физобъёма, колонка базового периода).
FIZOB_LEVELS = {
    "fizob_2": (2, "TNVED2", "fizob2", "fizob2_bp"),
    "fizob2": (2, "TNVED2", "fizob2", "fizob2_bp"),
    "fizob_4": (4, "TNVED4", "fizob4", "fizob4_bp"),
    "fizob4": (4, "TNVED4", "fizob4", "fizob4_bp"),
    "fizob_6": (6, "TNVED6", "fizob6", "fizob6_bp"),
    "fizob6": (6, "TNVED6", "fizob6", "fizob6_bp"),
}

TOTAL_STEMS = ("fizob_total", "fizob0")


def _quote(path: Path | str) -> str:
    return "'" + str(Path(path).resolve().as_posix()).replace("'", "''") + "'"


def build_fizob_select_sql(
    file_path: Path | str,
    file_stem: str,
    available_columns: Iterable[str],
    start_year: int | None = None,
) -> str | None:
    """SELECT, приводящий один fizob-файл к схеме fizob_index.

    Возвращает None, если файл неизвестного вида или в нём нет нужных колонок —
    как и прежняя реализация, такой файл просто пропускается.

    `available_columns` передаётся снаружи (из `DESCRIBE`), чтобы функция
    оставалась чистой и проверяемой без обращения к диску.
    """
    columns = set(available_columns)
    source = f"read_parquet({_quote(file_path)})"
    # PERIOD в файлах приходит меткой времени; в таблице хранится дата.
    period = "CAST(PERIOD AS DATE)"
    where = f" WHERE EXTRACT(YEAR FROM {period}) >= {int(start_year)}" if start_year else ""

    if "bp" in columns and "methodology_version" not in columns:
        has_fisher_price = bool(columns & {"price", "price2", "price4", "price6"})
        has_legacy_base = bool(columns & {"fizob_bp", "fizob2_bp", "fizob4_bp", "fizob6_bp"})
        if has_fisher_price and not has_legacy_base:
            raise ValueError("Unversioned Fisher file: rebuild with coverage and methodology metadata")

    if "methodology_version" in columns:
        if file_stem in TOTAL_STEMS:
            level, code, value, price = 0, "'0'", "fizob", "price"
        elif file_stem in FIZOB_LEVELS:
            level, code, value, _ = FIZOB_LEVELS[file_stem]
            price = f"price{level}"
        else:
            raise ValueError(f"Unknown Fisher file: {file_stem}")
        metadata = {"bp", "TYPE", "status", "coverage", "base_coverage", "stoim_12",
                    "available_stoim_12", "imp", "quantity_coverage", "mass_coverage",
                    "source_estimated_value", value, price}
        if level:
            metadata.add(code)
        if not metadata <= columns:
            raise ValueError(f"Incomplete Fisher schema: {sorted(metadata - columns)}")
        # bp is a DATE, not an index divisor. Fisher already has base level 1.
        # Keep metadata in the serving table; incomplete values remain NULL.
        return (
            f"SELECT upper(STRANA) AS STRANA, NAPR, {period} AS PERIOD, "
            f"{level} AS tn_level, {code} AS tn_code, "
            f"CASE WHEN methodology_version = 'fisher_annual_v2' AND TYPE = 'fact' "
            f"AND status IN ('complete', 'base') AND coverage = 1 AND bp IS NOT NULL "
            f"AND isfinite({value}) AND {value} >= 0 THEN {value} "
            f"ELSE NULL END AS fizob, CAST(1 AS DOUBLE) AS fizob_bp, CAST(bp AS DATE) AS base_period, "
            f"{price} AS price_index, methodology_version, TYPE, status, coverage, "
            "'observed_series_months' AS coverage_basis, "
            "base_coverage, stoim_12, available_stoim_12, imp, quantity_coverage, "
            f"mass_coverage, source_estimated_value FROM {source}{where}"
        )

    if file_stem in TOTAL_STEMS:
        if not {"fizob", "fizob_bp"} <= columns:
            logger.warning("%s: нет колонок fizob/fizob_bp, файл пропущен", file_stem)
            return None
        # Итоговый уровень: код обнуляется, но приводится через число, как это
        # делала прежняя реализация (fillna(0).astype(int).astype(str)).
        tn_code = (
            "CAST(CAST(COALESCE(TNVED2, '0') AS BIGINT) AS VARCHAR)"
            if "TNVED2" in columns
            else "'0'"
        )
        return (
            f"SELECT upper(STRANA) AS STRANA, NAPR, {period} AS PERIOD, "
            f"0 AS tn_level, {tn_code} AS tn_code, "
            f"fizob AS fizob, fizob_bp AS fizob_bp FROM {source}{where}"
        )

    if file_stem not in FIZOB_LEVELS:
        logger.warning("Неизвестный вид fizob-файла '%s', пропущен", file_stem)
        return None

    level, tnved_col, fizob_col, fizob_bp_col = FIZOB_LEVELS[file_stem]
    missing = [c for c in (tnved_col, fizob_col, fizob_bp_col) if c not in columns]
    if missing:
        logger.warning("%s: нет колонок %s, файл пропущен", file_stem, ", ".join(missing))
        return None

    return (
        f"SELECT upper(STRANA) AS STRANA, NAPR, {period} AS PERIOD, "
        f"{level} AS tn_level, {tnved_col} AS tn_code, "
        f"{fizob_col} AS fizob, {fizob_bp_col} AS fizob_bp FROM {source}{where}"
    )


__all__ = ["FIZOB_INDEX_COLUMNS", "FIZOB_LEVELS", "build_fizob_select_sql"]
