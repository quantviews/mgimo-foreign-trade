#!/usr/bin/env python3
"""
Выгрузка заданных кодов ТН ВЭД из итоговой базы за последние 12 месяцев.

Итоговая база — `db/unified_trade_data.duckdb`, view `unified_trade_data_enriched`
(факт-таблица `unified_trade_data` + справочники стран/ТН ВЭД).

Коды в базе хранятся как нормализованные 10-значные строки (см. docs/data_model.md),
поэтому короткие коды (6/8 знаков) сопоставляются по префиксу нормализованного TNVED.

По умолчанию выгружаются коды: 310210, 310540, 310530, 31023090 (удобрения, группа 31).
Окно «последние 12 месяцев» отсчитывается от последнего доступного месяца в базе
(а не от системной даты), чтобы не терять данные из-за лага публикации.

Обработка данных — на polars; SQL-фильтрация — в DuckDB, результат отдаётся сразу в
polars.DataFrame через conn.execute(...).pl().

Пример:
    python scripts/extract_hs_codes_last12m.py
    python scripts/extract_hs_codes_last12m.py --codes 310210 310540 --months 6 --format excel
    python scripts/extract_hs_codes_last12m.py --facts-only
"""

import argparse
import logging
from pathlib import Path

import duckdb
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CODES = ["310210", "310540", "310530", "31023090"]

# Служебные/избыточные колонки, отбрасываемые по умолчанию (если присутствуют).
# Вернуть их можно флагом --keep-all-columns.
DROP_BY_DEFAULT = ["ISTPOZ", "ISTPOZ_ADI", "TNVED_EN_NAME", "TNVED_RU_NAME", "period_rank"]

# Колонки, которые обязаны быть числовыми при записи.
NUMERIC_COLUMNS = ["STOIM", "NETTO", "KOL"]


def ensure_numeric(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Гарантировать числовой тип у указанных колонок перед записью.

    Уже числовые колонки оставляем как есть; нечисловые приводим к Float64.
    Если приведение порождает новые NULL (значение не было числом) — предупреждаем
    и показываем примеры проблемных значений.
    """
    for col in columns:
        if col not in df.columns:
            continue
        if df[col].dtype.is_numeric():
            continue
        logger.warning(f"Колонка {col} имеет нечисловой тип {df[col].dtype}; привожу к Float64")
        cast = df[col].cast(pl.Float64, strict=False)
        # Значения, ставшие NULL после каста, но не бывшие NULL до него — не числа.
        bad_mask = cast.is_null() & df[col].is_not_null()
        n_bad = int(bad_mask.sum())
        if n_bad:
            examples = df[col].filter(bad_mask).unique().head(5).to_list()
            logger.warning(f"  {col}: {n_bad} значений не преобразованы в число (→ NULL). Примеры: {examples}")
        df = df.with_columns(cast.alias(col))
    return df


def build_query(codes: list[str], months: int, facts_only: bool) -> tuple[str, list]:
    """Собрать SQL: фильтр по префиксам кодов + окно последних `months` месяцев.

    Окно считается от максимального PERIOD в таблице:
    PERIOD > max(PERIOD) - `months` месяцев, т.е. включительно последние `months`
    отчётных месяцев (при max=2026-05 и months=12 это 2025-06 .. 2026-05).
    """
    # Каждый код сопоставляется по префиксу нормализованного 10-значного TNVED.
    code_clauses = " OR ".join(["TNVED LIKE ?" for _ in codes])
    params = [f"{code}%" for code in codes]

    facts_clause = "AND e.TYPE = 'fact'" if facts_only else ""

    query = f"""
        WITH bounds AS (
            SELECT date_trunc('month', max(PERIOD)) AS max_period
            FROM unified_trade_data
        )
        SELECT e.*
        FROM unified_trade_data_enriched e, bounds b
        WHERE ({code_clauses})
          AND e.PERIOD > b.max_period - INTERVAL '{int(months)} months'
          {facts_clause}
        ORDER BY e.TNVED, e.STRANA, e.NAPR, e.PERIOD
    """
    return query, params


def extract(
    db_path: Path,
    output_dir: Path,
    codes: list[str],
    months: int,
    output_format: str,
    facts_only: bool,
    keep_all_columns: bool,
) -> None:
    if not db_path.exists():
        logger.error(f"База не найдена: {db_path}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Подключение к базе: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)

    try:
        query, params = build_query(codes, months, facts_only)
        logger.info(f"Коды (по префиксу): {', '.join(codes)}")
        logger.info(f"Окно: последние {months} мес." + (" (только TYPE='fact')" if facts_only else ""))

        # DuckDB отдаёт результат сразу в polars.DataFrame
        df: pl.DataFrame = conn.execute(query, params).pl()

        if df.is_empty():
            logger.warning("Запрос не вернул данных")
            return

        if not keep_all_columns:
            drop = [c for c in DROP_BY_DEFAULT if c in df.columns]
            if drop:
                df = df.drop(drop)
                logger.info(f"Отброшены служебные колонки: {', '.join(drop)}")

        logger.info(f"Получено строк: {len(df):,}")
        logger.info(f"Диапазон периодов: {df['PERIOD'].min()} .. {df['PERIOD'].max()} ({df['PERIOD'].n_unique()} мес.)")

        # Гарантируем числовой тип STOIM/NETTO/KOL перед записью
        df = ensure_numeric(df, NUMERIC_COLUMNS)

        codes_part = "_".join(codes)
        facts_part = "_facts" if facts_only else ""
        ext = ".xlsx" if output_format.lower() == "excel" else ".csv"
        out_path = output_dir / f"hs_{codes_part}_last{months}m{facts_part}{ext}"

        if output_format.lower() == "excel":
            df.write_excel(out_path)
        else:
            df.write_csv(out_path)

        logger.info(f"Сохранено: {out_path}")

        # Короткая сводка по кодам x страна для контроля
        summary = (
            df.with_columns(pl.col("TNVED").str.slice(0, 8).alias("TNVED8"))
            .group_by("TNVED8", "STRANA")
            .agg(
                pl.len().alias("rows"),
                pl.col("STOIM").sum().round(1).alias("stoim_ths_usd"),
            )
            .sort("TNVED8", "STRANA")
        )
        with pl.Config(tbl_rows=-1, tbl_cols=-1):
            logger.info(f"Сводка по TNVED8 x страна ({len(summary)} строк):\n{summary}")

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Выгрузка кодов ТН ВЭД из итоговой базы за последние N месяцев.")
    parser.add_argument("--codes", nargs="+", default=DEFAULT_CODES,
                        help=f"Коды ТН ВЭД (по префиксу). По умолчанию: {' '.join(DEFAULT_CODES)}")
    parser.add_argument("--months", type=int, default=12, help="Размер окна в месяцах (по умолчанию 12)")
    parser.add_argument("--facts-only", action="store_true",
                        help="Только фактические данные (TYPE='fact'), без nowcast-прогноза")
    parser.add_argument("--keep-all-columns", action="store_true",
                        help=f"Не отбрасывать служебные колонки ({', '.join(DROP_BY_DEFAULT)})")
    parser.add_argument("--db-path", type=str, default=None,
                        help="Путь к unified_trade_data.duckdb (по умолчанию db/unified_trade_data.duckdb)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Каталог для выгрузки (по умолчанию data_interim_csv)")
    parser.add_argument("--format", choices=["csv", "excel"], default="csv", help="Формат выгрузки (по умолчанию csv)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    db_path = Path(args.db_path) if args.db_path else project_root / "db" / "unified_trade_data.duckdb"
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "data_interim_csv"

    extract(db_path, output_dir, args.codes, args.months, args.format, args.facts_only, args.keep_all_columns)


if __name__ == "__main__":
    main()
