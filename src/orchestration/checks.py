"""SQL quality gates for the final DuckDB artifact."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"

REQUIRED_UNIFIED_COLUMNS = frozenset(
    {
        "NAPR",
        "PERIOD",
        "STRANA",
        "TNVED",
        "STOIM",
        "NETTO",
        "KOL",
        "SOURCE",
        "TYPE",
    }
)
REQUIRED_TABLES = (
    "unified_trade_data",
    "unified_trade_data_enriched",
    "country_reference",
    "tnved_reference",
    "hs4_reference",
)
OPTIONAL_FIZOB_TABLES = ("fizob_index", "fizob_index_v")
ALLOWED_NAPR = ("ИМ", "ЭК")
ALLOWED_TYPES = ("fact", "pred")
ALLOWED_SOURCES = ("national", "comtrade", "nowcast")

# Кириллица вплотную к латинице внутри слова — след машинного перевода,
# оборвавшегося на полуслове ("ПОТASSIUM МЕТАБИСУЛЬФИТ", "МОНOgидРИЧЕСКИЕ").
MIXED_SCRIPT_NAME_REGEX = r'[А-Яа-яЁё][A-Za-z]|[A-Za-z][А-Яа-яЁё]'
# Проверяются только наименования, которые производим мы сами. В официальном
# справочнике ФТС встречаются латинские двойники кириллических букв
# ("ДИЭЛЕКТPИЧЕСКИХ", "CЕЙФЫ") — это дефект исходника, его чинят отдельно,
# а не падением сборки.
GENERATED_NAME_SOURCES = ("mt", "manual")

# Completeness. National processors (Китай/Индия/Турция) публикуют помесячно
# сплошняком, поэтому у них ловим: скелет-месяцы (вся строка нулевая — плейсхолдер
# MEIDB, как майская Индия 2026), внутренние дырки в помесячном ряду и резкие
# провалы объёма (обрезка/частичный месяц, как экспорт Германии на 50000 строк).
NATIONAL_SOURCE = "national"
# Месяц считается «провалом», если fact-строк в нём меньше этой доли от медианы
# группы (STRANA, SOURCE); последние месяцы могут быть частичными по естественным
# причинам, поэтому свежий хвост из проверки исключаем.
LOW_VOLUME_RATIO = 0.4
LOW_VOLUME_SKIP_RECENT_MONTHS = 2
MAX_COMPLETENESS_EXAMPLES = 50


class SqlQualityCheckError(RuntimeError):
    """Raised when one or more SQL quality checks fail."""


def _sql_list(values: tuple[str, ...]) -> str:
    """Render a small trusted tuple of string literals for SQL IN clauses."""
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _scalar(conn: duckdb.DuckDBPyConnection, query: str) -> Any:
    """Return the first scalar value from a DuckDB query."""
    return conn.execute(query).fetchone()[0]


def _check_non_empty_relation(
    conn: duckdb.DuckDBPyConnection,
    relation_name: str,
    results: dict[str, Any],
    failures: list[str],
) -> None:
    count = int(_scalar(conn, f"SELECT COUNT(*) FROM {relation_name}"))
    results[f"{relation_name}_rows"] = count
    if count == 0:
        failures.append(f"{relation_name} is empty")


def _check_completeness(
    conn: duckdb.DuckDBPyConnection,
    results: dict[str, Any],
    failures: list[str],
    *,
    fail_on_national_gaps: bool,
) -> None:
    """Data-completeness checks over fact rows (TYPE <> 'pred').

    - all-zero "skeleton" months per (STRANA, SOURCE) -> hard failure for
      national sources (they should never carry a placeholder month);
    - internal month gaps in each national country's monthly run -> metric,
      optionally a failure;
    - months whose fact-row count collapses far below the group's median -> metric.
    """
    # 1. Skeleton months: STOIM, KOL and NETTO all sum to zero.
    zero_months = conn.execute(
        """
        SELECT UPPER(TRIM(STRANA)) AS STRANA, LOWER(TRIM(SOURCE)) AS SOURCE,
               CAST(PERIOD AS DATE) AS PERIOD
        FROM unified_trade_data
        WHERE LOWER(TRIM(TYPE)) <> 'pred' AND PERIOD IS NOT NULL
        GROUP BY 1, 2, 3
        HAVING SUM(COALESCE(STOIM, 0)) = 0
           AND SUM(COALESCE(KOL, 0)) = 0
           AND SUM(COALESCE(NETTO, 0)) = 0
        ORDER BY 1, 2, 3
        """
    ).fetchall()
    results["zero_value_fact_months"] = len(zero_months)
    results["zero_value_fact_month_examples"] = [
        {"STRANA": s, "SOURCE": src, "PERIOD": str(p)}
        for s, src, p in zero_months[:MAX_COMPLETENESS_EXAMPLES]
    ]
    national_zero = [f"{s} {p}" for s, src, p in zero_months if src == NATIONAL_SOURCE]
    if national_zero:
        failures.append(
            f"national source has {len(national_zero)} all-zero (skeleton) months: "
            f"{national_zero[:MAX_COMPLETENESS_EXAMPLES]}"
        )

    # 2. Internal month gaps per national country (between its own min and max).
    gaps = conn.execute(
        f"""
        WITH months AS (
            SELECT DISTINCT UPPER(TRIM(STRANA)) AS STRANA, CAST(PERIOD AS DATE) AS P
            FROM unified_trade_data
            WHERE LOWER(TRIM(SOURCE)) = '{NATIONAL_SOURCE}'
              AND LOWER(TRIM(TYPE)) <> 'pred' AND PERIOD IS NOT NULL
        ),
        bounds AS (SELECT STRANA, MIN(P) AS mn, MAX(P) AS mx FROM months GROUP BY STRANA),
        grid AS (
            SELECT b.STRANA, gs::DATE AS P
            FROM bounds b,
                 generate_series(b.mn, b.mx, INTERVAL 1 MONTH) AS t(gs)
        )
        SELECT g.STRANA, g.P
        FROM grid g
        LEFT JOIN months m ON g.STRANA = m.STRANA AND g.P = m.P
        WHERE m.P IS NULL
        ORDER BY g.STRANA, g.P
        """
    ).fetchall()
    gaps_by_country: dict[str, list[str]] = {}
    for strana, period in gaps:
        gaps_by_country.setdefault(strana, []).append(str(period))
    results["national_month_gap_count"] = len(gaps)
    results["national_month_gaps"] = [
        {"STRANA": strana, "missing": periods}
        for strana, periods in sorted(gaps_by_country.items())
    ]
    if gaps and fail_on_national_gaps:
        failures.append(
            f"national sources have {len(gaps)} internal month gaps: "
            f"{results['national_month_gaps']}"
        )

    # 3. Volume collapse in a national series: a month far below the country's
    #    median row count, ignoring the freshest months (which may be legitimately
    #    partial). Scoped to national sources on purpose — Comtrade carries many
    #    genuinely sparse series (e.g. UA after 2022) that would drown the signal,
    #    and its completeness is covered by the collector manifest + truncation SQL.
    low_volume = conn.execute(
        f"""
        WITH counts AS (
            SELECT UPPER(TRIM(STRANA)) AS STRANA, LOWER(TRIM(SOURCE)) AS SOURCE,
                   CAST(PERIOD AS DATE) AS P, COUNT(*) AS n
            FROM unified_trade_data
            WHERE LOWER(TRIM(SOURCE)) = '{NATIONAL_SOURCE}'
              AND LOWER(TRIM(TYPE)) <> 'pred' AND PERIOD IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY STRANA, SOURCE ORDER BY P DESC) AS recency,
                   MEDIAN(n) OVER (PARTITION BY STRANA, SOURCE) AS med
            FROM counts
        )
        SELECT STRANA, SOURCE, P, n, med
        FROM ranked
        WHERE recency > {LOW_VOLUME_SKIP_RECENT_MONTHS}
          AND med > 0 AND n < {LOW_VOLUME_RATIO} * med
        ORDER BY n * 1.0 / med
        """
    ).fetchall()
    results["low_volume_month_flag_count"] = len(low_volume)
    results["low_volume_month_flags"] = [
        {"STRANA": s, "SOURCE": src, "PERIOD": str(p), "rows": int(n), "median": int(med)}
        for s, src, p, n, med in low_volume[:MAX_COMPLETENESS_EXAMPLES]
    ]


def run_sql_quality_checks(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    min_unified_rows: int = 1,
    require_fizob: bool = False,
    fail_on_national_gaps: bool = False,
) -> dict[str, Any]:
    """Run SQL checks against the final DuckDB artifact.

    Returns a metrics dictionary when every check passes. Raises
    SqlQualityCheckError with all detected failures otherwise.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise SqlQualityCheckError(f"DuckDB file does not exist: {db_path}")

    results: dict[str, Any] = {"db_path": str(db_path)}
    failures: list[str] = []

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        table_names = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        results["tables"] = sorted(table_names)

        required_tables = set(REQUIRED_TABLES)
        if require_fizob:
            required_tables.update(OPTIONAL_FIZOB_TABLES)

        missing_tables = sorted(required_tables - table_names)
        if missing_tables:
            failures.append(f"Missing required tables/views: {missing_tables}")

        if "unified_trade_data" not in table_names:
            raise SqlQualityCheckError("; ".join(failures))

        unified_columns = {
            row[0] for row in conn.execute("DESCRIBE unified_trade_data").fetchall()
        }
        missing_columns = sorted(REQUIRED_UNIFIED_COLUMNS - unified_columns)
        results["unified_columns"] = sorted(unified_columns)
        if missing_columns:
            failures.append(f"unified_trade_data missing columns: {missing_columns}")

        unified_rows = int(_scalar(conn, "SELECT COUNT(*) FROM unified_trade_data"))
        results["unified_trade_data_rows"] = unified_rows
        if unified_rows < min_unified_rows:
            failures.append(
                f"unified_trade_data has {unified_rows} rows, expected at least {min_unified_rows}"
            )

        if not missing_columns:
            null_period_rows = int(
                _scalar(conn, "SELECT COUNT(*) FROM unified_trade_data WHERE PERIOD IS NULL")
            )
            results["null_period_rows"] = null_period_rows
            if null_period_rows:
                failures.append(f"PERIOD has {null_period_rows} NULL rows")

            invalid_napr_rows = int(
                _scalar(
                    conn,
                    f"""
                    SELECT COUNT(*)
                    FROM unified_trade_data
                    WHERE NAPR IS NULL OR TRIM(NAPR) NOT IN ({_sql_list(ALLOWED_NAPR)})
                    """,
                )
            )
            results["invalid_napr_rows"] = invalid_napr_rows
            if invalid_napr_rows:
                failures.append(f"NAPR has {invalid_napr_rows} invalid rows")

            invalid_type_rows = int(
                _scalar(
                    conn,
                    f"""
                    SELECT COUNT(*)
                    FROM unified_trade_data
                    WHERE TYPE IS NULL OR LOWER(TRIM(TYPE)) NOT IN ({_sql_list(ALLOWED_TYPES)})
                    """,
                )
            )
            results["invalid_type_rows"] = invalid_type_rows
            if invalid_type_rows:
                failures.append(f"TYPE has {invalid_type_rows} invalid rows")

            invalid_source_rows = int(
                _scalar(
                    conn,
                    f"""
                    SELECT COUNT(*)
                    FROM unified_trade_data
                    WHERE SOURCE IS NULL OR LOWER(TRIM(SOURCE)) NOT IN ({_sql_list(ALLOWED_SOURCES)})
                    """,
                )
            )
            results["invalid_source_rows"] = invalid_source_rows
            if invalid_source_rows:
                failures.append(f"SOURCE has {invalid_source_rows} invalid rows")

            pred_fact_overlap_rows = int(
                _scalar(
                    conn,
                    """
                    WITH fact_keys AS (
                        SELECT DISTINCT
                            CAST(PERIOD AS DATE) AS PERIOD,
                            UPPER(TRIM(STRANA)) AS STRANA,
                            TRIM(TNVED) AS TNVED,
                            TRIM(NAPR) AS NAPR
                        FROM unified_trade_data
                        WHERE LOWER(TRIM(TYPE)) <> 'pred'
                          AND PERIOD IS NOT NULL
                    ),
                    pred_keys AS (
                        SELECT
                            CAST(PERIOD AS DATE) AS PERIOD,
                            UPPER(TRIM(STRANA)) AS STRANA,
                            TRIM(TNVED) AS TNVED,
                            TRIM(NAPR) AS NAPR
                        FROM unified_trade_data
                        WHERE LOWER(TRIM(TYPE)) = 'pred'
                          AND PERIOD IS NOT NULL
                    )
                    SELECT COUNT(*)
                    FROM pred_keys p
                    INNER JOIN fact_keys f
                        ON p.PERIOD = f.PERIOD
                       AND p.STRANA = f.STRANA
                       AND p.TNVED = f.TNVED
                       AND p.NAPR = f.NAPR
                    """,
                )
            )
            results["pred_fact_overlap_rows"] = pred_fact_overlap_rows
            if pred_fact_overlap_rows:
                failures.append(
                    f"TYPE='pred' overlaps fact rows in {pred_fact_overlap_rows} trade cells"
                )

            period_min, period_max = conn.execute(
                "SELECT MIN(PERIOD), MAX(PERIOD) FROM unified_trade_data"
            ).fetchone()
            results["period_min"] = str(period_min) if period_min is not None else None
            results["period_max"] = str(period_max) if period_max is not None else None
            if period_min is None or period_max is None:
                failures.append("PERIOD min/max is NULL")

            source_type_counts = conn.execute(
                """
                SELECT SOURCE, TYPE, COUNT(*) AS row_count
                FROM unified_trade_data
                GROUP BY SOURCE, TYPE
                ORDER BY SOURCE, TYPE
                """
            ).fetchall()
            results["source_type_counts"] = [
                {"SOURCE": source, "TYPE": type_value, "row_count": count}
                for source, type_value, count in source_type_counts
            ]

            _check_completeness(
                conn, results, failures, fail_on_national_gaps=fail_on_national_gaps
            )

        if "tnved_reference" in table_names:
            reference_columns = {
                row[0] for row in conn.execute("DESCRIBE tnved_reference").fetchall()
            }
            if "NAME_SOURCE" in reference_columns:
                mixed_generated, mixed_official = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) FILTER (
                            WHERE NAME_SOURCE IN ({_sql_list(GENERATED_NAME_SOURCES)})
                        ),
                        COUNT(*) FILTER (
                            WHERE NAME_SOURCE NOT IN ({_sql_list(GENERATED_NAME_SOURCES)})
                        )
                    FROM tnved_reference
                    WHERE regexp_matches(TNVED_NAME, '{MIXED_SCRIPT_NAME_REGEX}')
                    """
                ).fetchone()
                results["mixed_script_generated_names"] = int(mixed_generated)
                results["mixed_script_official_names"] = int(mixed_official)
                if mixed_generated:
                    failures.append(
                        f"tnved_reference has {mixed_generated} generated names mixing "
                        "Cyrillic and Latin inside a word"
                    )

        for relation in REQUIRED_TABLES:
            if relation in table_names and relation != "unified_trade_data":
                _check_non_empty_relation(conn, relation, results, failures)

        if require_fizob:
            for relation in OPTIONAL_FIZOB_TABLES:
                if relation in table_names:
                    _check_non_empty_relation(conn, relation, results, failures)

    finally:
        conn.close()

    if failures:
        raise SqlQualityCheckError("; ".join(failures))

    return results
