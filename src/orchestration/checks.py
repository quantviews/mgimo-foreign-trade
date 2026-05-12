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
)
OPTIONAL_FIZOB_TABLES = ("fizob_index", "fizob_index_v")
ALLOWED_NAPR = ("ИМ", "ЭК")
ALLOWED_TYPES = ("fact", "pred")
ALLOWED_SOURCES = ("national", "comtrade", "nowcast")


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


def run_sql_quality_checks(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    min_unified_rows: int = 1,
    require_fizob: bool = False,
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
