#!/usr/bin/env python3
"""Tests for SQL quality checks against the final DuckDB artifact."""

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from orchestration.checks import SqlQualityCheckError, run_sql_quality_checks


def create_quality_test_db(path: Path, *, invalid_napr: bool = False, pred_overlap: bool = False) -> None:
    """Create a minimal DuckDB file with the relations required by quality checks."""
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE unified_trade_data (
                NAPR VARCHAR,
                PERIOD DATE,
                STRANA VARCHAR,
                TNVED VARCHAR,
                EDIZM VARCHAR,
                EDIZM_ISO VARCHAR,
                STOIM DOUBLE,
                NETTO DOUBLE,
                KOL DOUBLE,
                TNVED4 VARCHAR,
                TNVED6 VARCHAR,
                TNVED2 VARCHAR,
                SOURCE VARCHAR,
                TYPE VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO unified_trade_data VALUES
            ('ИМ', DATE '2024-01-01', 'CN', '0101010000', 'ШТУКА', '796',
             100.0, 10.0, 2.0, '0101', '010101', '01', 'national', 'fact')
            """
        )

        if invalid_napr:
            conn.execute(
                """
                INSERT INTO unified_trade_data VALUES
                ('IMPORT', DATE '2024-02-01', 'CN', '0101010000', 'ШТУКА', '796',
                 100.0, 10.0, 2.0, '0101', '010101', '01', 'national', 'fact')
                """
            )

        if pred_overlap:
            conn.execute(
                """
                INSERT INTO unified_trade_data VALUES
                ('ИМ', DATE '2024-01-01', 'CN', '0101010000', NULL, NULL,
                 90.0, 9.0, NULL, '0101', '010101', '01', 'nowcast', 'pred')
                """
            )
        else:
            conn.execute(
                """
                INSERT INTO unified_trade_data VALUES
                ('ИМ', DATE '2024-02-01', 'CN', '0101010000', NULL, NULL,
                 90.0, 9.0, NULL, '0101', '010101', '01', 'nowcast', 'pred')
                """
            )

        conn.execute(
            """
            CREATE TABLE country_reference AS
            SELECT 'CN'::VARCHAR AS STRANA, 'КИТАЙ'::VARCHAR AS STRANA_NAME
            """
        )
        conn.execute(
            """
            CREATE TABLE tnved_reference AS
            SELECT '01'::VARCHAR AS TNVED_CODE, 2::INTEGER AS TNVED_LEVEL,
                   'ЖИВЫЕ ЖИВОТНЫЕ'::VARCHAR AS TNVED_NAME, FALSE AS TRANSLATED
            """
        )
        conn.execute(
            """
            CREATE TABLE hs4_reference AS
            SELECT '0101'::VARCHAR AS TNVED4,
                   'Лошади'::VARCHAR AS TNVED4_NAME_SHORT,
                   'ЛОШАДИ'::VARCHAR AS TNVED4_NAME_FULL
            """
        )
        conn.execute(
            """
            CREATE VIEW unified_trade_data_enriched AS
            SELECT * FROM unified_trade_data
            """
        )
    finally:
        conn.close()


def test_sql_quality_checks_pass_on_valid_db(tmp_path):
    db_path = tmp_path / "valid.duckdb"
    create_quality_test_db(db_path)

    metrics = run_sql_quality_checks(db_path)

    assert metrics["unified_trade_data_rows"] == 2
    assert metrics["invalid_napr_rows"] == 0
    assert metrics["pred_fact_overlap_rows"] == 0


def test_sql_quality_checks_fail_on_invalid_napr(tmp_path):
    db_path = tmp_path / "invalid_napr.duckdb"
    create_quality_test_db(db_path, invalid_napr=True)

    with pytest.raises(SqlQualityCheckError, match="NAPR"):
        run_sql_quality_checks(db_path)


def test_sql_quality_checks_fail_on_pred_fact_overlap(tmp_path):
    db_path = tmp_path / "overlap.duckdb"
    create_quality_test_db(db_path, pred_overlap=True)

    with pytest.raises(SqlQualityCheckError, match="overlaps fact"):
        run_sql_quality_checks(db_path)


def test_sql_quality_checks_fail_when_required_tables_missing(tmp_path):
    db_path = tmp_path / "missing_tables.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE unified_trade_data AS SELECT 1 AS id")
    conn.close()

    with pytest.raises(SqlQualityCheckError, match="Missing required tables"):
        run_sql_quality_checks(db_path)
