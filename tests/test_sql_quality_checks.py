#!/usr/bin/env python3
"""Tests for SQL quality checks against the final DuckDB artifact."""

import datetime as dt
from pathlib import Path

import duckdb
import pytest

from orchestration.checks import SqlQualityCheckError, run_sql_quality_checks


def create_quality_test_db(
    path: Path,
    *,
    invalid_napr: bool = False,
    pred_overlap: bool = False,
    mixed_script_name: str | None = None,
    mixed_script_source: str = "mt",
    extra_rows: list[tuple] | None = None,
) -> None:
    """Create a minimal DuckDB file with the relations required by quality checks.

    ``extra_rows`` appends raw unified_trade_data rows (14-tuple in column order:
    NAPR, PERIOD, STRANA, TNVED, EDIZM, EDIZM_ISO, STOIM, NETTO, KOL, TNVED4,
    TNVED6, TNVED2, SOURCE, TYPE) so completeness scenarios can be built.
    """
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

        if extra_rows:
            conn.executemany(
                "INSERT INTO unified_trade_data VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                extra_rows,
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
                   'ЖИВЫЕ ЖИВОТНЫЕ'::VARCHAR AS TNVED_NAME,
                   'fts'::VARCHAR AS NAME_SOURCE, FALSE AS TRANSLATED
            """
        )
        if mixed_script_name is not None:
            conn.execute(
                "INSERT INTO tnved_reference VALUES ('0101', 4, ?, ?, FALSE)",
                [mixed_script_name, mixed_script_source],
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


def test_sql_quality_checks_fail_on_mixed_script_generated_name(tmp_path):
    """Машинное наименование, оборвавшееся на латинице, роняет сборку."""
    db_path = tmp_path / "mixed_generated.duckdb"
    create_quality_test_db(
        db_path, mixed_script_name="ПОТASSIUM МЕТАБИСУЛЬФИТ", mixed_script_source="mt"
    )

    with pytest.raises(SqlQualityCheckError, match="mixing"):
        run_sql_quality_checks(db_path)


def test_sql_quality_checks_report_mixed_script_official_name(tmp_path):
    """Латинские двойники в справочнике ФТС — метрика, а не отказ сборки."""
    db_path = tmp_path / "mixed_official.duckdb"
    create_quality_test_db(
        db_path, mixed_script_name="ДИЭЛЕКТPИЧЕСКИХ ПОТЕРЬ", mixed_script_source="fts"
    )

    metrics = run_sql_quality_checks(db_path)

    assert metrics["mixed_script_official_names"] == 1
    assert metrics["mixed_script_generated_names"] == 0


def test_sql_quality_checks_fail_when_required_tables_missing(tmp_path):
    db_path = tmp_path / "missing_tables.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE unified_trade_data AS SELECT 1 AS id")
    conn.close()

    with pytest.raises(SqlQualityCheckError, match="Missing required tables"):
        run_sql_quality_checks(db_path)


# --- Completeness checks ----------------------------------------------------

def _nat_row(strana, period, *, stoim=100.0, netto=10.0, kol=2.0, tnved="0202300000"):
    """One national fact row for unified_trade_data (14-tuple in column order)."""
    return (
        "ИМ", period, strana, tnved, "KGS", "166", stoim, netto, kol,
        tnved[:4], tnved[:6], tnved[:2], "national", "fact",
    )


def test_completeness_metrics_clean_on_valid_db(tmp_path):
    """Базовая валидная БД: метрики полноты присутствуют и пусты."""
    db_path = tmp_path / "clean.duckdb"
    create_quality_test_db(db_path)

    metrics = run_sql_quality_checks(db_path)

    assert metrics["zero_value_fact_months"] == 0
    assert metrics["national_month_gap_count"] == 0
    assert metrics["low_volume_month_flag_count"] == 0


def test_completeness_fails_on_national_skeleton_month(tmp_path):
    """Месяц national-источника со всеми нулями (скелет MEIDB) роняет сборку."""
    db_path = tmp_path / "skeleton.duckdb"
    create_quality_test_db(
        db_path,
        extra_rows=[_nat_row("IN", dt.date(2026, 5, 1), stoim=0.0, netto=0.0, kol=0.0)],
    )

    with pytest.raises(SqlQualityCheckError, match="skeleton"):
        run_sql_quality_checks(db_path)


def test_completeness_reports_internal_month_gap(tmp_path):
    """Дырка в помесячном ряду national-страны — метрика; опционально — отказ."""
    db_path = tmp_path / "gap.duckdb"
    create_quality_test_db(
        db_path,
        extra_rows=[
            _nat_row("TR", dt.date(2024, 1, 1)),
            _nat_row("TR", dt.date(2024, 3, 1)),  # февраль пропущен
        ],
    )

    metrics = run_sql_quality_checks(db_path)
    assert metrics["national_month_gap_count"] == 1
    tr_gap = [g for g in metrics["national_month_gaps"] if g["STRANA"] == "TR"]
    assert tr_gap and tr_gap[0]["missing"] == ["2024-02-01"]

    # По умолчанию не валит (у Индии бывает лаг); с флагом — валит.
    with pytest.raises(SqlQualityCheckError, match="internal month gaps"):
        run_sql_quality_checks(db_path, fail_on_national_gaps=True)


def test_completeness_flags_low_volume_month(tmp_path):
    """Месяц с обвалом числа строк (обрезка/частичный) попадает в метрику."""
    rows = []
    for month in range(1, 7):  # 2024-01 … 2024-06
        count = 1 if month == 2 else 10  # февраль — резкий провал
        for i in range(count):
            rows.append(_nat_row("TR", dt.date(2024, month, 1), tnved=f"02023000{i:02d}"))
    db_path = tmp_path / "lowvol.duckdb"
    create_quality_test_db(db_path, extra_rows=rows)

    metrics = run_sql_quality_checks(db_path)
    flagged = [
        f for f in metrics["low_volume_month_flags"]
        if f["STRANA"] == "TR" and f["PERIOD"] == "2024-02-01"
    ]
    assert flagged and flagged[0]["rows"] == 1 and flagged[0]["median"] == 10
