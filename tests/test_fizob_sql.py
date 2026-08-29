#!/usr/bin/env python3
"""Тесты приведения fizob-parquet к схеме fizob_index.

Преобразование выражено в SQL: 39,5 млн строк физобъёмов в pandas стоили около
8 ГБ памяти при том, что работы там на выбор колонок и константу уровня.
"""

import duckdb
import pandas as pd
import pytest

from core.fizob import FIZOB_LEVELS, build_fizob_select_sql

LEVEL_COLUMNS = ["STRANA", "NAPR", "TNVED4", "PERIOD", "fizob4", "fizob4_bp"]
TOTAL_COLUMNS = ["STRANA", "NAPR", "PERIOD", "fizob", "fizob_bp", "TNVED2"]


class TestSelectShape:
    def test_level_file_maps_columns_to_the_index_schema(self):
        sql = build_fizob_select_sql("f.parquet", "fizob_4", LEVEL_COLUMNS)
        assert "4 AS tn_level" in sql
        assert "TNVED4 AS tn_code" in sql
        assert "fizob4 AS fizob" in sql
        assert "fizob4_bp AS fizob_bp" in sql

    def test_strana_is_uppercased(self):
        sql = build_fizob_select_sql("f.parquet", "fizob_4", LEVEL_COLUMNS)
        assert "upper(STRANA) AS STRANA" in sql

    def test_period_is_cast_to_date(self):
        """В файлах PERIOD — метка времени, в таблице хранится дата."""
        sql = build_fizob_select_sql("f.parquet", "fizob_4", LEVEL_COLUMNS)
        assert "CAST(PERIOD AS DATE) AS PERIOD" in sql

    def test_total_file_uses_level_zero(self):
        sql = build_fizob_select_sql("f.parquet", "fizob_total", TOTAL_COLUMNS)
        assert "0 AS tn_level" in sql
        assert "AS tn_code" in sql

    def test_total_without_tnved2_falls_back_to_zero_code(self):
        sql = build_fizob_select_sql(
            "f.parquet", "fizob_total", ["STRANA", "NAPR", "PERIOD", "fizob", "fizob_bp"]
        )
        assert "'0' AS tn_code" in sql

    @pytest.mark.parametrize("stem", sorted(FIZOB_LEVELS))
    def test_every_known_stem_builds(self, stem):
        level, tnved_col, fizob_col, bp_col = FIZOB_LEVELS[stem]
        columns = ["STRANA", "NAPR", "PERIOD", tnved_col, fizob_col, bp_col]
        sql = build_fizob_select_sql("f.parquet", stem, columns)
        assert f"{level} AS tn_level" in sql


class TestSkipping:
    """Файл с неполной схемой пропускается, а не роняет сборку."""

    def test_unknown_stem_is_skipped(self):
        assert build_fizob_select_sql("f.parquet", "something_else", LEVEL_COLUMNS) is None

    def test_missing_value_column_is_skipped(self):
        columns = [c for c in LEVEL_COLUMNS if c != "fizob4_bp"]
        assert build_fizob_select_sql("f.parquet", "fizob_4", columns) is None

    def test_missing_code_column_is_skipped(self):
        columns = [c for c in LEVEL_COLUMNS if c != "TNVED4"]
        assert build_fizob_select_sql("f.parquet", "fizob_4", columns) is None

    def test_total_without_value_columns_is_skipped(self):
        assert build_fizob_select_sql("f.parquet", "fizob_total", ["STRANA", "NAPR"]) is None


class TestYearFilter:
    def test_start_year_filters_by_period(self):
        sql = build_fizob_select_sql("f.parquet", "fizob_4", LEVEL_COLUMNS, start_year=2019)
        assert "WHERE EXTRACT(YEAR FROM CAST(PERIOD AS DATE)) >= 2019" in sql

    def test_without_start_year_there_is_no_filter(self):
        sql = build_fizob_select_sql("f.parquet", "fizob_4", LEVEL_COLUMNS)
        assert "WHERE" not in sql


class TestAgainstRealParquet:
    """Сквозная проверка: SQL исполняется и даёт ожидаемые значения."""

    def make_parquet(self, tmp_path):
        path = tmp_path / "fizob_4.parquet"
        pd.DataFrame(
            {
                "STRANA": ["cn", "de"],
                "NAPR": ["ИМ", "ЭК"],
                "TNVED4": ["0101", "8471"],
                "PERIOD": pd.to_datetime(["2018-06-01", "2019-06-01"]),
                "fizob4": [1.5, 2.5],
                "price4": [10.0, 20.0],
                "bp": [1, 1],
                "fizob4_bp": [3.5, 4.5],
                "price4_bp": [30.0, 40.0],
            }
        ).to_parquet(path, index=False)
        return path

    def test_rows_match_the_index_schema(self, tmp_path):
        path = self.make_parquet(tmp_path)
        sql = build_fizob_select_sql(path, "fizob_4", pd.read_parquet(path).columns)
        rows = duckdb.connect().execute(sql).fetchall()
        assert len(rows) == 2
        strana, napr, period, level, code, fizob, fizob_bp = rows[0]
        assert (strana, napr, level, code, fizob, fizob_bp) == ("CN", "ИМ", 4, "0101", 1.5, 3.5)
        assert str(period) == "2018-06-01"

    def test_start_year_drops_earlier_periods(self, tmp_path):
        path = self.make_parquet(tmp_path)
        sql = build_fizob_select_sql(
            path, "fizob_4", pd.read_parquet(path).columns, start_year=2019
        )
        rows = duckdb.connect().execute(sql).fetchall()
        assert [r[4] for r in rows] == ["8471"]

    def test_path_with_apostrophe_is_quoted(self, tmp_path):
        odd = tmp_path / "it's data"
        odd.mkdir()
        path = odd / "fizob_4.parquet"
        pd.DataFrame(
            {
                "STRANA": ["cn"], "NAPR": ["ИМ"], "TNVED4": ["0101"],
                "PERIOD": pd.to_datetime(["2019-06-01"]),
                "fizob4": [1.0], "fizob4_bp": [2.0],
            }
        ).to_parquet(path, index=False)
        sql = build_fizob_select_sql(path, "fizob_4", pd.read_parquet(path).columns)
        assert duckdb.connect().execute(sql).fetchone()[0] == "CN"
