"""Fisher schema keeps dates, coverage and source-estimate metadata."""
import datetime as dt

import duckdb
import pytest

from core.fizob import build_fizob_select_sql
from pipelines.merge_pipeline import save_fizob_index
from core.reference_tables import build_fizob_enriched_table_sql


def fixture_file(tmp_path, status="complete", version="fisher_annual_v2", missing_base=False):
    path = tmp_path / "fizob_total.parquet"
    con = duckdb.connect()
    con.execute("""CREATE TABLE x AS SELECT 'ALL' AS STRANA, 'ЭК' AS NAPR,
      DATE '2026-08-01' AS PERIOD, 1.2 AS fizob, 1.1 AS price,
      DATE '2019-01-01' AS bp, 100.0 AS stoim_12,
      100.0 AS available_stoim_12, .2 AS imp, 1.0 AS coverage,
      1.0 AS base_coverage, .8 AS quantity_coverage, .9 AS mass_coverage,
      5.0 AS source_estimated_value, 'fact' AS TYPE,
      ? AS status, ? AS methodology_version""", [status, version])
    if missing_base:
        con.execute("UPDATE x SET bp=NULL")
    con.execute("COPY x TO ? (FORMAT PARQUET)", [str(path)])
    columns = [r[0] for r in con.execute("describe x").fetchall()]
    con.close()
    return path, columns


def test_date_base_and_quality_survive_loading(tmp_path):
    path, _ = fixture_file(tmp_path)
    database = tmp_path / "test.duckdb"
    save_fizob_index([path], database)
    with duckdb.connect(str(database)) as con:
        row = con.execute("select idx,base_period,price_index,coverage,source_estimated_value,TYPE from fizob_index_v").fetchone()
        con.execute("""CREATE TABLE country_reference(STRANA VARCHAR,STRANA_NAME VARCHAR);
          CREATE TABLE tnved_country_chapters(STRANA VARCHAR,TNVED_CODE VARCHAR,TNVED_NAME VARCHAR);
          CREATE TABLE tnved_reference(TNVED_CODE VARCHAR,TNVED_LEVEL INTEGER,TNVED_NAME VARCHAR);""")
        con.execute(build_fizob_enriched_table_sql())
        assert con.execute("SELECT base_period,coverage_basis FROM fizob_enriched").fetchone() == (
            dt.date(2019, 1, 1), "observed_series_months")
    assert float(row[0]) == pytest.approx(1.2)
    assert row[1] == dt.date(2019, 1, 1)
    assert float(row[2]) == pytest.approx(1.1)
    assert float(row[3]) == 1
    assert float(row[4]) == 5
    assert row[5] == "fact"


@pytest.mark.parametrize("status,version", [("incomplete_window", "fisher_annual_v2"), ("complete", "unrecognized")])
def test_unusable_index_is_not_exposed(tmp_path, status, version):
    path, columns = fixture_file(tmp_path, status, version)
    with duckdb.connect() as con:
        result = con.execute(build_fizob_select_sql(path, path.stem, columns)).fetchdf()
    assert result.fizob.isna().all()
    assert float(result.available_stoim_12.iloc[0]) == 100


def test_partial_schema_fails_explicitly(tmp_path):
    path, columns = fixture_file(tmp_path)
    with pytest.raises(ValueError, match="Incomplete Fisher schema"):
        build_fizob_select_sql(path, path.stem, set(columns) - {"coverage"})
    with pytest.raises(ValueError, match="Unversioned Fisher"):
        build_fizob_select_sql(path, path.stem, set(columns) - {"methodology_version"})


def test_missing_date_base_is_not_published(tmp_path):
    path, columns = fixture_file(tmp_path, missing_base=True)
    with duckdb.connect() as con:
        result = con.execute(build_fizob_select_sql(path, path.stem, columns)).fetchdf()
    assert result.fizob.isna().all()


def test_mixed_methods_do_not_replace_database(tmp_path):
    path, _ = fixture_file(tmp_path)
    legacy = tmp_path / "fizob_2.parquet"
    database = tmp_path / "test.duckdb"
    with duckdb.connect(str(database)) as con:
        con.execute("CREATE TABLE fizob_index AS SELECT 42 AS sentinel")
        con.execute("COPY (SELECT 'CN' AS STRANA,'ЭК' AS NAPR,DATE '2026-08-01' AS PERIOD,'27' AS TNVED2,2.0 AS fizob2,1.0 AS fizob2_bp) TO ? (FORMAT PARQUET)", [str(legacy)])
    with pytest.raises(ValueError, match="Cannot mix"):
        save_fizob_index([path, legacy], database)
    with duckdb.connect(str(database)) as con:
        assert con.execute("SELECT sentinel FROM fizob_index").fetchone() == (42,)
