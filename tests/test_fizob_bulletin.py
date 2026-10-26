"""Calendar-aligned growth, grain isolation, and zero/missing baselines."""
from pathlib import Path

import duckdb
import pytest


def test_fisher_never_compares_different_bases_or_synthesizes_all():
    con = duckdb.connect()
    con.execute("""CREATE TABLE fizob_enriched AS
      SELECT 'CN' STRANA,'Китай' COUNTRY_NAME,'ЭК' NAPR,
      CAST(p AS DATE) PERIOD,year(CAST(p AS DATE)) AS YEAR,0 tn_level,'0' tn_code,
      1.0 fizob,1.0 fizob_bp,CAST(NULL AS VARCHAR) TNVED4_NAME,
      CAST(NULL AS VARCHAR) TNVED2_NAME,CAST(b AS DATE) base_period,
      'fisher_annual_v2' methodology_version
      FROM (VALUES ('2025-12-01','2019-01-01'),('2026-12-01','2026-01-01')) t(p,b);
      CREATE TABLE tnved_reference(TNVED_CODE VARCHAR,TNVED_LEVEL INTEGER,TNVED_NAME VARCHAR);
    """)
    sql = (Path(__file__).resolve().parents[1] / 'sql/fizob_bulletin.sql').read_text(encoding='utf-8')
    rows = con.execute('SELECT STRANA,yoy FROM (' + sql + ') s ORDER BY PERIOD').fetchall()
    assert rows == [('CN', None), ('CN', None)]
    con.execute('UPDATE fizob_enriched SET base_period=NULL')
    assert con.execute('SELECT yoy FROM (' + sql + ') s WHERE YEAR=2026').fetchone()[0] is None
    con.close()


def test_calendar_yoy_and_series_isolation():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE fizob_enriched(
          STRANA VARCHAR,NAPR VARCHAR,PERIOD DATE,YEAR INTEGER,
          tn_level INTEGER,tn_code VARCHAR,fizob DOUBLE,
          TNVED4_NAME VARCHAR,TNVED2_NAME VARCHAR);
        CREATE TABLE tnved_reference(
          TNVED_CODE VARCHAR,TNVED_LEVEL INTEGER,TNVED_NAME VARCHAR);
        INSERT INTO tnved_reference VALUES ('8703',4,'Легковые автомобили');
        INSERT INTO fizob_enriched VALUES
          ('CN','ИМ','2025-01-01',2025,0,'0',100,NULL,NULL),
          ('CN','ИМ','2026-01-01',2026,0,'0',120,NULL,NULL),
          ('CN','ИМ','2026-02-01',2026,0,'0',130,NULL,NULL),
          ('CN','ИМ','2025-01-01',2025,4,'8703',20,NULL,NULL),
          ('CN','ИМ','2026-01-01',2026,4,'8703',10,NULL,NULL),
          ('TR','ИМ','2025-01-01',2025,0,'0',0,NULL,NULL),
          ('TR','ИМ','2026-01-01',2026,0,'0',5,NULL,NULL),
          ('IN','ЭК','2025-01-01',2025,0,'0',10,NULL,NULL),
          ('IN','ЭК','2026-01-01',2026,0,'0',0,NULL,NULL);
    """)
    con.execute("ALTER TABLE fizob_enriched ADD COLUMN COUNTRY_NAME VARCHAR")
    con.execute("ALTER TABLE fizob_enriched ADD COLUMN fizob_bp DOUBLE DEFAULT 1")
    sql = (Path(__file__).resolve().parents[1] / "sql/fizob_bulletin.sql").read_text(encoding="utf-8")
    con.execute("CREATE TEMP TABLE result AS " + sql)
    # Filtering the displayed year must not remove the previous-year denominator.
    rows = con.execute("""
        SELECT STRANA,tn_level,month(PERIOD),yoy FROM result
        WHERE YEAR=2026 AND STRANA <> 'ALL' ORDER BY STRANA,tn_level,PERIOD
    """).fetchall()
    assert rows[0][3] == pytest.approx(.2)
    assert rows[1][3] is None  # missing February baseline, not zero
    assert rows[2][3] == pytest.approx(-.5)  # product separate from total
    assert rows[3][3] == -1  # observed zero numerator is a valid -100%
    assert rows[4][3] is None  # zero denominator is not a valid growth rate
    assert con.execute("SELECT product FROM result WHERE tn_level=4 LIMIT 1").fetchone()[0] == "8703 · Легковые автомобили"
    con.execute("CREATE TEMP TABLE bulletin AS SELECT * FROM result")
    con.execute("""
        CREATE TEMP TABLE monthly_trade(
          STRANA VARCHAR,NAPR VARCHAR,tn_code VARCHAR,PERIOD DATE,
          value_usd DOUBLE,source_rows INTEGER);
        INSERT INTO monthly_trade VALUES
          ('CN','ИМ','0','2025-01-01',10,1),
          ('CN','ИМ','0','2026-01-01',0,1),
          ('CN','ИМ','8703','2025-01-01',3,1),
          ('CN','ИМ','8703','2026-01-01',6,1);
    """)
    context_sql = (Path(__file__).resolve().parents[1] / "sql/fizob_trade_context.sql").read_text(encoding="utf-8")
    con.execute("CREATE TEMP TABLE context AS " + context_sql)
    assert con.execute("""
      SELECT value_12m_usd,previous_value_12m_usd,months_with_records
      FROM context WHERE STRANA='CN' AND tn_level=0 AND PERIOD=DATE '2026-01-01'
    """).fetchone() == (0, 10, 1)  # observed zero is not a missing observation
    assert con.execute("""
      SELECT value_12m_usd,months_with_records FROM context
      WHERE STRANA='TR' AND PERIOD=DATE '2026-01-01'
    """).fetchone() == (None, 0)
    con.close()


def test_all_uses_saved_aggregate_not_average_country_indices():
    con = duckdb.connect()
    con.execute("""
      CREATE TABLE fizob_enriched AS
      SELECT STRANA,'ИМ' NAPR,CAST(PERIOD AS DATE) PERIOD,
        year(CAST(PERIOD AS DATE)) AS YEAR,4 tn_level,'8703' tn_code,fizob,
        CAST(NULL AS VARCHAR) TNVED4_NAME,CAST(NULL AS VARCHAR) TNVED2_NAME,
        STRANA COUNTRY_NAME
      FROM (VALUES ('CN','2025-01-01',1.0),('CN','2026-01-01',2.0),
        ('TR','2025-01-01',1.0),('TR','2026-01-01',0.5),
        ('ALL','2025-01-01',1.0),('ALL','2026-01-01',4.0)) t(STRANA,PERIOD,fizob);
      CREATE TABLE tnved_reference(TNVED_CODE VARCHAR,TNVED_LEVEL INTEGER,TNVED_NAME VARCHAR);
    """)
    con.execute("ALTER TABLE fizob_enriched ADD COLUMN fizob_bp DOUBLE DEFAULT 1")
    sql = (Path(__file__).resolve().parents[1] / "sql/fizob_bulletin.sql").read_text(encoding="utf-8")
    result = con.execute("SELECT country,yoy FROM (" + sql +
                         ") s WHERE STRANA='ALL' AND YEAR=2026").fetchone()
    assert result == ("Все страны в базе", 3.0)
    con.close()


def test_all_total_restores_raw_country_totals_before_normalizing():
    con = duckdb.connect()
    con.execute("""
      CREATE TABLE fizob_enriched AS
      SELECT STRANA,'ИМ' NAPR,CAST(PERIOD AS DATE) PERIOD,
        year(CAST(PERIOD AS DATE)) AS YEAR,0 tn_level,'0' tn_code,fizob,
        fizob_bp,CAST(NULL AS VARCHAR) TNVED4_NAME,
        CAST(NULL AS VARCHAR) TNVED2_NAME,STRANA COUNTRY_NAME
      FROM (VALUES ('CN','2025-01-01',1.0,100.0),('CN','2026-01-01',2.0,100.0),
        ('TR','2025-01-01',1.0,900.0),('TR','2026-01-01',0.5,900.0))
        t(STRANA,PERIOD,fizob,fizob_bp);
      CREATE TABLE tnved_reference(TNVED_CODE VARCHAR,TNVED_LEVEL INTEGER,TNVED_NAME VARCHAR);
    """)
    sql = (Path(__file__).resolve().parents[1] / "sql/fizob_bulletin.sql").read_text(encoding="utf-8")
    row = con.execute("SELECT product,fizob,yoy FROM (" + sql +
                      ") s WHERE STRANA='ALL' AND YEAR=2026").fetchone()
    assert row[0] == "Все товары"
    assert row[1] == pytest.approx(.65)  # (200+450)/(100+900), not mean(2,.5)
    assert row[2] == pytest.approx(-.35)
    # Existing official ALL total takes precedence, without double counting.
    con.execute("""INSERT INTO fizob_enriched
      SELECT 'ALL',NAPR,PERIOD,YEAR,tn_level,tn_code,3,1,
        TNVED4_NAME,TNVED2_NAME,'Все страны в базе'
      FROM fizob_enriched WHERE STRANA='CN'""")
    assert con.execute("SELECT count(*) FROM (" + sql +
                       ") s WHERE STRANA='ALL'").fetchone()[0] == 2
    con.close()
