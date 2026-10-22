"""Validate a locally built ALL/all-products snapshot before publishing."""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def check():
    con = duckdb.connect(str(ROOT / "db/unified_trade_data.duckdb"), read_only=True)
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")
    for name, path in (
        ("new_snapshot", ROOT / "site/data/fizob_bulletin_total.parquet"),
        ("old_snapshot", ROOT / "site/data/fizob_bulletin.parquet"),
        ("r_totals", ROOT / "data_processed/fizob_total.parquet"),
    ):
        escaped = path.as_posix().replace("'", "''")
        con.execute(f"CREATE TEMP VIEW {name} AS SELECT * FROM read_parquet('{escaped}')")
    # Country grids must contain the same set of countries throughout each flow.
    assert con.execute("""
        WITH n AS (SELECT NAPR,PERIOD,count(*) n FROM r_totals
                   WHERE STRANA!='ALL' GROUP BY NAPR,PERIOD)
        SELECT count(*) FROM (SELECT NAPR FROM n GROUP BY NAPR
                              HAVING min(n)!=max(n))
    """).fetchone()[0] == 0
    # Independent R output, including its original base-year dates.
    error = con.execute("""
        WITH raw AS (
          SELECT NAPR,PERIOD,sum(fizob*fizob_bp) raw,min(bp) bp
          FROM r_totals WHERE STRANA!='ALL' GROUP BY NAPR,PERIOD
        ), normalized AS (
          SELECT *,raw/avg(raw) FILTER (
            WHERE PERIOD>=bp AND PERIOD<bp+INTERVAL '12 months'
          ) OVER (PARTITION BY NAPR) expected FROM raw
        ), growth AS (
          SELECT *,expected/lag(expected,12) OVER (
            PARTITION BY NAPR ORDER BY PERIOD)-1 expected_yoy FROM normalized
        )
        SELECT count(*),max(abs(expected-n.fizob)),
          max(abs(expected_yoy-n.yoy)),
          count(*) FILTER(WHERE (expected_yoy IS NULL)!=(n.yoy IS NULL))
        FROM growth g JOIN new_snapshot n USING(NAPR,PERIOD)
        WHERE n.STRANA='ALL' AND n.tn_level=0
    """).fetchone()
    assert error[0] > 24 and error[1] < 1e-10 and error[2] < 1e-10 and error[3] == 0, error
    # Exact checksum of indices, keys and coverage. Floating-point monetary
    # SUM can vary at sub-cent precision with parallel aggregation order.
    fingerprints = []
    for source in ("old_snapshot", "new_snapshot"):
        fingerprints.append(con.execute(f"""
          SELECT tn_level,count(*),
            sum(hash(COLUMNS(* EXCLUDE(value_12m_usd,previous_value_12m_usd)))::HUGEINT)
          FROM {source} WHERE NOT(STRANA='ALL' AND tn_level=0)
          GROUP BY tn_level ORDER BY tn_level
        """).fetchall())
    assert fingerprints[0] == fingerprints[1], "Existing series changed"
    monetary_error = 0
    for level in (0, 2, 4, 6):
        delta = con.execute(f"""
          SELECT max(greatest(abs(a.value_12m_usd-b.value_12m_usd),
                              abs(a.previous_value_12m_usd-b.previous_value_12m_usd))),
            count(*) FILTER(WHERE
              (a.value_12m_usd IS NULL)!=(b.value_12m_usd IS NULL) OR
              (a.previous_value_12m_usd IS NULL)!=(b.previous_value_12m_usd IS NULL))
          FROM old_snapshot a JOIN new_snapshot b
          USING(STRANA,NAPR,PERIOD,tn_level,tn_code)
          WHERE a.tn_level={level} AND NOT(a.STRANA='ALL' AND a.tn_level=0)
        """).fetchone()
        assert (delta[0] or 0) < 0.01 and delta[1] == 0, delta
        monetary_error = max(monetary_error, delta[0] or 0)
    latest = con.execute("""
        SELECT direction,PERIOD,yoy,value_12m_usd,previous_value_12m_usd,
          months_with_records,previous_months_with_records
        FROM new_snapshot WHERE STRANA='ALL' AND tn_level=0
        AND PERIOD=(SELECT max(PERIOD) FROM new_snapshot)
        ORDER BY direction
    """).fetchall()
    # Currency totals independently from factual trade rows, excluding nowcast.
    for direction, period, _, current, previous, *_ in latest:
        flow = "ИМ" if direction == "Импорт" else "ЭК"
        expected = con.execute("""
          SELECT sum(STOIM) FILTER (WHERE PERIOD>=?::DATE-INTERVAL '11 months'),
                 sum(STOIM) FILTER (WHERE PERIOD<?::DATE-INTERVAL '11 months')
          FROM unified_trade_data WHERE TYPE='fact' AND STRANA!='ALL'
            AND NAPR=? AND PERIOD BETWEEN ?::DATE-INTERVAL '23 months' AND ?::DATE
        """, [period, period, flow, period, period]).fetchone()
        assert abs(current-expected[0]) < 0.1 and abs(previous-expected[1]) < 0.1
    report = {"all_total_rows": error[0], "max_index_error": error[1],
              "max_yoy_error": error[2], "existing_indices_unchanged": True,
              "max_existing_currency_difference_usd": monetary_error, "latest": latest}
    (ROOT / "tmp/fizob-all-total-validation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, default=str))


if __name__ == "__main__":
    check()
