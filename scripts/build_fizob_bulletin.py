"""Build the common bulletin/Superset snapshot locally; never run on VPS."""
import argparse
import json
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def build(database, output):
    con = duckdb.connect(str(database), read_only=True)
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    sql = (ROOT / "sql/fizob_bulletin.sql").read_text(encoding="utf-8")
    # ALL/all-products extends the country-total formula. Require a complete,
    # stable country grid so an absent country row cannot silently lower it.
    fisher = 'methodology_version' in {r[0] for r in con.execute('DESCRIBE fizob_enriched').fetchall()}
    if fisher:
        # The generic shared query also supports legacy schemas. Here the schema
        # is known: avoid serializing tens of millions of full rows to JSON.
        sql = sql.replace("json_extract_string(to_json(f), '$.base_period')", 'CAST(f.base_period AS VARCHAR)')
        sql = sql.replace("json_extract_string(to_json(f), '$.methodology_version')", 'f.methodology_version')
    def level_sql(level):
        return sql.replace('WHERE tn_level IN (0, 2, 4, 6)',f'WHERE tn_level={level}')
    invalid_total_grid = con.execute("""
      WITH counts AS (
        SELECT NAPR,PERIOD,count(*) n,
          count(*) FILTER(WHERE fizob*fizob_bp IS NULL
                         OR NOT isfinite(fizob*fizob_bp)) invalid
        FROM fizob_enriched WHERE tn_level=0 AND STRANA!='ALL'
        GROUP BY NAPR,PERIOD)
      SELECT count(*) FROM (
        SELECT NAPR FROM counts GROUP BY NAPR
        HAVING min(n)!=max(n) OR sum(invalid)>0)
    """).fetchone()[0]
    if not fisher:
        assert not invalid_total_grid, "Cannot build ALL total from incomplete country indices"
    else:
        # Fisher ALL is already pooled from A/B/C/D. Never reconstruct it by
        # summing country indices, and keep unavailable observations as NULL.
        assert con.execute("SELECT count(*) FROM fizob_enriched WHERE coverage<1 AND fizob IS NOT NULL").fetchone()[0] == 0
    # The three national totals are a fixed regression contract for bulletins.
    con.execute("CREATE TEMP TABLE bulletin AS SELECT * FROM (" + level_sql(0) +
                ") s WHERE tn_level=0 AND STRANA IN ('CN','IN','TR')")
    duplicate = con.execute("""
        SELECT count(*) FROM (
          SELECT STRANA,NAPR,PERIOD,tn_level,tn_code FROM bulletin
          GROUP BY ALL HAVING count(*) != 1)
    """).fetchone()[0]
    assert not duplicate, f"Duplicate series keys: {duplicate}"
    # Independently reproduce the existing R bulletin calculation.
    difference = con.execute("""
      WITH old AS (
        SELECT STRANA,NAPR,PERIOD,
               fizob / lag(fizob,12) OVER
                 (PARTITION BY STRANA,NAPR ORDER BY PERIOD) - 1 AS yoy
        FROM fizob_index WHERE tn_level=0 AND STRANA IN ('CN','IN','TR'))
      SELECT max(abs(o.yoy-b.yoy)), count(*) FILTER (
        WHERE (o.yoy IS NULL) != (b.yoy IS NULL))
      FROM old o JOIN bulletin b USING (STRANA,NAPR,PERIOD)
      WHERE b.tn_level=0
    """).fetchone()
    if not fisher:
        assert (difference[0] or 0) < 1e-12 and difference[1] == 0, difference
    else:
        difference = con.execute("""
          WITH expected AS (
            SELECT c.STRANA,c.NAPR,c.PERIOD,
              CASE WHEN c.base_period=p.base_period AND c.base_period IS NOT NULL
                AND c.methodology_version=p.methodology_version AND p.fizob>0
                AND isfinite(c.fizob) AND isfinite(p.fizob)
                THEN c.fizob/p.fizob-1 END yoy
            FROM fizob_index c LEFT JOIN fizob_index p
              ON c.STRANA=p.STRANA AND c.NAPR=p.NAPR AND c.tn_level=p.tn_level
              AND c.tn_code=p.tn_code AND p.PERIOD=c.PERIOD-INTERVAL '1 year'
            WHERE c.tn_level=0 AND c.STRANA IN ('CN','IN','TR'))
          SELECT max(abs(e.yoy-b.yoy)),count(*) FILTER(WHERE (e.yoy IS NULL)!=(b.yoy IS NULL))
          FROM expected e JOIN bulletin b USING(STRANA,NAPR,PERIOD)
        """).fetchone()
        assert (difference[0] or 0)<1e-12 and difference[1]==0, difference
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    escaped = output.resolve().as_posix().replace("'", "''")
    context_sql = (ROOT / "sql/fizob_trade_context.sql").read_text(encoding="utf-8")
    output_rows = 0
    with tempfile.TemporaryDirectory(prefix="fizob-build-", dir=ROOT / "tmp") as tmp:
        for level in (0, 2, 4, 6):
            print(f"Building level {level}", flush=True)
            con.execute("CREATE OR REPLACE TEMP TABLE bulletin AS SELECT * FROM (" + level_sql(level) +
                        f") s WHERE tn_level={level}")
            duplicates = con.execute("""
              SELECT count(*) FROM (SELECT STRANA,NAPR,PERIOD,tn_code
              FROM bulletin GROUP BY ALL HAVING count(*)>1)
            """).fetchone()[0]
            assert duplicates == 0
            code = "'0'" if level == 0 else f"substr(TNVED,1,{level})"
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE monthly_trade AS
              WITH facts AS (
                SELECT STRANA,NAPR,PERIOD,{code} AS tn_code,STOIM
                FROM unified_trade_data WHERE TYPE='fact' AND STRANA!='ALL'
              )
              SELECT CASE WHEN grouping(STRANA)=1 THEN 'ALL' ELSE STRANA END AS STRANA,
                NAPR,PERIOD,tn_code,sum(STOIM) AS value_usd,count(*) AS source_rows
              FROM facts GROUP BY GROUPING SETS
                ((STRANA,NAPR,PERIOD,tn_code),(NAPR,PERIOD,tn_code))
            """)
            part = (Path(tmp) / f"level-{level}.parquet").as_posix().replace("'", "''")
            con.execute(f"COPY ({context_sql}) TO '{part}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            output_rows += con.execute("SELECT count(*) FROM bulletin").fetchone()[0]
        pattern = (Path(tmp) / "*.parquet").as_posix().replace("'", "''")
        con.execute(f"COPY (SELECT * FROM read_parquet('{pattern}')) TO '{escaped}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")
    report = {
        "rows": output_rows,
        "max_country_yoy_error": difference[0],
        "latest": con.execute(f"""
            SELECT country,direction,PERIOD,yoy FROM read_parquet('{escaped}')
            WHERE tn_level=0 AND STRANA IN ('CN','IN','TR')
              AND PERIOD=(SELECT max(PERIOD) FROM read_parquet('{escaped}'))
            ORDER BY country,direction
        """).fetchall(),
    }
    output.with_suffix(".validation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, default=str))
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "db/unified_trade_data.duckdb")
    parser.add_argument("--output", type=Path, default=ROOT / "site/data/fizob_bulletin.parquet")
    args = parser.parse_args()
    build(args.db, args.output)
