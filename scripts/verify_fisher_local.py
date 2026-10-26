"""Read-only reconciliation of a local Fisher build with the source DuckDB.

Usage: python scripts/verify_fisher_local.py RESULT.parquet NEW_OUTPUT_DIRECTORY
Level 0 must cover all countries and products; level 2 must include group 27.
Price peers must be calculated on the full input before restricting output.
"""
from pathlib import Path
import json
import sys

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pipelines.merge_pipeline import save_fizob_index


def sql_path(path):
    return "'" + str(Path(path).resolve()).replace("'", "''") + "'"


def verify(result_path, output, database="db/unified_trade_data.duckdb"):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    con = duckdb.connect()
    con.read_parquet(str(result_path)).create_view("result")
    con.execute(f"ATTACH {sql_path(database)} AS original (READ_ONLY)")
    con.execute("""CREATE VIEW facts AS SELECT * FROM original.unified_trade_data
      WHERE TYPE='fact' AND SOURCE!='nowcast' AND STRANA!='ALL'""")
    assert con.execute("SELECT count(*) FROM result WHERE coverage<1 AND fizob IS NOT NULL").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM result WHERE TYPE!='fact'").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM result WHERE isfinite(fizob) AND bp IS NULL").fetchone()[0] == 0
    assert con.execute("""SELECT count(*) FROM result r LEFT JOIN result b
      ON r.STRANA=b.STRANA AND r.NAPR=b.NAPR AND r.level=b.level AND r.TNVED_G=b.TNVED_G
      AND r.bp=b.bp AND b.PERIOD=r.bp+INTERVAL '11 months'
      WHERE isfinite(r.fizob) AND (b.stoim_12 IS NULL OR b.stoim_12<=0)""").fetchone()[0] == 0
    assert con.execute("""SELECT count(*) FROM (
      SELECT STRANA,NAPR,level,TNVED_G,PERIOD,count(*) n FROM result GROUP BY ALL
    ) WHERE n!=1""").fetchone()[0] == 0
    # ALL's available cost must equal the countries, including incomplete windows.
    error = con.execute("""SELECT max(abs(a.available_stoim_12-b.v)) FROM result a
      JOIN (SELECT NAPR,level,TNVED_G,PERIOD,sum(available_stoim_12) v FROM result
        WHERE STRANA!='ALL' GROUP BY ALL) b USING(NAPR,level,TNVED_G,PERIOD)
      WHERE a.STRANA='ALL'""").fetchone()[0]
    assert error is not None and error < .01, error
    identity_rows, identity_error = con.execute("""SELECT count(*),
      max(abs(r.fizob*r.price-r.stoim_12/b.stoim_12)/greatest(1,abs(r.stoim_12/b.stoim_12)))
      FROM result r JOIN result b
      ON r.STRANA=b.STRANA AND r.NAPR=b.NAPR AND r.level=b.level AND r.TNVED_G=b.TNVED_G
      AND r.bp=b.bp AND b.PERIOD=r.bp+INTERVAL '11 months'
      WHERE b.stoim_12>0 AND isfinite(r.fizob) AND isfinite(r.price)""").fetchone()
    assert identity_rows == 0 or (identity_error is not None and identity_error < 1e-9), identity_error
    # Independent raw cost check, not another calculation from the index output.
    focus = con.execute("""SELECT STRANA,NAPR,level,TNVED_G,PERIOD,fizob,price,
      stoim_12,available_stoim_12,coverage,base_coverage,status
      FROM result WHERE STRANA='ALL' AND TNVED_G IN ('0','27')
        AND PERIOD=(SELECT max(PERIOD) FROM facts) ORDER BY level,NAPR""").fetchdf()
    assert len(focus) == 4
    for row in focus.itertuples():
        expected = con.execute("""SELECT sum(STOIM) FROM facts
          WHERE NAPR=? AND PERIOD BETWEEN CAST(? AS DATE)-INTERVAL '11 months' AND CAST(? AS DATE)
          AND (?='0' OR substr(TNVED,1,2)=?)""",
          [row.NAPR, row.PERIOD, row.PERIOD, row.TNVED_G, row.TNVED_G]).fetchone()[0]
        assert abs(expected-row.available_stoim_12) < .01
    # Check the actual new-schema loader on real results, without changing working DB.
    files = []
    for level in (0, 2):
        suffix = "" if level == 0 else str(level)
        path = output / ("fizob_total.parquet" if level == 0 else "fizob_2.parquet")
        con.execute(f"""COPY (SELECT * EXCLUDE(level,TNVED_G,fizob,price),
          TNVED_G AS TNVED2,fizob AS fizob{suffix},price AS price{suffix}
          FROM result WHERE level={level}) TO ? (FORMAT PARQUET)""", [str(path)])
        files.append(path)
    staging = output / "fisher-validation.duckdb"
    save_fizob_index(files, staging)
    con.execute(f"ATTACH {sql_path(staging)} AS staging (READ_ONLY)")
    expected_rows = con.execute("SELECT count(*) FROM result WHERE level IN (0,2)").fetchone()[0]
    assert con.execute("SELECT count(*) FROM staging.fizob_index_v").fetchone()[0] == expected_rows
    assert con.execute("SELECT count(*) FROM staging.fizob_index_v WHERE coverage<1 AND idx IS NOT NULL").fetchone()[0] == 0
    report = {
        "production_changed": False,
        "rows_loaded": expected_rows,
        "max_all_value_error_usd": error,
        "max_factor_identity_relative_error": identity_error,
        "factor_identity_rows": identity_rows,
        "latest_group27_and_total": json.loads(focus.to_json(orient="records", date_format="iso")),
    }
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    con.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    verify(sys.argv[1], sys.argv[2])
