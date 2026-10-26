"""Validate locally rendered bulletin inputs against the serving database.

Run after the summary and three country data-prep R scripts.
Unknown mass must stay NULL; Fisher YoY must match the published shared view.
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def validate():
    con = duckdb.connect(str(ROOT / "db/unified_trade_data.duckdb"), read_only=True)
    checks = []
    for country, code in (("china", "CN"), ("india", "IN"), ("turkey", "TR")):
        directory = ROOT / "site/country_bulletins" / country / "data"
        headline = str(directory / f"headline_{country}.parquet")
        differences = con.execute("""
          WITH expected AS (
            SELECT NAPR,PERIOD,sum(STOIM)/1e9 cost_value,
              sum(CASE WHEN TYPE!='pred' THEN STOIM ELSE 0 END)/1e9 fact
            FROM unified_trade_data WHERE STRANA=? GROUP BY ALL
          )
          SELECT count(*) FROM expected e FULL JOIN read_parquet(?) h USING(NAPR,PERIOD)
          WHERE e.PERIOD IS NULL OR h.PERIOD IS NULL
            OR abs(cost_value-stoim_bn)>1e-9 OR abs(fact-stoim_bn_fact)>1e-9
        """, [code, headline]).fetchone()[0]
        assert differences == 0, (country, "headline", differences)
        oil = str(directory / f"data_oil_export_{country}.parquet")
        differences = con.execute("""
          WITH expected AS (
            SELECT NAPR,PERIOD,CASE WHEN count(*)=count(NETTO)
              THEN sum(NETTO)/1e9 END mass FROM unified_trade_data
            WHERE STRANA=? AND NAPR='ЭК' AND TNVED4=? GROUP BY ALL
          )
          SELECT count(*) FROM expected e FULL JOIN read_parquet(?) h USING(NAPR,PERIOD)
          WHERE e.PERIOD IS NULL OR h.PERIOD IS NULL
            OR (mass IS NULL)!=(NETTO IS NULL) OR abs(mass-NETTO)>1e-9
        """, [code, "2710" if code == "TR" else "2709", oil]).fetchone()[0]
        assert differences == 0, (country, "oil mass", differences)
        checks.append(country + ": all monthly costs, facts and oil mass match DuckDB")
    differences = con.execute("""
      SELECT count(*) FROM read_parquet(?) h
      FULL JOIN (SELECT STRANA,NAPR,PERIOD,yoy FROM read_parquet(?)
        WHERE tn_level=0 AND STRANA IN ('CN','IN','TR') AND PERIOD>='2020-01-01') s
      USING(STRANA,NAPR,PERIOD)
      WHERE h.PERIOD IS NULL OR s.PERIOD IS NULL
        OR (h.fizob IS NULL)!=(s.yoy IS NULL) OR abs(h.fizob-s.yoy)>1e-12
    """, [str(ROOT / "site/data/bulletin_fo.parquet"),
          str(ROOT / "site/data/fizob_bulletin.parquet")]).fetchone()[0]
    assert differences == 0, ("Fisher parity", differences)
    for name in ("tab_stoim_oil", "data_oilgas", "bulletin_headline", "df_groups"):
        count = con.execute("SELECT count(*) FROM read_parquet(?)",
                            [str(ROOT / f"site/data/{name}.parquet")]).fetchone()[0]
        assert count > 0, (name, "empty snapshot: check R UTF-8 locale")
    checks.append("Fisher YoY and NULLs match the shared published view")
    return {"passed": True, "checks": checks}


if __name__ == "__main__":
    print(json.dumps(validate(), ensure_ascii=False, indent=2))
