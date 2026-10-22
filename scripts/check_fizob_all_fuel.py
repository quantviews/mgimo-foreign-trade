"""Local audit of ALL / Russian exports / HS27, December 2023 peak."""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def check():
    con = duckdb.connect(str(ROOT / "db/unified_trade_data.duckdb"), read_only=True)
    con.execute("SET threads=4")
    con.execute("SET memory_limit='4GB'")
    con.execute("""
      CREATE TEMP TABLE facts AS SELECT * FROM unified_trade_data
      WHERE TYPE='fact' AND NAPR='ЭК' AND starts_with(TNVED,'27')
    """)
    assert con.execute("""
      SELECT count(*) FROM (SELECT STRANA,TNVED,PERIOD FROM facts
      GROUP BY ALL HAVING count(*)>1)
    """).fetchone()[0] == 0
    # Reproduce the existing R imputation, rolling averages and ALL weights.
    con.execute("""
      CREATE TEMP TABLE parts AS
      WITH meta AS (
        SELECT STRANA,TNVED,arg_min(EDIZM,PERIOD) FILTER(WHERE EDIZM IS NOT NULL) unit,
          count(DISTINCT EDIZM) n_units,
          bool_or(coalesce(NETTO,0)>0 AND coalesce(KOL,0)=0) use_netto
        FROM facts GROUP BY ALL HAVING bool_or(STOIM>0)
      ), grid AS (
        SELECT m.*,cast(d AS DATE) PERIOD,
          unit IN ('?','NA') OR unit IS NULL OR n_units>1 OR use_netto AS mass_mode
        FROM meta m CROSS JOIN generate_series(
          (SELECT min(PERIOD) FROM unified_trade_data WHERE TYPE='fact'),
          (SELECT max(PERIOD) FROM unified_trade_data WHERE TYPE='fact'),
          INTERVAL '1 month') t(d)
      ), base AS (
        SELECT g.*,coalesce(f.STOIM,0) v,coalesce(f.NETTO,0) mass,coalesce(f.KOL,0) quantity
        FROM grid g LEFT JOIN facts f USING(STRANA,TNVED,PERIOD)
      ), prices AS (
        SELECT *,v/nullif(CASE WHEN mass_mode THEN mass ELSE quantity END,0) price
        FROM base
      ), rolling_prices AS (
        SELECT *,avg(price) FILTER(WHERE isfinite(price) AND price!=0)
          OVER(PARTITION BY STRANA,TNVED ORDER BY PERIOD
               ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) p12
        FROM prices
      ), filled AS (
        SELECT *,coalesce(
          last_value(p12 IGNORE NULLS) OVER(PARTITION BY STRANA,TNVED ORDER BY PERIOD
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
          first_value(p12 IGNORE NULLS) OVER(PARTITION BY STRANA,TNVED ORDER BY PERIOD
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) price_12
        FROM rolling_prices
      ), eligible AS (
        SELECT * FROM filled QUALIFY bool_or(price_12>0) OVER(PARTITION BY STRANA,TNVED)
      ), restored AS (
        SELECT *,CASE WHEN v>0 AND NOT(mass>0) THEN v/price_12 ELSE mass END restored_mass
        FROM eligible
      ), rolling AS (
        SELECT *,avg(restored_mass) OVER w mass12,avg(v) OVER w v12,
          avg(mass) OVER w observed_mass12
        FROM restored WINDOW w AS (PARTITION BY STRANA,TNVED ORDER BY PERIOD
          ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
      )
      SELECT *,v12/nullif(sum(v12) OVER(PARTITION BY PERIOD),0) weight,
        mass12*v12/nullif(sum(v12) OVER(PARTITION BY PERIOD),0) contribution
      FROM rolling
    """)
    pair = con.execute("""
      SELECT PERIOD,sum(contribution) raw,sum(mass12)*12 restored_annual_mass,
        sum(observed_mass12)*12 observed_annual_mass,sum(v12)*12 annual_value
      FROM parts WHERE PERIOD IN (DATE '2022-12-01',DATE '2023-12-01')
      GROUP BY PERIOD ORDER BY PERIOD
    """).fetchall()
    saved = con.execute("""
      SELECT yoy FROM read_parquet('site/data/fizob_bulletin.parquet')
      WHERE STRANA='ALL' AND NAPR='ЭК' AND tn_code='27' AND PERIOD=DATE '2023-12-01'
    """).fetchone()[0]
    reproduced = pair[1][1] / pair[0][1] - 1
    assert abs(reproduced-saved) < 1e-10, (reproduced, saved)
    con.execute("""
      CREATE TEMP TABLE drivers AS
      SELECT a.STRANA,a.TNVED,a.unit,a.mass_mode,
        b.weight prior_weight,a.weight current_weight,
        b.mass12 prior_mass12,a.mass12 current_mass12,
        b.contribution prior_contribution,a.contribution current_contribution,
        a.contribution-b.contribution change,
        (a.mass12-b.mass12)*b.weight volume_effect,
        a.mass12*(a.weight-b.weight) weight_effect
      FROM parts a JOIN parts b USING(STRANA,TNVED)
      WHERE a.PERIOD=DATE '2023-12-01' AND b.PERIOD=DATE '2022-12-01'
    """)
    drivers = con.execute("""
      SELECT * EXCLUDE(volume_effect,weight_effect) FROM drivers ORDER BY change DESC LIMIT 12
    """).df().to_dict(orient="records")
    effects = con.execute("SELECT sum(volume_effect),sum(weight_effect),sum(change) FROM drivers").fetchone()
    assert abs(effects[0]+effects[1]-effects[2]) < 0.001
    source_totals = con.execute("""
      SELECT year(PERIOD) AS year_number,sum(STOIM) usd,sum(NETTO) kg,count(*) AS row_count,
        count(DISTINCT STRANA) countries
      FROM facts WHERE year(PERIOD) IN (2022,2023) GROUP BY ALL ORDER BY year_number
    """).df().to_dict(orient="records")
    report = {
        "saved_yoy": saved, "reproduced_yoy": reproduced,
        "windows": pair, "source_totals": source_totals, "drivers": drivers,
        "decomposition_pp": {
            "volume_at_previous_weights": effects[0]/pair[0][1]*100,
            "changing_weights": effects[1]/pair[0][1]*100,
        },
        "warning": "Raw mass totals describe available records; country/source coverage may differ."
    }
    (ROOT / "tmp/fizob-fuel-all-audit.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, default=str))


if __name__ == "__main__":
    check()
