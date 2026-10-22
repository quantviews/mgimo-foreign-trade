"""Local audit of the CN / Russian exports / HS8703 fizob spike."""
import json
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def check():
    con = duckdb.connect(str(ROOT / "db/unified_trade_data.duckdb"), read_only=True)
    facts = con.execute("""
        SELECT TNVED,PERIOD,STOIM,KOL,NETTO,EDIZM FROM unified_trade_data
        WHERE STRANA='CN' AND NAPR='ЭК' AND starts_with(TNVED,'8703')
          AND TYPE='fact' ORDER BY PERIOD,TNVED
    """).df()
    assert set(facts.EDIZM) == {"ШТУКА"}
    assert (facts.KOL > 0).all()
    assert not facts.duplicated(["TNVED", "PERIOD"]).any()
    raw = []
    for month in pd.date_range("2024-03-01", "2026-02-01", freq="MS"):
        frame = pd.read_csv(
            ROOT / f"data_raw/china/EXPORT/data{month:%Y%m}.csv",
            dtype={"TNVED": str},
        )
        frame = frame[frame.TNVED.str.startswith("8703")].copy()
        # In these collector CSVs NETTO is the primary quantity (pieces);
        # Supplementary Quantity is kg. Check the source unit explicitly.
        assert frame["Supplementary Unit"].str.strip().eq("Kilogram").all()
        raw.append(pd.DataFrame({
            "TNVED": frame.TNVED.str.pad(10, side="right", fillchar="0"),
            "PERIOD": month, "STOIM": pd.to_numeric(frame.STOIM),
            "KOL": pd.to_numeric(frame.NETTO.astype(str).str.strip()),
            "NETTO": pd.to_numeric(frame["Supplementary Quantity"].astype(str).str.strip()),
        }))
    source = pd.concat(raw, ignore_index=True).sort_values(["PERIOD", "TNVED"])
    served = facts[facts.PERIOD.between("2024-03-01", "2026-02-01")].drop(columns="EDIZM")
    pd.testing.assert_frame_equal(
        source.reset_index(drop=True), served.reset_index(drop=True), check_dtype=False)
    # Reproduce the existing R formula on this small source slice only.
    con.register("audit_facts", facts)
    parts = con.execute("""
      WITH grid AS (
        SELECT TNVED,cast(m AS DATE) PERIOD FROM
          (SELECT DISTINCT TNVED FROM audit_facts) k CROSS JOIN
          generate_series(DATE '2019-01-01',DATE '2026-08-01',INTERVAL 1 MONTH) t(m)
      ), a AS (
        SELECT g.*,coalesce(f.KOL,0) q,coalesce(f.STOIM,0) v
        FROM grid g LEFT JOIN audit_facts f USING(TNVED,PERIOD)
      ), b AS (
        SELECT *,
          avg(q) OVER(PARTITION BY TNVED ORDER BY PERIOD ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) q12,
          avg(v) OVER(PARTITION BY TNVED ORDER BY PERIOD ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) v12
        FROM a
      )
      SELECT *,v12/nullif(sum(v12) OVER(PARTITION BY PERIOD),0) weight FROM b
    """).df()
    parts["weighted"] = parts.q12 * parts.weight
    summary = []
    for month in ["2025-02-01", "2026-02-01"]:
        frame = parts[parts.PERIOD.eq(month)]
        summary.append({
            "month": month, "observed_pieces_in_12_month_window": float(frame.q12.sum() * 12),
            "observed_usd_in_12_month_window": float(frame.v12.sum() * 12),
            "weighted_quantity": float(frame.weighted.sum()),
            "electric_cars_value_weight": float(frame.loc[frame.TNVED.eq("8703801000"), "weight"].iloc[0]),
        })
    reproduced = summary[1]["weighted_quantity"] / summary[0]["weighted_quantity"] - 1
    saved = con.execute("""
        SELECT PERIOD,fizob,fizob_previous_year,yoy FROM
          read_parquet('site/data/fizob_bulletin.parquet')
        WHERE STRANA='CN' AND NAPR='ЭК' AND tn_level=4 AND tn_code='8703'
          AND PERIOD=DATE '2026-02-01'
    """).fetchone()
    assert abs(reproduced - saved[3]) < 1e-12
    report = {"source_rows_checked": len(source), "windows": summary,
              "reproduced_yoy": reproduced, "snapshot": saved,
              "interpretation": "Russian exports to China; small observed flow and changing value weights. Missing source rows are zero-filled by the existing index method, not proof of zero trade."}
    path = ROOT / "tmp/fizob-cars-china-audit.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, default=str))


if __name__ == "__main__":
    check()
