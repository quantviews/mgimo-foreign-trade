"""Explain strict Fisher coverage blockers without changing the methodology."""
import json
from pathlib import Path
import duckdb

OUT = Path('tmp/fisher-coverage-blockers')
OUT.mkdir(exist_ok=True)
c = duckdb.connect()
c.execute("ATTACH 'tmp/china-revision-build/input.duckdb' AS input (READ_ONLY)")
c.execute("""CREATE TABLE monthly AS SELECT STRANA,NAPR,TNVED,PERIOD,
  CASE WHEN bool_and(STOIM IS NOT NULL AND isfinite(STOIM) AND STOIM>=0) THEN sum(STOIM) END v,
  sum(STOIM) FILTER(WHERE isfinite(STOIM) AND STOIM>=0) available,
  bool_or(NETTO>0 OR KOL>0) has_quantity
  FROM input.unified_trade_data WHERE TYPE='fact' AND SOURCE!='nowcast' AND STRANA!='ALL'
  GROUP BY ALL""")
c.execute("""CREATE TABLE series AS SELECT STRANA,NAPR,TNVED,min(PERIOD) first_seen,max(PERIOD) last_seen,
  count(*) FILTER(WHERE PERIOD BETWEEN DATE '2025-09-01' AND DATE '2026-08-01') current_rows,
  count(v) FILTER(WHERE PERIOD BETWEEN DATE '2025-09-01' AND DATE '2026-08-01') current_valid,
  count(*) FILTER(WHERE PERIOD BETWEEN DATE '2025-01-01' AND DATE '2025-12-01') base_rows,
  count(v) FILTER(WHERE PERIOD BETWEEN DATE '2025-01-01' AND DATE '2025-12-01') base_valid,
  coalesce(sum(available) FILTER(WHERE PERIOD BETWEEN DATE '2025-09-01' AND DATE '2026-08-01'),0) current_value,
  coalesce(sum(available) FILTER(WHERE PERIOD BETWEEN DATE '2025-01-01' AND DATE '2025-12-01'),0) base_value,
  count(*) FILTER(WHERE PERIOD BETWEEN DATE '2025-09-01' AND DATE '2026-08-01' AND NOT coalesce(has_quantity,false)) quantity_missing
 FROM monthly GROUP BY STRANA,NAPR,TNVED""")
c.execute("""CREATE VIEW classified AS SELECT *, CASE
 WHEN current_valid=12 AND base_valid=12 THEN 'both_windows_complete'
 WHEN current_rows=0 AND base_rows=0 THEN 'no_rows_in_either_window'
 WHEN current_rows=0 THEN 'no_rows_in_current_window'
 WHEN base_rows=0 THEN 'no_rows_in_base_window'
 ELSE 'partial_or_invalid_window' END category FROM series""")
report = []
for country in ('CN','ALL'):
    for group in ('0','27'):
        condition = "TRUE" if country=='ALL' else "STRANA='CN'"
        if group=='27': condition += " AND starts_with(TNVED,'27')"
        frame = c.execute(f"""SELECT NAPR,category,count(*) series,
          sum(12-current_rows) missing_current_cells,sum(12-base_rows) missing_base_cells,
          sum(current_rows-current_valid) invalid_current_cells,
          sum(current_value) current_available_usd,sum(base_value) base_available_usd,
          sum(quantity_missing) rows_without_positive_quantity
          FROM classified WHERE {condition} GROUP BY ALL ORDER BY NAPR,category""").fetchdf()
        report.append(dict(country=country,group=group,rows=json.loads(frame.to_json(orient='records'))))
        c.execute(f"COPY (SELECT * FROM classified WHERE {condition} ORDER BY NAPR,current_value DESC) TO ? (HEADER, DELIMITER ',')",[str(OUT/f'{country}-{group}-series.csv')])
c.execute("""COPY (SELECT s.*,d.PERIOD,
 CASE WHEN m.PERIOD IS NULL THEN 'row_absent' ELSE 'invalid_value' END blocker
 FROM classified s CROSS JOIN generate_series(DATE '2025-01-01',DATE '2026-08-01',INTERVAL '1 month') d(PERIOD)
 LEFT JOIN monthly m USING(STRANA,NAPR,TNVED,PERIOD)
 WHERE s.STRANA='CN' AND starts_with(s.TNVED,'27') AND m.v IS NULL
 ORDER BY s.NAPR,s.TNVED,d.PERIOD) TO ? (HEADER, DELIMITER ',')""",[str(OUT/'CN-27-missing-months.csv')])
(OUT/'summary.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(report,ensure_ascii=False,indent=2))
