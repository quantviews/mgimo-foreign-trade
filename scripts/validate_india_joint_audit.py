"""Independently validate the local Fisher scenario and compute value coverage."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def coverage_table(con, relation, latest):
    current_start = (latest - pd.DateOffset(months=11)).strftime('%Y-%m-%d')
    base_start = f'{latest.year-1}-01-01'
    base_end = f'{latest.year-1}-12-01'
    current = f"PERIOD BETWEEN DATE '{current_start}' AND DATE '{latest:%Y-%m-%d}'"
    base = f"PERIOD BETWEEN DATE '{base_start}' AND DATE '{base_end}'"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE coverage_codes AS
      WITH m AS (
        SELECT STRANA,NAPR,TNVED,PERIOD,
         coalesce(sum(STOIM) FILTER(WHERE isfinite(STOIM) AND STOIM>=0),0) AS available,
         bool_and(coalesce(isfinite(STOIM) AND STOIM>=0,false)) AS month_ok
        FROM {relation} GROUP BY STRANA,NAPR,TNVED,PERIOD
      ), windows AS (
        SELECT STRANA,NAPR,TNVED,
         coalesce(sum(available) FILTER(WHERE {current}),0) AS current_available_usd,
         coalesce(sum(available) FILTER(WHERE {base}),0) AS base_available_usd,
         count(*) FILTER(WHERE month_ok AND {current}) AS nc,
         count(*) FILTER(WHERE month_ok AND {base}) AS nb
        FROM m GROUP BY STRANA,NAPR,TNVED
      ), sources AS (
        SELECT STRANA,NAPR,TNVED,
         count(DISTINCT SOURCE) FILTER(WHERE {current}) AS current_source_count,
         count(DISTINCT SOURCE) FILTER(WHERE {base}) AS base_source_count,
         max(SOURCE) FILTER(WHERE {current}) AS current_source,
         max(SOURCE) FILTER(WHERE {base}) AS base_source
        FROM {relation} GROUP BY STRANA,NAPR,TNVED
      ) SELECT w.*,coalesce(nc=12 AND nb=12 AND current_source_count=1 AND base_source_count=1
           AND current_source=base_source,false) AS eligible_pair
        FROM windows w LEFT JOIN sources USING(STRANA,NAPR,TNVED)""")
    codes = ['0', '27', '2709', '29', '2924', '2933', '292419', '293349',
             '10', '1006', '100630', '2926', '292690', '86', '8608', '860800']
    rows = []
    for level in [0, 2, 4, 6]:
        code = "'0'" if level == 0 else f'substr(TNVED,1,{level})'
        literals = ','.join("'"+v+"'" for v in codes)
        rows.append(con.execute(f"""SELECT coalesce(STRANA,'ALL') AS STRANA,NAPR,
          {code} AS TNVED_G,{level} AS level,
          sum(current_available_usd) AS current_available_usd,sum(base_available_usd) AS base_available_usd,
          coalesce(sum(current_available_usd) FILTER(WHERE eligible_pair),0) AS matched_current_usd,
          coalesce(sum(base_available_usd) FILTER(WHERE eligible_pair),0) AS matched_base_usd,
          coalesce(sum(current_available_usd) FILTER(WHERE eligible_pair),0)/nullif(sum(current_available_usd),0) AS matched_current_share,
          coalesce(sum(base_available_usd) FILTER(WHERE eligible_pair),0)/nullif(sum(base_available_usd),0) AS matched_base_share
          FROM coverage_codes WHERE {code} IN ({literals})
          GROUP BY GROUPING SETS ((STRANA,NAPR,TNVED_G),(NAPR,TNVED_G))""").df())
    result = pd.concat(rows, ignore_index=True)
    result = result[result.STRANA.isin(['IN', 'ALL'])].copy()
    result['PERIOD'] = latest
    blockers = con.execute("""SELECT STRANA,NAPR,TNVED,current_available_usd,base_available_usd,
      12-nc AS missing_current_cells,12-nb AS missing_base_cells FROM coverage_codes
      WHERE STRANA='IN' AND NOT eligible_pair
      ORDER BY greatest(current_available_usd,base_available_usd) DESC""").df()
    return result, blockers


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--evidence', required=True)
    p.add_argument('--oil', required=True)
    args = p.parse_args()
    out = Path(args.out)
    patch = json.loads(Path(args.evidence).read_text(encoding='utf-8'))
    for item in patch['evidence']:
        assert hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() == item['sha256']
    oil = pd.read_csv(args.oil)
    assert len(oil) == 93 and not oil.period.duplicated().any()
    assert ((oil.status.eq('verified_monthly') & oil.difference_usd.abs().lt(.01)) |
            oil.status.eq('verified_exact_cumulative')).all()
    assert oil.unknown_positive_mass_rows.eq(0).all()
    before = pd.read_parquet(out/'before.parquet')
    after = pd.read_parquet(out/'after.parquet')
    before['PERIOD'] = pd.to_datetime(before.PERIOD)
    after['PERIOD'] = pd.to_datetime(after.PERIOD)
    keys = ['STRANA','NAPR','level','TNVED_G','PERIOD']
    assert not before.duplicated(keys).any() and not after.duplicated(keys).any()
    joined = before.merge(after, on=keys, suffixes=('_before','_after'), validate='one_to_one')
    assert len(joined) == len(before) == len(after)
    start = pd.Timestamp(patch['period'])
    changed_value = (joined.STRANA.isin(['IN','ALL']) & joined.NAPR.eq('ИМ') &
                     joined.TNVED_G.isin(['0','86','8608','860800']) &
                     joined.PERIOD.ge(start) & joined.PERIOD.lt(start+pd.DateOffset(months=12)))
    delta = joined.available_stoim_12_after - joined.available_stoim_12_before
    assert np.allclose(delta, np.where(changed_value, patch['value_usd'], 0), atol=.02, rtol=0, equal_nan=False)
    latest = after.PERIOD.max()
    columns = keys + [n for n in joined if n.startswith(('coverage_', 'base_coverage_', 'fizob_', 'status_', 'imp_', 'mass_coverage_'))]
    comparison = joined[joined.STRANA.isin(['IN','ALL']) & joined.PERIOD.eq(latest)][columns]
    comparison.to_csv(out/'comparison.csv', index=False)

    con = duckdb.connect()
    db = str(Path(args.db).resolve()).replace("'", "''")
    con.execute(f"ATTACH '{db}' AS baseline (READ_ONLY)")
    columns = 'STRANA,NAPR,TNVED,PERIOD,EDIZM,STOIM,NETTO,KOL,TYPE,SOURCE,KOL_ESTIMATED,NETTO_ESTIMATED'
    con.execute(f"""CREATE TEMP VIEW original AS SELECT {columns} FROM baseline.unified_trade_data
      WHERE TYPE='fact' AND SOURCE IS NOT NULL AND SOURCE!='nowcast' AND STRANA!='ALL'""")
    snapshot = str((out/'india-analysis-input-NOT-FOR-SERVING.parquet').resolve()).replace("'", "''")
    con.execute(f"CREATE TEMP VIEW india_after AS SELECT * FROM read_parquet('{snapshot}')")
    con.execute(f"CREATE TEMP VIEW combined AS SELECT {columns} FROM original WHERE STRANA!='IN' UNION ALL SELECT {columns} FROM india_after")
    # Restore the reviewed changes and compare all calculation inputs exactly.
    clauses = []
    for item in patch['mass_adjustments']:
        clause = f"NAPR='{item['napr']}' AND ORIGINAL_TNVED='{item['tnved']}' AND PERIOD=DATE '{item['period']}' AND SOURCE='national'"
        row = con.execute(f'SELECT STOIM,NETTO FROM india_after WHERE {clause}').fetchall()
        assert row == [(item['expected_cost'], item['value'])]
        clauses.append('('+clause+')')
    new_row = f"NAPR='{patch['napr']}' AND ORIGINAL_TNVED='{patch['tnved']}' AND PERIOD=DATE '{patch['period']}' AND SOURCE='national'"
    assert con.execute(f'SELECT STOIM,NETTO,KOL FROM india_after WHERE {new_row}').fetchall() == [(patch['value_usd'],patch['mass_kg'],None)]
    restored = f"""SELECT STRANA,NAPR,ORIGINAL_TNVED AS TNVED,PERIOD,EDIZM,STOIM,
       CASE WHEN {' OR '.join(clauses)} THEN NULL ELSE NETTO END AS NETTO,
       KOL,TYPE,SOURCE,KOL_ESTIMATED,NETTO_ESTIMATED FROM india_after WHERE NOT ({new_row})"""
    original_india = 'SELECT * FROM original WHERE STRANA=\'IN\''
    differences = con.execute(f"SELECT count(*) FROM (({restored} EXCEPT ALL {original_india}) UNION ALL ({original_india} EXCEPT ALL {restored}))").fetchone()[0]
    assert differences == 0
    assert con.execute("SELECT count(*) FROM india_after WHERE STOIM=0").fetchone() == con.execute("SELECT count(*) FROM original WHERE STRANA='IN' AND STOIM=0").fetchone()
    frames = []
    for label, relation in [('before','original'), ('after','combined')]:
        coverage, blockers = coverage_table(con, relation, latest)
        coverage['scenario'] = label
        frames.append(coverage)
        blockers.to_csv(out/f'{label}-blockers.csv', index=False)
    pd.concat(frames, ignore_index=True).to_csv(out/'value-coverage.csv', index=False)

    monthly = con.execute("""SELECT PERIOD,sum(NETTO) AS mass_kg FROM india_after
      WHERE NAPR='ЭК' AND substr(ORIGINAL_TNVED,1,4)='2709' AND SOURCE='national' AND STOIM>0
      GROUP BY PERIOD ORDER BY PERIOD""").df()
    monthly['mass_12'] = monthly.mass_kg.rolling(12, min_periods=12).sum()
    monthly['mass_12_yoy_pct'] = (monthly.mass_12/monthly.mass_12.shift(12)-1)*100
    series = after[after.STRANA.eq('IN') & after.NAPR.eq('ЭК') & after.TNVED_G.eq('2709')].sort_values('PERIOD').copy()
    series['index_yoy_pct'] = (series.fizob/series.fizob.shift(12)-1)*100
    series = series.merge(monthly,on='PERIOD',validate='one_to_one')
    recent = series[series.PERIOD.ge(latest-pd.DateOffset(months=11))]
    assert len(recent)==12 and recent.status.eq('complete').all() and recent.imp.eq(0).all()
    assert np.allclose(recent.index_yoy_pct, recent.mass_12_yoy_pct, atol=1e-8, rtol=0)
    series.to_csv(out/'oil-index-and-mass-check.csv', index=False)
    summary = dict(scenario_only=True, source_database_modified=False, zero_imputation=False,
                   fisher_formula_unchanged=True, added_rows=1, filled_mass_fields=len(patch['mass_adjustments']),
                   reconciled_oil_months=len(oil), oil_last_12_months_complete=True,
                   oil_index_yoy_matches_rolling_mass=True,
                   latest_oil_index_yoy_pct=float(recent.iloc[-1].index_yoy_pct),
                   all_value_changes_explained=True, unreviewed_input_changes=differences,
                   value_coverage_definition='Share of available monetary value in codes with complete current/reference windows and one unchanged source; not a published full aggregate index')
    (out/'audit.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == '__main__':
    main()
