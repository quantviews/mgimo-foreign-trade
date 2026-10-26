"""Read-only local publication/observation audit. Prints JSON; never modifies data."""
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd


def main():
    root = Path(__file__).resolve().parents[1]
    con = duckdb.connect(str(root / 'db/unified_trade_data.duckdb'), read_only=True)
    result = {}
    result['national_fact_rows'] = con.execute("""
        SELECT STRANA, NAPR, count(*) AS rows,
          count(*) FILTER (WHERE STOIM=0) AS zero_value,
          count(*) FILTER (WHERE STOIM=0 AND (NETTO>0 OR KOL>0)) AS zero_value_positive_physical,
          count(*) FILTER (WHERE TNVED LIKE '2709%') AS crude_oil_rows
        FROM unified_trade_data WHERE TYPE='fact' AND STRANA IN ('CN','IN','TR')
        GROUP BY ALL ORDER BY 1,2
    """).fetchdf().to_dict('records')
    result['confirmed_releases'] = []
    manifest = json.loads((root / 'metadata/national_release_coverage.json').read_text(encoding='utf-8'))
    for r in manifest['releases']:
        p = root / r['raw_file']
        d = pd.read_csv(p)
        result['confirmed_releases'].append({
            'file': r['raw_file'], 'hash_matches': hashlib.sha256(p.read_bytes()).hexdigest() == r['raw_sha256'],
            'rows_match': len(d) == r['rows'], 'value_matches': bool(abs(d.STOIM.sum()-r['stoim_usd']) < .01),
        })
    result['india_2026'] = []
    for p in sorted((root / 'data_raw/india_new').glob('india_2026_*.csv')):
        d = pd.read_csv(p)
        result['india_2026'].append({'file':p.name,'rows':len(d),
            'zero_usd_positive_quantity':int((d.STOIM.eq(0)&d.KOL.gt(0)).sum()),
            'zero_usd_positive_inr':int((d.STOIM.eq(0)&d.STOIM_NAC_VAL.gt(0)).sum())})
    p = sorted((root / 'data_raw/comtrade_data').glob('????-??.parquet'))[-1]
    d = pd.read_parquet(p)
    d = d[d.partner2Code.eq(0)&d.customsCode.eq('C00')&d.motCode.eq(0)]
    checks = []
    for (reporter, flow, classification), g in d.groupby(['reporterCode','flowCode','classificationCode']):
        for code in ['TOTAL','27']:
            total = g[g.cmdCode.eq(code)]
            leaf = g[g.cmdCode.str.len().eq(6)]
            if code != 'TOTAL':
                leaf = leaf[leaf.cmdCode.str.startswith(code)]
            if len(total) != 1:
                continue
            t = total.iloc[0]
            checks.append({'reporter':int(reporter),'flow':flow,'classification':classification,'code':code,
                'reported_total':bool(t.isReported),'aggregate_total':bool(t.isAggregate),
                'total':float(t.primaryValue),'hs6_sum':float(leaf.primaryValue.sum()),
                'difference':float(t.primaryValue-leaf.primaryValue.sum()),'hs6_rows':len(leaf)})
    result['comtrade_month'] = p.stem
    result['comtrade_reconciliation'] = checks
    con.close()
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == '__main__':
    main()
