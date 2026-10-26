"""Local evidence about absent rows. Reconciliation is not proof of zero trade."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def audit():
    result = {'china': [], 'comtrade': []}
    releases = json.loads((ROOT/'metadata/national_release_coverage.json').read_text(encoding='utf-8'))['releases']
    for release in releases:
        path = ROOT / release['raw_file']
        data = pd.read_csv(path, dtype={'TNVED': str})
        previous = path.with_name('data' + str(pd.Period(release['month'])-1).replace('-','') + '.csv')
        old = pd.read_csv(previous,dtype={'TNVED':str}) if previous.exists() else pd.DataFrame(columns=['TNVED'])
        absent = sorted(set(old.TNVED)-set(data.TNVED))
        result['china'].append(dict(file=release['raw_file'],
            sha256_matches=hashlib.sha256(path.read_bytes()).hexdigest()==release['raw_sha256'],
            rows_match=len(data)==release['rows'],
            value_matches=bool(abs(data.STOIM.sum()-release['stoim_usd'])<.01),
            rows=len(data), group27_rows=int(data.TNVED.str.startswith('27').sum()),
            codes_in_previous_but_absent_now=len(absent),
            absent_group27_codes=[c for c in absent if c.startswith('27')],
            original_remarks_preserved='remarks' in data.columns,
            partner_query_preserved='Trading partner code' in data.columns))
    paths = sorted((ROOT/'data_raw/comtrade_data').glob('202[56]-??.parquet'))
    for path in paths:
        data = pd.read_parquet(path)
        data = data[data.partner2Code.eq(0)&data.customsCode.eq('C00')&data.motCode.eq(0)]
        checks = []
        for (reporter,flow,classification), group in data.groupby(['reporterCode','flowCode','classificationCode']):
            for code in ['27','TOTAL']:
                total = group[group.cmdCode.eq(code)]
                leaves = group[group.cmdCode.str.len().eq(6)]
                if code=='27':
                    leaves = leaves[leaves.cmdCode.str.startswith(code)]
                if len(total)!=1:
                    continue
                row=total.iloc[0]
                checks.append(dict(reporter=int(reporter),flow=flow,classification=classification,code=code,
                    total=float(row.primaryValue),hs6_sum=float(leaves.primaryValue.sum()),
                    difference=float(row.primaryValue-leaves.primaryValue.sum()),
                    is_reported=bool(row.isReported),is_aggregate=bool(row.isAggregate)))
        special = data[data.cmdCode.eq('999999') & data.primaryValue.gt(0)]
        result['comtrade'].append(dict(month=path.stem,checks=len(checks),
            directly_reported_totals=sum(c['is_reported'] for c in checks),
            discrepancies=[c for c in checks if abs(c['difference'])>1],
            positive_unspecified_hs6_rows=len(special),
            explicit_zero_parent_rows=sum(c['total']==0 for c in checks)))
    return result


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    # Reserve output before auditing; do not overwrite prior evidence.
    with args.output.open('x',encoding='utf-8') as stream:
        json.dump(audit(),stream,ensure_ascii=False,indent=2)
