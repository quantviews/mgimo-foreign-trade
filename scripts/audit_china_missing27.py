"""Audit missing Chinese chapter-27 rows without converting absence to zero."""
import argparse
import io
import csv
import json
import sys
from pathlib import Path

import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from collectors.china_evidence import archive_export, save_manifest, decode_export


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    annual={}
    for year in (2025,2026):
        source=ROOT/f'data_raw/china/YEAR/{year}.csv'
        folder,meta,_=archive_export(source,args.output,dict(year=year,frequency='annual',
            china_flow='unknown',commodity_scope='unknown',origin='existing_local_year_file'))
        content,encoding=decode_export(source.read_bytes())
        records=list(csv.reader(io.StringIO(content)))
        header=records[0]
        malformed=[dict(record=i+2,fields=len(row),raw_fields=row) for i,row in enumerate(records[1:]) if len(row)!=len(header)]
        # Diagnostic lookup only: irregular records remain in the archive and error log.
        data=pd.DataFrame([row for row in records[1:] if len(row)==len(header)],columns=header)
        asset_type='nomenclature' if 'CODES' in header else 'annual_trade_export'
        (folder/'malformed_records.json').write_text(json.dumps(malformed,ensure_ascii=False,indent=2),encoding='utf-8')
        meta.update(validation_status='partial_diagnostic_only' if malformed else 'archived_only',
            asset_type=asset_type,malformed_records=len(malformed),encoding=encoding,parameter_provenance='historical_query_not_available',
            source_columns=list(data),observed_partners=sorted(data['Trading partner code'].unique()) if 'Trading partner code' in data else [],
            remarks_preserved='remarks' in data)
        save_manifest(folder,meta)
        annual[year]=data
    releases=json.loads((ROOT/'metadata/national_release_coverage.json').read_text(encoding='utf-8'))['releases']
    rows=[]
    for release in releases:
        source=ROOT/release['raw_file'];month=pd.Period(release['month'])
        data=pd.read_csv(source,dtype={'TNVED':str})
        before=pd.read_csv(source.with_name(f'data{str(month-1).replace("-", "")}.csv'),dtype={'TNVED':str})
        nextpath=source.with_name(f'data{str(month+1).replace("-", "")}.csv')
        after=pd.read_csv(nextpath,dtype={'TNVED':str}) if nextpath.exists() else None
        missing=sorted(set(before.TNVED[before.TNVED.str.startswith('27')])-set(data.TNVED))
        for code in missing:
            item=dict(month=str(month),russia_direction=release['direction'],code=code,
                previous_value_usd=float(before.loc[before.TNVED.eq(code),'STOIM'].sum()),
                next_month_row_present=bool(after.TNVED.eq(code).any()) if after is not None else None,
                decision='unknown_not_zero',reason='No original monthly query or code-specific absence evidence')
            for year,year_data in annual.items():
                if 'CODES' in year_data:
                    item[f'nomenclature_{year}_code_present']=bool(year_data.CODES.eq(code).any())
                    continue
                matched=year_data[year_data['Commodity code'].eq(code)]
                item[f'annual_{year}_rows']=len(matched)
                item[f'annual_{year}_remarks']=sorted(matched['remarks'].unique().tolist()) if 'remarks' in matched else []
            rows.append(item)
    (args.output/'missing_codes.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f'{len(rows)} missing code/month/direction cases. Confirmed zeros: 0.')


if __name__=='__main__':
    main()
