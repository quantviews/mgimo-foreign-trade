"""Read-only audit of Chinese group 27 code changes and missing source rows."""
import csv
import hashlib
import json
from pathlib import Path
import pandas as pd

out=Path('tmp/china27-code-transition')
out.mkdir(exist_ok=True)
x=pd.read_csv('tmp/fisher-coverage-blockers/CN-27-series.csv',dtype={'TNVED':str})
x=x[x.NAPR=='ИМ']
missing=pd.read_csv('tmp/fisher-coverage-blockers/CN-27-missing-months.csv',dtype={'TNVED':str})
top=x[x.category!='both_windows_complete'].nlargest(8,'current_value')
checks=[]
for row in top.itertuples():
    # Obsolete/new-code calendar cells are not treated as missing publications.
    periods=missing[(missing.NAPR=='ИМ')&(missing.TNVED==row.TNVED)].PERIOD.str[:7]
    for month in periods:
        if row.TNVED=='2710191100' and month>='2026-01': continue
        if row.TNVED=='2710191400' and month<'2026-01': continue
        path=Path('data_raw/china/IMPORT')/('data'+month.replace('-','')+'.csv')
        if not path.exists():
            checks.append(dict(code=row.TNVED,month=month,status='file_missing')); continue
        raw=path.read_bytes()
        for enc in ['utf-8-sig','gb18030','cp1252']:
            try: content=raw.decode(enc); break
            except UnicodeDecodeError: continue
        reader=csv.reader(content.splitlines())
        header=next(reader)
        col=header.index('TNVED') if 'TNVED' in header else header.index('Commodity code')
        found=[r for r in reader if len(r)>col and r[col].strip().ljust(10,'0')==row.TNVED]
        checks.append(dict(code=row.TNVED,month=month,source_file=str(path),
            sha256=hashlib.sha256(raw).hexdigest(),rows_found=len(found),
            status='present_in_source' if found else 'absent_in_local_source'))
f=x[x.category=='both_windows_complete']
add=x[x.TNVED.isin(['2708100000','2710191100','2710191400'])]
report=dict(official_crosswalk=dict(effective='2026-01-01',old=['27101911'],new=['27101913','27101914'],
    relation='one_to_many',source='https://gss.mof.gov.cn/gzdt/zhengcefabu/202512/P020251229510521800793.pdf',page=3),
    checks=checks,top_missing=json.loads(top.to_json(orient='records')),
    conditional_known_value_share=dict(current=(f.current_value.sum()+add.current_value.sum())/x.current_value.sum(),
      base=(f.base_value.sum()+add.base_value.sum())/x.base_value.sum(),
      status='diagnostic_only_not_achieved_coverage; missing values and child-code completeness unresolved'),
    production_changed=False)
(out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(checks,ensure_ascii=False,indent=2))
