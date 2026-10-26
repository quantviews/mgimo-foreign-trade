"""Prepare isolated national inputs and release evidence for September refresh."""
import hashlib
import json
import shutil
import sys
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from core.china_units import normalize_quantities
from core.edizm import load_common_edizm_mapping
from core.country_processor_contract import finalize_country_output,assert_country_output_contract

out=ROOT/'tmp/release-20261024'
out.mkdir(exist_ok=False)
processed=out/'processed';processed.mkdir()
for name in ['cn_full.parquet','in_full.parquet','tr_full.parquet']:
    shutil.copy2(ROOT/'data_processed'/name,processed/name)
manifest_path=ROOT/'metadata/national_release_coverage.json'
shutil.copy2(manifest_path,out/'national_release_coverage.before.json')
manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
audit=json.loads((ROOT/'tmp/china-jul-aug-original-review/report.json').read_text(encoding='utf-8'))
sept=json.loads((ROOT/'tmp/china-september-review/validated.json').read_text(encoding='utf-8'))
items=[dict(raw=r['file'],sha=r['sha256'],rows=r['rows'],value=r['value_usd'],
    staged='tmp/china-jul-aug-original-review/staged/'+Path(r['file']).parent.name+'/'+Path(r['file']).name) for r in audit]
items += [dict(raw=r['source'],sha=r['source_sha256'],rows=r['rows'],value=r['value_usd'],staged=r['processed']) for r in sept]
cn=pd.read_parquet(processed/'cn_full.parquet')
for item in items:
    raw=ROOT/item['raw']; assert hashlib.sha256(raw.read_bytes()).hexdigest()==item['sha']
    frame=pd.read_csv(ROOT/item['staged'],dtype={'TNVED':str})
    assert len(frame)==item['rows'] and abs(frame.STOIM.sum()-item['value'])<.01
    frame=normalize_quantities(frame,{},load_common_edizm_mapping(ROOT))
    frame['PERIOD']=pd.to_datetime(frame.PERIOD)
    frame=finalize_country_output(frame,country_code='CN',sort_by=('PERIOD','NAPR','TNVED'))
    month=frame.PERIOD.iloc[0];direction=frame.NAPR.iloc[0]
    cn=cn[~((pd.to_datetime(cn.PERIOD)==month)&(cn.NAPR==direction))]
    cn=pd.concat([cn,frame],ignore_index=True)
    record=dict(country='CN',month=str(month)[:7],direction=direction,status='confirmed_complete',
      raw_file=raw.relative_to(ROOT).as_posix(),raw_sha256=item['sha'],rows=item['rows'],stoim_usd=item['value'],
      confirmed_on='2026-10-24',basis='User-supplied all-commodity national release; archived original, partner, period, rows and value verified. Absence of a commodity is not a confirmed zero.')
    manifest['releases']=[r for r in manifest['releases'] if (r['country'],r['month'],r['direction'])!=('CN',record['month'],direction)]+[record]
# Apply only the observed September 2025 pitch revision, no monthly zero rows.
pitch=pd.read_csv(ROOT/'data_raw/china/verification/27081000.csv',dtype=str)
value=float(pitch.loc[pitch['Date of data']=='202509','US dollar'].iloc[0].replace(',',''))
mask=(cn.TNVED.astype(str).str.startswith('27081000'))&(cn.NAPR=='ИМ')&(pd.to_datetime(cn.PERIOD)==pd.Timestamp('2025-09-01'))
assert mask.sum()==1
cn.loc[mask,'STOIM']=value
assert_country_output_contract(cn,expected_strana='CN')
cn.to_parquet(processed/'cn_full.parquet',index=False)
manifest_path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
(out/'input-audit.json').write_text(json.dumps(items,ensure_ascii=False,indent=2),encoding='utf-8')
print('Prepared China rows:',len(cn),'max period:',cn.PERIOD.max())
