"""Local verified annual-window experiment; no synthetic monthly observations."""
from pathlib import Path
import hashlib
import json
import numpy as np
import pandas as pd
import duckdb

OUT=Path('tmp/china-control-totals'); OUT.mkdir(exist_ok=True)
ROOT=Path('data_raw/china/verification')
def read(name):
    x=pd.read_csv(ROOT/name,dtype=str)
    for col in ['US dollar','Quantity','Supplementary Quantity']:
        x[col]=pd.to_numeric(x[col].str.replace(',','',regex=False))
    assert set(x['Trading partner code'])=={'344'}
    return x
pitch=read('27081000.csv'); total=read('27081000-2025-total.csv')
jet=read('27101914.csv'); jt=read('27101914-2026-total.csv')
cols=['US dollar','Quantity','Supplementary Quantity']
assert len(total)==len(jt)==1
assert set(pitch['Date of data'].str[:4])=={'2025'}
assert (pitch[cols].sum().values==total[cols].iloc[0].values).all()
assert (jet[cols].sum().values==jt[cols].iloc[0].values).all()
jet_aug=jet[jet['Date of data']<='202608']
assert not (jet_aug[cols].sum().values==jt[cols].iloc[0].values).all()
links=pd.read_parquet('tmp/fisher-matched-basket-v2/series.parquet')
baseline=pd.read_parquet('tmp/fisher-matched-basket-v2/summary.parquet')

def price_and_summarize(x,denominators):
    x=x.copy()
    donors=x[x.eligible & np.isfinite(x.lr)]
    # Match original hierarchy: median log relatives before outlier removal.
    m0=donors.groupby('NAPR').lr.median()
    x['median']=x.NAPR.map(m0)
    for level in [2,4,6]:
        g=donors.assign(code=donors.TNVED.str[:level]).groupby(['NAPR','code']).lr.agg(['median','count'])
        keys=pd.MultiIndex.from_arrays([x.NAPR,x.TNVED.str[:level]])
        selected=g.reindex(keys)
        use=selected['count'].to_numpy()>=3
        x.loc[use,'median']=selected['median'].to_numpy()[use]
    own=x.lr.mask(abs(x.lr-x['median'])>np.log(3))
    x['imputed']=~np.isfinite(own)
    x['r']=np.exp(own.fillna(x['median']))
    x['included']=x.eligible & np.isfinite(x.r) & (x.r>0)
    result=[]
    for row in denominators.itertuples():
        s=x[x.included & (x.NAPR==row.NAPR)]
        if row.STRANA!='ALL': s=s[s.STRANA==row.STRANA]
        if row.TNVED_G!='0': s=s[s.TNVED.str.startswith(row.TNVED_G)]
        a=s.v0.sum(); b=s.v.sum(); c=(s.v0*s.r).sum(); d=(s.v/s.r).sum()
        q=np.sqrt(d/a*b/c); p=np.sqrt(c/a*b/d)
        assert abs(q*p-b/a)<1e-10
        result.append(dict(STRANA=row.STRANA,NAPR=row.NAPR,TNVED_G=row.TNVED_G,
            series=len(s),A=a,B=b,C=c,D=d,q_link=q,p_link=p,
            current_coverage=b/row.current_available,base_coverage=a/row.base_available,
            imp=(s.loc[s.imputed,'v']+s.loc[s.imputed,'v0']).sum()/(a+b)))
    return x,pd.DataFrame(result)

_,replicated=price_and_summarize(links,baseline)
for col in ['A','B','C','D','q_link','p_link','imp','current_coverage','base_coverage']:
    np.testing.assert_allclose(replicated[col],baseline[col],rtol=1e-11,atol=1e-6)
con=duckdb.connect('tmp/china-revision-build/input.duckdb',read_only=True)
raw=con.execute("SELECT PERIOD,STOIM,NETTO FROM unified_trade_data WHERE STRANA='CN' AND NAPR='ИМ' AND TYPE='fact' AND SOURCE='national' AND TNVED='2708100000' AND PERIOD BETWEEN '2025-01-01' AND '2026-08-01'").fetchdf()
old_base=raw.loc[raw.PERIOD<'2026-01-01','STOIM'].sum()
for r in pitch.to_dict('records'):
    month=pd.to_datetime(r['Date of data'],format='%Y%m')
    mask=raw.PERIOD==month
    assert mask.sum()==1
    raw.loc[mask,['STOIM','NETTO']]=[r['US dollar'],r['Quantity']]
cur=raw[raw.PERIOD>='2025-09-01']
assert len(cur)==12 and cur.NETTO.notna().all() and (cur.NETTO>0).all()
v=cur.STOIM.sum(); v0=total['US dollar'].iloc[0]
lr=np.log(v/cur.NETTO.sum())-np.log(v0/total.Quantity.iloc[0])
mask=(links.STRANA=='CN')&(links.NAPR=='ИМ')&(links.TNVED=='2708100000')
assert mask.sum()==1
links['base_window_evidence']='monthly_complete_or_unavailable'
links.loc[mask,['v','v0','lr','eligible','base_window_evidence']]=[v,v0,lr,True,'verified_annual_total_2025']
# Do not falsify observed month counts: base remains 10 observed monthly rows.
assert links.loc[mask,'observed0'].iloc[0]==10
delta=v0-old_base
denoms=baseline.copy()
affected=(denoms.NAPR=='ИМ')
denoms.loc[affected,'current_available']+=delta
denoms.loc[affected,'base_available']+=delta
updated,result=price_and_summarize(links,denoms)
assert result.current_coverage.between(0,1).all() and result.base_coverage.between(0,1).all()
updated.to_parquet(OUT/'series.parquet',index=False)
result.to_csv(OUT/'summary.csv',index=False,encoding='utf-8-sig')
result.to_parquet(OUT/'summary.parquet',index=False)
evidence=dict(pitch_base_usd=int(v0),pitch_base_kg=int(total.Quantity.iloc[0]),
    revision_usd=float(delta),jet_total_matches='January-September 2026 monthly sum; not January-August',
    jet_august_usd=int(jet_aug['US dollar'].sum()),jet_total_usd=int(jt['US dollar'].iloc[0]),
    period_direction_provenance='User supplied files for requested periods/direction; totals CSV contains neither field',
    zero_rows_created=0,production_changed=False,baseline_replication_passed=True,
    files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in ROOT.glob('*.csv')})
(OUT/'evidence.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')
print(result.to_string(index=False))
