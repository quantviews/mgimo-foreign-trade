"""Conservative audit of exact national-code candidates; no serving DB writes."""
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def main():
    out=Path('tmp/india-comtrade-20261119')
    con=duckdb.connect('db/unified_trade_data.duckdb',read_only=True)
    rows=[]
    for path in sorted(out.glob('tariff-allcodes-*.json')):
        obj=json.loads(path.read_text(encoding='utf-8'))
        data=obj['response']['data']
        assert 0<len(data)<100000
        for row in data:
            assert row['reporterCode']==699 and row['partnerCode']==643
            assert len(row['cmdCode'])==8 and row['cmdCode'].isdigit()
            if row['flowCode']=='X':
                rows.append({**row,'evidence_file':path.name,'geography_basis':'destination'})
    for path in sorted(out.glob('tariff-consignment-*.json')):
        data=json.loads(path.read_text(encoding='utf-8'))['response']['data']
        assert 0<len(data)<100000
        for row in data:
            assert row['reporterCode']==699 and row['partner2Code']==643 and row['flowCode']=='M'
            assert row['partnerCode']!=0 and len(row['cmdCode'])==8
            rows.append({**row,'evidence_file':path.name,'geography_basis':'consignment'})
    raw=pd.DataFrame(rows)
    assert not raw.duplicated(['period','flowCode','cmdCode','partnerCode','partner2Code','customsCode','motCode','mosCode','qtyUnitCode']).any()
    assert raw.customsCode.astype(str).eq('-1').all() and raw.motCode.eq(-1).all()
    # Tariff rows are disjoint secondary-partner details, not final-data totals.
    assert not raw.partner2Code.eq(0).any(), 'Mixed total/detail partners need explicit reconciliation'
    raw['PERIOD']=pd.to_datetime(raw.period.astype(str),format='%Y%m')
    raw['TNVED']=raw.cmdCode+'00'
    raw['NAPR']=raw.flowCode.map({'M':'ЭК','X':'ИМ'})
    assert raw.NAPR.notna().all()
    key=['NAPR','TNVED','PERIOD']
    grouped=[]
    for keys,g in raw.groupby(key,sort=True):
        positive=g[g.primaryValue>0]
        mass=g.netWgt.sum() if len(positive) and positive.netWgt.gt(0).all() else np.nan
        units=positive.qtyUnitCode.dropna().unique()
        quantity=g.qty.sum() if len(positive) and len(units)==1 and positive.qty.gt(0).all() else np.nan
        alt_mass=g.altQty.sum() if len(positive) and positive.altQtyUnitCode.eq(8).all() and positive.altQty.gt(0).all() else np.nan
        grouped.append(dict(zip(key,keys),ct_value=g.primaryValue.sum(),ct_mass=mass,
            ct_alt_mass_kg=alt_mass,
            ct_qty=quantity,ct_qty_unit=int(units[0]) if len(units)==1 else None,
            evidence_file=g.evidence_file.iloc[0]))
    ct=pd.DataFrame(grouped)
    nat=con.execute("""SELECT NAPR,TNVED,PERIOD,STOIM,NETTO,KOL,EDIZM FROM unified_trade_data
        WHERE STRANA='IN' AND TYPE='fact' AND SOURCE='national'""").df()
    assert not nat.duplicated(key).any()
    joined=ct.merge(nat,on=key,how='left',indicator=True)
    joined['cost_match']=joined.STOIM.notna() & ((joined.ct_value-joined.STOIM).abs()<=5000.01)
    joined['mass_comparable']=joined.ct_mass.notna() & joined.NETTO.notna() & joined.NETTO.gt(0)
    # Screening tolerance only, not permission to replace a national value.
    joined['mass_match']=joined.mass_comparable & ((joined.ct_mass-joined.NETTO).abs()<=np.maximum(1,joined.NETTO.abs()*.001))
    # Scope: chapter 27 and the ten largest incomplete codes in each direction.
    blockers=pd.read_csv('tmp/india-oil-check-20261119/ranked-coverage-blockers.csv',dtype={'TNVED':str})
    focus=set((r.NAPR,r.TNVED) for r in blockers.groupby('NAPR',sort=False).head(10).itertuples())
    joined['in_scope']=[code.startswith('27') or (flow,code) in focus for flow,code in zip(joined.NAPR,joined.TNVED)]
    stats=[]
    for (flow,code),g in joined.groupby(['NAPR','TNVED']):
        overlap=g[g.STOIM.notna()&(g.STOIM.gt(0)|g.ct_value.gt(0))]
        mass=overlap[overlap.mass_comparable&overlap.STOIM.gt(0)&overlap.ct_value.gt(0)]
        stats.append(dict(NAPR=flow,TNVED=code,positive_overlap=int((overlap.STOIM.gt(0)&overlap.ct_value.gt(0)).sum()),
            cost_mismatches=int((~overlap.cost_match).sum()),physical_overlap=len(mass),
            mass_mismatches=int((~mass.mass_match).sum())))
    stats=pd.DataFrame(stats)
    physical=joined[joined.in_scope&joined.STOIM.gt(0)&joined.NETTO.isna()&joined.ct_mass.gt(0)].merge(stats,on=['NAPR','TNVED'])
    physical['passes_overlap_screen']=physical.cost_match&(physical.positive_overlap>=3)&physical.cost_mismatches.eq(0)&(physical.physical_overlap>=3)&physical.mass_mismatches.eq(0)&physical.ct_alt_mass_kg.gt(0)&((physical.ct_alt_mass_kg-physical.ct_mass).abs()<=1)
    physical.to_csv(out/'physical-gap-candidates.csv',index=False)
    candidates=joined[joined._merge.eq('left_only')&joined.ct_value.gt(0)].merge(stats,on=['NAPR','TNVED'])
    # Mere similarity is not proof: require at least three positive overlaps,
    # no cost mismatches beyond source precision and masses within 0.1%.
    candidates['passes_overlap_screen']=(candidates.positive_overlap>=3)&candidates.cost_mismatches.eq(0)&(candidates.physical_overlap>=3)&candidates.mass_mismatches.eq(0)&candidates.ct_mass.notna()
    # Still requires checking the target month's national aggregate/source.
    candidates['decision']='not_confirmed'
    candidates.to_csv(out/'missing-row-candidates.csv',index=False)
    joined[joined.in_scope].to_csv(out/'overlap-detail.csv',index=False)
    stats.to_csv(out/'overlap-scores.csv',index=False)
    # Monthly chapter totals are diagnostic, never inserted beside tariff rows.
    nationals=nat.assign(group=nat.TNVED.str[:2]).query("group == '27'").groupby(['NAPR','PERIOD']).agg(national_usd=('STOIM','sum'),national_mass_available=('NETTO',lambda s:s.sum(min_count=1)))
    chapter=ct[ct.TNVED.str.startswith('27')].groupby(['NAPR','PERIOD']).agg(comtrade_usd=('ct_value','sum'),comtrade_mass_available=('ct_mass',lambda s:s.sum(min_count=1)))
    chapter.join(nationals).to_csv(out/'chapter27-comparison.csv')
    summary=dict(tariff_rows=len(raw),monthly_code_rows=len(ct),months=sorted(raw.period.unique().tolist()),
        positive_missing_rows=len(candidates),scope_missing_rows=int(candidates.in_scope.sum()),
        scope_screen_pass=int((candidates.in_scope&candidates.passes_overlap_screen).sum()))
    (out/'comparison-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False))
    print(candidates[candidates.in_scope].sort_values('ct_value',ascending=False).head(20)[['NAPR','TNVED','PERIOD','ct_value','positive_overlap','cost_mismatches','physical_overlap','mass_mismatches','passes_overlap_screen']].to_string(index=False))


if __name__=='__main__':main()
