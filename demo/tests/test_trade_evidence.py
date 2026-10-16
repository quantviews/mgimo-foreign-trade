import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('evidence',Path(__file__).parents[1]/'app/trade_evidence.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def test_turkey_totals_flags_and_small_mass():
    rows=[dict(period=f'2026-{i+2:02}-01',source='national',type='fact',edizm_iso='796',stoim=v,netto=w,kol=n,quality_flags=flag) for i,(v,w,n,flag) in enumerate([(549,37,37,'flag'),(261,19,18,'flag'),(176993,8695,5,None),(70800,3478,2,None),(1544,62,49,'flag'),(73351,3481,5,'flag')])]
    out=m.trade_evidence({'data':rows})
    s=out['computed_summary'][0]
    assert s['stoim']=='323498'
    assert s['netto']=='15772' and s['kol']=='116'
    assert s['netto_display']=='15,772 т'
    assert out['row_display'][0]['mass_display']=='37 кг'
    assert s['mean_kg_per_unit'] is None
    assert '2026-07' in s['flagged_aggregate_months']

def test_unknowns_partial_and_sources():
    row=dict(type='fact',source='national',edizm_iso='796',stoim=5,netto=None,kol=1)
    out=m.trade_evidence({'data':[row,dict(row,type='pred',source='nowcast',stoim=9)]})
    assert len(out['computed_summary'])==2
    assert out['computed_summary'][0]['netto'] is None
    assert out['computed_summary'][0]['quality_status']=='not_independently_verified'
    assert m.trade_evidence({'meta':{'has_more':True},'data':[row]})=={}

def test_both_services_same_implementation():
    root=Path(__file__).parents[2]
    assert (root/'demo/app/trade_evidence.py').read_bytes()==(root/'mcp-server/trade_evidence.py').read_bytes()
