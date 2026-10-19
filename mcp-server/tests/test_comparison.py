import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from comparison import compare

def test_matched_months_and_baseline():
    rows=[{'strana':c,'period':p,'stoim':v,'netto':v*2,'source':'national'} for c,p,v in [('IN','2026-07',100),('CN','2026-07',200),('CN','2026-08',999)]]
    def get(path,params):
        if path.endswith('meta'): return {'data':{'period_max':'2026-08-01'}}
        assert ('type','fact') in params
        return {'data':rows,'meta':{'has_more':False}}
    r=compare(get,['IN','CN'],'ex',2)
    assert r['common_fact_months']==['2026-07']
    assert r['excluded_months']==['2026-08']
    assert r['differences'][0]['stoim']['percent']=='100'
    rows[0]['stoim']=0
    assert compare(get,['IN','CN'],'ex',2)['differences'][0]['stoim']['percent'] is None
    rows[0]['netto']=None
    assert compare(get,['IN','CN'],'ex',2)['comparable'] is False

def test_truncation_rejected():
    def get(path,params):
        return {'data':{'period_max':'2026-08'}} if path.endswith('meta') else {'meta':{'has_more':True}}
    with pytest.raises(ValueError,match='Incomplete'):
        compare(get,['IN','CN'],'ex')


from comparison import compare_fizob

def fizob_response(**changes):
    rows=[]
    for c, values in [('IN',[2,3]),('CN',[100,110])]:
        for p,x in zip(['2026-06','2026-07'],values):
            rows.append(dict(STRANA=c,NAPR='ЭК',PERIOD=p,tn_level=4,tn_code='2709',idx=x,fizob_bp=5))
    for r in rows:
        if r['STRANA']=='IN' and r['PERIOD']=='2026-07':r.update(changes)
    return {'data':rows,'meta':{'has_more':False}}

def test_fizob_relative_change_not_cross_country_levels():
    result=compare_fizob(lambda *_:fizob_response(),['IN','CN'],'ЭК',{'tnved4':['2709']},['2026-06','2026-07'],{'IN':'India','CN':'China'})
    assert result['available']
    assert result['series'][0]['change_percent']=='50.0'
    assert result['series'][1]['change_percent']=='10.0'
    assert result['differences'][0]['growth_difference_pp']=='-40.0'

@pytest.mark.parametrize('changes',[{'idx':None},{'idx':float('nan')},{'idx':-1},{'fizob_bp':6}])
def test_bad_fizob_is_not_comparable(changes):
    result=compare_fizob(lambda *_:fizob_response(**changes),['IN','CN'],'ЭК',{'tnved4':['2709']},['2026-06','2026-07'],{'IN':'India','CN':'China'})
    assert not result['available']

def test_basket_and_full_code_not_silently_aggregated():
    def no_call(*_):raise AssertionError('API should not be called')
    for filters in [{'tnved4':['2709','2710']},{'tnved':['2709001000']}]:
        assert not compare_fizob(no_call,['IN','CN'],'ЭК',filters,['2026-06','2026-07'],{})['available']

def test_fizob_missing_month_and_truncation():
    for truncated in [True,False]:
        obj=fizob_response();obj['meta']['has_more']=truncated
        if not truncated:obj['data'].pop()
        assert not compare_fizob(lambda *_:obj,['IN','CN'],'ЭК',{'tnved4':['2709']},['2026-06','2026-07'],{'IN':'India','CN':'China'})['available']


def test_fizob_scale_invariance_and_zero_start():
    original=fizob_response()
    scaled=fizob_response()
    for r in scaled['data']:r['idx'] *= 1e-10
    args=(['IN','CN'],'ЭК',{'tnved4':['2709']},['2026-06','2026-07'],{'IN':'India','CN':'China'})
    a=compare_fizob(lambda *_:original,*args)
    b=compare_fizob(lambda *_:scaled,*args)
    from decimal import Decimal
    assert abs(Decimal(a['series'][0]['change_percent'])-Decimal(b['series'][0]['change_percent'])) < Decimal('1e-10')
    original['data'][0]['idx']=0
    assert not compare_fizob(lambda *_:original,*args)['available']
