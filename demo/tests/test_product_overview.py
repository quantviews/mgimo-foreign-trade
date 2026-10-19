from app.product_overview import overview, money
from app.numeric_facts import verify_answer
from decimal import Decimal


def row(c,m,v,source='national'):
    return dict(strana=c,period=m,stoim=v,source=source,type='fact')


def test_matched_country_source_months_and_zero():
    rows=[row('CN','2025-01',100),row('CN','2025-02',0),row('CN','2026-01',120),row('CN','2026-02',10),row('IN','2026-01',90)]
    result=overview(rows,'6107','Одежда',{'CN':'Китай','IN':'Индия'},'2026-01','2026-02','im')
    assert result['comparable_countries']==['CN']
    assert result['total_usd']=='220'
    text='\n'.join(f['text'] for f in result['numeric_facts'].values())
    assert '30,0% г/г' in text and 'Индия' in text and 'Китай' in text
    checked=verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in result['numeric_facts']),result['numeric_facts'])
    assert checked['valid']


def test_changed_source_null_and_nowcast_not_comparable():
    rows=[row('CN','2025-01',100,'comtrade'),row('CN','2026-01',120),row('IN','2025-01',100),row('IN','2026-01',None),dict(row('CN','2026-01',999),type='pred',source='nowcast')]
    result=overview(rows,'6107','Одежда',{},'2026-01','2026-01','im')
    assert result['comparable_countries']==[]
    assert result['total_usd']=='120'


def test_money_scales_without_float_tail():
    assert money(Decimal('347127.40699999995'))=='347,13 тыс. долл.'
    assert money(Decimal('60000000'))=='60,00 млн долл.'


def test_partial_country_uses_exact_month_pairs_and_not_full_comparison():
    rows=[row('CN',p,v) for p,v in [('2025-01',100),('2025-02',100),('2025-03',100),('2026-01',110),('2026-02',110),('2026-03',110)]]
    rows += [row('UZ',p,v,'comtrade') for p,v in [('2025-01',10),('2025-02',999),('2025-03',0),('2026-01',20),('2026-03',10)]]
    r=overview(rows,'6107','Одежда',{'UZ':'Узбекистан'},'2026-01','2026-03','im')
    assert r['comparable_countries']==['CN']
    p=r['partial_country_comparisons'][0]
    assert p['current_months']==['2026-01','2026-03']
    assert p['baseline_months']==['2025-01','2025-03']
    assert p['current_value']=='30' and p['baseline_value']=='10'
    assert Decimal(p['change_pct'])==200
    text=next(f['text'] for f in r['numeric_facts'].values() if f['kind']=='overview_partial')
    assert 'янв. 2026; март 2026' in text
    checked=verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in r['numeric_facts']),r['numeric_facts'])
    assert checked['valid']


def test_partial_zero_baseline_and_source_change():
    rows=[row('IN','2025-01',0),row('IN','2026-01',15),row('TR','2025-01',10),row('TR','2026-01',20,'comtrade')]
    r=overview(rows,'6107','Одежда',{},'2026-01','2026-02','im')
    assert len(r['partial_country_comparisons'])==1
    assert r['partial_country_comparisons'][0]['strana']=='IN'
    assert r['partial_country_comparisons'][0]['change_pct'] is None
