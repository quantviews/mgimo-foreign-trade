from trade_tools_shared.numeric_facts import numerical_facts,verify_answer

def row(p,v,kg=100,type='fact',**kw):
    return dict(strana='CN',napr='ex',type=type,source='national' if type=='fact' else 'nowcast',period=p,stoim=v,netto=kg,**kw)

def test_exact_sums_extrema_and_ties():
    f=numerical_facts([row('2025-01-01','1000000000'),row('2025-02-01','2000000000'),row('2025-03-01','2000000000')])
    texts='\n'.join(v['text'] for v in f.values())
    assert '5,00 млрд долл.' in texts
    assert 'максимум 2,00 млрд долл. (2025-02, 2025-03)' in texts

def test_no_full_year_vs_ytd():
    rows=[row(f'{y}-{m:02}-01',1000000000 if y==2025 else 2000000000) for y,ms in ((2025,range(1,13)),(2026,range(1,9))) for m in ms]
    facts=numerical_facts(rows)
    yoy=[v['text'] for v in facts.values() if v['kind']=='yoy' and 'Стоимость' in v['text']][0]
    assert '16,00 млрд долл. против 8,00 млрд долл.' in yoy
    assert '(100,00% г/г)' in yoy

def test_missing_zero_and_prediction_separate():
    rows=[row('2025-01-01',0,None),row('2026-01-01',100),row('2026-02-01',99,type='pred')]
    facts=numerical_facts(rows,{'period_from':'2026-01-01','period_to':'2026-02-01'})
    assert any('Полный итог окна не установлен' in v['text'] for v in facts.values())
    assert not any(v['kind']=='yoy' for v in facts.values())
    assert any('nowcast' in v['text'] for v in facts.values())

def test_verifier_rejects_numbers_and_inverted_extremum():
    facts=numerical_facts([row('2026-01-01',10)])
    k=next(iter(facts));assert verify_answer('{{fact:'+k+'}}',facts)['valid']
    assert not verify_answer('Итого 999 млрд',facts)['valid']
    assert not verify_answer('Снизилось. {{fact:'+k+'}}',facts)['valid']
    assert not verify_answer('{{fact:0000000000000000}}',facts)['valid']

def test_monthly_growth_requires_calendar_baseline():
    f=numerical_facts([row('2026-03-01',2000000000)],baseline=[row('2025-03-01',1000000000),row('2026-01-01',500000000)])
    t=next(v['text'] for v in f.values() if v['kind']=='monthly_changes')
    assert '| 2026-03 | нет данных | 100,00 |' in t

def test_index_not_summed_display_rebased():
    f=numerical_facts([{'strana':'CN','period':'2025-01-01','idx':1.41},{'strana':'CN','period':'2026-08-01','idx':1.49}])
    assert not any(v['kind']=='total' for v in f.values())
    assert any('5,7%' in v['text'] for v in f.values())

def test_tiny_index_rebased_with_calendar_yoy_and_missing():
    rows=[dict(strana='CN',napr='ex',tn_code='0',period='2026-01',idx='0.0000000002'),
          dict(strana='CN',napr='ex',tn_code='0',period='2026-02',idx='0.00000000022')]
    baseline=[dict(rows[0],period='2025-01',idx='0.0000000001')]
    f=numerical_facts(rows,baseline=baseline)
    text='\n'.join(v['text'] for v in f.values())
    assert '0,000000000' not in text and 'тип данных не указан' not in text and 'код 0' not in text
    assert '| 2026-01 | 100,0 | 100,0 |' in text
    assert '| 2026-02 | 110,0 | нет данных |' in text
    assert '10,0%' in text
    assert any(v['kind']=='index_caveat' for v in f.values())
    assert verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in f),f)['valid']

def test_index_missing_first_month_does_not_claim_full_window():
    rows=[dict(strana='IN',period='2026-02',idx=1),dict(strana='IN',period='2026-03',idx=2)]
    f=numerical_facts(rows,{'period_from':'2026-01','period_to':'2026-03'})
    assert any('2026-02 = 100' in v['text'] for v in f.values())
    assert any('есть пропуски' in v['text'] for v in f.values())

def test_index_zero_base_is_not_rebased():
    f=numerical_facts([dict(period='2026-01',idx=0),dict(period='2026-02',idx=2)])
    assert not any(v['kind']=='index_change' for v in f.values())
    assert any('неположительное' in v['text'] for v in f.values())

def test_duplicate_months_not_summed():
    f=numerical_facts([row('2025-01-01',10),row('2025-01-01',20)])
    assert not any(v['kind']=='total' for v in f.values())

def test_price_not_blocked_by_quantity_unit_flag():
    f=numerical_facts([row('2026-01-01',1000,2000,quality_flags='unit_unknown'),row('2026-02-01',2000,1000,quality_flags='netto_suspicious')])
    t=next(v['text'] for v in f.values() if v['kind']=='unit_value')
    assert '| 2026-01 | 500,00 |' in t
    assert '| 2026-02 | нет данных |' in t

def test_chat_repair_and_fallback_never_publish_wrong_total():
    import asyncio
    from unittest.mock import patch,AsyncMock
    from demo.app import deepseek
    from demo.app.exports import export_context
    facts=numerical_facts([row('2026-01-01',1000000000)])
    async def run():
        tok=export_context.set({'numeric_facts':facts})
        try:
            with patch.object(deepseek,'_completion',AsyncMock(return_value={'content':'Итого 999 млрд'})):
                out=await deepseek._checked_answer(None,[],{'content':'Итого 999 млрд'})
            assert '999' not in out['text']
            assert '1,00 млрд долл.' in out['text']
            assert out['type']=='answer'
        finally:export_context.reset(tok)
    asyncio.run(run())

def test_annual_aggregate_has_no_invented_months():
    f=numerical_facts([{'strana':'CN','year':2025,'stoim':1000000000}],{'period_from':'2025-01-01','period_to':'2026-08-01'})
    assert any(v['kind']=='aggregate' for v in f.values())
    assert not any(v['kind']=='yoy' for v in f.values())

def test_small_nonzero_is_never_rendered_zero():
    from trade_tools_shared.numeric_facts import fmt
    assert fmt('0.000115')=='0,000115'
    assert fmt('-0.000115')=='-0,000115'
    assert fmt(0)=='0,00'
