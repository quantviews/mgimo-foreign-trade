import asyncio
from app.answer_style import export_only, export_facts, explanation_facts
from trade_tools_shared.numeric_facts import numerical_facts, verify_answer
from app.exports import export_context
from app.deepseek import _checked_answer


def test_intent_is_not_country_or_product_specific():
    from app.answer_style import request_mode
    assert request_mode('Как менялся индекс физического объема по Бразилии?')=='index'
    assert request_mode('Сравни поставки ткани в две страны')=='compare'
    assert request_mode('Объясни вклад подкатегорий зерна')=='explain'
    assert request_mode('Покажи по месяцам и год к году')=='dynamics'
    assert request_mode('Выгрузи в Excel данные по меди')=='export'


def test_compact_fallback_preserves_missing_and_does_not_dump_extrema():
    from app.answer_style import fallback_draft
    f=numerical_facts([dict(period='2026-01',strana='TR',napr='im',
        type='fact',source='national',stoim=1000000,netto=None)],
        {'period_from':'2026-01','period_to':'2026-02'})
    result=verify_answer(fallback_draft(f,'Дай обзор импорта'),f)
    assert result['valid']
    assert '1,00 млн долл.' in result['answer']
    assert 'Полный итог окна не установлен' in result['answer']
    assert 'максимум' not in result['answer']


def test_truncated_generation_uses_validated_fallback():
    from unittest.mock import AsyncMock,patch
    import app.deepseek as agent
    f=numerical_facts([dict(period='2026-01',idx='0.0000000002'),
                      dict(period='2026-02',idx='0.00000000022')])
    key=next(k for k,v in f.items() if v['kind']=='index_change')
    message={'content':'{{fact:'+key+'}}\nНезаконченное предложение','_finish_reason':'length'}
    async def run():
        token=export_context.set({'question':'Как менялся физобъем?', 'numeric_facts':f})
        try:
            with patch.object(agent,'_completion',AsyncMock(return_value=message)):
                result=await agent._checked_answer(None,[],message)
            assert result['type']=='answer'
            assert 'Незаконченное' not in result['text']
            assert '110,0' in result['text']
        finally:export_context.reset(token)
    asyncio.run(run())


def test_repair_selects_facts_without_replaying_invalid_prose():
    from unittest.mock import AsyncMock, patch
    import app.deepseek as agent
    f=numerical_facts([dict(period='2026-01',idx=1),dict(period='2026-02',idx=2)])
    key=next(k for k,v in f.items() if v['kind']=='index_change')
    async def run():
        token=export_context.set({'question':'Как менялся физобъем?', 'numeric_facts':f})
        try:
            completion=AsyncMock(return_value={'content':'{{fact:'+key+'}}'})
            with patch.object(agent,'_completion',completion):
                result=await agent._checked_answer(None,[{'role':'tool','content':'raw transcript'}],
                    {'content':'За последние 12 месяцев рост составил 999%'})
            assert '999' not in result['text'] and result['type']=='answer'
            repair=completion.call_args.args[1]
            assert len(repair)==2
            assert 'raw transcript' not in str(repair) and '999%' not in str(repair)
            assert 'ТОЛЬКО ссылки' in repair[0]['content']
        finally:export_context.reset(token)
    asyncio.run(run())


def test_model_connection_timeout_and_retry_preserve_fact_contract():
    import httpx
    from unittest.mock import AsyncMock, patch
    import app.deepseek as agent
    async def run():
        token=export_context.set({'numeric_facts':{'test':{'kind':'total','text':'test'}}})
        try:
            request=httpx.Request('POST','https://example.test')
            response=httpx.Response(200,request=request,json={'choices':[
                {'message':{'content':'{{fact:test}}'},'finish_reason':'stop'}]})
            client=AsyncMock()
            client.post.side_effect=[httpx.ConnectTimeout('test',request=request),response]
            with patch.object(agent.asyncio,'sleep',AsyncMock()):
                result=await agent._completion(client,[{'role':'user','content':'test'}],False)
            assert result['content']=='{{fact:test}}' and client.post.call_count==2
            for call in client.post.call_args_list:
                assert call.kwargs['timeout'].connect==10
                assert call.kwargs['timeout'].read==120
                assert 'ТОЛЬКО ссылки' in call.kwargs['json']['messages'][-1]['content']
        finally:export_context.reset(token)
    asyncio.run(run())


def test_export_intent_does_not_confuse_months_with_analysis():
    assert export_only('Выгрузи в Excel автомобили по месяцам за последние 12 месяцев')
    for text in ['Выгрузи Excel и объясни динамику', 'Сравни Индию и Китай с Excel',
                 'Дай разбор автомобилей', 'Покажи доли и вклады с Excel']:
        assert not export_only(text)


def test_export_brief_is_verified_preserves_fractional_estimates_and_missing():
    rows = [dict(strana='CN', napr='im', tnved4='8703', type='fact', source='national',
                 period='2026-01', stoim=1000000, netto=None, kol=20, edizm='ШТУКА'),
            dict(strana='CN', napr='im', tnved4='8703', type='fact', source='national',
                 period='2026-02', stoim=2000000, netto=1500000, kol=0.115, edizm='ШТУКА')]
    facts = numerical_facts(rows, {'period_from':'2026-01', 'period_to':'2026-03'})
    brief = export_facts(facts)
    result = verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in brief), brief)
    assert result['valid']
    text = result['answer']
    assert '20,12 шт.' in text  # display rounding, never a false zero
    assert 'известная часть' in text and '2026-03' in text
    assert 'Значение неизвестно в 1' in text
    assert 'максимум' not in text and '| Месяц' not in text
    assert 'динамика итогов год к году' not in text
    assert 'шт..' not in text
    whole = numerical_facts([dict(rows[0], kol=20, netto=1000)], {})
    assert '20 шт.' in next(iter(export_facts(whole).values()))['text']


def test_checked_export_ignores_invented_model_numbers():
    facts = numerical_facts([dict(strana='CN', napr='im', period='2026-01',
        type='fact', source='national', stoim=1000000, netto=1000, kol=20, edizm='ШТУКА')], {})
    token = export_context.set({'export_only': True, 'download_link':'[Excel](/demo/exports/test)',
                                'numeric_facts': facts})
    try:
        result = asyncio.run(_checked_answer(None, [], {'content':'Всего 999999 автомобилей'}))
        assert result['type']=='answer' and '999999' not in result['text']
        assert '20 шт.' in result['text'] and '/demo/exports/test' in result['text']
        assert 'максимум' not in result['text']
    finally:
        export_context.reset(token)


def test_analysis_contains_driver_codes_shares_contributions_and_same_periods():
    result = explanation_facts(dict(available=True, country='CN', parent_code='8703',
        current_period=['2025-09','2026-08'], previous_period=['2024-09','2025-08'],
        parent=dict(current_usd=1200000, previous_usd=1000000, growth_pct=20),
        children=[dict(comparable=True, code='870340', name='Гибридные автомобили',
            change_usd=200000, current_share_pct=50, contribution_pp=20)],
        residual_change_usd=0, unmatched_codes=[]))
    checked = verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in result), result)
    assert checked['valid']
    for expected in ['2025-09 - 2026-08','2024-09 - 2025-08','870340','50,0','20,0']:
        assert expected in checked['answer']
    unavailable = explanation_facts(dict(available=False, current_period=['2026-01','2026-08'],
                                         missing_months=['2026-08']))
    assert all(v['kind']=='analysis_caveat' for v in unavailable.values())

def test_small_flow_units_and_float_tails():
    from trade_tools_shared.numeric_facts import numerical_facts, fmt
    facts=numerical_facts([dict(strana='KZ',napr='ex',tnved6='280410',source='comtrade',type='fact',period='2026-01',stoim=14936.47,netto=85.24000000000001)],{'period_from':'2026-01','period_to':'2026-02'})
    text='\n'.join(v['text'] for v in facts.values())
    assert '14,94 тыс. долл.' in text and '85,24 кг' in text
    assert 'Казахстан' in text and 'Полный итог окна не установлен' in text
    assert '000000000001' not in text
    assert fmt(0.00008524000000000001)=='0,00008524'


def test_short_names_keep_distinguishing_characteristics():
    from trade_tools_shared.numeric_facts import short_product_name
    assert '1,5 до 3' in short_product_name('870323','official')
    assert 'свыше 1 до 1,5' in short_product_name('870322','official')
    assert 'Подзаряжаемые' in short_product_name('870360','official')
    assert 'без внешней' in short_product_name('870340','official')
    assert short_product_name('999999','НЕИЗВЕСТНАЯ ГРУППА')=='Неизвестная группа'
