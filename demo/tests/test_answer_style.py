import asyncio
from app.answer_style import export_only, export_facts, explanation_facts
from app.numeric_facts import numerical_facts, verify_answer
from app.exports import export_context
from app.deepseek import _checked_answer


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
