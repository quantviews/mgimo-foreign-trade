"""Content contracts, in addition to arithmetic parity. No model/network in CI."""
import json,re
from pathlib import Path
from app.answer_style import explanation_facts
from app.numeric_facts import verify_answer, short_product_name
from app.product_overview import overview

ROOT=Path(__file__).resolve().parents[2]

def test_live_catalog_covers_every_site_example():
    cases=json.loads((ROOT/'demo/evals/analytical_cases.json').read_text(encoding='utf-8-sig'))
    questions={c['question'] for c in cases}
    page=(ROOT/'site/demo.qmd').read_text(encoding='utf-8')
    examples=re.findall(r'data-question="([^"]+)"',page)
    assert examples and set(examples)<=questions
    assert len({c['id'] for c in cases})==len(cases)
    assert all(c['review'] for c in cases)


def test_analysis_leads_with_change_and_distinguishes_categories():
    obj=dict(available=True,country='CN',direction='im',parent_code='8703',
      current_period=['2025-09','2026-08'],previous_period=['2024-09','2025-08'],
      parent=dict(current_usd=1500000,previous_usd=1000000,growth_pct=50),
      children=[dict(code=c,name='Официальное название',comparable=True,current_share_pct=share,
                     change_usd=delta,contribution_pp=contribution)
       for c,share,delta,contribution in [('870322',40,300000,30),('870360',20,200000,20)]],
      unmatched_codes=['870370'],residual_change_usd=0)
    facts=explanation_facts(obj)
    answer=verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in facts),facts)['answer']
    assert answer.index('выросла')<answer.index('Главные вклады')
    assert 'свыше 1 до 1,5 л (870322)' in answer
    assert 'Подзаряжаемые гибриды с бензиновым двигателем (870360)' in answer
    assert 'Подкатегории без сопоставимой пары: 870370' in answer
    assert 'санкци' not in answer and 'спрос' not in answer  # no invented causal mechanism
    assert '…' not in answer


def test_hydrogen_partial_is_available_not_absent_and_not_full_year():
    rows=[dict(strana='KZ',source='comtrade',type='fact',period=p,stoim=v)
          for p,v in [('2025-01',500),('2026-01',1000)]]
    result=overview(rows,'280410','ВОДОРОД',{'KZ':'Казахстан'},'2026-01','2026-02','ex')
    text='\n'.join(f['text'] for f in result['numeric_facts'].values())
    assert 'Экспорт России: Водород (код 280410)' in text
    assert '1,00 тыс. долл.' in text and 'Казахстан' in text
    assert 'Полный итог за выбранное окно не установлен' in text
    assert 'а не за все окно' in text and 'меньше тысячи долларов' in text
    assert 'общий темп выше' not in text and 'охватывает 0,0%' not in text
    assert 'млрд' not in text


def test_unknown_label_is_preserved_and_short_label_not_used_for_other_code():
    name='Товар с важным признаком в конце названия'
    assert short_product_name('999999',name)==name
    assert 'гибрид' not in short_product_name('999999',name)


def test_mean_mass_is_ratio_of_matched_totals_and_flags_block_only_physical_claim():
    from app.numeric_facts import numerical_facts
    from app.answer_style import presentation_facts
    rows=[dict(strana='TR',napr='im',type='fact',source='national',period='2026-01',stoim=50000,netto=4000,kol=2,edizm_iso='796')]
    f=numerical_facts(rows)
    mean=next(v['text'] for v in f.values() if v['kind']=='mean_mass')
    assert '2 000,00 кг/шт.' in mean
    rows[0]['quality_flags']='suspicious_net_weight'
    f=numerical_facts(rows)
    mean=next(v['text'] for v in f.values() if v['kind']=='mean_mass')
    assert 'не рассчитана' in mean and 'не вывод об ошибке стоимости' in mean
    assert '2 000' not in mean
    selected=presentation_facts(f,'Можно ли рассчитать среднюю массу автомобиля?')
    assert any(v['kind']=='mean_mass' for v in selected.values())
    assert not any(v['kind'] in ('unit_value','extrema','monthly_table') for v in selected.values())


def test_monthly_followup_uses_one_table_with_quantity_yoy_and_keeps_country_labels():
    from app.numeric_facts import numerical_facts,compact_text
    from app.answer_style import presentation_facts
    f=numerical_facts([dict(strana='CN',period='2026-01',type='fact',stoim=200,kol=4,edizm='ШТУКА')],
      baseline=[dict(strana='CN',period='2025-01',type='fact',stoim=100,kol=2,edizm='ШТУКА')])
    selected=presentation_facts(f,'Покажи по месяцам и динамику год к году')
    tables=[v for v in selected.values() if v['kind'].startswith('monthly')]
    assert len(tables)==1 and tables[0]['kind']=='monthly_dynamics'
    assert 'Количество' in tables[0]['text'] and '100,0' in tables[0]['text']
    assert 'Китай' in compact_text(tables[0]['text'],selected)
    unknown=numerical_facts([dict(period='2026-01',type='pred',stoim=2,kol=None,edizm=None,edizm_iso=None)])
    assert 'Количество, None' not in str(unknown)


def test_ranking_has_names_shares_and_verified_complete_denominator():
    from app.numeric_facts import ranking_facts,verify_answer
    obj={'data':[{'tnved2':'27','tnved2_name':'Топливо','stoim':80,'share_pct':80}],
         'ranking':{'complete_universe':True,'total_stoim':'100','other_groups_stoim':'20',
                    'requested_period':['2026-01','2026-02'],'fact_months':['2026-01'],'missing_months':['2026-02']}}
    f=ranking_facts(obj,{'strana':'IN','napr':'ex'})
    answer=verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in f),f)
    assert answer['valid']
    assert 'Топливо' in answer['answer'] and '80,0%' in answer['answer']
    assert '20,00' in answer['answer'] and 'Месяцы без записей: 2026-02' in answer['answer']


def test_country_filter_is_sent_to_overview_api():
    import asyncio,httpx
    from app.product_overview import fetch_overview
    calls=[]
    def handler(request):
        calls.append(request)
        if request.url.path.endswith('/meta'): data={'data':{'period_max':'2026-08-01'}}
        elif request.url.path.endswith('/tnved'):data=[{'code':'8703','name':'Автомобили'}]
        elif request.url.path.endswith('/countries'):data=[{'strana':'CN','country_name':'Китай'}]
        else:data={'data':[],'meta':{'has_more':False}}
        return httpx.Response(200,json=data)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            await fetch_overview(c,'https://test',{},'8703','im',strana=['CN'])
    asyncio.run(run())
    trade=next(r for r in calls if r.url.path.endswith('/trade'))
    assert trade.url.params.get_list('strana')==['CN']


def test_window_length_does_not_request_a_monthly_table():
    from app.answer_style import presentation_facts,missing_requested_evidence
    facts={'a':{'kind':'aggregate','text':'Country total'},
           'b':{'kind':'monthly_table','text':'Monthly rows'},
           'c':{'kind':'total','text':'Period total'}}
    selected=presentation_facts(facts,'Экспорт водорода за последние 12 месяцев')
    assert 'b' not in selected and 'c' in selected
    assert missing_requested_evidence({'a':facts['a']},'Динамика нефти помесячно')
    assert missing_requested_evidence(facts,'Динамика нефти помесячно') is None
