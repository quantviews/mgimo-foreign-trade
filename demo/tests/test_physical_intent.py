import pytest
from app.answer_style import physical_request
from app.analytical_writer import editorial_evidence, validate_blocks
from app.tools import physical_evidence_args


@pytest.mark.parametrize('question', [
    'экспорт водорода из рф в 2025 году - в натуральных единицах',
    'объём поставок в тоннах', 'водород в кг', 'автомобили в шт',
    'количество в литрах', 'масса',
])
def test_physical_questions_keep_metrics_and_unit_coverage(question):
    assert physical_request(question)
    original = {'tnved6': ['280410'], 'napr': ['ex'], 'group_by': 'strana', 'metrics': 'stoim'}
    args = physical_evidence_args(original, question)
    assert set(args['metrics'].split(',')) == {'stoim', 'netto', 'kol'}
    assert {'period','strana','edizm','edizm_iso','type','source'} <= set(args['group_by'].split(','))
    assert args['napr'] == original['napr'] and args['tnved6'] == original['tnved6']
    assert original['metrics'] == 'stoim'


def test_evidence_never_loses_physical_fields_for_unrecognized_wording():
    facts = {'mass': {'kind': 'total', 'text': 'Казахстан. Масса: 38,07 т.'},
             'aggregate': {'kind': 'aggregate', 'text': 'Казахстан: стоимость 10 долл., масса 145 кг. Покрытие не установлено.'}}
    assert editorial_evidence(facts, 'Расскажи о поставках') == facts


def test_money_only_answer_cannot_satisfy_natural_units_request():
    facts = {'m': {'kind': 'total', 'text': 'Казахстан. Масса: 38,07 т.'},
             'v': {'kind': 'total', 'text': 'Казахстан. Стоимость: 10 долл.'}}
    q = 'водород в натуральных единицах'
    assert not validate_blocks([{'text':'Стоимость: 10 долл.', 'evidence':['v']}],facts,q)
    assert validate_blocks([{'text':'Масса: 38,07 т.', 'evidence':['m']}],facts,q)


def test_money_query_is_not_expanded():
    args = {'group_by':'tnved2', 'metrics':'stoim'}
    assert physical_evidence_args(args, 'Структура импорта по стоимости') == args


def test_mass_table_cannot_label_tonnes_as_kilograms():
    facts = {'a': {'kind':'total', 'text':'Казахстан. Масса: 61,76 т.'}}
    bad = '| Страна | Масса, кг |\n|---|---|\n| Казахстан | 61,76 т |'
    assert not validate_blocks([{'text':bad, 'evidence':['a']}], facts, 'в натуральных единицах')
    assert validate_blocks([{'text':bad.replace('Масса, кг','Масса'), 'evidence':['a']}], facts, 'в натуральных единицах')


def test_georgia_is_not_germany():
    from trade_tools_shared.numeric_facts import COUNTRIES
    assert COUNTRIES['GE'] == 'Грузия'
    assert COUNTRIES['DE'] == 'Германия'


@pytest.mark.parametrize('explicit',[False,True])
def test_key_country_indices_use_complete_ranking_and_preserve_explicit_country(explicit):
    import asyncio
    asyncio.run(check_key_countries(explicit))


async def check_key_countries(explicit):
    import httpx
    from app.tools import execute_tool
    from app.exports import export_context
    requests=[]
    def respond(request):
        requests.append(request)
        if request.url.path.endswith('/trade'):
            assert request.url.params['tnved4']=='2709'
            return httpx.Response(200,json={'meta':{'has_more':False},'data':[{'strana':c} for c in ['CN','IN','HU','TR']]})
        return httpx.Response(200,json={'meta':{'has_more':False},'data':[]})
    token=export_context.set({'question':'Физобъем по ключевым странам'})
    try:
        args={'tn_level':['4'],'tn_code':['2709'],'napr':['ex'],'period_from':'2025-01-01'}
        if explicit:args['strana']=['TR']
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            await execute_tool(client,'fizob',args)
    finally:export_context.reset(token)
    assert len(requests)==(2 if explicit else 3)
    assert requests[-1].url.params.get_list('strana')==(['TR'] if explicit else ['CN','IN','HU'])


def test_reference_names_survive_tool_envelope_and_numeric_presentation():
    import asyncio,json,httpx
    from app.tools import execute_tool
    from app.answer_style import presentation_facts
    async def check():
        def respond(request):
            assert request.url.params['strana']=='IN'
            return httpx.Response(200,json=[{'code':'98','name':'Проектный импорт'}])
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            obj=json.loads(await execute_tool(client,'reference',{'name':'tnved','level':2,'strana':'IN','code':'98'}))
            facts=presentation_facts(obj['numeric_facts'],'Что означает группа 98?')
            assert 'Проектный импорт' in str(facts)
            obj=json.loads(await execute_tool(client,'reference',{'name':'tnved','level':2,'strana':'IN'}))
            assert obj['data'][0]['code']=='98'
    asyncio.run(check())
