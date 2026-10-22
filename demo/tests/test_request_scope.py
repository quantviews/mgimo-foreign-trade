from app.request_scope import bind_scope, enforce_scope, confirm_search
from app.analytical_writer import compact_fallback


def test_code_level_and_period_cannot_drift():
    scope=bind_scope([{'role':'user','content':'Автомобили (8703) за последние 12 доступных месяцев'}],'2026-08-01')
    args=enforce_scope('trade',{'tnved6':['870321','870322'],'period_from':'2019-01-01'},scope)
    assert args['tnved4']==['8703'] and 'tnved6' not in args
    assert args['period_from']=='2025-09-01' and args['period_to']=='2026-08-01'
    assert enforce_scope('export_trade',{'tnved6':['8703']},{})['tnved4']==['8703']


def test_natural_name_exact_search_and_calendar_year():
    scope=bind_scope([{'role':'user','content':'водород в 2025 году в натуральных единицах'}],'2026-08-01')
    confirm_search(scope,'водород',[{'code':'2804','name':'Водород и прочие газы','level':4,'matched_terms':1,'query_terms':1},
                                  {'code':'280410','name':'ВОДОРОД','level':6,'matched_terms':1,'query_terms':1}])
    assert scope['codes']==['280410']
    assert scope['window']==('2025-01-01','2025-12-01')


def test_followup_keeps_window_but_new_question_does_not():
    history=[{'role':'user','content':'автомобили (8703) за последние 12 месяцев'},
             {'role':'user','content':'Давай отдельно по месяцам и динамику'}]
    scope=bind_scope(history,'2026-08-01')
    assert scope['codes']==['8703'] and scope['window'][0]=='2025-09-01'
    history[-1]['content']='экспорт водорода в 2025 году'
    assert bind_scope(history,'2026-08-01')['codes']==[]


def test_physical_reserve_is_compact_and_keeps_units():
    facts={'a':{'kind':'total','text':'Грузия, экспорт России; окно 2025-01 - 2025-12. Масса в доступных записях (неполный итог): 0,72 кг.'},
           'b':{'kind':'total','text':'Грузия, экспорт России; окно 2025-01 - 2025-12. Количество в доступных записях (неполный итог): 8,70 КУБИЧЕСКИЙ МЕТР.'}}
    result=compact_fallback(facts,'в натуральных единицах')
    assert '0,72 кг' in result and '8,70 м³' in result
    assert 'Полный' not in result and 'полный итог периода не установлен' in result
    assert result.count('Грузия')==1


def test_requested_index_is_not_satisfied_by_cost_or_mass():
    from app.answer_style import missing_requested_evidence
    facts={'a':{'kind':'monthly_table','text':'Стоимость и масса'}}
    assert missing_requested_evidence(facts,'Динамика физобъема и цен нефти')
    assert missing_requested_evidence(facts,'Динамика стоимости нефти') is None
    facts['b']={'kind':'index_change','text':'Проверенный индекс'}
    assert missing_requested_evidence(facts,'Динамика физобъема и цен нефти') is None
