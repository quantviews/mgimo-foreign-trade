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

