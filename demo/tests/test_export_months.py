from app.exports import export_file_facts, export_monthly_facts
from trade_tools_shared.numeric_facts import verify_answer


def checked(rows,filters):
    facts=export_file_facts(rows,filters)
    result=verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in facts),facts)
    assert result['valid']
    return result['answer']


def test_export_counts_months_not_rows_or_latest_database_date():
    months=['2025-09','2025-10','2025-11','2025-12']+[f'2026-{m:02}' for m in range(1,9)]
    rows=[{'period':m+'-01'} for m in months for _ in range(2)]
    text=checked(rows,{'period_from':'2025-09-01','period_to':'2026-08-01'})
    assert 'месяцев с данными: 12' in text
    assert 'сен. 2025 - авг. 2026' in text
    assert 'нет строк' not in text


def test_gaps_and_missing_boundary_months_are_explicit():
    text=checked([{'period':'2026-02-01'},{'period':'2026-04-01'}],
                 {'period_from':'2026-01-01','period_to':'2026-05-01'})
    assert 'месяцев с данными: 2' in text
    assert 'Месяцы с данными: фев. 2026, апр. 2026' in text
    assert 'нет строк за: янв. 2026, мар. 2026, май 2026' in text


def test_monthly_rows_preserve_units_nulls_zeros_and_nowcast():
    rows=[dict(period='2026-07-01',strana='IN',type='fact',source='national',stoim=0,netto=None,kol=None,edizm='?'),
          dict(period='2026-07-01',strana='IN',type='fact',source='national',stoim=60000,netto=None,kol=2,edizm='ШТУКА'),
          dict(period='2026-08-01',strana='IN',type='pred',source='nowcast',stoim=53213.237,netto=53213.237,kol=None)]
    facts=export_monthly_facts(rows)
    result=verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in facts),facts)
    assert result['valid']
    text=result['answer']
    assert '| июл. 2026 | 0,00 | нет данных | не указана | нет данных |' in text
    assert '| июл. 2026 | 60,00 | 2 | шт. | нет данных |' in text
    assert 'Nowcast проекта' in text
    assert '| авг. 2026 | 53,21 | нет данных | не указана | 53,21 |' in text
    description=checked(rows,{'period_from':'2026-07-01','period_to':'2026-08-01'})
    assert 'Статистика источников: 1 мес.; nowcast проекта: 1 мес.' in description
    assert 'без строк статистики источников: авг. 2026' in description


def test_monthly_china_shows_twelve_months_and_fractional_quantities():
    rows=[dict(period=f'2025-{m:02}-01',strana='CN',type='fact',source='national',stoim=948764943,netto=102584911,kol=72610,edizm='ШТУКА') for m in range(1,13)]
    rows[0]['kol']=0.115
    text='\n'.join(v['text'] for v in export_monthly_facts(rows).values())
    assert sum(line.startswith('| ') and '2025' in line for line in text.splitlines())==12
    assert '| 948,76 | 0,115 | шт. | 102,58 |' in text
    assert 'Стоимость, млн долл.' in text
    assert 'Масса, тыс. т' in text
