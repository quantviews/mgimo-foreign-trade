from app.exports import export_file_facts
from app.numeric_facts import verify_answer


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
