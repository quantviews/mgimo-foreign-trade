from app.readable_series import compact_series
from trade_tools_shared.numeric_facts import verify_answer
from app.answer_style import fallback_draft


def test_series_join_preserves_numbers_and_puts_limitations_last():
    scope='Китай, экспорт России, код 2709; окно 2025-01 - 2025-02'
    facts={
      'a':{'kind':'index_change','text':f'**{scope}.**\n\nИндекс вырос на **5,8%**.'},
      'b':{'kind':'monthly_table','text':scope+'\n| Месяц | Индекс (2025-01 = 100) | Г/г, % |\n|---|---|---|\n| 2025-01 | 100,0 | -1,0 |\n| 2025-02 | 105,8 | 2,0 |'},
      'c':{'kind':'unit_value','text':scope+'\n| Месяц | Удельная стоимость, долл./т |\n|---|---|\n| 2025-01 | 547,07 |'},
      'd':{'kind':'coverage','text':scope+': Отсутствуют месяцы: 2025-02.'}}
    compact=compact_series(facts,'Физобъем и цены помесячно')
    result=verify_answer(fallback_draft(compact,'Физобъем и цены помесячно'),compact)
    assert result['valid']
    text=result['answer']
    assert text.count('| Месяц |')==1
    assert '| 2025-02 | 105,8 | 2,0 | нет данных |' in text
    assert text.index('5,8%')<text.index('| Месяц |')<text.index('полный итог')
    no_months=compact_series(facts,'Динамика физобъема и цен')
    assert not any(x['kind']=='series_monthly' for x in no_months.values())
    assert '547,07' in str(no_months)


def test_distinct_sources_not_silently_merged():
    head='Индия, экспорт России, код 2709; окно 2025-01 - 2025-02'
    table=head+'\n| Месяц | Цена |\n|---|---|\n| 2025-01 | 10,0 |'
    facts={'a':{'kind':'index_change','text':head+'\n\nИндекс снизился на 1,0%.'},
           'b':{'kind':'unit_value','text':table},'c':{'kind':'unit_value','text':table}}
    out=compact_series(facts,'Физобъем и цены помесячно')
    assert 'не объединены' in str(out)
    assert not any(x['kind']=='series_monthly' for x in out.values())


def test_editor_cannot_shorten_requested_months_or_mix_money_units():
    from app.analytical_writer import complete_series_blocks,consistent_money_headers
    facts={'a':{'kind':'series_summary','text':'Индекс вырос'},
           'b':{'kind':'series_monthly','text':'| 2025-01 | 100 |\n| 2025-02 | 101 |'},
           'c':{'kind':'series_notes','text':'Существенная оговорка'}}
    blocks=complete_series_blocks([{'text':'Краткий вывод','evidence':['a']},
                                  {'text':'| 2025-02 | 101 |','evidence':['b']}],facts)
    assert '2025-01' in blocks[1]['text'] and blocks[-1]['text']=='Существенная оговорка'
    assert not consistent_money_headers('| Стоимость, тыс. долл. |\n|---|\n| 367,15 долл. |')
    assert consistent_money_headers('| Стоимость |\n|---|\n| 367,15 долл. |')


def test_general_reserve_keeps_mass_and_distinguishes_duplicate_series():
    from app.analytical_writer import compact_fallback
    scope='Узбекистан, экспорт России, код 280410, статистика источника (UN Comtrade); окно 2025-09 - 2026-08.'
    facts={'a':{'kind':'total','text':scope+' Стоимость в доступных записях (неполный итог): 9,92 тыс. долл.'},
           'b':{'kind':'total','text':scope+' Масса в доступных записях (неполный итог): 273,51 кг.'},
           'c':{'kind':'total','text':scope+' Стоимость в доступных записях (неполный итог): 2,00 долл.'}}
    text=compact_fallback(facts,'Экспорт водорода')
    assert text.count('| Узбекистан')==1
    assert '273,51 кг' in text and '9,92 тыс. долл; 2,00 долл' in text
    assert 'отдельным рядам' in text and text.count('код 280410')==1
