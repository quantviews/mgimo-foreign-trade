"""Presentation of computed evidence, without asking the model to do arithmetic."""
import hashlib
import re
from decimal import Decimal

from trade_tools_shared.numeric_facts import fmt, number, COUNTRIES, short_product_name


def physical_request(question):
    """Used to request evidence, never to delete fields from evidence."""
    return bool(re.search(r'натуральн|масс|вес|количеств|штук|\bшт\b|\bкг\b|тонн|литр|кубометр|\bм[³3]\b|объ[её]м|физоб|сколько.*(?:автомоб|машин)', question, re.I))


def request_mode(question):
    """Intent controls presentation, never restricts the country/product domain."""
    if export_only(question):return 'export'
    if re.search(r'физоб|физическ.*объем|индекс',question,re.I):return 'index'
    if re.search(r'почему|объясн|драйвер|вклад|разбор|за счет',question,re.I):return 'explain'
    if re.search(r'сравн|сопостав',question,re.I):return 'compare'
    if re.search(r'динамик|менял|измен|по месяц|помесяч|год к году|г/г',question,re.I):return 'dynamics'
    return 'overview'

def presentation_facts(facts, question):
    """Exclude unrelated calculated material, without relaxing verification."""
    monthly=bool(re.search(r'по месяц|помесяч|кажд\w*\s+месяц',question,re.I))
    dynamics=bool(re.search(r'динамик|менял|измен|год к году|г/г',question,re.I))
    index=any(v['kind']=='index_change' for v in facts.values())
    mean=bool(re.search(r'средн\w*\s+масс|масс\w*\s+(?:одного|одной)',question,re.I))
    price=bool(re.search(r'цен|удельн\w*\s+стоим',question,re.I))
    allowed={'reference','total','aggregate','limitation','coverage','index_change','index_yoy','index_caveat','index_methodology'}
    if mean:allowed.add('mean_mass')
    if price:allowed.add('unit_value')
    if re.search(r'максим|миним|пик|экстрем',question,re.I):allowed.add('extrema')
    if dynamics and not monthly:allowed.add('yoy')
    if index:allowed.add('monthly_table')
    elif monthly and dynamics:allowed.add('monthly_dynamics')
    elif monthly:allowed.add('monthly_table')
    selected={k:v for k,v in facts.items() if v['kind'] in allowed}
    if index and request_mode(question)=='index':
        selected={k:v for k,v in selected.items()
                  if v['kind'] not in ('total','aggregate','limitation')
                  and (v['kind']!='monthly_table' or '| Индекс (' in v['text'])}
    if not re.search(r'прогноз|nowcast|наукаст|оценк',question,re.I):
        selected={k:v for k,v in selected.items() if 'оценка проекта (nowcast)' not in v['text']}
    # Unknown unit/nowcast-only tables are not a substitute for factual dynamics.
    if monthly and dynamics:
        selected={k:v for k,v in selected.items() if v['kind']!='total'}
    if mean:
        selected={k:v for k,v in selected.items() if v['kind'] not in ('limitation',)}
    # Keep coverage gaps; successful coverage is already apparent from the table.
    selected={k:v for k,v in selected.items() if v['kind']!='coverage' or 'Отсутствуют' in v['text']}
    if index:
        from .readable_series import compact_series
        return compact_series(selected, question)
    return selected


def missing_requested_evidence(facts, question):
    """A country ranking is a planning step, not a monthly dynamics answer."""
    kinds={v['kind'] for v in facts.values()}
    if kinds and re.search(r'физоб|физическ.*объ[её]м', question, re.I) and 'index_change' not in kinds and 'index_caveat' not in kinds:
        return ('Вопрос требует индекс физического объема, а получены только торговые показатели. '
                'Вызови fizob для запрошенного товара, направления, стран и периода. '
                'Сохрани полученные стоимость и удельную стоимость, если они нужны для ответа. '
                'Если индекс недоступен, объясни только этот пробел и покажи доступные показатели.')
    if re.search(r'по месяц|помесяч',question,re.I) and kinds and kinds <= {'aggregate'}:
        return ('Получены только итоги по странам, но вопрос требует помесячной динамики. '
                'Выбери три ведущие страны по полученной стоимости. Запроси trade отдельно '
                'по каждой: тот же товар, направление и период, group_by=period, '
                'metrics=stoim,netto. Если запрошен fizob, вызови fizob по этим же странам '
                'и периоду. Только затем дай проверенный ответ; не создавай Excel без просьбы.')
    return None


def fallback_draft(facts, question):
    """Bounded selection of verified facts; always keep material coverage notes."""
    mode=request_mode(question)
    if any(v['kind']=='series_summary' for v in facts.values()):
        order=('series_summary','series_monthly','series_notes')
    elif any(v['kind']=='index_change' for v in facts.values()):
        order=('index_change','index_yoy','monthly_table','unit_value','coverage','index_caveat','index_methodology')
    elif mode=='export':
        order=('total','limitation','coverage')
    elif mode=='explain':
        order=('analysis_headline','analysis_leader','analysis_drivers','analysis_caveat',
               'overview_dynamics','overview_drivers','total','yoy','limitation','coverage')
    elif mode=='dynamics':
        order=('monthly_dynamics','yoy','total','monthly_table','limitation','coverage')
    else:
        order=('overview','overview_dynamics','overview_drivers','suppliers',
               'overview_partial','overview_coverage','total','yoy','aggregate','limitation','coverage')
    chosen=[]
    for kind in order:
        matches=[(k,v) for k,v in facts.items() if v['kind']==kind]
        if kind=='coverage':
            matches=[(k,v) for k,v in matches if 'Отсутствуют месяцы' in v['text']]
        if kind in ('monthly_changes','monthly_table'):
            matches=matches[:2]
        chosen.extend(k for k,v in matches)
    if not chosen:chosen=list(facts)[:3]
    return '\n\n'.join('{{fact:'+key+'}}' for key in dict.fromkeys(chosen))


def export_only(question):
    return bool(re.search(r'excel|xlsx|выгруз|скача|файл', question, re.I)) and not re.search(
        r'сравн|динамик|объясн|разбор|проанализ|анализ|драйвер|вклад|причин|дол[яию]', question, re.I)


def fact(kind, text):
    return {hashlib.sha256(text.encode()).hexdigest()[:16]: {'kind': kind, 'text': text}}


def export_facts(facts):
    """Rephrase already calculated totals; retain source/status and every caveat."""
    groups = {}
    for item in facts.values():
        if item['kind'] != 'total':
            continue
        match = re.match(r'(.+?); окно (\d{4}-\d{2} - \d{4}-\d{2})\. (Стоимость|Масса|Количество) (.+)', item['text'])
        if not match:
            continue
        label, period, metric, rest = match.groups()
        rest = rest.replace('по имеющимся записям за окно: ', '')
        rest = rest.replace('в доступных записях (неполный итог): ', 'известная часть: ')
        # Only whole quantities lose decimal zeros; fractional source estimates stay.
        rest = re.sub(r'(\d),00 (?:ШТУКА|штука)\b', r'\1 шт.', rest)
        rest = re.sub(r'\bШТУКА\b', 'шт.', rest)
        rest = rest.replace('шт..', 'шт.')
        label = label.replace('статистика источника (национальная статистика)', 'национальная статистика')
        groups.setdefault((label, period), []).append(f'- **{metric}:** {rest}')
    out = {}
    for (label, period), lines in groups.items():
        out.update(fact('export_brief', f'**{label}. {period}**\n\n' + '\n'.join(lines)))
    for item in facts.values():
        if (item['kind'] == 'limitation' and 'динамика итогов год к году' not in item['text']) or (
            item['kind'] == 'coverage' and 'Отсутствуют месяцы:' in item['text']
        ):
            out.update(fact('export_caveat', item['text']))
    return out


def explanation_facts(obj):
    """Use service-computed parent totals, shares and contributions verbatim."""
    if obj.get('error'):
        return fact('analysis_caveat', 'Разбор подкатегорий не удалось получить. Драйверы изменения не установлены.')
    current = ' - '.join(str(x)[:7] for x in obj.get('current_period', []))
    previous = ' - '.join(str(x)[:7] for x in obj.get('previous_period', []))
    if not obj.get('available'):
        missing = ', '.join(str(x)[:7] for x in obj.get('missing_months', []))
        return fact('analysis_caveat', f'За {current} полный сопоставимый итог не установлен. '
                    f'Месяцы без данных в текущем или предыдущем окне: {missing}. Разбор вкладов не рассчитан.')
    def money(v):
        return fmt(number(v) / Decimal(10**6)) if number(v) is not None else 'нет данных'
    parent = obj['parent']
    partial = obj.get('partial_period', False)
    if partial:
        def months_label(values):
            labels=[str(m)[:7] for m in values]
            ids=[int(m[:4])*12+int(m[5:7]) for m in labels]
            return (labels[0]+' - '+labels[-1] if ids==list(range(ids[0],ids[-1]+1)) else ', '.join(labels))
        current = months_label(obj['matched_months'])
        previous = months_label(obj['matched_previous_months'])
    route = 'Импорт России' if obj.get('direction') in ('im', 'ИМ') else 'Экспорт России'
    growth=number(parent.get('growth_pct'))
    change=(f"Стоимость {'выросла' if growth>0 else 'снизилась'} на **{fmt(abs(growth),1)}%**"
            if growth is not None and growth!=0 else 'Стоимость не изменилась' if growth==0
            else 'Темп стоимости не рассчитан')
    out = fact('analysis_headline',
        f"**{route}, {COUNTRIES.get(obj['country'], obj['country'])}, код {obj['parent_code']}: {current}.**\n\n"
        f"{change} на сопоставимом периоде. "
        f"Сейчас: **{money(parent['current_usd'])} млн долл.**, "
        f"годом ранее ({previous}) - {money(parent['previous_usd'])} млн долл.")
    if partial:
        out.update(fact('analysis_caveat', 'Это отдельное сравнение только общих месяцев: '+current+
            '. Полный итог запрошенного окна не установлен. Доли и вклады ниже относятся только к этим месяцам; '
            'имеющиеся нулевые записи сохранены, пропуски нулями не заменены. Excel содержит исходные данные всего запрошенного окна и прошлого года.'))
    rows = [r for r in obj['children'] if r.get('comparable')]
    rows.sort(key=lambda r: abs(number(r['change_usd'])), reverse=True)
    if rows:
        main = rows[0]
        out.update(fact('analysis_leader',
            f"Главный вклад в изменение стоимости: {short_product_name(main['code'],main.get('name'))} ({main['code']}): "
            f"{money(main['change_usd'])} млн долл., {fmt(main['contribution_pp'], 1)} п.п. "
            "Детализация по кодам приведена ниже."))
    table = ['**Главные вклады подкатегорий в изменение стоимости**', '',
             '| Подкатегория | Доля сейчас, % | Изменение, млн долл. | Вклад, п.п. |',
             '|---|---:|---:|---:|']
    for row in rows[:3]:
        name = re.sub(r'[|\r\n]', ' ', short_product_name(row['code'], row.get('name')))
        name = ' '.join(name.split())
        if name.isupper():
            name = name.capitalize()
        table.append(f"| {name} ({row['code']}) | {fmt(row['current_share_pct'], 1)} | "
                     f"{money(row['change_usd'])} | {fmt(row['contribution_pp'], 1)} |")
    if rows:
        out.update(fact('analysis_drivers', '\n'.join(table)))
    if obj.get('unmatched_codes') or number(obj.get('residual_change_usd', 0)):
        out.update(fact('analysis_caveat',
            'Нераспределенное изменение: ' + money(obj.get('residual_change_usd')) +
            ' млн долл. Подкатегории без сопоставимой пары: ' +
            (', '.join(obj.get('unmatched_codes', [])) or 'нет') + '.'))
    unmatched=[r for r in obj['children'] if not r.get('comparable') and r.get('current_usd') is not None]
    if unmatched:
        out.update(fact('analysis_structure', 'В текущих выбранных месяцах также представлены: '+
            '; '.join(f"{short_product_name(r['code'],r.get('name'))} ({r['code']}): {money(r['current_usd'])} млн долл., доля {fmt(r['current_share_pct'],1)}%" for r in unmatched[:5])+
            '. Сопоставимое значение прошлого года отсутствует, поэтому динамика и вклад этих подкатегорий не рассчитаны.'))
    if obj.get('source_note'):
        out.update(fact('analysis_source', obj['source_note']))
    return out
