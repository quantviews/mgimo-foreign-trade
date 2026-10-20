"""Presentation of computed evidence, without asking the model to do arithmetic."""
import hashlib
import re
from decimal import Decimal

from .numeric_facts import fmt, number, COUNTRIES


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
    route = 'Импорт России' if obj.get('direction') in ('im', 'ИМ') else 'Экспорт России'
    out = fact('analysis_headline',
        f"**{route}, {COUNTRIES.get(obj['country'], obj['country'])}, код {obj['parent_code']}: {current}.**\n\n"
        f"Стоимость: **{money(parent['current_usd'])} млн долл.**, "
        f"годом ранее ({previous}) - {money(parent['previous_usd'])} млн долл. "
        f"Изменение: **{fmt(parent['growth_pct'], 1)}%**.")
    rows = [r for r in obj['children'] if r.get('comparable')]
    rows.sort(key=lambda r: abs(number(r['change_usd'])), reverse=True)
    if rows:
        main = rows[0]
        out.update(fact('analysis_leader',
            f"Главный вклад в изменение стоимости дала подкатегория {main['code']}: "
            f"{money(main['change_usd'])} млн долл., {fmt(main['contribution_pp'], 1)} п.п. "
            "Детализация по кодам приведена ниже."))
    table = ['**Главные вклады подкатегорий в изменение стоимости**', '',
             '| Подкатегория | Доля сейчас, % | Изменение, млн долл. | Вклад, п.п. |',
             '|---|---:|---:|---:|']
    for row in rows[:3]:
        name = re.sub(r'[|\r\n]', ' ', row.get('name') or 'Название отсутствует')
        name = ' '.join(name.split())
        if name.isupper():
            name = name.capitalize()
        if len(name) > 100:
            name = name[:97].rsplit(' ', 1)[0] + '…'
        table.append(f"| {name} ({row['code']}) | {fmt(row['current_share_pct'], 1)} | "
                     f"{money(row['change_usd'])} | {fmt(row['contribution_pp'], 1)} |")
    if rows:
        out.update(fact('analysis_drivers', '\n'.join(table)))
    if obj.get('unmatched_codes') or number(obj.get('residual_change_usd', 0)):
        out.update(fact('analysis_caveat',
            'Нераспределенное изменение: ' + money(obj.get('residual_change_usd')) +
            ' млн долл. Подкатегории без сопоставимой пары: ' +
            (', '.join(obj.get('unmatched_codes', [])) or 'нет') + '.'))
    if obj.get('source_note'):
        out.update(fact('analysis_source', obj['source_note']))
    return out
