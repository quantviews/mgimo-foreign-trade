"""Verified product overview over complete country/source/month aggregates."""
from collections import defaultdict
from decimal import Decimal
import hashlib

from .numeric_facts import number, fmt, mid, month


def money(value):
    scale, unit = (Decimal('1000000000'), 'млрд долл.') if abs(value) >= 10**9 else (Decimal('1000000'), 'млн долл.') if abs(value) >= 10**6 else (Decimal('1000'), 'тыс. долл.') if abs(value) >= 1000 else (Decimal(1), 'долл.')
    return f'{fmt(value / scale)} {unit}'


def overview(rows, code, name, countries, start, end, direction):
    facts = {}
    name = name.capitalize() if name.isupper() else name
    countries = {k: v.title() if v.isupper() else v for k,v in countries.items()}
    def add(kind, text):
        text = text.replace('долл..', 'долл.')
        facts[hashlib.sha256(text.encode()).hexdigest()[:16]] = {'kind': kind, 'text': text}
    lo, hi = mid(start), mid(end)
    if lo is None or hi is None or not 1 <= hi-lo+1 <= 12:
        raise ValueError('Обзор поддерживает окно от одного до двенадцати месяцев.')
    cells = defaultdict(list)
    for r in rows:
        if r.get('type') != 'fact' or r.get('source') == 'nowcast':
            continue
        cells[(r.get('strana'), r.get('source'), mid(r.get('period')))].append(number(r.get('stoim')))
    # More than one source for a country/month is ambiguous: do not double count.
    counts = defaultdict(int)
    for c, s, m in cells:
        counts[c, m] += 1
    known = {k: v[0] for k, v in cells.items() if len(v) == 1 and v[0] is not None and counts[k[0], k[2]] == 1}
    current = {k: v for k, v in known.items() if k[2] is not None and lo <= k[2] <= hi}
    if not current:
        add('overview', f'По коду {code} ({name}) за {month(lo)} - {month(hi)} нет однозначных фактических значений стоимости в полученном срезе.')
        return {'numeric_facts': facts}
    totals = defaultdict(Decimal)
    for (c, s, m), v in current.items(): totals[c] += v
    total = sum(totals.values(), Decimal(0))
    route = 'Импорт России' if direction in ('im', 'ИМ') else 'Экспорт России'
    add('overview', f'### {route}: {name} (код {code})\n\nЗа {month(lo)} - {month(hi)} в доступных фактических записях отражено **{money(total)}** по представленным в базе партнерам.')
    leaders = sorted(totals, key=totals.get, reverse=True)[:5]
    table = ['| Партнер | Стоимость | Доля доступной суммы | Месяцев с данными |', '|---|---:|---:|---:|']
    for c in leaders:
        n = len({m for cc, s, m in current if cc == c})
        share = fmt(totals[c]/total*100)+'%' if total > 0 else 'не рассчитана'
        table.append(f'| {countries.get(c,c)} | {money(totals[c])} | {share} | {n} из {hi-lo+1} |')
    add('suppliers', 'Основные партнеры по стоимости:\n\n'+'\n'.join(table))
    # Use the full requested calendar window and the identical country/source set.
    eligible = []
    for c, s in {(c,s) for c,s,m in current}:
        if all((c,s,m) in known and (c,s,m-12) in known for m in range(lo,hi+1)):
            eligible.append((c,s))
    pairs = []
    for c,s in eligible:
        a = sum((known[c,s,m] for m in range(lo,hi+1)), Decimal(0))
        b = sum((known[c,s,m-12] for m in range(lo,hi+1)), Decimal(0))
        pairs.append((c,a,b))
    if pairs:
        a = sum((p[1] for p in pairs),Decimal(0)); b = sum((p[2] for p in pairs),Decimal(0))
        change = f'{fmt((a/b-1)*100)}% г/г' if b > 0 else 'темп не рассчитан: база неположительна'
        names = ', '.join(countries.get(c,c) for c,_,_ in sorted(pairs))
        add('overview_dynamics', f'На сопоставимом составе партнеров ({names}) стоимость за {month(lo)} - {month(hi)} составила **{money(a)}** против {money(b)} за {month(lo-12)} - {month(hi-12)}: **{change}**. Для каждого партнера сохранены источник и все месяцы обоих окон.')
        drivers = sorted(pairs, key=lambda p: abs(p[1]-p[2]), reverse=True)[:3]
        table = ['| Партнер | Изменение стоимости | Вклад в общий темп |','|---|---:|---:|']
        for c, x, y in drivers:
            contribution = fmt((x-y)/b*100)+' п.п.' if b > 0 else 'не рассчитан'
            table.append(f'| {countries.get(c,c)} | {money(x-y)} | {contribution} |')
        main=drivers[0]
        verb='увеличение' if main[1]>=main[2] else 'сокращение'
        lead=f'Главный вклад в изменение сопоставимого итога: {countries.get(main[0],main[0])}, {verb} стоимости поставок на {money(abs(main[1]-main[2]))}. '
        add('overview_drivers', lead+'Вклад партнеров в общий темп:\n\n'+'\n'.join(table))
    else:
        add('overview_dynamics', 'Годовой темп за выбранное окно не рассчитан: нет партнеров с однозначной стоимостью за все одинаковые месяцы обоих лет. Доступные суммы и партнеры показаны выше.')
    excluded = set(totals)-{p[0] for p in pairs}
    if excluded:
        coverage = fmt(sum((p[1] for p in pairs),Decimal(0))/total*100) if total>0 else None
        note = f'Сопоставимый состав охватывает {coverage}% доступной стоимости текущего окна. ' if coverage is not None else ''
        add('overview_coverage', note+'Остальные партнеры учитываются в доступной сумме и таблице поставщиков; их годовая динамика за все окно не установлена из-за пропусков или различий источников.')
    sources = sorted({s for c,s,m in current})
    labels={'national':'национальная статистика стран-партнеров','comtrade':'UN Comtrade'}
    add('overview_source','Источники: '+', '.join(labels.get(s,s) for s in sources)+'. В обзоре использованы фактические записи базы.')
    return {'numeric_facts':facts, 'period_from':month(lo), 'period_to':month(hi), 'total_usd':str(total), 'comparable_countries':[p[0] for p in pairs]}


async def fetch_overview(client, base, headers, code, napr, period_from=None, period_to=None):
    if not code.isdigit() or len(code) not in (2,4,6):
        raise ValueError('Нужен код ТН ВЭД длиной 2, 4 или 6 знаков.')
    async def get(path, params=None):
        r=await client.get(base+path,params=params,headers=headers,timeout=60)
        r.raise_for_status(); return r.json()
    latest=(await get('/v1/meta'))['data']['period_max']
    end=period_to or str(latest)[:10]; start=period_from or end[:4]+'-01-01'
    lo,hi=mid(start),mid(end)
    if lo is None or hi is None or not 1<=hi-lo+1<=12: raise ValueError('Укажите окно не более 12 месяцев.')
    refs=await get('/v1/reference/tnved',{'level':len(code)})
    name=next((r['name'] for r in refs if str(r['code'])==code),'Название отсутствует в справочнике')
    countries={r['strana']:r['country_name'] for r in await get('/v1/reference/countries')}
    params={'tnved'+str(len(code)):code,'napr':napr,'type':'fact','group_by':'strana,period,type,source','metrics':'stoim','period_from':month(lo-12)+'-01','period_to':end,'limit':10000}
    result=await get('/v1/trade',params)
    if result.get('meta',{}).get('has_more'):
        return {'result_status':'needs_refinement','message':'Срез превышает лимит строк обзора; требуется более узкий период.'}
    return overview(result['data'],code,name,countries,start,end,napr)
