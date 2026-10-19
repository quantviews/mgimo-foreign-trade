"""Verified product overview over complete country/source/month aggregates."""
from collections import defaultdict
from decimal import Decimal
import hashlib
from html import escape

from numeric_facts import number, fmt, mid, month


def money(value):
    scale, unit = (Decimal('1000000000'), 'млрд долл.') if abs(value) >= 10**9 else (Decimal('1000000'), 'млн долл.') if abs(value) >= 10**6 else (Decimal('1000'), 'тыс. долл.') if abs(value) >= 1000 else (Decimal(1), 'долл.')
    return f'{fmt(value / scale)} {unit}'


MONTH_NAMES = ('янв.', 'февр.', 'март', 'апр.', 'май', 'июнь', 'июль', 'авг.', 'сент.', 'окт.', 'нояб.', 'дек.')

def period_label(months):
    months = sorted(set(months))
    if not months: return 'нет общих месяцев'
    runs=[]; first=last=months[0]
    for m in months[1:]:
        if m==last+1: last=m
        else: runs.append((first,last)); first=last=m
    runs.append((first,last))
    def label(m): return f'{MONTH_NAMES[m%12]} {m//12}'
    return '; '.join(label(a) if a==b else f'{label(a)} - {label(b)}' for a,b in runs)


def table_value(value, scale):
    scaled=value/scale
    if scaled and abs(scaled)<Decimal('0.01'):
        return ('-' if scaled<0 else '')+'<0,01'
    return fmt(scaled)


def overview(rows, code, name, countries, start, end, direction):
    facts = {}
    name = name.capitalize() if name.isupper() else name
    countries = {k: v.title() if v.isupper() else v for k,v in countries.items()}
    def add(kind, text):
        text = text.replace('долл..', 'долл.').replace('п.п..', 'п.п.')
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
    short_name = name if len(name)<=90 else name[:87].rsplit(' ',1)[0]+'…'
    leader=max(totals,key=totals.get)
    lead=f'**{countries.get(leader,leader)}**: {fmt(totals[leader]/total*100,1)}% доступной стоимости.' if total>0 else ''
    add('overview', f'### {route}: {short_name} (код {code})\n\n**{period_label(range(lo,hi+1))}**\n\nПо представленным странам-партнерам: **{money(total)}**. '+lead)
    scale, unit = (Decimal('1000000'),'млн долл.') if abs(total)>=10**6 else (Decimal('1000'),'тыс. долл.') if abs(total)>=1000 else (Decimal(1),'долл.')
    leaders = sorted(totals, key=totals.get, reverse=True)[:5]
    table = [f'| Страна-партнер | {unit} | Доля, % | Месяцев |', '|---|---:|---:|---:|']
    for c in leaders:
        n = len({m for cc, s, m in current if cc == c})
        share = fmt(totals[c]/total*100,1) if total > 0 else 'не рассчитана'
        table.append(f'| {countries.get(c,c)} | {table_value(totals[c],scale)} | {share} | {n} из {hi-lo+1} |')
    add('suppliers', '**Основные страны-партнеры**\n\n'+'\n'.join(table))
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
        change = f'{fmt((a/b-1)*100,1)}% г/г' if b > 0 else 'темп не рассчитан: база неположительна'
        names = ', '.join(countries.get(c,c) for c,_,_ in sorted(pairs))
        add('overview_dynamics', f'**На сопоставимом составе стран: {change}** ({money(b)} → {money(a)}). Сравниваются {period_label(range(lo,hi+1))} и те же месяцы годом ранее: {names}.')
        drivers = sorted(pairs, key=lambda p: abs(p[1]-p[2]), reverse=True)[:3]
        items=[]
        for c,x,y in drivers:
            delta=x-y
            sign='+' if delta>0 else ''
            contribution= f', {fmt(delta/b*100,1)} п.п.' if b>0 else ''
            items.append(f'{countries.get(c,c)}: {sign}{table_value(delta,scale)} {unit}{contribution}')
        add('overview_drivers', '**Вклад стран в изменение:** '+ '; '.join(items)+'.')
    else:
        add('overview_dynamics', 'Годовой темп за выбранное окно не рассчитан: нет партнеров с однозначной стоимостью за все одинаковые месяцы обоих лет. Доступные суммы и партнеры показаны выше.')
    excluded = set(totals)-{p[0] for p in pairs}
    if excluded:
        coverage = fmt(sum((p[1] for p in pairs),Decimal(0))/total*100,1) if total>0 else None
        note = f'Сопоставимый состав охватывает {coverage}% доступной стоимости текущего окна. ' if coverage is not None else ''
        add('overview_coverage', note+'Темп относится только к этому составу стран.')
    partial=[]
    for c in sorted(excluded,key=lambda c:(-totals[c],c)):
        matches=[(m,src,known[c,src,m],known[c,src,m-12]) for cc,src,m in current if cc==c and (c,src,m-12) in known]
        # A country series must retain the same source throughout its matched window.
        if not matches or len({src for m,src,x,y in matches})!=1: continue
        months=sorted(m for m,src,x,y in matches)
        x=sum((x for m,src,x,y in matches),Decimal(0)); y=sum((y for m,src,x,y in matches),Decimal(0))
        pct=(x/y-1)*100 if y>0 else None
        partial.append({'strana':c,'source':matches[0][1],'current_months':[month(m) for m in months],
                        'baseline_months':[month(m-12) for m in months],'current_value':str(x),'baseline_value':str(y),
                        'change_pct':str(pct) if pct is not None else None})
    if partial:
        table=[f'| Страна-партнер | Общие месяцы текущего окна | Сейчас / годом ранее, {unit} | Г/г, % |','|---|---|---:|---:|']
        for row in partial[:5]:
            pct=number(row['change_pct']); months=[mid(m) for m in row['current_months']]
            rate=fmt(pct,1) if pct is not None else 'не рассчитан'
            table.append(f"| {countries.get(row['strana'],row['strana'])} | {period_label(months)} | {table_value(number(row['current_value']),scale)} / {table_value(number(row['baseline_value']),scale)} | {rate} |")
        note='Каждая страна сравнивается с теми же месяцами годом ранее. Эти темпы не входят в общий темп выше; пропуски не заменены нулями.'
        if len(partial)>5: note+=f' Показаны 5 из {len(partial)} стран с отдельным сопоставлением, по доступной стоимости текущего окна.'
        add('overview_partial', '**Отдельная динамика стран с неполным периодом**\n\n'+'\n'.join(table)+'\n\n'+note)
    sources = sorted({s for c,s,m in current})
    labels={'national':'национальная статистика стран-партнеров','comtrade':'UN Comtrade'}
    add('overview_source','<details><summary>Товар и источники</summary><p>'+escape(name)+f' (код {code}).</p><p>Источники: '+escape(', '.join(labels.get(s,s) for s in sources))+'. Фактические записи базы; доли относятся к доступной стоимости по странам-партнерам.</p></details>')
    return {'numeric_facts':facts, 'period_from':month(lo), 'period_to':month(hi), 'total_usd':str(total), 'comparable_countries':sorted(p[0] for p in pairs), 'partial_country_comparisons':partial}


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
