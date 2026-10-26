"""Deterministic, bounded comparisons of countries on common factual months."""
from datetime import date
from decimal import Decimal
import json


def month(value):
    return date.fromisoformat(str(value)[:7] + "-01").strftime("%Y-%m")


def shift(value, count):
    d = date.fromisoformat(value + "-01")
    n = d.year * 12 + d.month - 1 + count
    return f"{n // 12:04d}-{n % 12 + 1:02d}"


def number(value, places=2):
    return f"{value:,.{places}f}".replace(",", "\u00a0").replace(".", ",")


def compare(get, countries, direction, months=12, period_from=None, period_to=None, products=None, include_fizob=True):
    countries = list(dict.fromkeys(c.upper() for c in countries))
    if not 2 <= len(countries) <= 10: raise ValueError("Choose 2 to 10 distinct countries")
    if not 1 <= months <= 60: raise ValueError("months must be between 1 and 60")
    direction = {"ex":"ЭК", "im":"ИМ", "ЭК":"ЭК", "ИМ":"ИМ"}.get(direction)
    if not direction: raise ValueError("direction must be ex or im")
    latest = month(get('/v1/meta', [])['data']['period_max'])
    end = month(period_to) if period_to else latest
    start = month(period_from) if period_from else shift(end, 1-months)
    expected=[]
    cursor=start
    while cursor <= end and len(expected)<=60:
        expected.append(cursor); cursor=shift(cursor,1)
    if not expected or len(expected)>60: raise ValueError("Choose a period of 1 to 60 months")
    params=[('strana',c) for c in countries]+[('napr',direction),('type','fact'),('period_from',start),('period_to',end),('group_by','strana,period,source'),('metrics','stoim,netto'),('include','country_name'),('limit','1000')]
    for key, values in (products or {}).items():
        params.extend((key,v) for v in (values or []))
    obj=get('/v1/trade',params)
    if obj.get('meta',{}).get('has_more'):
        raise ValueError("Incomplete API response; narrow filters. No comparison calculated")
    rows=obj['data']
    available={c:set() for c in countries}
    buckets={}
    names={'IN':'Индия','CN':'Китай','TR':'Турция'}
    for row in rows:
        c=row['strana']; p=month(row['period'])
        if c not in available or p not in expected: raise ValueError("Unexpected country or period in response")
        if c not in names: names[c]=row.get('country_name') or c
        buckets.setdefault((c,p),[]).append(row)
    for (c,p), group in buckets.items():
        if all(r.get(m) is not None for r in group for m in ('stoim','netto')):
            available[c].add(p)
    common=sorted(set.intersection(*(available[c] for c in countries)))
    result={'requested_period':{'from':start,'to':end,'months':len(expected)},'latest_available_period':latest,
            'common_fact_months':common,'common_month_count':len(common),
            'excluded_months':sorted(set(expected)-set(common)),
            'missing_or_null_months':{c:sorted(set(expected)-available[c]) for c in countries},
            'type':'fact','filters':{'strana':countries,'napr':direction,**(products or {})},
            'units':{'stoim':'USD','netto':'kg'},'baseline_country':countries[0],
            'coverage_note':'Common observed months, not proof of full product coverage; missing observations are not zero. Project nowcast excluded; source-estimated physical values may remain (see quality_flags).'}
    if not common:
        result.update(comparable=False,answer_ru='Нет общих месяцев с известными стоимостью и полной массой у всех выбранных стран. Сравнение не рассчитано; неизвестная масса не равна нулю.')
        return result
    totals={c:{m:sum((Decimal(str(r[m])) for p in common for r in buckets[c,p]),Decimal(0)) for m in ('stoim','netto')} for c in countries}
    result['comparable']=True
    result['totals']=[{'strana':c,'country_name':names[c],**{m:str(v) for m,v in totals[c].items()}} for c in countries]
    result['sources']=sorted({r.get('source','') for r in rows})
    base=countries[0]; differences=[]
    for c in countries[1:]:
        entry={'country':c,'baseline':base}
        for m in ('stoim','netto'):
            delta=totals[c][m]-totals[base][m];denom=totals[base][m]
            entry[m]={'difference':str(delta),'percent':str(delta/denom*100) if denom>0 else None}
        differences.append(entry)
    result['differences']=differences
    span=common[0]+' - '+common[-1] if all(shift(common[0],i)==p for i,p in enumerate(common)) else ', '.join(common)
    lines=[f"Сравнение по факту: **{span}, {len(common)} мес.** из {len(expected)} запрошенных. Nowcast проекта исключен. Оценки физических показателей самим источником отмечены отдельно.", '',
           '| Страна | Стоимость, млрд долл. | Масса, млн т |','|---|---:|---:|']
    for c in countries:
        lines.append(f"| {names[c]} | {number(totals[c]['stoim']/Decimal(10**9))} | {number(totals[c]['netto']/Decimal(10**9))} |")
    for d in differences:
        for m,label,scale,unit in [('stoim','Стоимость',Decimal(10**9),'млрд долл.'),('netto','Масса',Decimal(10**9),'млн т')]:
            delta=Decimal(d[m]['difference']); pct=d[m]['percent']
            text=f"{label}: {names[d['country']]} относительно {names[base]} - разница **{number(delta/scale)} {unit}**"
            text+=f" (**{number(Decimal(pct),1)}%**, база: {names[base]})." if pct is not None else '. Процент не рассчитан: база неположительная.'
            lines+=['',text]
    if result['excluded_months']:lines+=['','Исключены месяцы без общего факта: '+', '.join(result['excluded_months'])+'. Окно не сдвигалось назад.']
    lines+=['','Одинаковые месяцы не гарантируют одинаковую полноту товарного покрытия. Масса не является индексом физического объема.']
    if include_fizob:
        result['physical_volume'] = compare_fizob(get, countries, direction, products or {}, common, names)
        lines += ['', result['physical_volume']['answer_ru']]
    flags = sorted({r.get('quality_flags') for r in rows if r.get('quality_flags')})
    if flags: lines += ['', '**Качество данных:** ' + '; '.join(flags)]
    result['quality_flags'] = flags
    result['answer_ru']='\n'.join(lines)
    return result


def compare_fizob(get, countries, direction, products, common, names):
    """Endpoint changes of an existing smoothed index, never summed indices."""
    def unavailable(reason):
        return {'available':False, 'reason':reason,
                'answer_ru':'**Динамика индекса физического объёма:** '+reason+' Сравнение индексов не рассчитано.'}
    active={k:list(dict.fromkeys(v)) for k,v in products.items() if v}
    if not active:
        level,code=0,'0'
    elif len(active)==1:
        key,values=next(iter(active.items()))
        if key not in ('tnved2','tnved4','tnved6') or len(values)!=1:
            return unavailable('Нужен итог по стране или один код ТН ВЭД уровня 2, 4 либо 6 знаков; произвольную корзину индексов не складываем.')
        level,code=int(key[5:]),values[0]
    else:
        return unavailable('Комбинация товарных фильтров не соответствует одному готовому агрегату fizob.')
    if len(common)<2: return unavailable('Для динамики нужны хотя бы два общих месяца факта.')
    start,end=common[0],common[-1]
    params=[('strana',c) for c in countries]+[('napr',direction),('tn_level',str(level)),('tn_code',code),('period_from',start),('period_to',end),('limit','1000')]
    try:
        obj=get('/v1/fizob',params)
        if obj.get('meta',{}).get('has_more'):return unavailable('Ответ API fizob обрезан.')
        data={}
        for raw in obj['data']:
            r={k.lower():v for k,v in raw.items()}
            c=r['strana'];p=month(r['period'])
            if c not in countries or int(r['tn_level'])!=level or str(r['tn_code'])!=code or r['napr']!=direction:
                return unavailable('API вернул другой агрегат или направление.')
            if p not in common:continue
            if (c,p) in data:return unavailable('Обнаружены дубли индекса для одного месяца.')
            data[c,p]=r
        output=[]
        for c in countries:
            if any((c,p) not in data for p in common):return unavailable('Не для всех стран есть индекс за каждый общий месяц факта; период не сокращаем молча.')
            series=[data[c,p] for p in common]
            if any(r.get('methodology_version') for r in series):
                bases={(r.get('methodology_version'),str(r.get('base_period') or '')) for r in series}
                if len(bases)!=1 or any(not r.get('base_period') for r in series):
                    return unavailable('Период пересекает разрыв цепочки индекса или смену базы.')
            if any(r.get('idx') is None for r in series):
                return unavailable('Для части месяцев полный индекс не установлен по покрытию данных.')
            idx=[Decimal(str(r['idx'])) for r in series]
            bp=[Decimal(str(r['fizob_bp'])) for r in series]
            if any(not x.is_finite() or x<0 for x in idx) or idx[0]<=0:
                return unavailable('Индекс отсутствует, нечисловой, отрицательный или имеет нулевую начальную базу.')
            if any(not x.is_finite() or x<=0 for x in bp):return unavailable('Некорректный знаменатель нормировки индекса.')
            if any(abs(x-bp[0])>abs(bp[0])*Decimal('1e-9') for x in bp):return unavailable('Знаменатель нормировки изменился; отношения idx несопоставимы.')
            growth=(idx[-1]/idx[0]-1)*100
            output.append({'strana':c,'country_name':names[c],'index_start':str(idx[0]),'index_end':str(idx[-1]),
                           'rebased_start':'100','rebased_end':str(idx[-1]/idx[0]*100),'change_percent':str(growth)})
    except Exception:
        return unavailable('Не удалось получить или проверить данные fizob.')
    lines=[f"#### Динамика индекса физического объёма: {start} → {end}", '',
           'Сглаженный показатель на основе 12-месячных средних. Изменение между двумя месяцами, не рост суммы объёмов за весь интервал.', '',
           '| Страна | Начало = 100 | Конец | Изменение |', '|---|---:|---:|---:|']
    for r in output:lines.append(f"| {r['country_name']} | 100 | {number(Decimal(r['rebased_end']),1)} | {number(Decimal(r['change_percent']),1)}% |")
    lines+=['','Формула: (индекс конца / индекс начала - 1) × 100. Каждая страна приведена к собственной начальной точке = 100; абсолютные уровни стран не сопоставляются.']
    differences=[{'country':r['strana'],'baseline':countries[0],'growth_difference_pp':str(Decimal(r['change_percent'])-Decimal(output[0]['change_percent']))} for r in output[1:]]
    if all(r.get('methodology_version')=='fisher_annual_v2' for r in data.values()):
        lines+=['','Индекс Фишера построен по 12-месячным окнам с вменением части относительных цен по методологии проекта. Он отражает динамику разнородной товарной корзины.']
    else:
        lines+=['','Это показатель методологии проекта со сглаживанием и восстановлением части количеств; он не тождественен динамике тоннажа и не является точным разложением стоимости на цену и объём.']
    if level>0:
        lines += ['', 'В товарных агрегатах используются меняющиеся стоимостные веса, поэтому изменение структуры тоже влияет на индекс.']
    if level==0:
        lines+=['','Агрегация товаров с разными физическими единицами в страновом итоге - осознанная особенность методологии fizob: индекс предназначен для анализа динамики разнородной товарной корзины.']
    return {'available':True,'tn_level':level,'tn_code':code,'period_from':start,'period_to':end,
            'method':'endpoint_ratio_of_12_month_smoothed_index','formula':'(idx_end / idx_start - 1) * 100',
            'common_fact_months':common,'series':output,'differences':differences,
            'answer_ru':'\n'.join(lines)}
