"""Compact presentation of already verified series, without new arithmetic."""
import hashlib
import re


def compact_series(facts, question):
    if not any(v['kind']=='index_change' for v in facts.values()):
        return facts
    monthly=bool(re.search(r'по месяц|помесяч|кажд\w* месяц',question,re.I))
    price=bool(re.search(r'цен|удельн\w*\s+стоим',question,re.I))
    groups={}; notes=[]; summaries=[]
    for item in facts.values():
        text=item['text'];kind=item['kind']
        match=re.search(r'([^,\n*]+), (экспорт России|импорт России), код (\d+).*?; окно (\d{4}-\d{2} - \d{4}-\d{2})',text)
        if not match:continue
        country,route,code,period=match.groups()
        key=(country,route,code,period)
        group=groups.setdefault(key,{'index':None,'price':None,'change':None,'yoy':None,'has_index':False})
        if kind.startswith('index_') or (kind=='monthly_table' and '| Индекс (' in text):group['has_index']=True
        if kind=='index_change':
            group['change']=text.split('\n\n',1)[-1]
        elif kind=='index_yoy':
            group['yoy']=text.split(': ',1)[-1]
        elif kind=='index_caveat':
            # Preserve the reason a metric cannot be interpreted, once per country.
            notes.append((key,country+': '+text.split(': ',1)[-1]))
        elif kind=='coverage' and 'Отсутствуют' in text:
            notes.append((key,country+': стоимость и масса доступны не за все месяцы выбранного окна; полный итог периода не установлен.'))
        elif kind in ('monthly_table','unit_value'):
            rows=[line for line in text.splitlines() if line.startswith('|')]
            if len(rows)<3:continue
            cells=[[c.strip() for c in line.strip('|').split('|')] for line in rows]
            target='index' if '| Индекс (' in text else 'price' if kind=='unit_value' and price else None
            if target:
                # Different sources/units are never silently collapsed into one series.
                if group[target] is not None:
                    group[target]=False
                    notes.append((key,country+': получено несколько рядов одного показателя; они не объединены из-за различий источников или единиц.'))
                else:
                    group[target]=(cells[0],{r[0]:r[1:] for r in cells[2:] if re.fullmatch(r'\d{4}-\d{2}',r[0])})
    out={}
    def add(kind,text):
        out[hashlib.sha256(text.encode()).hexdigest()[:16]]={'kind':kind,'text':text}
    tables=[]
    for (country,route,code,period),group in groups.items():
        if not group['has_index']:continue
        if group['change']:
            summaries.append(f'**{route}, {country}, код {code}: {period}.**\n\n'+group['change']+(' '+group['yoy'] if group['yoy'] else ''))
        idx=group['index'];unit=group['price']
        if unit and not monthly:
            available=[(m,v[0]) for m,v in sorted(unit[1].items()) if v[0]!='нет данных']
            if available:
                ends=list(dict.fromkeys([available[0],available[-1]]))
                summaries.append(country+', удельная стоимость, долл./т: '+'; '.join(m+' - '+v for m,v in ends)+'.')
        # Entirely unavailable normalized series adds no information to a table.
        if idx and not any(v[0]!='нет данных' for v in idx[1].values()):idx=None
        if not monthly or not (idx or unit):continue
        cols=['Месяц'];data={}
        for series in [idx,unit]:
            if series:
                cols+=series[0][1:]
                for month in series[1]:data.setdefault(month,[])
        for month in data:
            for series in [idx,unit]:
                if series:data[month]+=series[1].get(month,['нет данных']*(len(series[0])-1))
        table=[f'**{route}, {country}, код {code}: {period}.**','',
               '| '+' | '.join(cols)+' |','|---|'+'---:|'*(len(cols)-1)]
        table+=['| '+m+' | '+' | '.join(data[m])+' |' for m in sorted(data)]
        tables.append('\n'.join(table))
    if not summaries:return facts
    add('series_summary','\n\n'.join(summaries))
    for table in tables:add('series_monthly',table)
    method='Индекс сглажен за 12 месяцев; начальный месяц принят за 100. Изменение за окно и год к году - разные показатели.'
    if price:method+=' Удельная стоимость рассчитана как стоимость поставок на тонну; это не рыночная котировка.'
    notes=[text for key,text in notes if groups[key]['has_index']]
    add('series_notes',method+('\n\n'+'\n'.join(dict.fromkeys(notes)) if notes else ''))
    return out
