"""Lexical candidates from the official reference, never invented code mappings."""
import re


def search_candidates(query, rows, limit=15):
    words=re.findall(r'[а-яa-z0-9]+',query.lower().replace('ё','е'))
    stop={'импорт','экспорт','россии','товар','товара','товары','обзор','дай','данные','информацию','код','коду','по','из','в','и','для','о','за'}
    # These are search hints, not equivalences between products or code mappings.
    aliases={'бель':('кальсон','трус','сороч','пижам','белье','белья','белью'),
             'трикотаж':('трикотаж',),'муж':('муж','мальчик'),
             'автомоб':('автомоб',),'нефт':('нефт',)}
    groups=[]
    for word in words:
        if word in stop:continue
        match=next((v for k,v in aliases.items() if word.startswith(k)),None)
        groups.append(match or (word[:5] if len(word)>5 else word,))
    if not groups:return []
    candidates=[]
    exact=query.strip()
    for row in rows:
        code=str(row['code']);name=row.get('name') or ''
        normalized=name.lower().replace('ё','е')
        tokens=re.findall(r'[а-яa-z0-9]+',normalized)
        hits=sum(any(t.startswith(prefix) for t in tokens for prefix in group) for group in groups)
        if exact.isdigit():
            if code!=exact:continue
            hits=len(groups)
        elif not hits:continue
        candidates.append({'code':code,'name':name,'level':len(code),'parent_code':code[:-2] if len(code)>2 else None,'matched_terms':hits,'query_terms':len(groups)})
    candidates.sort(key=lambda r:(-r['matched_terms'],{4:0,2:1,6:2}.get(r['level'],3),r['code']))
    return candidates[:limit]
