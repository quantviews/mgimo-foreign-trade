"""Bind model tool arguments to user-selected products and calendar windows."""
import re
from datetime import date

TRADE_TOOLS = {'trade','fizob','export_trade','compare_trade','explain_trade','product_overview'}


def window(question, latest):
    end = date.fromisoformat(str(latest)[:7] + '-01')
    def label(m): return f'{m//12:04d}-{m%12+1:02d}-01'
    last = end.year*12 + end.month-1
    relative = re.search(r'последни\w*\s+(\d+)\s+(?:доступн\w*\s+)?месяц', question, re.I)
    if relative:
        n = int(relative[1])
        if not 1 <= n <= 60: raise ValueError('Requested period must be 1-60 months')
        return label(last-n+1), label(last)
    if re.search(r'(?:этом|текущем)\s+год', question, re.I):
        return f'{end.year}-01-01', label(last)
    start = re.search(r'с\s+января\s+(20\d{2})', question, re.I)
    if start: return f'{start[1]}-01-01', label(last)
    year = re.search(r'(?:в|за)\s+(20\d{2})\s*(?:год|г\b)', question, re.I)
    if year:
        y=int(year[1]); return f'{y}-01-01', label(min(y*12+11,last))
    return None


def bind_scope(messages, latest):
    questions=[m['content'] for m in messages if m.get('role')=='user']
    q=questions[-1] if questions else ''
    follow=bool(re.match(r'\s*(?:давай отдельно|а теперь|подробнее|покажи динамику|давай.*динамик)',q,re.I))
    context=' '.join(questions) if follow else q
    codes=re.findall(r'(?<!\d)(\d{4}|\d{6}|\d{8}|\d{10})(?!\d)',context)
    codes+=re.findall(r'(?:код\w*|групп\w*)\s+(\d{2})(?!\d)',context,re.I)
    codes=list(dict.fromkeys(c for c in codes if not (len(c)==4 and 1900<=int(c)<=2099)))
    bounds=window(q,latest)
    if not bounds and follow:
        bounds=next((w for text in reversed(questions[:-1]) if (w:=window(text,latest))),None)
    return {'codes':codes, 'window':bounds}


def confirm_search(scope, query, candidates):
    if scope.get('codes'):return
    norm=lambda s:' '.join(re.findall(r'[а-яa-z0-9]+',s.lower().replace('ё','е')))
    exact=[c for c in candidates if norm(c['name'])==norm(query)]
    full=[c for c in candidates if c['level']==4 and c['matched_terms']==c['query_terms']]
    chosen=exact if len(exact)==1 else full
    if len(chosen)==1:scope['codes']=[chosen[0]['code']]


def enforce_scope(name, args, scope):
    if name not in TRADE_TOOLS:return dict(args)
    args=dict(args)
    # A four-digit code placed in tnved6 is not an empty dataset.
    normalized={}
    for field in ('tnved2','tnved4','tnved6','tnved'):
        values=args.pop(field,None)
        if values is None:continue
        for code in ([values] if isinstance(values,str) else values):
            code=str(code)
            if not code.isdigit() or len(code) not in (2,4,6,8,10):
                raise ValueError('Invalid commodity code; use reference search')
            key='tnved'+str(len(code)) if len(code)<=6 else 'tnved'
            normalized.setdefault(key,[]).append(code)
    codes=scope.get('codes') or []
    if codes:
        normalized={}
        for code in codes:
            key='tnved'+str(len(code)) if len(code)<=6 else 'tnved'
            normalized.setdefault(key,[]).append(code)
    if name in ('explain_trade','product_overview'):
        if len(codes)==1:args['code']=codes[0]
    elif name=='fizob':
        if len(codes)==1:args.update(tn_code=codes,tn_level=[str(len(codes[0]))])
    else:args.update(normalized)
    bounds=scope.get('window')
    if bounds:
        start,end=bounds
        if name=='explain_trade':
            a=date.fromisoformat(start);b=date.fromisoformat(end)
            n=(b.year-a.year)*12+b.month-a.month+1
            if n>12:raise ValueError('Decomposition supports up to 12 months; use trade for the requested longer window')
            args.update(months=n,period_to=end)
        else:args.update(period_from=start,period_to=end)
        if name=='export_trade':args['end_mode']='fixed'
    return args
