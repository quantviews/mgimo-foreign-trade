"""Bounded read-only monetary decomposition shared by MCP and the demo."""
from datetime import date
import math


def month_id(value):
    d = date.fromisoformat(str(value)[:7] + '-01')
    return d.year * 12 + d.month - 1


def month_label(m):
    return f'{m//12:04d}-{m%12+1:02d}-01'


def decompose(current, previous, parent_current, parent_previous, dimension):
    def index(rows):
        out = {}
        for r in rows:
            code = r.get(dimension)
            value = r.get('stoim')
            if not code or code in out or value is None or not math.isfinite(value):
                raise ValueError('Missing code/value or duplicate child aggregate')
            out[code] = float(value)
        return out
    a,b=index(current),index(previous)
    for values,total in [(a,parent_current),(b,parent_previous)]:
        if not math.isclose(sum(values.values()), total, rel_tol=1e-9, abs_tol=.01):
            raise ValueError('Child totals do not reconcile with parent')
    def pct(v,den): return 100*v/den if den>0 else None
    children=[]
    for code in a.keys() | b.keys():
        v,p=a.get(code),b.get(code)
        delta=v-p if v is not None and p is not None else None
        children.append(dict(code=code,current_usd=v,previous_usd=p,
            current_share_pct=pct(v,parent_current) if v is not None else None,
            previous_share_pct=pct(p,parent_previous) if p is not None else None,
            change_usd=delta,growth_pct=pct(delta,p) if delta is not None else None,
            contribution_pp=pct(delta,parent_previous) if delta is not None else None,
            comparable=delta is not None))
    children.sort(key=lambda r:abs(r['change_usd'] or 0),reverse=True)
    change=parent_current-parent_previous
    residual=change-sum(r['change_usd'] or 0 for r in children)
    return dict(parent=dict(current_usd=parent_current,previous_usd=parent_previous,
            change_usd=change,growth_pct=pct(change,parent_previous)),children=children,
        unmatched_codes=[r['code'] for r in children if not r['comparable']],
        decomposition_complete=all(r['comparable'] for r in children),
        residual_change_usd=residual,residual_contribution_pp=pct(residual,parent_previous))


def explain(client, base, headers, strana, napr, code, months=12, period_to=None, source=None):
    if len(code) not in (2,4) or not code.isdigit() or not 1<=months<=12:
        raise ValueError('Use one HS2/HS4 code and 1-12 months')
    if len(strana)!=2 or napr not in ('im','ex','ИМ','ЭК'):
        raise ValueError('Use one ISO2 country and im/ex direction')
    source=source or ('national' if strana.upper() in ('CN','IN','TR') else 'comtrade')
    if source not in ('national','comtrade'): raise ValueError('Factual source required')
    def get(path,params=None):
        r=client.get(base+path,headers=headers,params=params,timeout=60);r.raise_for_status()
        obj=r.json()
        if obj.get('meta',{}).get('has_more'): raise ValueError('Incomplete API result')
        return obj
    latest=get('/v1/meta')['data']['period_max']
    end=month_id(period_to or latest)
    if end>month_id(latest): raise ValueError('End exceeds latest data month')
    start=end-months+1
    common=dict(strana=strana.upper(),napr=napr,source=source,type='fact',metrics='stoim',limit=1000)
    common['tnved'+str(len(code))]=code
    parent=get('/v1/trade',dict(common,group_by='period',period_from=month_label(start-12),period_to=month_label(end)))['data']
    lookup={month_id(r['period']):r['stoim'] for r in parent}
    periods=list(range(start,end+1));prior=[m-12 for m in periods]
    missing=[month_label(m) for m in periods+prior if m not in lookup or lookup[m] is None]
    matched=[m for m in periods if lookup.get(m) is not None and lookup.get(m-12) is not None]
    if not matched:
        return dict(available=False,missing_months=missing,current_period=[month_label(start),month_label(end)],previous_period=[month_label(start-12),month_label(end-12)],period_max=latest,export_instruction='User requested Excel: call export_trade now for current_period with end_mode=fixed; no further permission needed. For evidence use trade with these exact dates and group_by=period,type,source,edizm,edizm_iso. Missing months listed here are specific gaps, not absence of all comparable observations. Existing zero rows remain observed records.',reason='No complete matched parent periods; missing months are not zero.')
    dimension='tnved'+str(len(code)+2)
    def children(first,last,selected):
        rows=get('/v1/trade',dict(common,group_by=dimension+(',period' if missing else ''),include=dimension+'_name',period_from=month_label(first),period_to=month_label(last)))['data']
        if not missing:return rows
        grouped={}
        for row in rows:
            if month_id(row['period']) not in selected:continue
            if row.get('stoim') is None:raise ValueError('Unknown child value in matched months')
            key=row[dimension]
            grouped.setdefault(key,{dimension:key,dimension+'_name':row.get(dimension+'_name'),'stoim':0.0})['stoim']+=row['stoim']
        return list(grouped.values())
    current=children(start,end,set(matched))
    previous=children(start-12,end-12,{m-12 for m in matched})
    result=decompose(current,previous,sum(lookup[m] for m in matched),sum(lookup[m-12] for m in matched),dimension)
    labels={r[dimension]:r.get(dimension+'_name') for r in previous+current}
    for child in result['children']:
        child['name']=labels.get(child['code'])
        child['name_source']='database '+dimension+'_name'
        child['name_status']='available' if child['name'] else 'missing'
    result.update(available=True,country=strana.upper(),direction=napr,parent_code=code,source=source,
        partial_period=bool(missing),missing_months=missing,
        matched_months=[month_label(m) for m in matched],
        matched_previous_months=[month_label(m-12) for m in matched],
        period_max=latest,current_period=[month_label(start),month_label(end)],
        previous_period=[month_label(start-12),month_label(end-12)],months=months,
        method='Value only, USD. When partial_period is true, all sums, shares and contributions use only matched_months and matched_previous_months, never the complete requested windows. Explicit zero rows are retained; missing rows stay unknown. Shares use their own period parent; contribution pp = child absolute change / previous parent * 100. Read the computed values, do not sum or recalculate rounded figures. Missing child is unknown, not zero. Residual is unallocated change, not a measured child contribution. No quantity or fizob decomposition is inferred. Use Russian database names with exact codes in parentheses; concise faithful labels are allowed. If name is missing, retain code and state that name is unavailable.',
        source_note='Источник: национальная таможенная статистика.' if source=='national' else 'Источник: UN Comtrade. Признаки оценки физических показателей указаны в данных источника.')
    return result

def rank_chapters(client, base, headers, strana, napr, top_n=10, period_from=None, period_to=None):
    """Top chapters calculated from the complete factual universe, with coverage."""
    from decimal import Decimal
    if len(strana)!=2 or napr not in ('im','ex','ИМ','ЭК') or not 1<=top_n<=40:
        raise ValueError('One ISO2 country, direction, and top_n=1..40 required')
    def get(path,params=None):
        r=client.get(base+path,headers=headers,params=params,timeout=60);r.raise_for_status()
        o=r.json()
        if o.get('meta',{}).get('has_more') or o.get('_truncated'):raise ValueError('Incomplete API result')
        return o
    latest=get('/v1/meta')['data']['period_max']
    end=month_id(period_to or latest)
    start=month_id(period_from or (month_label(end)[:4]+'-01-01'))
    if end>month_id(latest) or not 1<=end-start+1<=60:raise ValueError('Invalid period; max 60 months')
    params=dict(strana=strana.upper(),napr=napr,type='fact',period_from=month_label(start),period_to=month_label(end),metrics='stoim',limit=1000)
    rows=get('/v1/trade',dict(params,group_by='tnved2',include='tnved2_name',order_by='-stoim'))['data']
    if any(r.get('stoim') is None or not math.isfinite(r['stoim']) or not r.get('tnved2') for r in rows):raise ValueError('Missing code/value')
    if len({r['tnved2'] for r in rows})!=len(rows):raise ValueError('Duplicate chapter')
    rows=sorted(rows,key=lambda r:(-Decimal(str(r['stoim'])),r['tnved2']))
    total=sum((Decimal(str(r['stoim'])) for r in rows),Decimal(0))
    monthly=get('/v1/trade',dict(params,group_by='period,source',order_by='period'))['data']
    if any(r.get('stoim') is None for r in monthly):raise ValueError('Missing monthly value')
    if not math.isclose(float(total),sum(r['stoim'] for r in monthly),rel_tol=1e-9,abs_tol=.01):raise ValueError('Totals do not reconcile')
    actual={str(r['period'])[:7] for r in monthly}
    missing=[month_label(m)[:7] for m in range(start,end+1) if month_label(m)[:7] not in actual]
    top=[{**r,'share_pct':float(Decimal(str(r['stoim']))/total*100) if total>0 else None} for r in rows[:top_n]]
    selected=sum((Decimal(str(r['stoim'])) for r in rows[:top_n]),Decimal(0))
    return dict(data=top,meta=dict(period_max=latest,has_more=False),ranking=dict(
        complete_universe=True,total_groups=len(rows),shown_groups=len(top),omitted_groups=len(rows)-len(top),
        total_stoim=str(total),other_groups_stoim=str(total-selected),
        top_share_pct=float(selected/total*100) if total>0 else None,
        requested_period=[month_label(start),month_label(end)],fact_months=sorted(actual),missing_months=missing,
        sources=sorted({r['source'] for r in monthly}),type='fact'),
        instruction='Requested top-N from complete chapter aggregates, not a truncated response. Shares use ALL groups as denominator. Report actual months, total and leaders with Russian names and codes. Missing months are not zero; totals cover available observations, not a full year automatically. Source coverage is not guaranteed by complete_universe. No Excel required. В этом денежном рейтинге физические показатели не анализируются: не выводи примечания о массе, количестве или единицах из quality_flags. Не утверждай ни ошибочность, ни гарантированную достоверность стоимости на основании физических флагов. Сами флаги сохранены для запросов о соответствующих показателях.')
