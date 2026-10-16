"""Deterministic monthly changes; missing months/values never become zero."""
from decimal import Decimal


def monthly_evidence(rows, baseline=()):
    rows = [{k.lower(): v for k, v in r.items()} for r in rows]
    dimensions = ('strana', 'napr', 'type', 'source', 'edizm', 'edizm_iso', 'tnved', 'tnved2', 'tnved4', 'tnved6')
    def key(r):
        return tuple(r.get(k) for k in dimensions)
    def month(r):
        p = str(r.get('period', ''))
        try: return int(p[:4])*12 + int(p[5:7]) - 1
        except ValueError: return None
    baseline = [{k.lower():v for k,v in r.items()} for r in baseline]
    lookup = {}
    for r in [*baseline, *rows]:
        lookup.setdefault((key(r), month(r)), []).append(r)
    changes = []
    for r in rows:
        m = month(r)
        if m is None: continue
        item = {k:r[k] for k in (*dimensions, 'period') if k in r}
        item['netto_thousand_tonnes'] = float(Decimal(str(r['netto']))/Decimal(1000000)) if r.get('netto') is not None else None
        for lag, label in ((1,'mom'), (12,'yoy')):
            previous = lookup.get((key(r),m-lag), [])
            for metric in ('stoim','netto','kol'):
                if metric not in r: continue
                delta = pct = None
                if r.get('type') == 'fact' and len(previous)==1 and len(lookup[(key(r),m)])==1:
                    v,b = r.get(metric),previous[0].get(metric)
                    if v is not None and b is not None:
                        delta = Decimal(str(v))-Decimal(str(b))
                        if Decimal(str(b))>0: pct=delta/Decimal(str(b))*100
                item[metric+'_'+label+'_change'] = float(delta) if delta is not None else None
                item[metric+'_'+label+'_pct'] = round(float(pct),4) if pct is not None else None
        changes.append(item)
    return {'sources_present':sorted({str(r.get('source')) for r in rows if r.get('source')}), 'source_rule':'national = national customs, not Comtrade. nowcast = project model. Only source=comtrade with explicit true estimate flags or matching quality_flags is a Comtrade estimate. Never infer source estimates merely from TYPE=fact or missing flags.', 'monthly_changes':changes, 'change_rules':'MoM: previous calendar month; YoY: same month last year. Same country, source, type and unit only. Null means missing or nonpositive baseline, not zero.'}
