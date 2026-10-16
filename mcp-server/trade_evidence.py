"""Computed evidence for complete trade responses; no model arithmetic required."""
from decimal import Decimal

RULE = ("Use computed_summary totals and row_display verbatim; do not add numbers yourself. Do not derive additional per-month or total mass/count ratios; do not show a blocked ratio even as an illustration. "
        "Totals cover returned observations, not a complete calendar period. Missing months are not zero. "
        "A quality flag on an aggregate means at least one underlying record is flagged; "
        "it does not say the aggregate ratio crosses the threshold. Absence of a flag is not validation. "
        "Never declare a month reliable just because its ratio looks plausible. "
        "When mass/count quality is flagged, do not calculate or present a reliable mean vehicle mass. "
        "Physical flags do not establish an error in monetary value. Use adaptive mass labels, never round a positive mass to zero.")

def fmt(value):
    if value is None: return "нет полных данных"
    s=format(value, ',.6f').rstrip('0').rstrip('.')
    return s.replace(',', '\u00a0').replace('.', ',')

def mass_label(kg):
    if kg is None: return 'нет полных данных'
    divisor,unit=(Decimal(1000000),'тыс. т') if abs(kg)>=1000000 else (Decimal(1000),'т') if abs(kg)>=1000 else (Decimal(1),'кг')
    return fmt(kg/divisor)+' '+unit

def trade_evidence(obj):
    if obj.get('meta',{}).get('has_more') or obj.get('_truncated'): return {}
    rows=obj.get('data',[])
    if not rows: return {}
    dims=('strana','napr','type','source','edizm','edizm_iso','tnved','tnved2','tnved4','tnved6')
    groups={};display=[]
    for row in rows:
        flag=row.get('quality_flags')
        if flag:
            row['quality_flags_raw']=flag
            row['quality_flags']='В агрегате есть исходные записи с предупреждением: '+str(flag)+'. Порог относится к исходным записям, не к месячному отношению сумм. Проверка проекта, не подтвержденная ошибка источника.'

    for row in rows:
        key=tuple(row.get(k) for k in dims)
        groups.setdefault(key,[]).append(row)
        kg=Decimal(str(row['netto'])) if row.get('netto') is not None else None
        display.append({**{k:row[k] for k in (*dims,'period') if k in row},'mass_display':mass_label(kg)})
    summaries=[]
    for key,items in groups.items():
        summary={k:v for k,v in zip(dims,key) if v is not None}
        flags=any(r.get('quality_flags') for r in items)
        for metric in ('stoim','netto','kol'):
            values=[r.get(metric) for r in items]
            total=sum((Decimal(str(v)) for v in values),Decimal(0)) if all(v is not None for v in values) else None
            if metric=='kol' and not (summary.get('edizm') or summary.get('edizm_iso')): total=None
            summary[metric]=str(total) if total is not None else None
            summary[metric+'_display']=mass_label(total) if metric=='netto' else fmt(total)
        summary['observed_months']=sorted({str(r['period'])[:7] for r in items if r.get('period')})
        summary['flagged_aggregate_months']=sorted({str(r['period'])[:7] for r in items if r.get('period') and r.get('quality_flags')})
        summary['quality_status']='underlying_records_flagged' if flags else 'not_independently_verified'
        kg,count=summary['netto'],summary['kol']
        summary['mean_kg_per_unit']=str(Decimal(kg)/Decimal(count)) if not flags and kg is not None and count is not None and Decimal(count)>0 and summary.get('edizm_iso')=='796' else None
        summary['mean_status']='blocked_by_quality_or_missing_unit' if summary['mean_kg_per_unit'] is None else 'arithmetic_only_not_validated'
        summaries.append(summary)
    return {'computed_summary':summaries,'row_display':display,'arithmetic_rules':RULE}
