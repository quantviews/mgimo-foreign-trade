def coverage_evidence(rows, filters):
    if rows and any('period' not in r or 'type' not in r for r in rows):
        return {'status':'not_assessable_without_period_and_type','instruction':'Request monthly rows split by type/source to assess factual coverage; do not infer missing months from a non-monthly aggregate.'}
    def mid(v):
        y,m=map(int,str(v)[:7].split('-'));return y*12+m-1
    try:
        start,end=mid(filters['period_from']),mid(filters['period_to'])
        expected=[f'{v//12:04d}-{v%12+1:02d}' for v in range(start,end+1)]
    except (KeyError,ValueError,TypeError): return {}
    fact=[r for r in rows if r.get('type')=='fact' and r.get('source')!='nowcast' and str(r.get('period',''))[:7] in expected]
    observed=sorted({str(r['period'])[:7] for r in fact})
    return {'requested_months':expected,'observed_fact_months':observed,
        'missing_fact_months':sorted(set(expected)-set(observed)),
        'zero_value_rows':sum(r.get('stoim')==0 for r in fact),
        'missing_mass_rows':sum(r.get('netto') is None for r in fact),
        'missing_quantity_rows':sum(r.get('kol') is None for r in fact),
        'instruction':'Назови запрошенный период и отдельно месяцы имеющихся записей. При missing_fact_months сумма относится только к доступным записям, полный итог запрошенного периода не установлен. Не называй поставки малыми/редкими по неполному срезу. При пропусках физических показателей количество и масса только известная часть. Нулевая стоимость в существующей строке не равна отсутствию строки: пиши «в исходных записях указано 0», не «наблюдений нет». Не утверждай причину нуля (округление, отсутствие торговли, непубликация) без подтверждения источника. Отсутствующий месяц не заменяй нулем. Доли и вклады только при explanation.available=true за указанные сопоставимые периоды.'}
