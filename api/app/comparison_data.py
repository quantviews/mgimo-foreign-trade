"""Bounded database adapter shared by comparisons and workbook exports."""
from . import db
from .query import build_trade_query
from .comparison import compare


def database_get(path, pairs, cap=1000):
    params = {}
    for k, v in pairs:
        params.setdefault(k, []).append(v)
    if path == '/v1/meta':
        return {'data': db.data_version()}
    if path == '/v1/trade':
        sql, args, _ = build_trade_query(
            filters={k: v for k, v in params.items() if k in ('strana','napr','type','tnved','tnved2','tnved4','tnved6')},
            group_by=['strana','period','source'], metrics=['stoim','netto'], include=['country_name'],
            period_from=params['period_from'][0], period_to=params['period_to'][0],
            order_by='period', limit=cap, offset=0)
    elif path == '/v1/fizob':
        countries = params['strana']
        sql = "SELECT STRANA,NAPR,PERIOD,tn_level,tn_code,idx,fizob_bp,json_extract_string(to_json(f),'$.base_period') base_period,json_extract_string(to_json(f),'$.methodology_version') methodology_version FROM fizob_enriched f WHERE STRANA IN (" + ','.join('?' for _ in countries) + ') AND NAPR=? AND tn_level=? AND tn_code=? AND PERIOD>=? AND PERIOD<=? ORDER BY PERIOD LIMIT ?'
        args = countries + [params['napr'][0], int(params['tn_level'][0]), params['tn_code'][0], params['period_from'][0]+'-01', params['period_to'][0]+'-01', cap+1]
    else:
        raise ValueError('Unsupported comparison source')
    columns, rows = db.run_query(sql, args)
    return {'data': [dict(zip((c.lower() for c in columns), row)) for row in rows[:cap]],
            'meta': {'has_more': len(rows)>cap}}


def export_comparison(rows, version, filters):
    """Compare only the exported observations, never expand the selected slice."""
    def unavailable(reason):
        return {'comparable': False, 'answer_ru': reason}
    countries = list(dict.fromkeys(c.upper() for c in filters.get('strana', [])))
    if not countries:
        countries = sorted({r['strana'] for r in rows})
    directions = list(dict.fromkeys({'ex':'ЭК','im':'ИМ','эк':'ЭК','им':'ИМ'}.get(v.lower(), v) for v in filters.get('napr', [])))
    if not 2 <= len(countries) <= 10:
        return unavailable('Для сравнения выберите от 2 до 10 стран.')
    if len(directions) != 1:
        return unavailable('Для сравнения выберите одно направление торговли.')
    if not {'stoim','netto'}.issubset(filters.get('metrics','').split(',')):
        return unavailable('Для сравнения нужны обе меры: стоимость и масса.')
    restricted = any(filters.get(k) for k in ('source','edizm','edizm_iso'))
    def get(path, pairs):
        if path == '/v1/meta': return {'data': version}
        if path == '/v1/trade':
            return {'data': [r for r in rows if r.get('type') == 'fact'], 'meta': {'has_more':False}}
        return database_get(path, pairs)
    result = compare(get, countries, directions[0], period_from=filters['period_from'],
                     period_to=filters['period_to'],
                     products={k:filters[k] for k in ('tnved','tnved2','tnved4','tnved6') if filters.get(k)},
                     include_fizob=not restricted)
    if restricted and result['comparable']:
        result['physical_volume'] = {'available':False, 'reason':'Фильтр по источнику или единице не соответствует готовому агрегату fizob. Фильтры не расширялись.'}
    return result
