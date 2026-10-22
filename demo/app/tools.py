"""Tool schemas (DeepSeek / OpenAI function-calling) and their executor.

Each tool maps to a GET on the read-only data API, called with the single shared
demo-plan key. Results are trimmed so a chat turn cannot balloon the token budget.
"""

from __future__ import annotations

import json
import copy
import logging
import re
import hashlib

import httpx

from .query_contract import resolve_trade_route
from .config import settings
from .numeric_facts import numerical_facts, index_baseline_filters, ranking_facts
from .analysis import monthly_evidence
from .trade_evidence import trade_evidence
from .exports import export_context, save_export, MAX_EXPORT_ROWS
from .answer_style import physical_request


def physical_evidence_args(args, question):
    """Keep monthly coverage and separate units for a physical-quantity request."""
    if not physical_request(question):
        return args
    args = dict(args)
    metrics = (args.get('metrics') or 'stoim').split(',')
    dims = (args.get('group_by') or 'strana,period').split(',')
    for metric in ('netto', 'kol'):
        if metric not in metrics:
            metrics.append(metric)
    for dim in ('strana', 'period', 'type', 'source', 'edizm', 'edizm_iso'):
        if dim not in dims:
            dims.append(dim)
    args.update(metrics=','.join(metrics), group_by=','.join(dims))
    return args

_STR_ARRAY = {"type": "array", "items": {"type": "string"}}

TOOLS = [
    {"type":"function","function":{"name":"search_products","description":"FIRST step for a product named in free text. Search official Russian reference at levels 2/4/6. Results are candidates, not confirmed classification. Choose 2 for a broad chapter, 4 for a product group, 6 for an explicit subtype. Ask clarification if material, use, gender or knit/woven makes competing codes possible. Do not query trade or create a file for an ambiguous choice.","parameters":{"type":"object","properties":{"query":{"type":"string"},"level":{"type":"integer","enum":[2,4,6]}},"required":["query"]}}},
    {"type":"function","function":{"name":"product_overview","description":"Use FIRST for a general overview of one product code, especially requests like information on imports of code 6107. Returns official Russian name, leading partner-country shares and YoY on identical country/source/month coverage, plus separate matched-month changes for countries with incomplete periods. Include overview_partial when present; never combine country-specific rates with the full-window rate. All narrative numbers are verified facts. Preserve requested strana country filter. For a country-specific quantity/mass question use trade instead; this overview covers value only.","parameters":{"type":"object","properties":{"code":{"type":"string"},"strana":_STR_ARRAY,"napr":{"type":"string","enum":["im","ex"]},"period_from":{"type":"string"},"period_to":{"type":"string"}},"required":["code","napr"]}}},
    {
        "type": "function",
        "function": {
            "name": "meta",
            "description": "Data coverage and the latest available month, plus valid "
                           "dimensions/metrics. Rarely needed: trade() already returns "
                           "meta.period_max.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "reference",
            "description": "Lookup dictionaries. name='countries' for ISO-2 codes and "
                           "Russian names; name='tnved' for HS/ТН ВЭД code names at a "
                           "level. Use only when you do not know a code.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "enum": ["countries", "tnved"]},
                    "level": {"type": "integer", "description": "for tnved: 2,4,6,8,10"},
                    "strana": {"type":"string","description":"ISO2 country for national chapter names, e.g. IN for India chapter 98"},
                    "code": {"type":"string","description":"Exact code whose reference name is requested, e.g. 98; use with level and strana"},
                    "limit": {"type": "integer"},
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trade",
            "description": "Foreign-trade rows or server-side aggregates. Work general "
                           "to specific: aggregate high (tnved2/strana/year) first, then "
                           "drill down. To explain changes, fetch complete child-code aggregates for current and prior matched periods; calculate shares and contribution pp, not just child growth rates. Every response includes meta.period_max.",
            "parameters": {
                "type": "object",
                "properties": {
                    "strana": {**_STR_ARRAY, "description": "ISO-2 partner, e.g. ['CN','IN','TR']"},
                    "napr": {**_STR_ARRAY, "description": "Russia perspective: ex/ЭК FROM Russia; im/ИМ TO Russia. India importing from Russia = ex."},
                    "tnved2": _STR_ARRAY, "tnved4": _STR_ARRAY,
                    "tnved6": _STR_ARRAY, "tnved": _STR_ARRAY,
                    "type": {**_STR_ARRAY, "description": "'fact' or 'pred' (nowcast)"},
                    "source": _STR_ARRAY,
                    "edizm": _STR_ARRAY,
                    "period_from": {"type": "string", "description": "YYYY-MM-DD (month start)"},
                    "period_to": {"type": "string", "description": "YYYY-MM-DD (month start)"},
                    "group_by": {"type": "string", "description": "comma dims: strana,napr,"
                                 "type,source,tnved2/4/6/tnved,edizm,period,year"},
                    "metrics": {"type": "string", "description": "comma: stoim,netto,kol "
                                "(kol needs edizm in group_by)"},
                    "include": {"type": "string", "description": "name cols, e.g. tnved2_name"},
                    "order_by": {"type": "string", "description": "dim or metric, '-' desc"},
                    "limit": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fizob",
            "description": "Physical-volume indices (real volumes with the price effect "
                           "removed) - a project speciality. Filter by strana, tn_level "
                           "(0/2/4/6), tn_code, period.",
            "parameters": {
                "type": "object",
                "properties": {
                    "strana": _STR_ARRAY,
                    "napr": {**_STR_ARRAY, "description": "'ЭК'/'ИМ'"},
                    "tn_level": {**_STR_ARRAY, "description": "'0','2','4','6'"},
                    "tn_code": _STR_ARRAY,
                    "period_from": {"type": "string"},
                    "period_to": {"type": "string"},
                    "order_by": {"type": "string"},
                    "limit": {"type": "integer"},
                },
            },
        },
    },
]

_export_tool = copy.deepcopy(next(t for t in TOOLS if t["function"]["name"] == "trade"))
_export_tool["function"]["name"] = "export_trade"
_export_tool["function"]["description"] = (
    "Prepare monthly XLSX and a short computed summary. Requires period_from and countries or product filter; max 60 months, 1000 rows, 5 MB. Use FIRST for "
    "requests for monthly series over multiple years/countries. Use product code "
    "2710 for petroleum products. Default end is the latest available month; "
    "set end_mode=fixed ONLY if the user explicitly specified an end date. "
    "Missing observations are not zeros. Do not use continent names as country codes.")
_export_tool["function"]["parameters"]["properties"]["end_mode"] = {
    "type": "string", "enum": ["latest", "fixed"]}
TOOLS.append(_export_tool)

TOOLS.append({"type": "function", "function": {
    "name": "compare_trade",
    "description": "FIRST tool for comparing countries, before trade or export_trade. Computes value, mass and physical-volume index (fizob) on common factual periods. For crude oil to India vs China: strana=['IN','CN'], napr='ex', tnved4=['2709'], months=12. Resolves latest month itself. If the user also requests Excel, the chat automatically creates the matching workbook. Do not call export_trade separately.",
    "parameters": {"type":"object", "additionalProperties":False,
        "properties": {
            "strana": {**_STR_ARRAY, "minItems":2, "maxItems":10},
            "napr": {"type":"string", "enum":["ex","im","ЭК","ИМ"]},
            "months": {"type":"integer", "minimum":1, "maximum":60},
            "period_from": {"type":"string"}, "period_to": {"type":"string"},
            "tnved2": _STR_ARRAY, "tnved4": _STR_ARRAY, "tnved6": _STR_ARRAY, "tnved": _STR_ARRAY,
        }, "required":["strana","napr"]}
}})

TOOLS.append({"type":"function","function":{
    "name":"explain_trade",
    "description":"FIRST for explaining year-on-year drivers/structure for one country and HS2/HS4. Calculates parent totals, child shares, changes and contributions pp; no exploratory calls needed. China cars: strana='CN',napr='im',code='8703',months=12. Monetary analysis, not quantity/fizob decomposition. When Excel is requested the service creates it automatically; do not call export_trade again.",
    "parameters":{"type":"object","additionalProperties":False,"properties":{
        "strana":{"type":"string"},"napr":{"type":"string","enum":["im","ex"]},
        "code":{"type":"string"},"months":{"type":"integer","minimum":1,"maximum":12},
        "period_to":{"type":"string"},"source":{"type":"string","enum":["national","comtrade"]}
    },"required":["strana","napr","code"]}}})

_LIST_FIELDS = {
    "strana", "napr", "tnved2", "tnved4", "tnved6", "tnved", "type", "source",
    "edizm", "tn_level", "tn_code",
}
_SCALAR_FIELDS = {"period_from", "period_to", "group_by", "metrics", "include",
                  "order_by", "limit"}
_MAX_RESULT_CHARS = 48000
_MAX_RESULT_ROWS = 200


def _build_params(args: dict) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = []
    for key in _LIST_FIELDS:
        val = args.get(key)
        if val is None:
            continue
        for v in (val if isinstance(val, list) else [val]):
            if v is not None and str(v) != "":
                params.append((key, str(v)))
    for key in _SCALAR_FIELDS:
        val = args.get(key)
        if val is not None and str(val) != "":
            params.append((key, str(val)))
    return params


def _trim(text: str) -> str:
    """Keep meta + a bounded number of rows so tool results stay small."""
    if len(text) <= _MAX_RESULT_CHARS:
        return text
    try:
        obj = json.loads(text)
        data = obj.get("data")
        if isinstance(data, list):
            total = len(data)
            obj["data"] = data[:_MAX_RESULT_ROWS]
            while True:
                obj["_truncated"] = {
                    "returned_rows": len(obj["data"]), "received_rows": total,
                    "instruction": "Partial result. Narrow the period/countries or aggregate by year. Do not infer totals from this subset.",
                }
                result = json.dumps(obj, ensure_ascii=False)
                if len(result) <= _MAX_RESULT_CHARS:
                    return result
                if not obj["data"]:
                    break
                obj["data"].pop()
        return json.dumps({"error": "Result too large; narrow the query."})
    except Exception:
        return json.dumps({"error": "Invalid API response."})


async def execute_tool(client: httpx.AsyncClient, name: str, args: dict) -> str:
    from .request_scope import enforce_scope, confirm_search
    ctx = export_context.get() or {}
    try:
        corrected = enforce_scope(name, args, ctx.get('request_scope', {}))
        args.clear()
        args.update(corrected)
    except ValueError as exc:
        return json.dumps({'error':str(exc), 'instruction':'Correct the query before claiming missing data.'})
    if name == "resolve_trade_route":
        resolved=resolve_trade_route(**args)
        if resolved.get('supported') and resolved.get('strana') and export_context.get() is not None:
            export_context.get()['route_partner']=resolved['strana']
        return json.dumps(resolved, ensure_ascii=False)
    headers = {"Authorization": f"Bearer {settings.service_api_key}",
               "Accept": "application/json"}
    if name == "search_products":
        from .product_search import search_candidates
        level = args.get('level')
        if level is not None and level not in (2,4,6):
            return json.dumps({'error':'level must be 2, 4 or 6'})
        rows=[]
        for n in ([level] if level else [2,4,6]):
            response=await client.get(settings.api_base+'/v1/reference/tnved',params={'level':n},headers=headers,timeout=60)
            response.raise_for_status()
            rows.extend(response.json())
        candidates = search_candidates(args['query'],rows)
        ctx = export_context.get()
        if ctx is not None:
            ctx['product_search_codes'] = [r['code'] for r in candidates]
            confirm_search(ctx.setdefault('request_scope', {}), args['query'], candidates)
        return json.dumps({'query':args['query'],'candidates':candidates,
            'selection_rule':'Lexical matches are candidates, not confirmation. Choose a real code and level matching all requested attributes; explain the scope. If ambiguous, ask one concise clarification and do not request trade data yet. Never sum a parent and its children.'},ensure_ascii=False)
    if name == "product_overview":
        from .product_overview import fetch_overview
        args=dict(args)
        ctx=export_context.get() or {}
        if not args.get('strana') and ctx.get('route_partner'): args['strana']=[ctx['route_partner']]
        result = await fetch_overview(client, settings.api_base, headers, **args)
        return json.dumps(result, ensure_ascii=False)
    if name == "explain_trade":
        return await _compare_trade(client, args, tool_name="explain_trade")
    if name == "compare_trade":
        return await _compare_trade(client, args)
    if name == "meta":
        path, params = "/v1/meta", []
    elif name == "reference":
        sub = args.get("name") or "countries"
        if sub not in ("countries", "tnved"):
            return json.dumps({"error": f"unknown reference '{sub}'"})
        path = f"/v1/reference/{sub}"
        params = []
        if args.get("level") is not None:
            params.append(("level", str(args["level"])))
        if args.get("limit") is not None:
            params.append(("limit", str(args["limit"])))
        if sub=='tnved' and args.get('strana'):
            params.append(('strana',args['strana'].upper()))
    elif name == "export_trade":
        if export_context.get() is not None and export_context.get().get('wants_export') is False:
            return await execute_tool(client, 'trade', args)
        return await _export_trade(client, args, headers)
    elif name in ("trade", "fizob"):
        args = dict(args)
        ctx = export_context.get() or {}
        if name=='fizob' and not args.get('strana') and re.search(r'(ключев|основн)\w*\s+стран',ctx.get('question',''),re.I):
            # Resolve "key countries" from complete value aggregates, rather
            # than requesting an arbitrary truncated world index page.
            ranking={k:v for k,v in args.items() if k in ('napr','period_from','period_to')}
            levels=args.get('tn_level') or []
            codes=args.get('tn_code') or []
            if len(levels)==1 and levels[0] in ('2','4','6') and codes:
                ranking['tnved'+levels[0]]=codes
            ranking.update(group_by='strana',metrics='stoim',type=['fact'],limit=1000,order_by='-stoim')
            response=await client.get(settings.api_base+'/v1/trade',headers=headers,params=_build_params(ranking),timeout=60)
            response.raise_for_status()
            ranked=response.json()
            if not ranked.get('meta',{}).get('has_more'):
                args['strana']=[r['strana'] for r in ranked.get('data',[])[:3]]
                args['limit']=1000
        if name == 'trade':
            args = physical_evidence_args(args, ctx.get('question', ''))
        if (name == 'trade' and ctx.get('wants_ranking')
                and set(args.get('group_by', '').split(',')) <= {'tnved2','type','source'} and 'tnved2' in args.get('group_by','')
                and args.get('metrics', 'stoim') == 'stoim'
                                and args.get('type', ['fact']) == ['fact']):
            return await _rank_chapters(client, args, headers)
        if name == 'trade' and args.get('tnved4') == ['8703']:
            metrics = (args.get('metrics') or 'stoim,netto').split(',')
            dims = (args.get('group_by') or 'strana,period').split(',')
            for d in ('strana','type','source','edizm','edizm_iso'):
                if d not in dims: dims.append(d)
            if 'kol' not in metrics: metrics.append('kol')
            if args.get('include'):
                for label in args['include'].split(','):
                    dim = 'strana' if label=='country_name' else label.replace('_name','')
                    if dim in ('tnved','tnved2','tnved4','tnved6') and dim not in dims: dims.append(dim)
            args.update(group_by=','.join(dims),metrics=','.join(metrics))
        if args.get("group_by") or name == "fizob":
            args.setdefault("limit", 1000)
            if args.get("group_by", "").strip() == "strana":
                args["limit"] = 1000
        if name == 'trade' and args.get('group_by'):
            dims = args['group_by'].split(',')
            for dim in ('type', 'source'):
                if dim not in dims: dims.append(dim)
            args['group_by'] = ','.join(dims)
        path, params = f"/v1/{name}", _build_params(args)
    else:
        return json.dumps({"error": f"unknown tool '{name}'"})

    try:
        r = await client.get(f"{settings.api_base}{path}", params=params,
                             headers=headers, timeout=60)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": f"request failed: {e}"})
    if r.status_code >= 400:
        return json.dumps({"error": f"HTTP {r.status_code}", "detail": r.text[:400]})
    obj = r.json()
    if name=='reference' and args.get('name')=='tnved' and args.get('code'):
        rows=[row for row in obj if row.get('code')==args['code']]
        facts={}
        for row in rows:
            text=f"Код {row['code']}"+(f", национальная номенклатура {args['strana'].upper()}" if args.get('strana') else '')+f": {row['name']}."
            facts[hashlib.sha256(text.encode()).hexdigest()[:16]]={'kind':'reference','text':text}
        return json.dumps({'data':rows,'numeric_facts':facts},ensure_ascii=False)
    if name=='reference':
        return json.dumps({'data':obj},ensure_ascii=False)
    if name == 'fizob' and not obj.get('meta',{}).get('has_more'):
        baseline=[]
        previous=index_baseline_filters(args,obj.get('data',[]))
        if previous:
            try:
                response=await client.get(f"{settings.api_base}/v1/fizob",
                    params=_build_params(previous),headers=headers,timeout=60)
                response.raise_for_status()
                previous_data=response.json()
                if not previous_data.get('meta',{}).get('has_more'):
                    baseline=previous_data.get('data',[])
            except (httpx.HTTPError,ValueError):
                pass  # Current observations remain usable; facts disclose absent YoY.
        obj['numeric_facts']=numerical_facts(obj.get('data',[]),args,baseline)
        return json.dumps({'meta':obj.get('meta',{}),'numeric_facts':obj['numeric_facts']},ensure_ascii=False)
    if name == "trade" and export_context.get() is not None:
        dims = (args.get("group_by") or "").split(",")
        if export_context.get().get("wants_export") and "period" in dims and (len(obj.get("data", [])) > 40 or obj.get("meta", {}).get("has_more")):
            return await _export_trade(client, args, headers)
    if name == 'trade' and not obj.get('meta',{}).get('has_more') and len(obj.get('data',[])) <= 40:
        obj.update(await _monthly_analysis(client,args,obj.get('data',[]),headers))
        obj.update(trade_evidence(obj))
        enriched = json.dumps(obj,ensure_ascii=False)
        if len(enriched)<=48000: return enriched
        if obj.get("numeric_facts"): return json.dumps({"meta":obj.get("meta",{}),"numeric_facts":obj["numeric_facts"]},ensure_ascii=False)
    if name in ('trade','fizob') and not obj.get('meta',{}).get('has_more'):
        filters=dict(args)
        filters.setdefault('period_to',obj.get('meta',{}).get('period_max'))
        obj['numeric_facts']=numerical_facts(obj.get('data',[]), filters)
        if obj['numeric_facts']:
            return json.dumps({'meta':obj.get('meta',{}),'numeric_facts':obj['numeric_facts']},ensure_ascii=False)
    return _trim(r.text)


async def _rank_chapters(client, args, headers):
    """Calculate a top-N from complete chapter aggregates; never hide genuine truncation."""
    from decimal import Decimal
    params = dict(args, type=['fact'], group_by='tnved2', metrics='stoim',
                  include='tnved2_name', order_by='-stoim', limit=1000)
    try:
        response = await client.get(settings.api_base+'/v1/trade', params=_build_params(params), headers=headers, timeout=60)
        response.raise_for_status()
        obj = response.json()
        rows = obj.get('data', [])
        if obj.get('meta', {}).get('has_more') or obj.get('_truncated'):
            raise ValueError('Incomplete chapter universe')
        if any(r.get('stoim') is None for r in rows):
            raise ValueError('Missing chapter value')
        codes = [r.get('tnved2') for r in rows]
        if len(set(codes)) != len(codes): raise ValueError('Duplicate chapter')
        rows = sorted(rows, key=lambda r: (-Decimal(str(r['stoim'])), str(r['tnved2'])))
        total = sum((Decimal(str(r['stoim'])) for r in rows), Decimal(0))
        n = max(1, min(40, int(args.get('limit') or 10)))
        top = [{**r, 'share_pct': float(Decimal(str(r['stoim']))/total*100) if total>0 else None} for r in rows[:n]]
        top_total = sum((Decimal(str(r['stoim'])) for r in rows[:n]), Decimal(0))
        coverage_params = dict(params, group_by='period,source', include=None, order_by='period')
        coverage_response = await client.get(settings.api_base+'/v1/trade', params=_build_params(coverage_params),headers=headers,timeout=60)
        coverage_response.raise_for_status()
        coverage = coverage_response.json()
        if coverage.get('meta', {}).get('has_more'): raise ValueError('Incomplete monthly coverage')
        months = sorted({str(r['period'])[:7] for r in coverage.get('data', [])})
        result={'meta': {**obj.get('meta', {}), 'has_more': False}, 'data': top,
            'ranking': {'complete_universe':True,'total_groups':len(rows),'shown_groups':len(top),
                'total_stoim':str(total),'other_groups_stoim':str(total-top_total),
                'top_share_pct':float(top_total/total*100) if total>0 else None,
                'filters':params,'fact_months':months,
                'sources':sorted({r['source'] for r in coverage.get('data',[])})},
            'instruction':'Это запрошенный топ из полного списка групп, а не усечение API. Итог и доли рассчитаны по всем группам. Покажи лидеров с названиями и кодами, стоимостью и долями; назови фактические месяцы из fact_months. Это данные за доступную часть года, не за весь календарный год. Рейтинг по факту. Отсутствие месяца не равно нулю. Общая сумма относится к выбранным фильтрам. В этом денежном рейтинге физические показатели не анализируются: не выводи примечания о массе, количестве или единицах из quality_flags. Не утверждай ни ошибочность, ни гарантированную достоверность стоимости на основании физических флагов. Сами флаги сохранены для запросов о соответствующих показателях.'}
        result['numeric_facts']=ranking_facts(result,params)
        return json.dumps(result,ensure_ascii=False)
    except (ValueError, KeyError, httpx.HTTPError) as exc:
        ctx = export_context.get()
        if ctx is not None:
            ctx['error_code']='ranking_unavailable'
            ctx['answer']='Не удалось получить полный список групп для рейтинга. Повторите запрос позже.'
        return json.dumps({'error':'Ranking unavailable','code':type(exc).__name__})


async def _monthly_analysis(client, args, rows, headers):
    """Fetch 12 baseline months privately; never expand displayed/exported rows."""
    evidence = monthly_evidence(rows)
    evidence["numeric_facts"] = numerical_facts(rows, args)
    start = args.get('period_from')
    if not start or 'period' not in (args.get('group_by') or '').split(',') or not rows:
        return evidence
    try:
        from datetime import date
        d = date.fromisoformat(str(start)[:7]+'-01')
        month = d.year*12+d.month-1
        def label(m): return f'{m//12:04d}-{m%12+1:02d}-01'
        previous = dict(args,period_from=label(month-12),period_to=label(month-1),limit=1000,type=['fact'])
        previous.pop('offset',None)
        r = await client.get(f"{settings.api_base}/v1/trade", params=_build_params(previous),headers=headers,timeout=60)
        r.raise_for_status()
        data = r.json()
        if data.get('meta',{}).get('has_more') or not isinstance(data.get('data'),list):
            raise ValueError('Incomplete baseline')
        evidence = monthly_evidence(rows,data['data'])
        evidence['numeric_facts'] = numerical_facts(rows, args, data['data'])
        evidence['baseline_period'] = {'from':previous['period_from'],'to':previous['period_to']}
        evidence['display_rule'] = 'Show only requested current rows. Baseline months are calculation inputs, not additional displayed months or current totals. Use stoim_yoy_pct and kol_yoy_pct; mass in netto_thousand_tonnes (thousand metric tonnes).'
    except (ValueError,httpx.HTTPError):
        evidence['baseline_note'] = 'Previous-year baseline could not be loaded completely. Do not invent YoY changes; current data are still available.'
    return evidence


def _export_explanation_args(filters):
    """Only explain a slice the decomposition tool can represent exactly."""
    countries=filters.get('strana') or []
    directions=filters.get('napr') or []
    codes=[(k,filters[k]) for k in ('tnved2','tnved4','tnved6','tnved') if filters.get(k)]
    if len(countries)!=1 or len(directions)!=1 or len(codes)!=1: return None
    key,values=codes[0]
    if key not in ('tnved2','tnved4') or len(values)!=1: return None
    if filters.get('edizm') or filters.get('edizm_iso'): return None
    sources=[v for v in filters.get('source',[]) if v!='nowcast']
    if len(sources)>1: return None
    if filters.get('type') and 'fact' not in filters['type']: return None
    try:
        def mid(s):
            y,m=map(int,str(s)[:7].split('-'));return y*12+m
        months=mid(filters['period_to'])-mid(filters['period_from'])+1
    except (KeyError,ValueError,TypeError): return None
    if not 1<=months<=12: return None
    return dict(strana=countries[0],napr=directions[0],code=values[0],months=months,
                period_to=filters['period_to'],source=sources[0] if sources else ('national' if countries[0] in ('CN','IN','TR') else 'comtrade'))


async def _attach_export_explanation(client, filters, ctx):
    if ctx.get('export_only'):
        return
    args=_export_explanation_args(filters)
    if not args or ctx.get('export_explanation_in_progress'): return
    previous=ctx.get('wants_export')
    prior_answer,prior_error=ctx.get('answer'),ctx.get('error_code')
    ctx['export_explanation_in_progress']=True
    ctx['wants_export']=False
    try:
        result=json.loads(await _compare_trade(client,args,tool_name='explain_trade'))
        evidence=ctx.setdefault('analysis_evidence',{})
        evidence['explanation']=result
        if result.get('available') is False:
            evidence['incomplete_period_answer_rule'] = 'ОБЯЗАТЕЛЬНО начни ответ: «Выгрузка содержит доступные записи за запрошенное окно. Полный фактический итог за 12 месяцев не установлен». Затем назови отсутствующие месяцы. Сумму называй только «В доступных фактических записях отражено ...». Запрещены выводы о масштабе торговли (мало, крайне мало, редко, незначительно) по этому неполному периоду. Явно сообщи, что доли и вклады год к году не рассчитаны из-за несопоставимых периодов. Нулевые строки назови существующими записями со стоимостью 0, не пропусками. Не называй выгрузку полными 12 месяцами факта.'
        evidence['explanation_instruction']='Начни с итога и 2-3 главных подкатегорий: названия с кодами, доли и рассчитанные вклады в изменение стоимости год к году. Используй числа explanation, не пересчитывай их. При available=false или error не придумывай разложение. Excel содержит запрошенный помесячный ряд; не утверждай, что в нем есть подкатегории или прошлый год, если их нет в file_manifest. Национальный источник сам по себе не доказывает, что масса и количество непосредственно наблюдаемые, а не оценки.'
    except Exception:
        ctx.setdefault('analysis_evidence',{})['explanation_note']='Разбор подкатегорий недоступен; файл создан. Не придумывай драйверы.'
    finally:
        ctx['wants_export']=previous
        ctx['answer']=prior_answer
        if prior_error is None: ctx.pop('error_code',None)
        else: ctx['error_code']=prior_error
        ctx.pop('export_explanation_in_progress',None)


async def _export_trade(client, args, headers):
    ctx = export_context.get()
    if not ctx:
        return json.dumps({"error": "Export requires a chat session."})
    if ctx.get("completed_export"):
        return json.dumps(ctx["completed_export"], ensure_ascii=False)
    allowed = _LIST_FIELDS | {"period_from", "period_to", "group_by", "metrics", "include", "end_mode", "compare_yoy"}
    body = {k: v for k, v in args.items() if k in allowed and v is not None}
    body["inline"] = True
    # Cars are naturally counted in items; retain units in the API grouping.
    if body.get('tnved4') == ['8703']:
        metrics = (body.get('metrics') or 'stoim,netto').split(',')
        if 'kol' not in metrics: metrics.append('kol')
        dims = (body.get('group_by') or 'strana,period').split(',')
        for dim in ('edizm', 'edizm_iso'):
            if dim not in dims: dims.append(dim)
        body.update(metrics=','.join(metrics), group_by=','.join(dims))
    labels = [v.strip() for v in (body.get("include") or "").split(",") if v.strip()]
    if "country_name" not in labels: labels.append("country_name")
    # A single filtered code can safely be added without splitting aggregates.
    dims = [v.strip() for v in body.get("group_by", "strana,period").split(",") if v.strip()]
    required = {"country_name": "strana", "tnved2_name": "tnved2", "tnved4_name": "tnved4", "tnved6_name": "tnved6"}
    kept = []
    for label in labels:
        dim = required.get(label, "tnved")
        if dim in dims or dim == "strana":
            kept.append(label)
        elif len(body.get(dim) or []) == 1:
            dims.append(dim)
            kept.append(label)
    body["group_by"] = ",".join(dict.fromkeys(dims))
    body["include"] = ",".join(kept)
    # Identity comes from the signed chat session, never from model arguments.
    headers = {**headers, "X-Export-User-Id": str(ctx["user_id"])}
    try:
        response = await client.post(f"{settings.api_base}/v1/exports/trade",
                                     json=body, headers=headers, timeout=90)
        if response.is_error:
            ctx["error_code"] = f"export_http_{response.status_code}"
            try:
                detail = response.json().get("detail", "")
            except ValueError:
                detail = ""
            logging.getLogger(__name__).warning("Export rejected: status=%s detail=%s", response.status_code, str(detail)[:500])
        if response.status_code == 429:
            retry = response.headers.get("Retry-After", "")
            ctx["answer"] = ("Лимит выгрузок временно исчерпан. За последние 24 часа можно создать до 10 файлов, "
                             "суммарно до 5 000 строк и 20 МБ. Новый файл не создан. "
                             "Ранее созданные файлы доступны по прежним ссылкам.")
            if retry.isdigit():
                ctx["answer"] += f" Повторите запрос примерно через {max(1, (int(retry)+59)//60)} мин."
            return json.dumps({"error": "Export rate or volume limit reached", "retry_after": retry})
        if response.status_code == 400:
            messages = {
                "period_from is required; exports span at most 60 months": "Укажите начальный месяц выгрузки.",
                "Export period must span 1 to 60 months": "Период выгрузки должен составлять от 1 до 60 месяцев.",
                "Select countries or a product group for export": "Укажите страну или товарную группу.",
                "Invalid export period": "Не удалось распознать даты выгрузки.",
            }
            ctx["answer"] = messages.get(str(detail), "Не удалось согласовать параметры выгрузки. Это ошибка подготовки запроса, а не отсутствие данных.") + " Файл не создан."
            return json.dumps({"error": "Invalid export filters", "detail": response.text[:400]})
        if response.status_code == 413:
            ctx["answer"] = "Запрос превышает размер выгрузки. Полный файл не создан. Сузьте страны, товары или период; по обрезанным данным итоги не рассчитаны."
            return json.dumps({"error": "Incomplete result; no export created."})
        if response.status_code == 404:
            ctx["answer"] = "По выбранным фильтрам нет наблюдений. Это не означает отсутствие торговли."
            return json.dumps({"rows": 0})
        response.raise_for_status()
        obj = response.json()
        if not obj["meta"].get("complete") or obj["meta"].get("has_more"):
            raise ValueError("Incomplete export")
        saved = await save_export(obj["data"], obj["meta"], obj["filters"], prepared=obj)
        if len(obj["data"])<=40 and not ctx.get('export_only'):
            ctx.setdefault('analysis_evidence', {}).update(await _monthly_analysis(client,obj['filters'],obj['data'],headers))
        ctx["completed_export"] = saved
        await _attach_export_explanation(client, obj["filters"], ctx)
        return json.dumps(saved)
    except Exception as exc:
        ctx["error_code"] = "export_failed"
        ctx["answer"] = "Не удалось создать Excel из-за сбоя сервиса. Повторите запрос позже."
        return json.dumps({"error": "Export failed", "code": type(exc).__name__})



def summarize_tool_result(name: str, args: dict, result: str) -> str:
    """Short, human-readable line for the transparency panel ('which data')."""
    rows = None
    try:
        obj = json.loads(result)
        if isinstance(obj.get("data"), list):
            rows = len(obj["data"])
    except Exception:
        pass
    bits = []
    for k in ("strana", "tnved4", "tnved2", "tnved", "napr", "group_by",
              "period_from", "period_to", "name", "tn_level"):
        if args.get(k):
            v = args[k]
            bits.append(f"{k}={','.join(v) if isinstance(v, list) else v}")
    tail = f" -> {rows} rows" if rows is not None else ""
    return f"{name}: {' '.join(bits)}{tail}".strip()


async def _compare_trade(client, args, tool_name="compare_trade"):
    """Use the deployed MCP implementation; do not maintain a second calculator."""
    headers = {"Authorization": f"Bearer {settings.service_api_key}",
               "Accept": "application/json, text/event-stream"}
    seq = 0
    async def rpc(method, params):
        nonlocal seq
        seq += 1
        r = await client.post(settings.mcp_url, headers=headers,
            json={"jsonrpc":"2.0","id":seq,"method":method,"params":params},timeout=90)
        r.raise_for_status()
        if r.headers.get("mcp-session-id"):
            headers["mcp-session-id"] = r.headers["mcp-session-id"]
        if "text/event-stream" in r.headers.get("content-type", ""):
            # Split SSE lines on LF, not unicode whitespace inside numeric strings.
            events = r.content.decode("utf-8").replace("\r\n", "\n").split("\n\n")
            values = [json.loads("\n".join(line[5:].lstrip() for line in event.split("\n") if line.startswith("data:")))
                      for event in events if any(line.startswith("data:") for line in event.split("\n"))]
            obj = next(v for v in values if v.get("id") == seq)
        else:
            obj = r.json()
        if obj.get("error"): raise ValueError("MCP RPC failed")
        return obj["result"]
    ctx = export_context.get()
    try:
        await rpc("initialize", {"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"mgimo-demo","version":"1"}})
        r = await client.post(settings.mcp_url,headers=headers,
            json={"jsonrpc":"2.0","method":"notifications/initialized"},timeout=30)
        r.raise_for_status()
        allowed={"strana","napr","months","period_from","period_to","tnved2","tnved4","tnved6","tnved"}
        body={k:v for k,v in args.items() if k in allowed}
        body["include_fizob"]=True
        if tool_name == "explain_trade": body={k:v for k,v in args.items() if k in {"strana","napr","code","months","period_to","source"}}
        if tool_name == 'explain_trade':
            import asyncio
            from .decomposition import explain
            def local_explanation():
                with httpx.Client() as read_client:
                    return explain(read_client, settings.api_base,
                        {'Authorization':f'Bearer {settings.service_api_key}'}, **body)
            local_obj = await asyncio.to_thread(local_explanation)
            result = {'content':[{'type':'text','text':json.dumps(local_obj)}]}
        else:
            result = await rpc("tools/call", {"name":tool_name,"arguments":body})
        if result.get("isError"): raise ValueError("Comparison tool failed")
        obj=json.loads(next(item["text"] for item in result["content"] if item.get("type")=="text"))
        if tool_name == "explain_trade":
            if obj.get("error"): raise ValueError("Decomposition unavailable")
            from .answer_style import explanation_facts
            obj['numeric_facts'] = explanation_facts(obj)
            if ctx is not None:
                ctx.setdefault('numeric_facts', {}).update(obj['numeric_facts'])
            if ctx is not None and ctx.get("wants_export"):
                if not obj.get("available"):
                    # Export the requested windows as partial observations, without inventing a complete YoY result.
                    end = str(body.get("period_to") or ctx["period_max"])[:7]
                    year, month = map(int, end.split("-"))
                    last = year * 12 + month - 1
                    first = last - int(body.get("months", 12)) + 1
                    def month_date(value): return f"{value//12:04d}-{value%12+1:02d}-01"
                    obj.update(country=body["strana"].upper(),
                        source=body.get("source") or ("national" if body["strana"].upper() in ("CN","IN","TR") else "comtrade"),
                        current_period=[month_date(first),month_date(last)],
                        previous_period=[month_date(first-12),month_date(last-12)],
                        coverage_warning="Полного сопоставимого периода нет. Excel содержит только доступные наблюдения. Не рассчитывай и не называй полный итог, рост год к году, доли или вклады. Назови отсутствующие месяцы и ограничения физических показателей.")
                code = str(body["code"])
                dimension = "tnved4" if len(code) == 2 else "tnved6"
                export_args = {"strana": [obj["country"]], "napr": [body["napr"]],
                    "tnved" + str(len(code)): [code],
                    "source": [obj["source"], "nowcast"], "type": ["fact", "pred"],
                    "period_from": obj["current_period"][0], "period_to": obj["current_period"][1],
                    "end_mode": "fixed", "compare_yoy": True,
                    "group_by": dimension + ",period,edizm,edizm_iso", "metrics": "stoim,netto,kol",
                    "include": dimension + "_name"}
                ctx["export_explanation_in_progress"] = True
                exported = json.loads(await _export_trade(client, export_args,
                    {"Authorization": f"Bearer {settings.service_api_key}"}))
                ctx.pop("export_explanation_in_progress", None)
                if exported.get("export_ready"):
                    ctx["analysis_evidence"] = {"explanation": obj,
                        "file_manifest": exported.get("file_manifest"),
                        "coverage": exported.get("coverage"), "coverage_note": exported.get("coverage_note"),
                        "instruction": "Объясни динамику по explanation. Excel содержит исходные строки обоих периодов на листе Факт, оценки проекта отдельно на Nowcast, методологию на Описание. Не утверждай, что в файле есть расчетные доли и вклады. Названия можно сократить, но указывай точный код. Не называй ненулевые оценки нулевыми. Не суммируй оба периода."}
                return json.dumps(exported, ensure_ascii=False)
            return json.dumps(obj,ensure_ascii=False)
        if obj.get("error") or not isinstance(obj.get("answer_ru"),str): raise ValueError("Comparison unavailable")
        if ctx is not None:
            ctx["answer"] = obj["answer_ru"]
            ctx["analysis_evidence"] = obj
            ctx["verified_comparison_answer"] = obj["answer_ru"]
        if ctx is not None and ctx.get('wants_export'):
            # Reuse the resolved requested window and filters, not rounded totals
            # or a second interpretation by the model. File calculation is authoritative.
            export_args = {k:v for k,v in body.items() if k in {'strana','tnved2','tnved4','tnved6','tnved'}}
            export_args.update(napr=[body['napr']], metrics='stoim,netto',
                               group_by='strana,period', end_mode='fixed',
                               period_from=obj['requested_period']['from'],
                               period_to=obj['requested_period']['to'])
            exported = json.loads(await _export_trade(client, export_args,
                {'Authorization': f'Bearer {settings.service_api_key}'}))
            if not exported.get('export_ready'):
                failure = ctx.get('answer')
                if failure == obj['answer_ru']:
                    failure = 'Excel не удалось создать. Повторите запрос выгрузки позже.'
                ctx['answer'] = obj['answer_ru'] + '\n\n' + failure
                ctx['verified_comparison_answer'] = ctx['answer']
            return json.dumps(exported)
        return json.dumps({"comparison_ready":True,"common_month_count":obj.get("common_month_count"),
                           "fizob_available":obj.get("physical_volume",{}).get("available",False)})
    except Exception as exc:
        logging.getLogger(__name__).exception("MCP comparison failed (tool=%s)", tool_name)
        if ctx is not None:
            ctx["error_code"]="comparison_failed"
            ctx["answer"]="Произошла техническая ошибка при подготовке сравнения. Повторите запрос позже."
        return json.dumps({"error":"Comparison unavailable","code":type(exc).__name__})
    finally:
        if headers.get("mcp-session-id"):
            try: await client.delete(settings.mcp_url, headers=headers, timeout=10)
            except httpx.HTTPError: pass

TOOLS.append({"type":"function","function":{
    "name":"resolve_trade_route", "description":"Resolve origin and destination before queries expressed from a foreign importer perspective. Russia=RU; unknown=UNKNOWN; worldwide=WORLD. Returns Russia-perspective strana/napr or scope clarification.",
    "parameters":{"type":"object","properties":{"origin":{"type":"string"},"destination":{"type":"string"},"question":{"type":"string","description":"Verbatim user question, required to validate perspective"}},"required":["origin","destination","question"],"additionalProperties":False}}})
