import json
from pathlib import Path
from demo.app.query_contract import resolve_trade_route, safe_result
from demo.app import tools, deepseek
from demo.app.exports import export_context

def test_routes():
    for partner in ('CN','IN','US'):
        assert resolve_trade_route('RU',partner)['napr']=='ex'
        assert resolve_trade_route(partner,'RU')['napr']=='im'
    assert not resolve_trade_route('UNKNOWN','US')['supported']
    assert not resolve_trade_route('CN','IN')['supported']

def test_partial_does_not_poison_complete_evidence():
    partial=safe_result({'data':[{'stoim':123}], 'meta':{'has_more':True,'next_offset':100}})
    assert 'data' not in partial
    assert partial['meta']['next_offset']==100
    complete={'data':[{'stoim':456}], 'meta':{'has_more':False}}
    assert safe_result(complete)==complete
    token=export_context.set({'partial':True})
    try:
        assert deepseek._answer({'content':'Проверенный результат'})['type']=='answer'
    finally: export_context.reset(token)

def test_monthly_countries_fit():
    raw=json.dumps({'data':[{'strana':c,'period':f'2025-{m:02}-01','stoim':100,'netto':20,'type':'fact','source':'national'} for c in ('CN','IN','TR') for m in range(1,13)]})
    assert '_truncated' not in json.loads(tools._trim(raw))

def test_shared_contract():
    assert Path('demo/app/query_contract.py').read_bytes()==Path('mcp-server/query_contract.py').read_bytes()

def test_explicit_direction_guard():
    from demo.app.query_contract import explicit_direction
    assert explicit_direction('Динамика импорта нефти из России по партнерам')=='ex'
    assert explicit_direction('Импорт нефти Китаем из России')=='ex'
    assert explicit_direction('Импорт автомобилей из Китая в Россию')=='im'
    assert explicit_direction('Импорт чипов в США') is None
    assert explicit_direction('Импорт и экспорт России') is None

def test_route_validates_foreign_importer_question():
    assert resolve_trade_route('WORLD','RU','Динамика импорта нефти из России')['napr']=='ex'
    assert resolve_trade_route('CN','RU','Импорт нефти Китаем из России')['strana']=='CN'
    assert resolve_trade_route('RU','CN','Импорт автомобилей из Китая в Россию')['napr']=='im'

def test_agent_recovers_after_partial_call():
    import asyncio
    from unittest.mock import AsyncMock, patch
    import httpx
    def msg(n):
        return {'content':'','tool_calls':[{'id':str(n),'type':'function','function':{'name':'trade','arguments':'{}'}}]}
    responses=[msg(1),msg(2),{'content':'Доступный полный срез: 456.'}]
    captured=[]
    async def completion(client,messages,use_tools):
        captured.append(list(messages))
        return responses.pop(0)
    original_client=httpx.AsyncClient
    def factory(*a,**kw):
        return original_client(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'data':{'period_max':'2026-08-01'}})))
    async def run():
        with patch.object(deepseek.httpx,'AsyncClient',factory), patch.object(deepseek,'_completion',completion), patch.object(deepseek,'execute_tool',AsyncMock(side_effect=[json.dumps({'data':[{'stoim':123}],'meta':{'has_more':True}}),json.dumps({'data':[{'stoim':456}],'meta':{'has_more':False}})])):
            return [e async for e in deepseek.run_agent([{'role':'user','content':'Данные'}])]
    events=asyncio.run(run())
    assert events[-1]['type']=='answer'
    first=json.loads(next(m['content'] for m in captured[1] if m['role']=='tool'))
    assert 'data' not in first
