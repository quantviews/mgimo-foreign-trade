import asyncio
import json
from pathlib import Path
import httpx

from app.tools import execute_tool, TOOLS
from app.product_search import search_candidates


def test_search_matches_mcp():
    root=Path(__file__).resolve().parents[1]
    assert (root/'app/product_search.py').read_text(encoding='utf-8') == (root.parent/'mcp-server/product_search.py').read_text(encoding='utf-8')


def test_search_tool_uses_reference_and_preserves_alternatives():
    levels=[]
    def handle(req):
        assert req.url.path.endswith('/v1/reference/tnved')
        levels.append(int(req.url.params['level']))
        rows=[{'code':'6107','name':'БЕЛЬЕ МУЖСКОЕ ТРИКОТАЖНОЕ'}, {'code':'6108','name':'БЕЛЬЕ ЖЕНСКОЕ ТРИКОТАЖНОЕ'}, {'code':'6302','name':'БЕЛЬЕ ПОСТЕЛЬНОЕ'}] if req.url.params['level']=='4' else []
        return httpx.Response(200,json=rows)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as c:
            specific=json.loads(await execute_tool(c,'search_products',{'query':'мужское трикотажное белье'}))
            assert specific['candidates'][0]['code']=='6107'
            broad=json.loads(await execute_tool(c,'search_products',{'query':'белье','level':4}))
            assert len(broad['candidates'])==3
            assert 'ambiguous' in broad['selection_rule']
    asyncio.run(run())
    assert levels==[2,4,6,4]
    assert any(t['function']['name']=='search_products' for t in TOOLS)


def test_unverified_codes_in_clarification_are_not_published(monkeypatch):
    from app import deepseek
    from unittest.mock import AsyncMock
    client_class=httpx.AsyncClient
    monkeypatch.setattr(deepseek.httpx,'AsyncClient',lambda:client_class(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'data':{'period_max':'2026-08-01'}}))))
    completion=AsyncMock(return_value={'content':'Постельное белье (2101/6302)?'})
    monkeypatch.setattr(deepseek,'_completion',completion)
    async def run():
        return [e async for e in deepseek.run_agent([{'role':'user','content':'импорт белья'}])]
    result=asyncio.run(run())
    answer=next(e['text'] for e in result if e['type']=='answer')
    assert '2101' not in answer and '6302' not in answer
    assert 'Уточните' in answer and completion.await_count==2
