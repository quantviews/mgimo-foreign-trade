import asyncio
import json
import sys
from pathlib import Path

import httpx

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from trade_tools_shared.product_search import search_candidates
from trade_tools_shared.product_overview import overview


def test_search_uses_reference_and_keeps_ambiguity():
    rows=[{'code':'6107','name':'БЕЛЬЕ МУЖСКОЕ ТРИКОТАЖНОЕ'},
          {'code':'6108','name':'БЕЛЬЕ ЖЕНСКОЕ ТРИКОТАЖНОЕ'},
          {'code':'6207','name':'БЕЛЬЕ МУЖСКОЕ'}]
    assert search_candidates('мужское трикотажное белье',rows)[0]['code']=='6107'
    assert len(search_candidates('белье',rows))==3
    assert search_candidates('6107',rows)[0]['level']==4
    assert search_candidates('неизвестныйтовар',rows)==[]
    assert search_candidates('белье',[{'code':'4010','name':'БЕЛЬТИНГ ИЗ РЕЗИНЫ'},{'code':'5805','name':'ГОБЕЛЕНЫ БЕЛЬГИЙСКИЕ'}])==[]


def test_mcp_overview_evidence_and_verification(monkeypatch):
    monkeypatch.setenv('MCP_TRANSPORT','stdio')
    import server
    monkeypatch.setattr(server,'_resolve_token',lambda ctx:'test-key')
    client_class=httpx.AsyncClient
    def handle(req):
        if req.url.path.endswith('/meta'):
            return httpx.Response(200,json={'data':{'period_max':'2026-01-01'}})
        if req.url.path.endswith('/tnved'):
            return httpx.Response(200,json=[{'code':'6107','name':'Трикотажное белье'}])
        if req.url.path.endswith('/countries'):
            return httpx.Response(200,json=[{'strana':'CN','country_name':'Китай'}])
        assert req.url.params['type']=='fact'
        assert req.url.params['group_by']=='strana,period,type,source'
        return httpx.Response(200,json={'data':[dict(strana='CN',period=p,type='fact',source='national',stoim=v) for p,v in [('2025-01',100),('2026-01',120)]],'meta':{'has_more':False}})
    monkeypatch.setattr(server.httpx,'AsyncClient',lambda:client_class(transport=httpx.MockTransport(handle)))
    result=json.loads(asyncio.run(server.product_overview('6107','im')))
    draft='\n\n'.join('{{fact:'+k+'}}' for k in result['numeric_facts'])
    checked=json.loads(server.verify_trade_answer(draft,[result['evidence_id']]))
    assert checked['valid'] and '20,0% г/г' in checked['answer']
    assert not json.loads(server.verify_trade_answer(draft+' Итого 999.',[result['evidence_id']]))['valid']
