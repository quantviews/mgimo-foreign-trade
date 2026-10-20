import pytest
import asyncio,json
import httpx
from demo.app import tools,exports

def test_comparison_uses_mcp_and_preserves_answer():
    seen=[]
    answer='Индекс: 107,4; масса: 112\u00a0756,9. Факт за общие месяцы.'
    def handler(req):
        if req.method=='DELETE':return httpx.Response(200)
        body=json.loads(req.content);seen.append(body)
        if body['method']=='notifications/initialized':return httpx.Response(202)
        if body['method']=='initialize':return httpx.Response(200,json={'result':{},'id':body['id']},headers={'mcp-session-id':'test'})
        result={'content':[{'type':'text','text':json.dumps({'answer_ru':answer,'common_month_count':11,'physical_volume':{'available':True}})}]}
        return httpx.Response(200,content=('event: message\r\ndata: '+json.dumps({'id':body['id'],'result':result},ensure_ascii=False)+'\r\n\r\n').encode(),headers={'content-type':'text/event-stream'})
    async def go():
        state={'user_id':1};tok=exports.export_context.set(state)
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                result=json.loads(await tools.execute_tool(client,'compare_trade',{'strana':['IN','CN'],'napr':'ex','months':12,'user_id':999}))
            assert result['fizob_available'] and state['answer']==answer
        finally:exports.export_context.reset(tok)
    asyncio.run(go())
    params=seen[-1]['params']
    assert params['name']=='compare_trade' and params['arguments']['include_fizob'] is True
    assert 'user_id' not in params['arguments']

def test_mcp_failure_does_not_invent_comparison():
    async def go():
        state={};tok=exports.export_context.set(state)
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda req:httpx.Response(503))) as client:
                result=json.loads(await tools.execute_tool(client,'compare_trade',{'strana':['IN','CN'],'napr':'ex'}))
            assert 'error' in result and 'техническая ошибка' in state['answer']
            assert state['error_code']=='comparison_failed'
        finally:exports.export_context.reset(tok)
    asyncio.run(go())


def test_comparison_export_uses_resolved_window():
    from unittest.mock import patch
    async def go():
        state={'user_id':42,'wants_export':True};tok=exports.export_context.set(state)
        obj={'answer_ru':'Сравнение','requested_period':{'from':'2025-09','to':'2026-08'},'common_month_count':11}
        def handler(req):
            if req.method=='DELETE': return httpx.Response(200)
            body=json.loads(req.content)
            if body['method']=='initialize': return httpx.Response(200,json={'result':{},'id':body['id']})
            if body['method']=='notifications/initialized': return httpx.Response(202)
            return httpx.Response(200,json={'result':{'content':[{'type':'text','text':json.dumps(obj)}]},'id':body['id']})
        async def save(client,args,headers):
            assert args['period_from']=='2025-09' and args['period_to']=='2026-08'
            assert args['end_mode']=='fixed' and args['napr']==['ex']
            assert args['tnved4']==['2709'] and args['metrics']=='stoim,netto'
            state['answer']='Ответ из файла со ссылкой'
            return json.dumps({'export_ready':True})
        try:
            with patch.object(tools,'_export_trade',side_effect=save):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    r=json.loads(await tools._compare_trade(client,{'strana':['IN','CN'],'napr':'ex','tnved4':['2709']}))
            assert r['export_ready'] and state['answer']=='Ответ из файла со ссылкой'
        finally: exports.export_context.reset(tok)
    asyncio.run(go())


def test_chat_answer_is_workbook_comparison():
    from unittest.mock import AsyncMock,patch
    import base64
    async def go():
        state={'user_id':42};tok=exports.export_context.set(state)
        obj={'xlsx_base64':base64.b64encode(b'xlsx').decode(),'summary':[],
             'comparison':{'comparable':True,'answer_ru':'Проверенные значения 49,38 и 53,55; fizob -3,5% и 7,4%'}}
        try:
            with patch.object(exports.db,'_require_pool',new=AsyncMock(return_value=AsyncMock())):
                await exports.save_export([{'strana':'IN'}],{'has_more':False},{},obj)
            assert state['answer'].startswith(obj['comparison']['answer_ru'])
            assert '/demo/exports/' in state['answer']
        finally: exports.export_context.reset(tok)
    asyncio.run(go())

@pytest.mark.parametrize("available", [True, False])
def test_explanation_export_uses_calculator_periods_and_manifest(available):
    from unittest.mock import patch
    async def go():
        state={'user_id':42,'wants_export':True,'period_max':'2026-08-01'};tok=exports.export_context.set(state)
        obj={'available':available,'country':'CN','source':'national','current_period':['2025-09-01','2026-08-01'],
             'previous_period':['2024-09-01','2025-08-01']}
        if available:
            obj.update(parent_code='8703',direction='im',
                       parent={'current_usd':120,'previous_usd':100,'growth_pct':20},
                       children=[],unmatched_codes=[],residual_change_usd=0)
        def handler(req):
            if req.method=='DELETE': return httpx.Response(200)
            body=json.loads(req.content)
            if body['method']=='initialize': return httpx.Response(200,json={'result':{},'id':body['id']})
            if body['method']=='notifications/initialized': return httpx.Response(202)
            return httpx.Response(200,json={'result':{'content':[{'type':'text','text':json.dumps(obj)}]},'id':body['id']})
        async def save(client,args,headers):
            assert args['period_from']==obj['current_period'][0]
            assert args['period_to']==obj['current_period'][1]
            assert args['compare_yoy'] and args['end_mode']=='fixed'
            assert args['source']==['national','nowcast']
            assert args['tnved4']==['8703'] and args['group_by'].startswith('tnved6,period')
            state['answer']='export ready'
            return json.dumps({'export_ready':True,'file_manifest':{'periods':obj['previous_period']}})
        try:
            with patch.object(tools,'_export_trade',side_effect=save):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    result=json.loads(await tools._compare_trade(client,{'strana':'CN','napr':'im','code':'8703'},tool_name='explain_trade'))
            assert result['export_ready']
            assert state['analysis_evidence']['explanation']['available']==available
            if not available: assert state['analysis_evidence']['explanation']['coverage_warning']
            assert state['analysis_evidence']['file_manifest']==result['file_manifest']
        finally: exports.export_context.reset(tok)
    asyncio.run(go())

def test_repeated_export_in_same_turn_reuses_file():
    async def go():
        state={'user_id':42,'completed_export':{'export_ready':True,'rows':278,'file_manifest':{'sheets':{'Факт':253,'Nowcast':25}}}}
        tok=exports.export_context.set(state)
        def handler(req): raise AssertionError('Duplicate export must not call API')
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                result=json.loads(await tools._export_trade(client,{'period_from':'2024-09'},{}))
            assert result==state['completed_export']
        finally: exports.export_context.reset(tok)
    asyncio.run(go())
