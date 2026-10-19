import asyncio,json
from unittest.mock import AsyncMock,patch
import httpx
from demo.app import main,tools,exports
from demo.tests.test_exports import ExportTests

def test_car_label_group_is_valid():
 req,_,_=ExportTests().export_request({'strana':['CN'],'tnved4':['8703'],'group_by':'period,edizm','include':'tnved4_name','period_from':'2025-09-01'})
 b=json.loads(req.content)
 assert 'tnved4' in b['group_by'].split(',') and 'tnved4_name' in b['include']

def test_error_reason_is_not_replaced_with_period():
 async def check():
  ctx={'user_id':1}; t=exports.export_context.set(ctx)
  try:
   async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(400,json={'detail':"include 'tnved4_name' requires 'tnved4' in group_by"}))) as c:
    await tools._export_trade(c,{}, {})
   assert ctx['error_code']=='export_http_400'
   assert '60' not in ctx['answer']
  finally: exports.export_context.reset(t)
 asyncio.run(check())

def run_chat(events,crash=False):
 async def agent(*a,**k):
  for e in events: yield e
  if crash: raise RuntimeError('test')
 async def run():
  with patch.object(main,'_session',return_value={'uid':7}),patch.object(main.db,'try_consume_turn',new=AsyncMock(return_value=(True,0,'ok'))),patch.object(main.db,'refund_turn',new=AsyncMock(return_value=1)) as refund,patch.object(main.db,'log_chat',new=AsyncMock()),patch.object(main,'run_agent',agent):
   response=await main.chat(None,main.ChatIn(messages=[{'role':'user','content':'test'}]))
   data=[x async for x in response.body_iterator]
   return refund.await_count,data
 return asyncio.run(run())

def test_failed_turn_refunded_once():
 count,data=run_chat([{'type':'error','code':'export_http_400','text':'failed'}])
 assert count==1 and '"remaining": 1' in data[-1]

def test_success_charged():
 assert run_chat([{'type':'answer','text':'ok'}])[0]==0

def test_unexpected_failure_refunded():
 assert run_chat([],True)[0]==1
