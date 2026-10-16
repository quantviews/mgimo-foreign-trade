import asyncio,json,base64
from unittest.mock import AsyncMock,patch
import httpx
from demo.app import deepseek,exports

def test_invalid_markup_retried_before_display():
 async def run():
  token=exports.export_context.set({})
  calls=[]
  def handle(req):
   calls.append(json.loads(req.content))
   return httpx.Response(200,json={'choices':[{'message':{'content':'<DSML calls>' if len(calls)==1 else 'Проверенный ответ'},'finish_reason':'stop'}]})
  try:
   async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
    result=await deepseek._completion(client,[{'role':'user','content':'Вопрос'}],False)
   assert result['content']=='Проверенный ответ' and len(calls)==2
  finally: exports.export_context.reset(token)
 asyncio.run(run())

def test_missing_measures_not_reported_as_complete():
 async def run():
  token=exports.export_context.set({'user_id':42})
  rows=[dict(strana='IN',type='fact',source='national',comparison_period='current',period='2026-07-01',stoim=100,kol=None,netto=None),
        dict(strana='IN',type='pred',source='nowcast',comparison_period='current',period='2026-07-01',stoim=10,kol=1,netto=2)]
  prepared={'xlsx_base64':base64.b64encode(b'xlsx').decode(),'summary':[],
      'file_manifest':{'periods':{'current':['2025-08','2026-07'],'previous':['2024-08','2025-07']}}}
  try:
   with patch.object(exports.db,'_require_pool',new=AsyncMock(return_value=AsyncMock())):
    result=await exports.save_export(rows,{}, {},prepared)
   c=result['coverage']['current']
   assert c['rows']==1 and c['missing_quantity_rows']==1 and c['missing_mass_rows']==1
   assert c['sources']==['national']
  finally:exports.export_context.reset(token)
 asyncio.run(run())
