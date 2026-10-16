import asyncio,json
from unittest.mock import patch,AsyncMock
import httpx
from demo.app import deepseek,tools,exports
from demo.app.analysis import monthly_evidence

def test_changes_respect_gaps_missing_and_types():
 rows=[dict(strana='CN',type='fact',period=p,kol=v,edizm='шт') for p,v in [('2026-01',100),('2026-02',120),('2026-04',150)]]
 rows.append(dict(strana='CN',type='pred',period='2026-02',kol=200,edizm='шт'))
 r=monthly_evidence(rows)['monthly_changes']
 assert r[1]['kol_mom_pct']==20
 assert r[2]['kol_mom_pct'] is None and r[3]['kol_mom_pct'] is None
 assert r[0]['kol_yoy_pct'] is None

def test_followup_cannot_create_export():
 async def run():
  token=exports.export_context.set({'user_id':1,'wants_export':False})
  try:
   seen=[]
   def handle(r):
    seen.append(r)
    return httpx.Response(200,json={'data':[],'meta':{'has_more':False}})
   async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as c:
    await tools.execute_tool(c,'export_trade',{'strana':['CN'],'tnved4':['8703'],'group_by':'period'})
   assert len(seen)==1 and seen[0].method=='GET' and seen[0].url.path=='/v1/trade'
  finally:exports.export_context.reset(token)
 asyncio.run(run())

def test_export_continues_to_model_analysis():
 async def run():
  async def completion(c,messages,use_tools):
   if use_tools:return {'tool_calls':[{'id':'x','function':{'name':'export_trade','arguments':'{}'}}]}
   assert 'verified_summary' in messages[-1]['content']
   return {'content':'Поставки выросли в феврале на 20%.'}
  async def execute(c,n,a):
   state=exports.export_context.get();state['answer']='Шаблон'
   state['analysis_evidence']={'verified_summary':'Проверенные числа'}
   state['download_link']='[Excel](/demo/exports/test)'
   return '{}'
  client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'data':{'period_max':'2026-08-01'}})))
  with patch.object(deepseek.httpx,'AsyncClient',return_value=client),patch.object(deepseek,'_completion',side_effect=completion),patch.object(deepseek,'execute_tool',side_effect=execute):
   events=[e async for e in deepseek.run_agent([{'role':'user','content':'Дай Excel'}],1)]
  assert '20%' in events[-1]['text'] and '/demo/exports/test' in events[-1]['text']
  assert 'Шаблон' not in events[-1]['text']
 asyncio.run(run())

def test_baseline_yoy_only_requested_rows():
 base=dict(strana='CN',type='fact',source='national',edizm='шт')
 prior=[dict(base,period='2025-01',stoim=100,kol=10,netto=1000000),dict(base,period='2025-12',stoim=150,kol=15)]
 current=[dict(base,period='2026-01',stoim=200,kol=25,netto=1250000)]
 r=monthly_evidence(current,prior)['monthly_changes']
 assert len(r)==1 and r[0]['period']=='2026-01'
 assert r[0]['stoim_yoy_pct']==100 and r[0]['kol_yoy_pct']==150
 assert r[0]['netto_thousand_tonnes']==1.25
 assert r[0]['stoim_mom_pct']==33.3333
 for field,value in [('source','comtrade'),('edizm','кг'),('type','pred')]:
  assert monthly_evidence(current,[dict(prior[0],**{field:value})])['monthly_changes'][0]['kol_yoy_pct'] is None
 assert monthly_evidence(current,[dict(prior[0],kol=0)])['monthly_changes'][0]['kol_yoy_pct'] is None
 assert monthly_evidence(current,[dict(prior[0],kol=None)])['monthly_changes'][0]['kol_yoy_pct'] is None

def test_baseline_fetch_dates_and_partial_failure():
 async def run():
  args=dict(strana=['CN'],tnved4=['8703'],napr=['im'],group_by='strana,period,type,source,edizm',period_from='2025-09-01',period_to='2026-08-01',metrics='stoim,kol,netto')
  rows=[dict(strana='CN',type='fact',source='national',edizm='шт',period='2025-09-01',stoim=200,kol=20)]
  for status,more in [(200,False),(200,True),(503,False)]:
   def handle(r):
    q=r.url.params
    assert q['period_from']=='2024-09-01' and q['period_to']=='2025-08-01'
    assert q['strana']=='CN' and q['tnved4']=='8703' and q['type']=='fact'
    return httpx.Response(status,json={'data':[dict(rows[0],period='2024-09-01',stoim=100,kol=10)],'meta':{'has_more':more}})
   async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as c:
    evidence=await tools._monthly_analysis(c,args,rows,{})
   if status==200 and not more:
    assert evidence['monthly_changes'][0]['kol_yoy_pct']==100
   else:
    assert 'baseline_note' in evidence
    assert evidence['monthly_changes'][0]['kol_yoy_pct'] is None
   assert len(evidence['monthly_changes'])==1
 asyncio.run(run())
