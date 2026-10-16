import asyncio,json
import httpx,pytest
from demo.app import tools,exports

@pytest.mark.parametrize('has_more',[False,True])
def test_ranking_uses_full_denominator_and_rejects_real_partial(has_more):
 async def run():
  ctx={'wants_ranking':True};token=exports.export_context.set(ctx)
  def handle(req):
   assert req.url.params['type']=='fact' and req.url.params['limit']=='1000'
   if req.url.params['group_by']=='period,source':
    return httpx.Response(200,json={'meta':{'has_more':False},'data':[{'period':'2026-08-01','source':'national'}]})
   return httpx.Response(200,json={'meta':{'has_more':has_more},'data':[
    {'tnved2':'01','tnved2_name':'A','stoim':20}, {'tnved2':'02','tnved2_name':'B','stoim':80}]})
  try:
   async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as c:
    result=json.loads(await tools.execute_tool(c,'trade',{'strana':['TR'],'group_by':'tnved2','metrics':'stoim','order_by':'-stoim','limit':1}))
   if has_more:
    assert result['error'] and ctx['error_code']=='ranking_unavailable'
   else:
    assert result['data'][0]['tnved2']=='02'
    assert result['data'][0]['share_pct']==80
    assert result['ranking']['total_stoim']=='100'
    assert result['ranking']['other_groups_stoim']=='20'
    assert result['ranking']['total_groups']==2 and not result['meta']['has_more']
  finally:exports.export_context.reset(token)
 asyncio.run(run())
