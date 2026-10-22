import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import httpx,pytest
from trade_tools_shared.decomposition import rank_chapters

@pytest.mark.parametrize('partial',[False,True])
def test_full_ranking_and_source_gaps(partial):
 def handle(req):
  if req.url.path.endswith('/meta'):return httpx.Response(200,json={'data':{'period_max':'2026-02-01'}})
  assert req.url.params['type']=='fact'
  assert req.url.params['limit']=='1000'
  assert req.url.params['period_from']=='2026-01-01'
  if req.url.params['group_by']=='period,source':
   return httpx.Response(200,json={'data':[{'period':'2026-01-01','source':'national','stoim':100}],'meta':{'has_more':False}})
  return httpx.Response(200,json={'data':[{'tnved2':'01','tnved2_name':'A','stoim':30},{'tnved2':'02','tnved2_name':'B','stoim':70}],'meta':{'has_more':partial}})
 with httpx.Client(transport=httpx.MockTransport(handle)) as c:
  if partial:
   with pytest.raises(ValueError,match='Incomplete'):rank_chapters(c,'https://test',{},'TR','im',1)
  else:
   r=rank_chapters(c,'https://test',{},'TR','im',1)
   assert r['data'][0]['share_pct']==70
   assert r['ranking']['total_stoim']=='100'
   assert r['ranking']['omitted_groups']==1
   assert r['ranking']['other_groups_stoim']=='30'
   assert r['ranking']['missing_months']==['2026-02']
   assert not r['meta']['has_more']
