import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pytest,httpx
from decomposition import decompose,explain

def test_contributions_reconcile_with_offsetting_children():
 r=decompose([{'tnved6':'a','stoim':140},{'tnved6':'b','stoim':60}],
             [{'tnved6':'a','stoim':50},{'tnved6':'b','stoim':100}],200,150,'tnved6')
 assert r['decomposition_complete']
 assert sum(x['contribution_pp'] for x in r['children'])==pytest.approx(r['parent']['growth_pct'])
 assert r['children'][0]['contribution_pp']==60
 assert r['children'][1]['contribution_pp']<0
 assert sum(x['current_share_pct'] for x in r['children'])==100

def test_missing_child_is_unallocated_not_zero():
 r=decompose([{'tnved6':'a','stoim':120}],[{'tnved6':'a','stoim':100},{'tnved6':'b','stoim':10}],120,110,'tnved6')
 assert r['unmatched_codes']==['b'] and not r['decomposition_complete']
 assert r['children'][1]['current_usd'] is None
 assert r['residual_change_usd']==-10
 assert r['children'][1]['contribution_pp'] is None

def test_no_zero_denominators_and_no_false_reconciliation():
 r=decompose([{'tnved6':'a','stoim':120}],[{'tnved6':'a','stoim':0}],120,0,'tnved6')
 assert r['parent']['growth_pct'] is None and r['children'][0]['contribution_pp'] is None
 with pytest.raises(ValueError):decompose([{'tnved6':'a','stoim':10}],[],20,0,'tnved6')

def test_missing_parent_month_stops_and_truncation_rejected():
 def handler(req):
  if req.url.path.endswith('meta'):return httpx.Response(200,json={'data':{'period_max':'2026-08-01'}})
  assert req.url.params['source']=='national' and req.url.params['type']=='fact'
  return httpx.Response(200,json={'data':[],'meta':{'has_more':False}})
 with httpx.Client(transport=httpx.MockTransport(handler)) as client:
  r=explain(client,'http://api',{},'CN','im','8703')
 assert not r['available'] and len(r['missing_months'])==24
 def truncated(req):
  if req.url.path.endswith('meta'):return handler(req)
  return httpx.Response(200,json={'meta':{'has_more':True},'data':[]})
 with httpx.Client(transport=httpx.MockTransport(truncated)) as client:
  with pytest.raises(ValueError):explain(client,'http://api',{},'CN','im','8703')

def test_canonical_russian_names_are_taken_from_database():
 def handler(req):
  q=req.url.params
  if req.url.path.endswith('meta'):return httpx.Response(200,json={'data':{'period_max':'2026-08-01'}})
  if q.get('group_by')=='period':data=[dict(period='2025-08-01',stoim=100),dict(period='2026-08-01',stoim=150)]
  else:
   assert q.get('group_by')=='tnved6' and q['include']=='tnved6_name'
   value=150 if q['period_from']=='2026-08-01' else 100
   data=[dict(tnved6='870360',stoim=value,tnved6_name='КАНОНИЧЕСКОЕ РУССКОЕ НАЗВАНИЕ')]
  return httpx.Response(200,json={'data':data,'meta':{'has_more':False}})
 with httpx.Client(transport=httpx.MockTransport(handler)) as client:
  r=explain(client,'http://api',{},'CN','im','8703',months=1)
 child=r['children'][0]
 assert child['name']=='КАНОНИЧЕСКОЕ РУССКОЕ НАЗВАНИЕ' and child['name_status']=='available'
 assert 'name_en' not in child and child['contribution_pp']==50
