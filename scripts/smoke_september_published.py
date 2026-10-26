"""Read-only HTTP smoke test, run inside trade-demo; never print credentials."""
import json
import os
from urllib.request import Request, urlopen
from urllib.parse import urlencode

base=os.environ['MGIMO_DEMO_API_BASE'].rstrip('/')
headers={'Authorization':'Bearer '+os.environ['MGIMO_DEMO_SERVICE_API_KEY']}
def get(path,params=()):
    with urlopen(Request(base+path+('?' + urlencode(params) if params else ''),headers=headers),timeout=45) as r:
        return json.load(r)

health=get('/health')
assert health['status']=='ok' and '2026-09' in str(health)
for country in ('CN','ALL'):
    data=get('/v1/fizob',[('strana',country),('tn_level',0),('period_from','2026-09'),('period_to','2026-09')])
    rows=[{k.lower():v for k,v in r.items()} for r in data['data']]
    assert len(rows)==2
    assert all(r['idx'] is None and r['coverage']<1 and r['status']=='incomplete_window' and r['methodology_version']=='fisher_annual_v2' for r in rows)
    print(country,'full September index unavailable, as expected; no substituted zero')
data=get('/v1/trade',[('strana','CN'),('type','fact'),('period_from','2026-09'),('period_to','2026-09'),('group_by','napr,period'),('metrics','stoim')])
rows=[{k.lower():v for k,v in r.items()} for r in data['data']]
assert len(rows)==2 and not data.get('meta',{}).get('has_more')
expected={'ИМ':13398453133,'ЭК':15665983447}
for row in rows:
    assert abs(float(row['stoim'])-expected[row['napr']])<.01
print('China September factual values match local original CSV totals')
print('Published HTTP checks passed')
