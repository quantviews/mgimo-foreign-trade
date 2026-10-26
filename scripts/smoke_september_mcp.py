"""Read-only MCP check, executed inside trade-demo without exposing its key."""
import json
import os
from urllib.request import Request,urlopen

headers={'Authorization':'Bearer '+os.environ['MGIMO_DEMO_SERVICE_API_KEY'],
         'Content-Type':'application/json','Accept':'application/json, text/event-stream'}
def rpc(body):
    with urlopen(Request('http://trade-mcp:8000/mcp',data=json.dumps(body).encode(),headers=headers),timeout=45) as r:
        session=r.headers.get('Mcp-Session-Id')
        if session: headers['Mcp-Session-Id']=session
        text=r.read().decode()
    if not text:return None
    if text.lstrip().startswith('{'):return json.loads(text)
    return next(json.loads(line[5:].strip()) for line in text.splitlines() if line.startswith('data:'))

initial=rpc({'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'release-smoke','version':'1'}}})
assert 'result' in initial,initial
rpc({'jsonrpc':'2.0','method':'notifications/initialized'})
result=rpc({'jsonrpc':'2.0','id':2,'method':'tools/call','params':{'name':'fizob','arguments':{'strana':['CN'],'tn_level':['0'],'period_from':'2026-09-01','period_to':'2026-09-01'}}})
assert 'result' in result and not result['result'].get('isError'),result
payload=json.loads(next(x['text'] for x in result['result']['content'] if x['type']=='text'))
rows=[{k.lower():v for k,v in r.items()} for r in payload['data']]
assert len(rows)==2 and all(r['idx'] is None for r in rows)
assert all(r['methodology_version']=='fisher_annual_v2' for r in rows)
print('MCP fizob: same two unavailable full September indices as the API; metadata preserved')
