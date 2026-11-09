"""Run on host. Uses the local demo service credential without printing it.

Checks API, authenticated XLSX, MCP protocol initialization and tools listing.
Does not send email or call the language-model provider.
"""
import io
import json
from pathlib import Path
import urllib.request
import urllib.error
import zipfile

HERE = Path(__file__).resolve().parent
env = dict(line.split('=', 1) for line in (HERE / 'demo.env').read_text().splitlines() if '=' in line)
token = env['MGIMO_DEMO_SERVICE_API_KEY'].strip("'")
headers = {'Authorization': 'Bearer ' + token}

def request(url, body=None, extra=None):
    h = dict(headers)
    h.update(extra or {})
    if body is not None:
        h['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers=h)
    with urllib.request.urlopen(req, timeout=90) as r:
        return r.read(), dict(r.headers)

base = 'http://127.0.0.1:8090'
meta = json.loads(request(base + '/v1/meta')[0])
assert meta['data']['rows'] == 7399486 and meta['data']['data_version'] == '2026-09'
print('API metadata OK', meta['data'])
body = dict(strana=['CN'], napr=['im'], tnved4=['8703'], type=['fact'],
            period_from='2025-10-01', period_to='2026-09-01', end_mode='fixed',
            group_by='strana,period,edizm', metrics='stoim,netto,kol')
result = json.loads(request(base + '/v1/exports/trade', body)[0])
xlsx = request(base + result['download_path'])[0]
assert zipfile.is_zipfile(io.BytesIO(xlsx))
assert result['meta']['complete'] and result['meta']['rows'] >= 12
(HERE / 'smoke-export.xlsx').write_bytes(xlsx)
(HERE / 'smoke-export.json').write_text(json.dumps(result, ensure_ascii=False), encoding='utf-8')
print('China cars XLSX OK, rows:', result['meta']['rows'], 'bytes:', len(xlsx))
payload = {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {
    'protocolVersion': '2025-03-26', 'capabilities': {}, 'clientInfo': {'name': 'deployment-check', 'version': '1'}}}
mcp_headers = {'Accept': 'application/json, text/event-stream', 'Host': 'nts.mgimo.ru'}
raw, response_headers = request('http://127.0.0.1:8092/mcp', payload, mcp_headers)
def parse(raw):
    text = raw.decode()
    return json.loads(next((line[6:] for line in text.splitlines() if line.startswith('data: ')), text))
initialized = parse(raw)
assert 'result' in initialized, initialized
session = next((v for k,v in response_headers.items() if k.lower() == 'mcp-session-id'), None)
if session:
    mcp_headers['Mcp-Session-Id'] = session
mcp_headers['MCP-Protocol-Version'] = initialized['result']['protocolVersion']
request('http://127.0.0.1:8092/mcp', {'jsonrpc': '2.0', 'method': 'notifications/initialized'}, mcp_headers)
raw, _ = request('http://127.0.0.1:8092/mcp', {'jsonrpc': '2.0','id':2,'method':'tools/list','params':{}}, mcp_headers)
names = [t['name'] for t in parse(raw)['result']['tools']]
assert {'trade','compare_trade','verify_trade_answer','export_trade'}.issubset(names)
print('MCP tools OK', len(names))
raw, _ = request('http://127.0.0.1:8092/mcp', {'jsonrpc':'2.0','id':3,'method':'tools/call','params':{'name':'meta','arguments':{}}}, mcp_headers)
answer = parse(raw)['result']
assert not answer.get('isError'), answer
assert '2026-09' in json.dumps(answer)
print('MCP authenticated meta OK')
try:
    request('http://127.0.0.1:8092/mcp', payload, {**mcp_headers, 'Host': 'untrusted.invalid'})
except urllib.error.HTTPError as e:
    assert e.code == 421, e.code
else:
    raise AssertionError('Untrusted Host was accepted')
print('MCP untrusted Host rejected')
