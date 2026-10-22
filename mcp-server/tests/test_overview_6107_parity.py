"""Offline regression of MCP and the real demo agent loop on a fixed 6107 slice.

Synthetic USD observations, not published trade figures. Expected totals and
rates are deliberately simple and independent of either implementation.
Only HTTP data and LLM completion are stubbed; tools and verification are real.
"""
import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'demo'))
sys.path.insert(0, str(ROOT / 'mcp-server'))


def fixed_rows():
    rows = []
    def add(country, year, month, value, source='national', kind='fact'):
        rows.append(dict(strana=country, period=f'{year}-{month:02}-01',
                         stoim=value, source=source, type=kind))
    for m in range(1, 9):
        for country, before, after in [('CN', 100, 110), ('TR', 50, 55)]:
            add(country, 2025, m, before)
            add(country, 2026, m, after)
    # Zero is a real observation. February is absent in the current window;
    # its large prior value must not be included in the partial comparison.
    for year, value in [(2025, 150), (2026, 180)]:
        add('UZ', year, 1, 0, 'comtrade')
        add('UZ', year, 3, value, 'comtrade')
    add('UZ', 2025, 2, 9000, 'comtrade')
    add('IT', 2025, 1, 125, 'comtrade')
    add('IT', 2026, 1, 100, 'comtrade')
    # A change of source cannot establish a year-on-year comparison.
    add('IT', 2025, 2, 500, 'national')
    add('IT', 2026, 2, None, 'comtrade')
    for year, value in [(2025, 15), (2026, 30)]:
        for m in (1, 8):
            add('AM', year, m, value, 'comtrade')
    add('UZ', 2026, 2, 99999, 'nowcast', 'pred')
    return rows


def test_6107_mcp_and_chat_share_verified_compact_answer(monkeypatch):
    monkeypatch.setenv('MCP_TRANSPORT', 'stdio')
    import server
    from app import deepseek
    from app import analytical_writer
    async def no_editor(*args):
        return None  # This test covers calculated evidence, not live model prose.
    monkeypatch.setattr(analytical_writer, 'write_answer', no_editor)
    names = {'CN': 'Китай', 'TR': 'Турция', 'UZ': 'Узбекистан',
             'IT': 'Италия', 'AM': 'Армения'}
    calls = []
    def handle(req):
        calls.append(req.url.path)
        if req.url.path.endswith('/meta'):
            return httpx.Response(200, json={'data': {'period_max': '2026-08-01'}})
        if req.url.path.endswith('/tnved'):
            return httpx.Response(200, json=[{'code': '6107', 'name': 'Мужское трикотажное белье'}])
        if req.url.path.endswith('/countries'):
            return httpx.Response(200, json=[{'strana': c, 'country_name': n} for c, n in names.items()])
        assert req.url.path.endswith('/trade'), req.url
        for k, value in {'tnved4': '6107', 'napr': 'im', 'type': 'fact',
                         'group_by': 'strana,period,type,source', 'metrics': 'stoim',
                         'period_from': '2025-01-01', 'period_to': '2026-08-01'}.items():
            assert req.url.params[k] == value
        return httpx.Response(200, json={'data': fixed_rows(), 'meta': {'has_more': False}})
    client = httpx.AsyncClient
    monkeypatch.setattr(httpx, 'AsyncClient', lambda **kw: client(transport=httpx.MockTransport(handle), **kw))
    monkeypatch.setattr(server, '_resolve_token', lambda ctx: 'fixture-key')
    completion_count = 0
    async def completion(client, messages, use_tools=True):
        nonlocal completion_count
        completion_count += 1
        if completion_count == 1:
            return {'role': 'assistant', 'content': None, 'tool_calls': [{
                'id': 'overview-fixture', 'type': 'function', 'function': {
                    'name': 'product_overview', 'arguments': '{"code":"6107","napr":"im"}'}}]}
        # Invented prose must never replace the verified overview.
        return {'role': 'assistant', 'content': 'Итого 999999, рост 999%.'}
    monkeypatch.setattr(deepseek, '_completion', completion)

    async def run():
        result = json.loads(await server.product_overview('6107', 'im'))
        order = ('overview', 'overview_dynamics', 'overview_drivers', 'suppliers',
                 'overview_coverage', 'overview_partial', 'overview_source')
        draft = '\n\n'.join('{{fact:' + k + '}}' for kind in order
                            for k, f in result['numeric_facts'].items() if f['kind'] == kind)
        verified = json.loads(server.verify_trade_answer(draft, [result['evidence_id']]))
        assert verified['valid']
        events = [e async for e in deepseek.run_agent([{'role': 'user', 'content':
            'Дай информацию по импорту по коду 6107: основные страны-партнеры, их доли и динамика на сопоставимых данных.'}])]
        assert not any(e['type'] == 'error' for e in events)
        answers = [e['text'] for e in events if e['type'] == 'answer']
        assert answers == [verified['answer']]
        assert [e['name'] for e in events if e['type'] == 'tool'] == ['product_overview']
        return result, answers[0]
    result, answer = asyncio.run(run())
    assert completion_count == 2
    assert sum(p.endswith('/trade') for p in calls) == 2
    assert Decimal(result['total_usd']) == 1660
    assert result['comparable_countries'] == ['CN', 'TR']
    assert '**На сопоставимом составе стран: 10,0% г/г**' in answer
    for row in ['| Китай | 0,88 | 53,0 | 8 из 8 |',
                '| Турция | 0,44 | 26,5 | 8 из 8 |',
                '| Узбекистан | 0,18 | 10,8 | 2 из 8 |',
                '| Италия | 0,10 | 6,0 | 1 из 8 |',
                '| Армения | 0,06 | 3,6 | 2 из 8 |']:
        assert row in answer
    partial = {p['strana']: p for p in result['partial_country_comparisons']}
    for code, months, before, after, rate in [
        ('UZ', ['01', '03'], 150, 180, 20),
        ('IT', ['01'], 125, 100, -20),
        ('AM', ['01', '08'], 30, 60, 100),
    ]:
        p = partial[code]
        assert p['current_months'] == ['2026-' + m for m in months]
        assert p['baseline_months'] == ['2025-' + m for m in months]
        assert p['source'] == 'comtrade'
        assert Decimal(p['current_value']) == after
        assert Decimal(p['baseline_value']) == before
        assert Decimal(p['change_pct']) == rate
    assert set(partial) == {'UZ', 'IT', 'AM'}
    assert 'янв. 2026; март 2026' in answer
    assert 'янв. 2026; авг. 2026' in answer
    assert '79,5%' in answer  # 1320 / 1660, not all-country growth.
    assert answer.index('На сопоставимом составе') < answer.index('Основные страны-партнеры')
    assert answer.index('Основные страны-партнеры') < answer.index('Отдельная динамика')
    assert '<details><summary>Товар и источники</summary>' in answer
    assert 'поставщик' not in answer.lower()
    assert '999999' not in answer
