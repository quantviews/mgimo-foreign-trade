import asyncio
import csv
import io
import json
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from demo.app import exports, tools, main, deepseek


class ExportTests(unittest.TestCase):
    def export_request(self, args, has_more=False):
        seen = []
        def handler(req):
            seen.append(req)
            return httpx.Response(413 if has_more else 200, json={'meta': {'complete': True, 'has_more': False}, 'filters': {}, 'data': [{'period': '2026-08-01'}]})
        async def run():
            token = exports.export_context.set({'user_id': 7, 'period_max': '2026-08-01'})
            try:
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    return await tools._export_trade(client, args, {})
            finally:
                exports.export_context.reset(token)
        with patch.object(tools, 'save_export', new=AsyncMock(return_value={'export_ready': True})) as save:
            result = asyncio.run(run())
        return seen[0], json.loads(result), save

    def test_latest_overrides_model_stale_end(self):
        req, result, _ = self.export_request({'period_to': '2025-12-01'})
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.headers['X-Export-User-Id'], '7')
        self.assertEqual(req.url.path, '/v1/exports/trade')
        self.assertTrue(json.loads(req.content)['inline'])
        self.assertTrue(result['export_ready'])

    def test_cars_request_quantity_with_units(self):
        req, _, _ = self.export_request({'tnved4':['8703'], 'metrics':'stoim,netto'})
        body = json.loads(req.content)
        self.assertIn('kol',body['metrics'].split(','))
        self.assertIn('edizm',body['group_by'].split(','))
        self.assertIn('edizm_iso',body['group_by'].split(','))

    def test_explicit_end_preserved(self):
        req, _, _ = self.export_request({'period_to': '2025-12-01', 'end_mode': 'fixed'})
        self.assertEqual(json.loads(req.content)['period_to'], '2025-12-01')
        self.assertEqual(json.loads(req.content)['end_mode'], 'fixed')

    def test_partial_page_never_published(self):
        _, result, save = self.export_request({}, has_more=True)
        self.assertIn('error', result)
        save.assert_not_called()

    def test_download_requires_owner_and_expiry(self):
        async def run():
            pool = AsyncMock()
            pool.fetchrow.return_value = None
            with patch.object(main, '_session', return_value={'uid': 9}), patch.object(main.db, '_require_pool', return_value=pool):
                response = await main.download_export(uuid4(), None)
            self.assertEqual(response.status_code, 404)
            sql, _, uid = pool.fetchrow.call_args.args
            self.assertIn('user_id=$2', sql)
            self.assertIn('expires_at>now()', sql)
            self.assertEqual(uid, 9)
        asyncio.run(run())

    def test_download_without_login_denied(self):
        with patch.object(main, '_session', return_value=None):
            response = asyncio.run(main.download_export(uuid4(), None))
        self.assertEqual(response.status_code, 401)

    def test_partial_rows_withheld_before_model(self):
        from trade_tools_shared.query_contract import safe_result
        result = safe_result({'data': [{'stoim': 123}], 'meta': {'has_more': True}})
        self.assertNotIn('data', result)
        self.assertEqual(result['result_status'], 'needs_refinement')



def test_export_quota_message_and_no_file():
    async def run():
        state = {'user_id':7}
        tok = exports.export_context.set(state)
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(429, headers={'Retry-After':'3600'}))) as client:
                with patch.object(tools,'save_export',new=AsyncMock()) as save:
                    result = await tools._export_trade(client, {}, {})
                    save.assert_not_called()
            assert 'retry_after' in result
            assert '60 мин' in state['answer']
        finally:
            exports.export_context.reset(tok)
    asyncio.run(run())


def test_summary_uses_readable_numbers_and_separates_estimates():
    from decimal import Decimal
    rows = [{'strana':'CN','type':'fact','period':'2026-08-01'},
            {'strana':'IN','type':'fact','period':'2026-07-01'},
            {'strana':'IN','type':'pred','period':'2026-08-01'}]
    totals = {('CN','fact'):[Decimal('60020000000'),Decimal('112756900000')],
              ('IN','fact'):[Decimal('49376000000'),Decimal('77855200000')],
              ('IN','pred'):[Decimal('4797000000'),Decimal('8126200000')]}
    for row in rows:
        row['stoim'], row['netto'] = totals[row['strana'], row['type']]
    answer = exports.render_summary(rows,totals,{'stoim','netto'}, {'napr':['ex'],'tnved4':['2709']}, {'period_max':'2026-08-01'},uuid4())
    assert '112\u00a0756,9' in answer
    assert 'Китай' in answer and 'Индия' in answer
    assert 'Сопоставимого периода' in answer and '4,80' in answer
    assert 'июл. 2026' in answer and 'авг. 2026' in answer
    assert 'stoim' not in answer and 'Фильтры:' not in answer
    assert 'Скачать данные в Excel' in answer


def test_comparison_uses_only_common_fact_months():
    from decimal import Decimal as D
    rows = [dict(strana=c,type=t,period=p,stoim=v,netto=v*2) for c,t,p,v in [
        ('CN','fact','2026-06-01',100),('CN','fact','2026-07-01',200),('CN','fact','2026-08-01',999),
        ('IN','fact','2026-06-01',50),('IN','fact','2026-07-01',100),
        ('IN','pred','2026-07-01',0),('IN','pred','2026-08-01',500)]]
    lines = exports.comparison(rows,{'stoim','netto'},{'strana':['CN','IN']},exports.COUNTRIES)
    text = '\n'.join(lines)
    assert '2 мес.' in text and '100,0%' in text and 'авг.' not in text
    totals={('CN','fact'):[D(1299),D(2598)],('IN','fact'):[D(150),D(300)],('IN','pred'):[D(500),D(1000)]}
    text=exports.render_summary(rows,totals,{'stoim','netto'},{'strana':['CN','IN']},{'period_max':'2026-08-01'},uuid4())
    assert '| Индия | Оценка | авг. 2026 |' in text
    assert 'нулевые модельные строки' in text


def test_missing_country_prevents_comparison_and_gaps_are_explicit():
    rows=[dict(strana=c,type='fact',period=p,stoim=1) for c in ['CN','IN'] for p in ['2026-01-01','2026-03-01']]
    text=' '.join(exports.comparison(rows,{'stoim'},{'strana':['CN','IN']},exports.COUNTRIES))
    assert 'янв. 2026, мар. 2026' in text
    text=' '.join(exports.comparison(rows,{'stoim'},{'strana':['CN','IN','TR']},exports.COUNTRIES))
    assert 'нет' in text and 'выше' not in text


def test_car_counts_and_small_estimate():
    from decimal import Decimal
    rows=[{'strana':'CN','type':'fact','period':'2026-08','stoim':13e9,'netto':None,'kol':988874,'edizm':'ШТУКА','edizm_iso':'796'},
          {'strana':'CN','type':'pred','period':'2026-08','stoim':3030000,'netto':None,'kol':None,'edizm':'ШТУКА','edizm_iso':'796'}]
    totals={('CN','fact'):[Decimal('13000000000'),Decimal(0)],('CN','pred'):[Decimal('3030000'),Decimal(0)]}
    answer=exports.render_summary(rows,totals,{'stoim','netto','kol'},{'napr':['im'],'tnved4':['8703']},{},uuid4())
    assert '988\u00a0874' in answer and 'ШТУКА' in answer
    assert '3,03 млн' in answer and 'нет полных данных' in answer
