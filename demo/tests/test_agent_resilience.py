import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from demo.app import deepseek, tools


class ResilienceTests(unittest.TestCase):
    def complete(self, statuses):
        calls = []

        def handler(request):
            calls.append(request)
            status = statuses[len(calls) - 1]
            return httpx.Response(status, json={"choices": [{"message": {"content": "ok"}}]})

        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await deepseek._completion(client, [], True)

        with patch.object(deepseek.asyncio, "sleep", new=AsyncMock()):
            result = asyncio.run(run())
        return result, calls

    def test_transient_failure_retried(self):
        result, calls = self.complete([503, 200])
        self.assertEqual(result["content"], "ok")
        self.assertEqual(len(calls), 2)

    def test_auth_failure_not_retried(self):
        with self.assertRaises(httpx.HTTPStatusError):
            self.complete([401])

    def test_retry_exhausted(self):
        with self.assertRaises(httpx.HTTPStatusError):
            self.complete([503, 503])

    def test_large_result_is_valid_json_and_marked_partial(self):
        raw = json.dumps({"meta": {"has_more": True}, "data": [
            {"strana": "CN", "label": "товар" * 60} for _ in range(200)
        ]}, ensure_ascii=False)
        result = tools._trim(raw)
        obj = json.loads(result)
        self.assertLessEqual(len(result), tools._MAX_RESULT_CHARS)
        self.assertTrue(obj["meta"]["has_more"])
        self.assertEqual(obj["_truncated"]["received_rows"], 200)
        self.assertLess(len(obj["data"]), 200)

    def test_small_result_unchanged(self):
        raw = '{"data": [{"stoim": 10}]}'
        self.assertEqual(tools._trim(raw), raw)

    def test_error_does_not_expose_provider_details(self):
        event = deepseek._error_event(RuntimeError("secret-token"))
        self.assertNotIn("secret-token", json.dumps(event))
        self.assertEqual(event["code"], "RuntimeError")
