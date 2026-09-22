"""DeepSeek agent loop: drive tool calls over our data API, yield stream events."""

from __future__ import annotations

import json
from typing import AsyncIterator

import httpx

from .config import settings
from .prompts import SYSTEM_PROMPT
from .tools import TOOLS, execute_tool, summarize_tool_result


async def _completion(client: httpx.AsyncClient, messages: list[dict],
                      use_tools: bool) -> dict:
    payload: dict = {
        "model": settings.deepseek_model,
        "messages": messages,
        "max_tokens": settings.max_output_tokens,
        "temperature": 0.2,
    }
    if use_tools:
        payload["tools"] = TOOLS
        payload["tool_choice"] = "auto"
    r = await client.post(
        f"{settings.deepseek_base_url}/chat/completions",
        headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
        json=payload,
        timeout=120,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]


async def run_agent(user_messages: list[dict]) -> AsyncIterator[dict]:
    """Yield events: {'type':'tool',...}, {'type':'answer','text'}, {'type':'error'}.

    user_messages: sanitized [{role:'user'|'assistant', content:str}, ...].
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, *user_messages]
    async with httpx.AsyncClient() as client:
        for _ in range(settings.max_tool_rounds):
            try:
                msg = await _completion(client, messages, use_tools=True)
            except Exception as e:  # noqa: BLE001
                yield {"type": "error", "text": f"Ошибка модели: {e}"}
                return

            tool_calls = msg.get("tool_calls")
            if not tool_calls:
                yield {"type": "answer", "text": msg.get("content") or ""}
                return

            # Echo the assistant turn (with tool_calls) before appending results.
            messages.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": tool_calls,
            })
            for tc in tool_calls:
                fn = tc.get("function", {})
                name = fn.get("name", "")
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    args = {}
                result = await execute_tool(client, name, args)
                yield {"type": "tool", "name": name,
                       "summary": summarize_tool_result(name, args, result)}
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })

        # Tool budget exhausted: force a final answer with what we have.
        try:
            final = await _completion(client, messages, use_tools=False)
            yield {"type": "answer", "text": final.get("content") or ""}
        except Exception as e:  # noqa: BLE001
            yield {"type": "error", "text": f"Ошибка модели: {e}"}
