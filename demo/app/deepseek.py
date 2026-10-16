"""DeepSeek agent loop: drive tool calls over our data API, yield stream events."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import AsyncIterator

import httpx

from .config import settings
from .prompts import SYSTEM_PROMPT
from .tools import TOOLS, execute_tool, summarize_tool_result
from .exports import export_context

logger = logging.getLogger(__name__)


def _error_event(exc: Exception) -> dict:
    code = type(exc).__name__
    if isinstance(exc, httpx.HTTPStatusError):
        code = f"model_http_{exc.response.status_code}"
    logger.warning("Model request failed: %s", code)
    return {"type": "error", "code": code,
            "text": "Не удалось получить ответ модели. Попробуйте повторить вопрос чуть позже."}


async def _completion(client: httpx.AsyncClient, messages: list[dict],
                      use_tools: bool) -> dict:
    payload: dict = {
        "model": settings.deepseek_model,
        "messages": messages,
        "max_tokens": settings.analytical_output_tokens if (export_context.get() or {}).get("analytical") else settings.max_output_tokens,
        "temperature": 0.2,
    }
    if use_tools:
        payload["tools"] = [t for t in TOOLS if t["function"]["name"] != "export_trade" or (export_context.get() or {}).get("wants_export", True)]
        payload["tool_choice"] = "auto"
    for attempt in range(2):
        try:
            r = await client.post(
                f"{settings.deepseek_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                json=payload, timeout=120,
            )
            r.raise_for_status()
            choice = r.json()["choices"][0]
            message = choice["message"]
            if "DSML" in (message.get("content") or ""):
                if attempt:
                    raise ValueError("Invalid model output: tool markup instead of answer")
                payload["messages"] = [*messages, {"role": "user", "content":
                    "Файл уже подготовлен, если инструмент вернул export_ready. Дай обычный аналитический ответ по полученным данным. Не выводи служебную разметку DSML и не имитируй вызовы инструментов."}]
                continue
            return {**message, "_finish_reason": choice.get("finish_reason")}
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            retryable = (isinstance(exc, httpx.TransportError) or
                         exc.response.status_code in (429, 500, 502, 503, 504))
            if attempt or not retryable:
                raise
            logger.warning("Retrying model request: %s", type(exc).__name__)
            await asyncio.sleep(2)


async def run_agent(user_messages: list[dict], user_id: int | None = None) -> AsyncIterator[dict]:
    ctx = {"user_id": user_id, "analytical": True}
    # Never silently treat a continent as one of the requested country codes.
    question = user_messages[-1].get("content", "").lower() if user_messages else ""
    ctx['wants_ranking'] = bool(re.search(r'\bтоп\b|\btop\b|крупнейш|лидирующ', question))
    ctx['wants_export'] = bool(re.search(r'excel|xlsx|выгруз|скача|файл', question))
    if "африк" in question:
        ctx["region_note"] = "Африка в эту выгрузку не включена: для неё нужно отдельно согласовать список стран."
    token = export_context.set(ctx)
    try:
        async for event in _run_agent(user_messages):
            yield event
    finally:
        export_context.reset(token)


def _answer(msg):
    if msg.get("_finish_reason") == "length":
        return {"type": "error", "code": "answer_too_long", "text": "Ответ модели превысил допустимую длину и не показан целиком. Запросите XLSX-выгрузку или сузьте вопрос."}
    if export_context.get().get("partial"):
        return {"type": "error", "code": "partial_data", "text": "Получен неполный набор данных. Итоги по нему не рассчитаны. Сузьте запрос или запросите помесячную XLSX-выгрузку."}
    text = msg.get("content") or ""
    link = export_context.get().get('download_link')
    if link:
        text = re.sub(r'\[[^\]]*\]\([^)]*exports/[^)]*\)', '', text)
        text += '\n\n' + link + '\n\nИсточники, единицы и фильтры приведены в Excel. Ссылка действует 7 дней.'
    return {"type": "answer", "text": text}


async def _run_agent(user_messages: list[dict]) -> AsyncIterator[dict]:
    """Yield events: {'type':'tool',...}, {'type':'answer','text'}, {'type':'error'}.

    user_messages: sanitized [{role:'user'|'assistant', content:str}, ...].
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, *user_messages]
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{settings.api_base}/v1/meta",
                                 headers={"Authorization": f"Bearer {settings.service_api_key}"}, timeout=30)
            r.raise_for_status()
            latest = r.json()["data"]["period_max"]
            export_context.get()["period_max"] = latest
            messages[0]["content"] += (f"\nПоследний доступный период базы: {latest}. "
                "Если пользователь не указал конечный период, используй эту дату, включая текущий доступный год. "
                "Для сравнения стран первым вызови compare_trade. Excel создавай только по явной просьбе в последней реплике. На уточнение о динамике используй trade, покажи до 12 месяцев и изменения. История задает срез, но не повторное согласие на создание файла.")
        except Exception as exc:
            yield {"type": "error", "code": "data_period_unavailable",
                   "text": "Не удалось проверить актуальность базы. Повторите запрос позже."}
            return
        for _ in range(settings.max_tool_rounds):
            try:
                msg = await _completion(client, messages, use_tools=True)
            except Exception as e:  # noqa: BLE001
                yield _error_event(e)
                return

            tool_calls = msg.get("tool_calls")
            if not tool_calls:
                yield _answer(msg)
                return

            # Echo the assistant turn (with tool_calls) before appending results.
            messages.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": tool_calls,
            })
            # Resolve analytical windows before a model's redundant export in the same batch.
            ordered_calls = sorted(tool_calls, key=lambda tc: tc.get("function", {}).get("name") == "export_trade")
            for tc in ordered_calls:
                fn = tc.get("function", {})
                name = fn.get("name", "")
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    args = {}
                if not isinstance(args, dict): args = {}
                result = await execute_tool(client, name, args)
                yield {"type": "tool", "name": name,
                       "summary": summarize_tool_result(name, args, result)}
                ctx = export_context.get()
                if ctx.get("error_code"):
                    yield {"type":"error", "code":ctx["error_code"], "text":ctx.get("answer", "Не удалось завершить запрос.")}
                    return
                if ctx.get("answer"):
                    result = json.dumps(ctx.get('analysis_evidence') or {'verified_summary':ctx['answer']},ensure_ascii=False)
                try:
                    obj = json.loads(result)
                    if obj.get("_truncated") or obj.get("meta", {}).get("has_more"):
                        ctx["partial"] = True
                except (ValueError, AttributeError):
                    ctx["partial"] = True
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })

            if export_context.get().get('answer'):
                try:
                    final = await _completion(client, messages, use_tools=False)
                    yield _answer(final)
                except Exception as exc:
                    yield _error_event(exc)
                return

        # Tool budget exhausted: force a final answer with what we have.
        try:
            final = await _completion(client, messages, use_tools=False)
            yield _answer(final)
        except Exception as e:  # noqa: BLE001
            yield _error_event(e)
