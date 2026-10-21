"""DeepSeek agent loop: drive tool calls over our data API, yield stream events."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import AsyncIterator

import httpx

from .query_contract import safe_result, explicit_direction
from .numeric_facts import verify_answer, compact_text, INSTRUCTION
from .config import settings
from .prompts import SYSTEM_PROMPT
from .tools import TOOLS, execute_tool, summarize_tool_result
from .exports import export_context
from .answer_style import export_only, export_facts, request_mode, fallback_draft

logger = logging.getLogger(__name__)
CHAT_NUMERIC_INSTRUCTION = INSTRUCTION.split('Это формат черновика;')[0] + 'Каждую ссылку ставь отдельным абзацем, это целое предложение или таблица, не число для вставки в предложение. Верни черновик со ссылками {{fact:ID}}. Проверку и замену ссылок выполнит сервис автоматически. Не вызывай verify_trade_answer: в чате такого инструмента нет.'
FACT_SELECTION_INSTRUCTION = (
    "Финальный числовой ответ: выбери подходящие numeric_facts и верни ТОЛЬКО ссылки "
    "{{fact:ID}}, каждую отдельным абзацем. Не добавляй заголовок, вступление, "
    "пересказ, даты или фразу о числе месяцев: все это уже есть внутри фактов. "
    "Сначала вывод, затем подтверждение и относящиеся к ответу оговорки. "
    "Для fizob выбери index_change, index_yoy, monthly_table, index_methodology "
    "и все index_caveat. Если нужны дополнительные данные, сначала вызови инструмент."
)


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
    if (export_context.get() or {}).get("numeric_facts"):
        payload["messages"] = [*messages, {"role": "system", "content": FACT_SELECTION_INSTRUCTION}]
    for attempt in range(2):
        started = time.monotonic()
        try:
            r = await client.post(
                f"{settings.deepseek_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {settings.deepseek_api_key}"},
                json=payload, timeout=httpx.Timeout(120, connect=10, pool=10),
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
            logger.info("Model completion tools=%s attempt=%s elapsed=%.1fs finish=%s",
                        use_tools, attempt + 1, time.monotonic() - started, choice.get("finish_reason"))
            return {**message, "_finish_reason": choice.get("finish_reason")}
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            retryable = (isinstance(exc, httpx.TransportError) or
                         exc.response.status_code in (429, 500, 502, 503, 504))
            if attempt or not retryable:
                raise
            logger.warning("Retrying model request: %s tools=%s elapsed=%.1fs",
                           type(exc).__name__, use_tools, time.monotonic() - started)
            await asyncio.sleep(2)


async def run_agent(user_messages: list[dict], user_id: int | None = None) -> AsyncIterator[dict]:
    ctx = {"user_id": user_id, "analytical": True}
    # Never silently treat a continent as one of the requested country codes.
    question = user_messages[-1].get("content", "").lower() if user_messages else ""
    ctx['question'] = question
    ctx['request_mode'] = request_mode(question)
    ctx['explicit_direction'] = explicit_direction(question)
    ctx['wants_ranking'] = bool(re.search(r'\bтоп\b|\btop\b|крупнейш|лидирующ', question))
    ctx['wants_export'] = bool(re.search(r'excel|xlsx|выгруз|скача|файл', question))
    ctx['export_only'] = export_only(question)
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
    text = msg.get("content") or ""
    link = export_context.get().get('download_link')
    if link:
        text = re.sub(r'\[[^\]]*\]\([^)]*exports/[^)]*\)', '', text)
        text += '\n\n' + link + '\n\nИсточники, единицы и фильтры приведены в Excel. Ссылка действует 7 дней.'
    return {"type": "answer", "text": text}


async def _checked_answer(client, messages, msg):
    comparison = export_context.get().get('verified_comparison_answer')
    if comparison:
        # Export summaries cover a wider window; preserve matched-period results.
        return _answer({'content': comparison})
    facts = export_context.get().get('numeric_facts', {})
    ctx = export_context.get()
    if ctx.get('export_only') and ctx.get('download_link'):
        brief = dict(ctx.get('export_file_facts', {}))
        monthly=ctx.get('export_monthly_facts',{}) if re.search(r'по месяц|помесяч',ctx.get('question','')) else {}
        brief.update(monthly or export_facts(facts))
        brief.update(ctx.get('export_caveats', {}))
        if brief:
            checked = verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in brief), brief)
            if checked['valid']:
                return _answer({'content': 'Excel готов.\n\n' + checked['answer']})
    analytical = {k:v for k,v in facts.items() if v['kind'].startswith('analysis_')}
    if analytical:
        checked = verify_answer('\n\n'.join('{{fact:'+k+'}}' for k in analytical), analytical)
        if checked['valid']:
            return _answer({'content': checked['answer']})
    overview_facts = {k:v for k,v in facts.items() if v['kind'] in ('overview','suppliers','overview_dynamics','overview_drivers','overview_coverage','overview_partial','overview_source')}
    if overview_facts:
        # Render this bounded analytical overview in a stable order. The model
        # selects the slice; all findings and tables remain server-verified.
        order = ('overview','overview_dynamics','overview_drivers','suppliers','overview_coverage','overview_partial','overview_source')
        draft = '\n\n'.join('{{fact:'+key+'}}' for kind in order for key,value in overview_facts.items() if value['kind']==kind)
        checked = verify_answer(draft, overview_facts)
        if checked['valid']:
            return _answer({'content':checked['answer']})
        facts = overview_facts
    if not facts:
        return _answer(msg)
    checked = verify_answer(msg.get('content') or '', facts)
    if msg.get('_finish_reason')=='length':
        checked={'valid':False,'errors':['Incomplete model generation']}
    if not checked['valid']:
        logger.warning("Numerical draft rejected: %s", checked.get('errors'))
        # Do not repeat a long tool transcript or the invalid draft: ask for a
        # selection from the verified evidence, without inviting another paraphrase.
        repair = [{'role':'system','content':CHAT_NUMERIC_INSTRUCTION + '\n' + FACT_SELECTION_INSTRUCTION},
                  {'role':'user','content':'Вопрос: '+ctx.get('question','')+
                   '\nРазрешенные факты: '+json.dumps(facts,ensure_ascii=False)}]
        try:
            msg = await _completion(client, repair, use_tools=False)
            checked = verify_answer(msg.get('content') or '', facts)
            if msg.get('_finish_reason')=='length':checked={'valid':False}
        except (httpx.HTTPError, ValueError):
            checked = {'valid':False}
        if not checked['valid']:
            logger.warning("Numerical repair rejected: %s", checked.get('errors', ['incomplete or unavailable']))
    if checked['valid']:
        text = checked['answer']
    else:
        # A model failure must not suppress verified available data.
        fallback=verify_answer(fallback_draft(facts,ctx.get('question','')),facts)
        text=fallback['answer'] if fallback['valid'] else 'Не удалось проверить ответ. Попробуйте повторить вопрос.'
        logger.warning('Numerical answer used verified fallback')
    return _answer({'content':compact_text(text,facts)})


async def _run_agent(user_messages: list[dict]) -> AsyncIterator[dict]:
    """Yield events: {'type':'tool',...}, {'type':'answer','text'}, {'type':'error'}.

    user_messages: sanitized [{role:'user'|'assistant', content:str}, ...].
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT + "\n" + CHAT_NUMERIC_INSTRUCTION}, *user_messages]
    messages[0]['content'] += (
        '\nСначала суть ответа, затем подтверждающие факты. Режим текущего запроса: '
        +export_context.get().get('request_mode','overview')+
        '. Для общего запроса о торговле товаром (в том числе названным свободно, например водород) после search_products используй product_overview с найденным кодом и запрошенными датами. Не собирай обзор отдельными trade по странам и месяцам: их покрытие может различаться. Без просьбы о помесячной динамике не добавляй месячные таблицы, экстремумы и темпы м/м. Для индекса выбирай index_change, index_yoy, monthly_table и index_methodology. '
        'index_caveat показывай при наличии. Не выводи внутренние idx и технические экстремумы. '
        'Не подменяй динамику физобъема денежным разбором explain_trade. '
        'Уточнение наследует страну, товар и период из истории, если пользователь их не изменил. '
        'Новый файл создавай только по явной просьбе в последней реплике. '
        'Для объяснения отличай вклад подкатегории от экономической причины: данные о структуре '
        'не доказывают влияние санкций, спроса или тарифов. '
        'Оговорки относись к конкретному показателю: неизвестная масса не ограничивает стоимость. '
        'Сохраняй доступные данные при неполном покрытии; не называй частичный итог полным. '
        'Оценки UN Comtrade относятся к источнику; nowcast является отдельной моделью проекта.'
    )
    if export_context.get().get('export_only'):
        messages[0]['content'] += '\nПользователь просит только выгрузку: вызови export_trade, без explain_trade и дополнительных расчетов динамики. Ответ: краткие итоги и Excel; помесячная таблица уже в файле.'
    elif export_context.get().get('request_mode') == 'explain':
        messages[0]['content'] += '\nДля анализа динамики одного товара сначала explain_trade: нужны вывод о сопоставимом периоде и вклады подкатегорий. Не заменяй драйверы перечислением экстремумов.'
    direction = export_context.get().get('explicit_direction')
    if direction:
        messages[0]['content'] += f"\nМаршрут в последнем вопросе явно задан: napr={direction}, относительно России. Все торговые запросы должны использовать это направление."
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{settings.api_base}/v1/meta",
                                 headers={"Authorization": f"Bearer {settings.service_api_key}"}, timeout=30)
            r.raise_for_status()
            latest = r.json()["data"]["period_max"]
            export_context.get()["period_max"] = latest
            messages[0]["content"] += (f"\nПоследний доступный период базы: {latest}. "
                "Если пользователь не указал конечный период, используй эту дату, включая текущий доступный год. "
                "Товар назван свободным текстом и код еще не подтвержден в диалоге: сначала search_products по описанию товара. Выбирай только реальный код из справочника, подходящий всем признакам. 2 знака - широкая глава, 4 - товарная группа, 6 - явно указанный подтип. Мужское трикотажное белье: проверь 6107 по названию. Просто белье неоднозначно: уточни постельное/нательное, пол и трикотажное/нетрикотажное исполнение; до уточнения не вызывай trade/product_overview/export_trade. Не расширяй конкретный товар до родителя молча и не складывай родителя с детьми. Ответ пользователя на уточнение соедини с исходным запросом из истории, затем повтори поиск при необходимости. Для общего обзора одного товарного кода первым вызови product_overview: название, основные партнеры, доли и динамика на сопоставимом составе. Используй его факты overview, suppliers, overview_dynamics, overview_drivers, overview_coverage, overview_partial и overview_source. Если есть overview_partial, обязательно покажи отдельные темпы стран за их общие месяцы; не смешивай их с общим темпом. Не дублируй заголовок, он уже в overview. Не добавляй своих утверждений об источниках. Покажи динамику и главный драйвер перед таблицами, не добавляй технические экстремумы и массу без запроса пользователя. Для сравнения стран первым вызови compare_trade. Excel создавай только по явной просьбе в последней реплике. На уточнение о динамике используй trade, покажи до 12 месяцев и изменения. История задает срез, но не повторное согласие на создание файла.")
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
                ctx = export_context.get()
                # Even clarification questions must not invent classification codes.
                if not ctx.get('numeric_facts'):
                    codes = set(re.findall(r'\b(?:\d{4}|\d{6}|\d{8}|\d{10})\b', msg.get('content') or ''))
                    codes = {c for c in codes if not (len(c)==4 and 1900 <= int(c) <= 2099)}
                    allowed = set(ctx.get('product_search_codes', []))
                    if codes - allowed:
                        if not ctx.get('classification_repair'):
                            ctx['classification_repair'] = True
                            messages.append({'role':'user','content':'Не указывай неподтвержденные коды в ответе или уточняющем вопросе. Сначала вызови search_products по названию товара из моего запроса. Затем используй только подходящие коды из результатов либо задай уточняющий вопрос вообще без кодов.'})
                            continue
                        msg = {**msg, 'content':'Уточните, пожалуйста, вид и назначение товара, чтобы выбрать подходящую группу в справочнике.'}
                yield await _checked_answer(client, messages, msg)
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
                direction = export_context.get().get('explicit_direction')
                if direction and name in ('trade','fizob','export_trade','compare_trade','explain_trade','product_overview'):
                    args['napr'] = direction if name in ('compare_trade','explain_trade','product_overview') else [direction]
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
                    if obj.get('numeric_facts') and not obj.get('meta',{}).get('has_more'):
                        ctx.setdefault('numeric_facts',{}).update(obj['numeric_facts'])
                    result = json.dumps(safe_result(obj), ensure_ascii=False)
                except (ValueError, AttributeError, TypeError):
                    result = json.dumps({"error": "Invalid tool response. Retry the affected query; other complete results remain usable."})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id"),
                    "content": result,
                })

            if export_context.get().get('answer'):
                try:
                    final = await _completion(client, messages, use_tools=False)
                    yield await _checked_answer(client, messages, final)
                except Exception as exc:
                    yield _error_event(exc)
                return

        # Tool budget exhausted: force a final answer with what we have.
        try:
            final = await _completion(client, messages, use_tools=False)
            yield await _checked_answer(client, messages, final)
        except Exception as e:  # noqa: BLE001
            yield _error_event(e)
