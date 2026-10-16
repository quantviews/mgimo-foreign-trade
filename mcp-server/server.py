#!/usr/bin/env python3
"""MGIMO foreign-trade MCP server.

Exposes the read-only trade API as MCP tools so any MCP client (Claude Desktop,
IDEs, agents) can query Russian foreign-trade data. Thin wrapper over the HTTP
API, so authentication, quotas and audit stay in the API.

Two ways to run (env `MCP_TRANSPORT`):
  stdio (default)  local server launched by the client; the API key comes from
                   MGIMO_API_TOKEN (env var or a .env line).
  streamable-http  remote server behind a reverse proxy; each request carries the
                   client's own API key in `Authorization: Bearer <key>`, which
                   is forwarded to the API per-request (quotas/audit stay per-user).

Environment variables:
  MGIMO_API_TOKEN  personal API key (stdio mode). Get one from the Superset
                   cabinet at https://nts.mgimo.ru/superset/apikey/ .
  MGIMO_API_BASE   API base URL (default https://nts.mgimo.ru/api).
  MCP_TRANSPORT    stdio | streamable-http | sse (default stdio).
  MCP_HOST         bind host for http transports (default 127.0.0.1).
  MCP_PORT         bind port for http transports (default 8000).

The token is only ever sent in the Authorization header to the API, never
returned to the model or logged.
"""
from __future__ import annotations

import os
import json
import base64
from uuid import UUID
from pathlib import Path

import httpx
from mcp.server.fastmcp import Context, FastMCP

DEFAULT_BASE = "https://nts.mgimo.ru/api"

API_BASE = os.environ.get("MGIMO_API_BASE", DEFAULT_BASE).rstrip("/")

INSTRUCTIONS = """
Неполные периоды и нулевые строки: различай запрошенное окно, месяцы имеющихся фактических записей и nowcast. Если фактического месяца нет, пиши «Полный фактический итог за запрошенный период не установлен; в доступных записях отражено ...». Не характеризуй масштаб торговли как малый или редкий по неполному срезу. Количество или массу при пропусках называй только известной частью. Нулевая стоимость в существующей строке означает запись со значением 0, а не отсутствие наблюдения; не объясняй ее округлением или отсутствием торговли без подтверждения источника. NULL не равен нулю. Национальный источник и TYPE=fact не доказывают непосредственно наблюдаемую природу физических показателей.
Для Excel одного товара HS2/HS4 и одной страны с аналитическим разбором сначала вызови explain_trade за запрошенное окно (не более 12 месяцев), затем export_trade за тот же период. available=false или missing_months запрещает полный рост год к году, доли и вклады в изменение за это окно. Не сдвигай период молча. Выгрузка доступных записей допустима. complete/has_more описывают передачу строк API, а не полноту исходной статистики. Nowcast не заполняет пробелы факта в сопоставлении; оценки физических показателей Comtrade сохраняют источник и не являются nowcast проекта.
Не добавляй собственные отношения массы к количеству, включая иллюстрации неправильного среднего, если mean_status блокирует расчет. Предупреждения сформированы проверками проекта; не приписывай их таможне. Для июля нельзя утверждать ни массу менее 100 кг по всему месяцу, ни среднюю 1,7 т: флаг касается исходных записей. В выводе достаточно: «Средняя масса не приводится: отдельные исходные записи отмечены проверкой качества». Не добавляй утверждений о гарантированной достоверности стоимости.
Используй computed_summary для итогов, row_display.mass_display для массы: малые значения показывай в тоннах или кг, без округления до нуля. Эти указания приоритетнее общего правила тыс. тонн. quality_flags агрегата означает наличие подозрительной исходной записи, а не нарушение порога всем месячным итогом. Отсутствие флага не подтверждает достоверность. Не объявляй апрель/май надежными только по правдоподобному отношению массы к количеству. Итог относится к имеющимся наблюдениям; явно называй отсутствующие месяцы запрошенного окна.
Область предупреждений о качестве: связывай каждый флаг только с проверяемым показателем. Предупреждение о массе относится к массе и производным от нее (цена за кг, динамика массы); предупреждение о количестве или единице относится к количеству и производным от него. Низкая масса на транспортное средство сама по себе не доказывает ошибку стоимости. Стоимость, ее динамику и доли по стоимости ставь под сомнение только при отдельном свидетельстве проблемы стоимости или покрытия. Для топа по стоимости не добавляй оговорку о массе, если масса не используется в выводах. Не скрывай соответствующие флаги при ответах о физических показателях. Если предупреждение неоднозначно, не расширяй его смысл на другие показатели.
Качество данных: передавай пользователю существенные quality_flags. Оценки количества и массы самим Comtrade отличаются от нашего nowcast: TYPE=fact не отменяет NETTO_ESTIMATED/KOL_ESTIMATED и предупреждения об оценке источника. Не называй такие физические показатели непосредственно наблюдаемыми. Не округляй оцененное количество автомобилей до целых как достоверное число машин. Если стандартная единица не установлена, сохраняй обозначение источника и не складывай его с другими единицами. NULL массы или количества не означает ноль; не рассчитывай цену за кг по неизвестной массе.
Турция: по информации команды проекта от 15.10.2026, статистика сырой нефти не раскрывается. Скрытая позиция 27999999, вероятно, связана с ней, но ее состав не подтвержден: не присваивай всему итогу код 2709 и не трактуй отсутствие 2709 как отсутствие поставок нефти.
\
Russian foreign-trade data (exports/imports by partner country, HS / ТН ВЭД code
and month), served read-only.

Work from the general to the specific ("от общего к частному") - do not start by
pulling raw rows:

1. Get the overall shape first with a server-side aggregate: trade(group_by=...)
   at a high level (a tnved2 chapter, strana, or year) with order_by="-stoim".
   Let the server sum; never fetch raw rows just to total them yourself.
2. Then drill down into what matters: take the significant chapter / country /
   month from step 1 and re-query at a finer level (tnved4/6, period, a specific
   strana), narrowing the filters as you go.
3. Resolve a product/country name to a code with reference() only when you do not
   already know it.

Every trade() response includes the latest available month (meta.period_max), so
you rarely need meta() first - reserve it for quota or discovering valid fields.
Prefer one well-chosen call over several narrow ones."""
INSTRUCTIONS += """
For a long monthly series use export_trade first: show its short summary and
provide the export, not hundreds of rows in chat. The server defaults to the
latest period. Never call a has_more/truncated response complete. Completeness
of an export only covers the selected filters, not missing source observations.
Use download_export to obtain XLSX for a client-side file/artifact; never paste a
large XLSX into the chat. API download URLs require the user's Authorization header.
"""

INSTRUCTIONS += """
## Comparable periods and analyst-facing answers

- For country comparisons, fetch monthly aggregates by country, period, type and source (not raw records). Compare the intersection of months with factual observations for every requested country within the requested interval. Missing countries or months are not zero. If no common months exist, say a factual comparison is unavailable.
- For "latest 12 months", anchor the requested window to meta.period_max. If only 11 common fact months exist, label the comparison as 11 months and list the excluded months; do not silently shift the window backwards. For year-on-year comparisons, align the same calendar months in both years before summing. Enumerate gaps rather than presenting a discontinuous set as a continuous period.
- Show absolute differences and percentages with an explicit denominator (comparison baseline). Do not compute percentages against a zero baseline; flag small bases. Verify comparable direction, product scope and units. Matching months alone does not guarantee equal source/product coverage. A null metric is not zero and cannot establish comparable coverage for that metric.
- Keep fact and pred separate. Do not add estimates to factual totals or use them to fill a comparison without explicitly labeling a separate scenario. A zero-valued forecast row must not extend the displayed period of an estimate; retain it in the export and explain its presence when relevant. Do not infer a whole month is forecast from the mere existence of one pred row. Check overlap at the detailed product-code level before claiming double counting.
- An export's summary totals may cover different periods by country: never compare those totals directly. Use the bounded monthly data to calculate matched-period totals. Never call has_more/truncated results complete. Respect export quotas and Retry-After; do not split requests to evade them.
- Lead with 1-2 substantive findings, then a compact table, then necessary qualifications and the download link if available. Use country names and plain labels (стоимость, масса, факт, модельная оценка), not internal API field names. Keep technical filters out of the main narrative.
- In Russian answers use space-separated thousands and a decimal comma: 112 756,9; 60,02 млрд долл.; 8,4%. Calculate with unrounded values, round only for display, and name units. Mass is not a price-adjusted physical-volume index. Do not invent explanations for observed differences.

"""

INSTRUCTIONS += """
For country comparisons ALWAYS use compare_trade first, before the general-to-specific
workflow or export_trade. It resolves latest periods internally. For crude oil use
2709, not chapter 27. Present answer_ru or its concise Russian paraphrase, preserving
computed values, common months and exclusions. Do not expose scratch calculations,
add an unrequested prior-year analysis, or recompute rounded differences. Use spaces
for thousands, decimal commas and ordinary hyphens. Export only when requested.
"""

INSTRUCTIONS += """
## Explain dynamics from aggregate to subcategories

For analytical questions about changes, drivers, structure or "why", start with the parent total and its comparable-period change, then drill down ONE level (HS2 -> HS4, HS4 -> HS6). Do not add this investigation to a simple file request. For 8703 use tnved6, never assume every subcategory is an electric vehicle.

1. Establish the same calendar months, direction, country, source and fact status in current and prior-year periods. First verify parent monthly coverage. Retain the chosen window; missing months are not zeros. National statistics, Comtrade source estimates and project nowcast remain distinct.
2. Fetch complete child aggregates for BOTH periods, retaining the parent filter. Usually two trade calls with group_by=tnved6,type,source (plus edizm,edizm_iso for quantity), metrics=stoim,netto,kol and include=tnved6_name suffice after coverage checks. Aggregate over each entire matched window; do not fetch 24 months times all child codes unless necessary. Check has_more/truncation; never use a top-N subset as the denominator. Reconcile the sum of children with the independently obtained parent totals for both periods. Show a residual as unallocated, not zero; unresolved coverage means decomposition is incomplete.
3. For value V_i of child i and parent T, compute prior and current shares 100*V_i/T, absolute change d_i=V_i(current)-V_i(prior), child's growth 100*d_i/V_i(prior), and contribution to PARENT growth in percentage points 100*d_i/T(prior). Contributions reconcile to parent percentage growth. Do not confuse a child's growth %, its share %, and contribution pp. Avoid d_i/sum(d_i) as the default contribution: offsetting movements or a near-zero net change make it misleading. No percentages with zero/unknown denominator. An absent child in one period is unknown unless zero coverage is independently confirmed, not an automatic zero.
4. Rank by absolute change magnitude, show 3-5 leading positive/negative contributors, their names and codes, previous/current shares, absolute changes and contributions pp. Account for other categories as a residual ONLY from complete reconciled data. Lead with a substantive finding (broad-based change or concentrated in specific categories). Keep tables compact. Retain full precision for calculations; Russian display uses spaced thousands, decimal commas, mass in thousand tonnes.
5. Quantity is additive only within matching units and complete coverage. Do not convert partial known quantities into a total. For fizob use project methodology: heterogeneous physical units can be aggregated by the index methodology, but child indices cannot simply be summed or treated as additive contributions. Monetary decomposition is not a decomposition of real-volume growth. Structural contributions explain WHICH categories changed, not WHY tariffs, prices or consumer behavior caused it without other evidence.
"""

INSTRUCTIONS += "\nFor year-on-year drivers of ONE country/product, call explain_trade FIRST (CN, im, 8703). Use its calculated shares/contribution_pp and retain unmatched_codes/residual; do not reconstruct arithmetic from monthly rows. For country comparisons compare_trade remains first.\n"

INSTRUCTIONS += '\nUse names from the database in human-facing tables and prose, followed by codes in parentheses. explain_trade includes the canonical Russian name from the database, name_source and name_status. Use the Russian database label as the semantic reference; concise labels with exact codes are allowed. Never infer subcategory names from the parent. Show only 3-5 leading contributors plus the largest negative contributors and a brief unallocated-residual note.\n'

# OAuth is enabled for HTTP transports (unless MCP_OAUTH=0). It lets OAuth-only
# clients (Claude web/mobile, ChatGPT) connect; clients that send the raw API key
# as a Bearer token keep working (load_access_token also accepts a raw key).
_TRANSPORT = os.environ.get("MCP_TRANSPORT", "stdio")
_OAUTH = _TRANSPORT != "stdio" and os.environ.get("MCP_OAUTH", "1") != "0"

_fastmcp_kwargs: dict = dict(
    host=os.environ.get("MCP_HOST", "127.0.0.1"),
    port=int(os.environ.get("MCP_PORT", "8000")),
)
oauth_provider = None
if _OAUTH:
    from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions, RevocationOptions

    from oauth import ApiKeyOAuthProvider, PostgresClientStore

    # Client registrations (not secret) persist in Postgres so token refresh
    # survives a restart; without a DSN they live in memory. Tokens are stateless
    # (the token is the API key), so tool calls survive a restart regardless.
    _dsn = os.environ.get("MGIMO_API_POSTGRES_DSN") or os.environ.get("MCP_OAUTH_DB_DSN")
    _client_store = PostgresClientStore(_dsn) if _dsn else None
    oauth_provider = ApiKeyOAuthProvider(API_BASE, _client_store)
    _fastmcp_kwargs.update(
        auth_server_provider=oauth_provider,
        auth=AuthSettings(
            issuer_url=os.environ.get("MCP_ISSUER_URL", "https://nts.mgimo.ru"),
            resource_server_url=os.environ.get("MCP_RESOURCE_URL", "https://nts.mgimo.ru/mcp"),
            required_scopes=[],
            client_registration_options=ClientRegistrationOptions(enabled=True),
            revocation_options=RevocationOptions(enabled=True),
        ),
    )

INSTRUCTIONS += '\n## Аналитический ответ: суть и драйверы\n\nНачинай с результата для аналитика: изменение агрегата, величина и сопоставимый период. В первом абзаце назови также 1-2 главных драйвера, до таблицы. Источник достаточно кратко указать в конце. Для одного короткого ограничения используй примечание, без отдельного большого раздела «Оговорки». Пиши содержательно: «Динамика стоимости включает изменение количества и цен», без противопоставления «это не физические объемы». Затем покажи 3-5 главных подкатегорий с названиями и точными кодами, их доли и вклад в динамику. Объясни, насколько рост сосредоточен в отдельных категориях и как изменилась структура. Лидеров определяй по данным: лидер по вкладу не обязательно лидер по доле или темпу роста. Причины вне данных (спрос, цены, политика) обозначай как гипотезы, только если они полезны вопросу и имеют основание; не придумывай их.\n\nПиши прямо: «Источник: таможенная статистика Китая». Не перечисляй исключенные источники («не Comtrade», «не nowcast») и не строй ответ на оборотах «это не X, а Y», если пользователь не просит разъяснить различия. Внутренние source/type, флаги и формулы не пересказывай как чек-лист. Проверки делай всегда, публикуй только выводы из них, нужные для понимания ответа.\n\nСущественное ограничение ставь рядом с затронутым показателем: отсутствующий месяц, неполное количество, оценка вместо наблюдения, несопоставимость периодов. Если оно делает основной вывод невозможным, сообщи об этом сразу. Иначе краткую справку об источнике, периоде и существенном остатке разложения оставь в конце. Не добавляй общие оговорки о пропусках и оценках без конкретного основания в выбранном срезе. Малый нераспределенный остаток укажи соразмерно его влиянию, без длинного методологического вступления. Не скрывай остаток и не считай пропуски нулями.\n\nНазвания в чате можно сократить для читаемости, сохранив смысл русского справочника и точный код в скобках. В Excel остаются исходные названия. Факт, оценки источника Comtrade и nowcast проекта учитывай раздельно; поясняй различие там, где оно влияет на рассматриваемый показатель. Сначала содержание ответа, затем краткая справка и ссылка на файл, если запрошен.\n'

INSTRUCTIONS += "\nFor top product groups by value use rank_trade FIRST (HS2). For Turkey imports this year call rank_trade(strana='TR',napr='im',top_n=10), without dates. It resolves the latest period, calculates shares over all chapters and returns only requested leaders. ranking.complete_universe confirms the full denominator, not full source coverage. Do not treat shown_groups < total_groups as truncation. Describe fact_months/missing_months, not a full year by default. Ordinary trade has_more/_truncated still requires completing the result before totals.\n"

mcp = FastMCP("mgimo-trade", instructions=INSTRUCTIONS, **_fastmcp_kwargs)


if _OAUTH:
    from starlette.requests import Request
    from starlette.responses import HTMLResponse, RedirectResponse

    def _login_html(login_id: str, error: str | None) -> str:
        err = f'<p class="err">{error}</p>' if error else ""
        return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Подключение MCP - Национальная торговая статистика</title>
<style>
  body {{ font-family: system-ui, Segoe UI, Arial, sans-serif; background:#f4f6f9;
         margin:0; display:flex; min-height:100vh; align-items:center; justify-content:center; }}
  .card {{ background:#fff; padding:32px 28px; border-radius:12px; max-width:420px; width:90%;
          box-shadow:0 6px 24px rgba(0,0,0,.08); }}
  h1 {{ font-size:1.15rem; color:#003d7a; margin:0 0 6px; }}
  p {{ color:#4a5568; font-size:.92rem; line-height:1.45; }}
  input {{ width:100%; box-sizing:border-box; padding:11px 12px; margin:10px 0 4px;
           border:1px solid #cbd5e0; border-radius:8px; font-size:1rem; }}
  button {{ width:100%; padding:11px; background:#003d7a; color:#fff; border:0;
            border-radius:8px; font-size:1rem; cursor:pointer; margin-top:10px; }}
  .err {{ color:#c0392b; font-size:.9rem; margin:6px 0 0; }}
  a {{ color:#003d7a; }}
</style></head><body>
<div class="card">
  <h1>Подключение к данным (MCP)</h1>
  <p>Вставьте персональный API-ключ, чтобы разрешить агенту доступ к данным.
     Ключ выдаётся в <a href="https://nts.mgimo.ru/superset/apikey/" target="_blank">кабинете</a>
     и начинается с <code>mgt_</code>. Он идёт только на наш сервер и не показывается агенту.</p>
  {err}
  <form method="post" action="/oauth/login">
    <input type="hidden" name="login_id" value="{login_id}">
    <input name="api_key" type="password" placeholder="mgt_..." autocomplete="off" autofocus required>
    <button type="submit">Продолжить</button>
  </form>
</div></body></html>"""

    @mcp.custom_route("/oauth/login", methods=["GET", "POST"])
    async def oauth_login(request: Request):
        if request.method == "GET":
            return HTMLResponse(_login_html(request.query_params.get("login_id", ""), None))
        form = await request.form()
        login_id = str(form.get("login_id", ""))
        api_key = str(form.get("api_key", "")).strip()
        result = await oauth_provider.complete_login(login_id, api_key)
        if result is None:
            return HTMLResponse(_login_html("", "Сессия входа истекла. Начните подключение заново."), status_code=400)
        if result == "INVALID":
            return HTMLResponse(_login_html(login_id, "Неверный ключ. Проверьте токен в кабинете."), status_code=401)
        return RedirectResponse(result, status_code=302)


def _env_token() -> str | None:
    tok = os.environ.get("MGIMO_API_TOKEN")
    if not tok:
        for d in (Path.cwd(), Path(__file__).resolve().parent):
            env = d / ".env"
            if env.exists():
                for line in env.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if line.strip().startswith("MGIMO_API_TOKEN="):
                        tok = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
            if tok:
                break
    return tok


def _resolve_token(ctx: Context | None) -> str:
    """Per-request API key: OAuth access token (mapped to the key), else the
    client's raw Bearer header (remote), else env / .env (local stdio)."""
    # HTTP + OAuth: the validated access token carries our API key.
    try:
        from mcp.server.auth.middleware.auth_context import get_access_token

        at = get_access_token()
        if at is not None and getattr(at, "api_key", None):
            return at.api_key
    except Exception:  # noqa: BLE001 - no auth context (e.g. stdio, or OAuth off)
        pass
    # HTTP without OAuth: the client's Bearer header is the API key directly.
    if ctx is not None:
        try:
            req = ctx.request_context.request
            auth = req.headers.get("authorization") if req is not None else None
            if auth and auth.lower().startswith("bearer "):
                tok = auth.split(" ", 1)[1].strip()
                if tok:
                    return tok
        except Exception:  # noqa: BLE001 - no HTTP request context (e.g. stdio)
            pass
    tok = _env_token()
    if not tok:
        raise RuntimeError(
            "No API token. Remote: send Authorization: Bearer <key>. "
            "Local (stdio): set MGIMO_API_TOKEN. "
            "Get a key from the Superset cabinet at https://nts.mgimo.ru/superset/apikey/ ."
        )
    return tok


def _get(path: str, params: list[tuple[str, str]], ctx: Context | None) -> str:
    """GET a JSON endpoint with the resolved token; return the response text."""
    try:
        token = _resolve_token(ctx)
    except RuntimeError as e:
        return str(e)
    pairs = [(k, str(v)) for k, v in params if v is not None and str(v) != ""]
    try:
        resp = httpx.get(
            f"{os.environ.get('MGIMO_API_BASE', DEFAULT_BASE).rstrip('/')}{path}",
            params=pairs,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=90,
        )
    except Exception as e:  # noqa: BLE001 - surface any transport error to the model
        return f"Request failed for {path}: {e}"
    if resp.status_code >= 400:
        return f"HTTP {resp.status_code} for {path}: {resp.text[:800]}"
    if path == "/v1/trade":
        from trade_evidence import trade_evidence
        obj = resp.json()
        obj.update(trade_evidence(obj))
        from coverage_evidence import coverage_evidence
        filters = dict(pairs)
        filters.setdefault('period_to', obj.get('meta',{}).get('period_max'))
        if not obj.get('meta',{}).get('has_more') and not obj.get('_truncated'):
            obj['coverage_evidence'] = coverage_evidence(obj.get('data',[]), filters)
        return json.dumps(obj, ensure_ascii=False)
    return resp.text


def _multi(name: str, values: list[str] | None) -> list[tuple[str, str]]:
    return [(name, v) for v in (values or [])]


@mcp.tool()
def rank_trade(strana: str, napr: str, top_n: int = 10,
               period_from: str | None = None, period_to: str | None = None,
               ctx: Context | None = None) -> str:
    """FIRST for top product groups by value (HS2 chapters), e.g. Turkey imports this year.
    Use strana='TR', napr='im', top_n=10. Without dates uses January through latest
    available month of the database year. Fetches ALL factual chapters, computes
    total and shares before selecting top-N. Intentional top-N is not truncation.
    Returns actual/missing months. No preliminary meta call or Excel required.
    """
    from decomposition import rank_chapters
    try:
        with httpx.Client() as client:
            result = rank_chapters(client, API_BASE, {"Authorization": f"Bearer {_resolve_token(ctx)}"},
                                   strana, napr, top_n, period_from, period_to)
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error":"Ranking unavailable", "code":type(exc).__name__})


@mcp.tool()
def explain_trade(strana: str, napr: str, code: str, months: int = 12,
                  period_to: str | None = None, source: str | None = None,
                  ctx: Context | None = None) -> str:
    """FIRST tool to explain year-on-year drivers for ONE country and HS2/HS4 code.
    Example: strana="CN", napr="im", code="8703", months=12.
    Computes matched parent totals, child shares, absolute changes and contribution
    percentage points. No preliminary meta/trade needed. Missing child is not zero;
    retain unmatched_codes and unallocated residual. Monetary decomposition only.
    Does not create a file or claim a causal explanation of prices/tariffs.
    """
    from decomposition import explain
    try:
        with httpx.Client() as client:
            result = explain(client, API_BASE, {"Authorization": f"Bearer {_resolve_token(ctx)}"},
                             strana, napr, code, months, period_to, source)
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error":"Decomposition unavailable", "code":type(exc).__name__})


@mcp.tool()
def compare_trade(
    strana: list[str], napr: str, months: int = 12,
    period_from: str | None = None, period_to: str | None = None,
    tnved2: list[str] | None = None, tnved4: list[str] | None = None,
    tnved6: list[str] | None = None, tnved: list[str] | None = None,
    include_fizob: bool = True, ctx: Context | None = None,
) -> str:
    """FIRST tool for comparing Russia's trade with countries, e.g. oil to India vs China.

    Use strana=["IN","CN"], napr="ex", tnved4=["2709"], months=12.
    Crude oil=2709, NOT 27. No preliminary meta, trade, or export call needed.
    Anchors to latest database month, intersects factual months inside requested window,
    computes totals, signed differences and percentages relative to FIRST country.
    Returns excluded months and answer_ru with correctly rounded Russian figures.
    Reuse computed numbers; do not recompute from rounded totals, show scratch work,
    add unsolicited year-on-year comparisons or mix predictions into factual totals.
    Automatically adds fizob dynamics for country totals or one HS2/4/6 code:
    a ratio of smoothed index endpoints on the SAME common factual months.
    It is NOT growth of summed period volumes or a comparison of index levels
    across countries. Never sum indices. include_fizob=False disables this block.
    Aggregation of different physical units in country totals is an intentional
    feature of the project methodology, not by itself a defect or reason to
    disclaim the index. Distinguish this from summing raw KOL across units.
    Up to 10 countries and 60 months; rejects truncated data. Does not create a file.
    """
    body = {k: v for k, v in locals().items() if k != "ctx" and v is not None}
    try:
        response = httpx.post(f"{API_BASE}/v1/compare/trade", json=body,
                              headers={"Authorization": f"Bearer {_resolve_token(ctx)}"}, timeout=90)
        response.raise_for_status()
        return json.dumps(response.json(), ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error": "Comparison unavailable", "code": type(exc).__name__})


@mcp.tool()
def export_trade(
    strana: list[str] | None = None, napr: list[str] | None = None,
    tnved2: list[str] | None = None, tnved4: list[str] | None = None,
    tnved6: list[str] | None = None, tnved: list[str] | None = None,
    type: list[str] | None = None, source: list[str] | None = None,
    edizm: list[str] | None = None, edizm_iso: list[str] | None = None,
    period_from: str | None = None, period_to: str | None = None,
    end_mode: str = "latest", group_by: str = "strana,period",
    metrics: str = "stoim,netto", include: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Create a full monthly XLSX and computed summary. Use for broad monthly requests.

    Default end_mode=latest uses the current database period, even if a stale
    period_to is passed. Use fixed ONLY for a user-specified end date. Source and
    fact/pred are separate. Summary totals are NOT matched-period comparisons.
    Use result.comparison for common-fact totals and fizob dynamics; never compare mismatched summary totals. The XLSX includes the same comparison and methodology.
    Petroleum products=2710, crude oil=2709.
    Requires period_from and a country or product filter. At most 60 months, 1000 rows (also capped by plan), and 5 MB. Per user across all tokens and chat: 5000 rows, 20 MB, 10 new files per rolling 24h. On 429 stop and respect Retry-After; never split queries to bypass limits. A partial result is rejected.
    Returns export_id, summary in USD/kg, metadata and an authenticated download
    path. Files expire in 7 days. Use download_export to create a local attachment.
    No API key is returned or embedded in the download URL.
    """
    body = {k: v for k, v in locals().items() if k != "ctx" and v is not None}
    try:
        token = _resolve_token(ctx)
        response = httpx.post(f"{API_BASE}/v1/exports/trade", json=body,
                              headers={"Authorization": f"Bearer {token}"}, timeout=90)
        if response.status_code >= 400:
            return f"HTTP {response.status_code}: {response.text[:800]}"
        result = response.json()
        result["download_url"] = DEFAULT_BASE + result["download_path"]
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error": "Export request failed", "code": type(exc).__name__})


@mcp.tool()
def download_export(export_id: str, ctx: Context | None = None) -> str:
    """Fetch an owned, unexpired XLSX by export_id using the current user's token.

    Decode the returned base64 content to a binary file/artifact if supported.
    Never paste base64 into chat or invent attachments. The workbook has data
    plus a comparison sheet and a description sheet with sources, units, filters, formulas, and limitations.
    """
    try:
        eid = UUID(export_id)
    except ValueError:
        return "Invalid export_id"
    try:
        response = httpx.get(f"{API_BASE}/v1/exports/{eid}",
                            headers={"Authorization": f"Bearer {_resolve_token(ctx)}"}, timeout=90)
        response.raise_for_status()
        if len(response.content) > 5_000_000:
            return json.dumps({"error": "Export exceeds size limit"})
        is_xlsx = "spreadsheetml" in response.headers.get("content-type", "")
        return json.dumps({"filename": f"trade-{eid}.{'xlsx' if is_xlsx else 'csv'}",
                           "mime_type": response.headers.get("content-type"),
                           "encoding": "base64", "content": base64.b64encode(response.content).decode("ascii")})
    except Exception as exc:
        return json.dumps({"error": "Export download failed", "code": type(exc).__name__})


@mcp.tool()
def meta(ctx: Context | None = None) -> str:
    """Your API plan and remaining monthly quota, plus data coverage and the list
    of valid dimensions / metrics / filters.

    You usually do NOT need this before querying: every trade() response already
    carries the latest available month as meta.period_max. Call meta() only to
    check your remaining quota, the earliest period, or the exact names you may
    pass to group_by / metrics / include. No parameters. Returns JSON.
    """
    return _get("/v1/meta", [], ctx)


@mcp.tool()
def reference(name: str = "countries", level: int | None = None, limit: int | None = None,
              ctx: Context | None = None) -> str:
    """Lookup dictionaries for resolving codes and names.

    name="countries": ISO-2 country codes and their Russian names.
    name="tnved": HS/ТН ВЭД commodity-code names; pass level 2, 4, 6, 8 or 10.
    Use this to turn a product/country name into a code before filtering, or a
    code into a human name. Returns JSON.
    """
    params: list[tuple[str, str]] = []
    if level is not None:
        params.append(("level", str(level)))
    if limit is not None:
        params.append(("limit", str(limit)))
    return _get(f"/v1/reference/{name}", params, ctx)


@mcp.tool()
def trade(
    strana: list[str] | None = None,
    napr: list[str] | None = None,
    tnved2: list[str] | None = None,
    tnved4: list[str] | None = None,
    tnved6: list[str] | None = None,
    tnved: list[str] | None = None,
    type: list[str] | None = None,
    source: list[str] | None = None,
    edizm: list[str] | None = None,
    period_from: str | None = None,
    period_to: str | None = None,
    group_by: str | None = None,
    metrics: str | None = None,
    include: str | None = None,
    order_by: str | None = None,
    limit: int | None = None,
    ctx: Context | None = None,
) -> str:
    """Query Russian foreign-trade rows or server-side aggregates. This one tool
    For explanations of change, obtain matched-period child aggregates with shares and contributions (see server instructions). It answers simple questions in a SINGLE call - prefer one well-chosen call over
    several narrow ones. Work from the general to the specific: aggregate at a
    high level first (a tnved2 chapter / strana / year), then drill into the
    significant part at a finer level - do not pull raw rows to total them.

    Repeatable filters (pass a list of strings):
      strana  ISO-2 partner country, e.g. ["CN"], ["IN","TR"] (China/India/Turkey
              are the detailed national sources; 138 more come from UN Comtrade).
      napr    direction: "ЭК" (export from Russia) or "ИМ" (import to Russia).
      tnved2 / tnved4 / tnved6 / tnved  HS / ТН ВЭД codes at 2, 4, 6 or full digits.
      type    "fact" or "pred" (model estimate for the newest months).
      source  "national", "comtrade" or "nowcast".
      edizm   unit of measure.
    Period (month start, YYYY-MM-DD): period_from, period_to.
    group_by  comma-separated dimensions from: strana, napr, type, source,
              tnved2/4/6/tnved, edizm, period (monthly), year (yearly). Omit for
              raw rows; set it for aggregates (sums over the group).
    metrics   comma list from: stoim (value, USD), netto (net weight, kg),
              kol (quantity in the extra unit). Default "stoim,netto". Request all
              you need at once, e.g. "stoim,netto". Note: kol is only additive
              within one unit, so "...,kol" requires "edizm" in group_by.
    include   extra name columns, e.g. "tnved2_name".
    order_by  a grouped dimension or a selected metric; prefix "-" for descending
              (e.g. "-stoim"). limit: max rows returned.

    Every response's meta block includes period_max (latest available month) and
    has_more - so you do NOT need a separate meta() call to bound "this year" or
    "latest". Answer common questions in one call:
      - year-to-date total: period_from="<year>-01-01", no period in group_by.
      - monthly dynamics: add "period" to group_by.
      - compare years: first verify matching calendar-month coverage with monthly facts (ONE call,
        not one call per year).
      - product/country breakdown: group_by a tnved level or strana, order_by
        "-stoim", set limit for a top-N.
    Only call reference() when you do not know a code. Returns JSON.
    """
    params: list[tuple[str, str]] = []
    for key, vals in (
        ("strana", strana), ("napr", napr), ("type", type), ("source", source),
        ("tnved2", tnved2), ("tnved4", tnved4), ("tnved6", tnved6), ("tnved", tnved),
        ("edizm", edizm),
    ):
        params += _multi(key, vals)
    for key, val in (
        ("period_from", period_from), ("period_to", period_to), ("group_by", group_by),
        ("metrics", metrics), ("include", include), ("order_by", order_by), ("limit", limit),
    ):
        if val is not None:
            params.append((key, str(val)))
    return _get("/v1/trade", params, ctx)


@mcp.tool()
def fizob(
    strana: list[str] | None = None,
    napr: list[str] | None = None,
    tn_level: list[str] | None = None,
    tn_code: list[str] | None = None,
    period_from: str | None = None,
    period_to: str | None = None,
    order_by: str | None = None,
    limit: int | None = None,
    ctx: Context | None = None,
) -> str:
    """Physical-volume indices: real trade volumes with the price effect removed.

    This indicator is a project speciality (not available in TradeMap or UN
    Comtrade). Filters (repeatable, pass a list):
      strana    ISO-2 partner country.
      napr      "ЭК" (export) or "ИМ" (import).
      tn_level  index granularity: "0" (whole country), or "2" / "4" / "6" (HS level).
      tn_code   specific HS / ТН ВЭД code at the chosen level.
    Period (YYYY-MM-DD): period_from, period_to.
    order_by  one of: period, strana, napr, tn_level, tn_code, idx (ascending).
    limit: max rows. Returns JSON (idx is the index; fact-only, no nowcast).
    """
    params: list[tuple[str, str]] = []
    for key, vals in (("strana", strana), ("napr", napr), ("tn_level", tn_level), ("tn_code", tn_code)):
        params += _multi(key, vals)
    for key, val in (("period_from", period_from), ("period_to", period_to), ("order_by", order_by), ("limit", limit)):
        if val is not None:
            params.append((key, str(val)))
    return _get("/v1/fizob", params, ctx)


def main() -> None:
    mcp.run(transport=os.environ.get("MCP_TRANSPORT", "stdio"))


if __name__ == "__main__":
    main()
