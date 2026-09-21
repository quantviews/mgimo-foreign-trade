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
from pathlib import Path

import httpx
from mcp.server.fastmcp import Context, FastMCP

DEFAULT_BASE = "https://nts.mgimo.ru/api"

API_BASE = os.environ.get("MGIMO_API_BASE", DEFAULT_BASE).rstrip("/")

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

mcp = FastMCP("mgimo-trade", **_fastmcp_kwargs)


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
    return resp.text


def _multi(name: str, values: list[str] | None) -> list[tuple[str, str]]:
    return [(name, v) for v in (values or [])]


@mcp.tool()
def meta(ctx: Context | None = None) -> str:
    """Data version and coverage, plus your API plan and remaining monthly quota.

    Call this first to learn the latest available period (period_max) and how
    much of your quota is left. No parameters. Returns JSON.
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
    """Query Russian foreign-trade rows or server-side aggregates.

    Repeatable filters (pass a list of strings):
      strana  ISO-2 partner country, e.g. ["CN"], ["IN","TR"] (China/India/Turkey
              are the detailed national sources; 138 more come from UN Comtrade).
      napr    direction: "ЭК" (export from Russia) or "ИМ" (import to Russia).
      tnved2 / tnved4 / tnved6 / tnved  HS / ТН ВЭД codes at 2, 4, 6 or full digits.
      type    "fact" or "nowcast" (model estimate for the newest months).
      source  "national", "comtrade" or "nowcast".
      edizm   unit of measure.
    Period (month start, YYYY-MM-DD): period_from, period_to.
    group_by  comma-separated dimensions, e.g. "strana,tnved2,period". Omit for
              raw rows; set it for aggregates (sums over the group).
    metrics   comma list from: stoim (value, USD), netto (net weight, kg),
              kol (quantity in the extra unit). Default "stoim,netto".
    include   extra name columns, e.g. "tnved2_name".
    order_by  a grouped dimension or a selected metric; prefix "-" for descending
              (e.g. "-stoim"). limit: max rows returned.
    Returns JSON. Tip: call meta() for the latest period and reference() for codes.
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
