"""Demo AI-chat service.

Flow: register (corporate email) -> verification email -> verify link provisions a
demo user and sets a session cookie -> chat (DeepSeek + our data tools), capped at
N turns per user with a global daily backstop. The chat uses one shared demo-plan
API key; no per-user raw key is ever stored.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import json
import time

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel

from . import db, email
from .config import settings
from .deepseek import run_agent
from .domains import is_corporate, is_valid_email

COOKIE = "demo_session"

# --- Registration anti-abuse (in-memory backstop behind the nginx rate limit) --
_reg_ip_hits: dict[str, list[float]] = {}
_reg_global_hits: list[float] = []


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "?"


def _reg_allowed(ip: str) -> bool:
    """Per-IP and global hourly caps on registrations, to blunt form spam."""
    now = time.time()
    cutoff = now - 3600
    _reg_global_hits[:] = [t for t in _reg_global_hits if t > cutoff]
    hits = [t for t in _reg_ip_hits.get(ip, []) if t > cutoff]
    if (len(hits) >= settings.reg_max_per_ip_hour
            or len(_reg_global_hits) >= settings.reg_max_global_hour):
        _reg_ip_hits[ip] = hits
        return False
    hits.append(now)
    _reg_ip_hits[ip] = hits
    _reg_global_hits.append(now)
    return True


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    try:
        yield
    finally:
        await db.close_pool()


app = FastAPI(title="MGIMO Trade Demo Chat", lifespan=lifespan)


# --- Signed session cookie (identity only, no secret token inside) ----------

def _sign(payload: dict) -> str:
    body = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode()
    ).decode()
    sig = hmac.new(settings.session_secret.encode(), body.encode(),
                   hashlib.sha256).hexdigest()[:32]
    return f"{body}.{sig}"


def _unsign(token: str) -> dict | None:
    try:
        body, sig = token.split(".", 1)
        expected = hmac.new(settings.session_secret.encode(), body.encode(),
                            hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(sig, expected):
            return None
        data = json.loads(base64.urlsafe_b64decode(body.encode()))
        if time.time() - data.get("iat", 0) > settings.session_ttl_hours * 3600:
            return None
        return data
    except Exception:
        return None


def _session(request: Request) -> dict | None:
    tok = request.cookies.get(COOKIE)
    return _unsign(tok) if tok else None


# --- Schemas ----------------------------------------------------------------

class RegisterIn(BaseModel):
    email: str
    org: str | None = None


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatIn(BaseModel):
    messages: list[ChatMessage]


# --- Endpoints --------------------------------------------------------------

@app.get("/demo/health")
async def health():
    return {"ok": True}


_REG_OK = {"ok": True, "message": "Проверьте почту: мы отправили ссылку для подтверждения."}


@app.post("/demo/register")
async def register(request: Request, body: RegisterIn):
    if not _reg_allowed(_client_ip(request)):
        return JSONResponse({"error": "Слишком много запросов. Попробуйте позже."},
                            status_code=429)
    e = body.email.strip().lower()
    if not is_valid_email(e):
        return JSONResponse({"error": "Некорректный адрес почты."}, status_code=400)
    existing = await db.user_exists(e)
    # Existing users (already approved for API/MCP) skip the corporate-domain gate;
    # email verification still applies as the way to log in.
    if not is_corporate(e) and not existing:
        return JSONResponse(
            {"error": "Пожалуйста, используйте корпоративную (рабочую) почту. "
                      "Бесплатные почтовые сервисы не подходят для демо-доступа."},
            status_code=400,
        )
    # Dedup: if we mailed this address recently, do not send again (anti-spam).
    if await db.recent_verification(e, settings.reg_dedup_minutes):
        return _REG_OK
    org = body.org or None
    token = await db.create_verification(e, org)
    link = f"{settings.site_base_url}/demo/verify?token={token}"
    await email.send_verification(e, link)
    # Admin notice is sent on confirmation (see /demo/verify), not here, so the open
    # form cannot be used to spam the team.
    return _REG_OK


@app.get("/demo/verify")
async def verify(token: str):
    rec = await db.consume_verification(token)
    if rec is None:
        return _html("Ссылка недействительна или истекла. Запросите доступ заново.")
    existing = await db.user_exists(rec["email"])
    user_id = await db.provision_user(rec["email"], rec["org"])
    # Notify the team only on a CONFIRMED registration (the email link was clicked),
    # so bots hitting the open register endpoint do not generate notices.
    await email.send_admin_notice(rec["email"], rec["org"], existing)
    cookie = _sign({"uid": user_id, "email": rec["email"], "iat": int(time.time())})
    resp = RedirectResponse(f"{settings.site_base_url}/site/demo.html", status_code=302)
    resp.set_cookie(COOKIE, cookie, max_age=settings.session_ttl_hours * 3600,
                    httponly=True, secure=True, samesite="lax", path="/")
    return resp


@app.get("/demo/me")
async def me(request: Request):
    s = _session(request)
    if not s:
        return JSONResponse({"authenticated": False}, status_code=401)
    acc = await db.get_access(s["uid"])
    if not acc:
        return JSONResponse({"authenticated": False}, status_code=401)
    remaining = max(0, acc["turn_limit"] - acc["turns_used"])
    return {"authenticated": True, "email": s["email"],
            "turns_used": acc["turns_used"], "turn_limit": acc["turn_limit"],
            "remaining": remaining}


@app.post("/demo/chat")
async def chat(request: Request, body: ChatIn):
    s = _session(request)
    if not s:
        return JSONResponse({"error": "Требуется вход."}, status_code=401)

    ok, remaining, reason = await db.try_consume_turn(s["uid"])
    if not ok:
        text = {
            "limit": "Лимит демо-запросов исчерпан. Оставьте заявку на полный доступ.",
            "daily": "Дневной лимит демо исчерпан. Попробуйте завтра или запросите доступ.",
            "no_access": "Доступ не найден. Пройдите регистрацию заново.",
        }.get(reason, "Доступ недоступен.")
        return _sse_single({"type": "error", "reason": reason, "text": text,
                            "remaining": remaining})

    msgs = _sanitize(body.messages)
    if not msgs:
        return _sse_single({"type": "error", "text": "Пустой запрос."})

    question = msgs[-1]["content"]

    async def gen():
        yield _event({"type": "start", "remaining": remaining})
        answer_text, tool_calls, err = "", 0, None
        async for ev in run_agent(msgs):
            t = ev.get("type")
            if t == "answer":
                answer_text = ev.get("text", "")
            elif t == "tool":
                tool_calls += 1
            elif t == "error":
                err = ev.get("text")
            yield _event(ev)
        # Log the question (and answer) for later analysis. Best-effort.
        try:
            await db.log_chat(s["uid"], question, answer_text, tool_calls,
                             "error" if err else "ok")
        except Exception:  # noqa: BLE001
            pass
        yield _event({"type": "done", "remaining": remaining})

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# --- Helpers ----------------------------------------------------------------

def _sanitize(messages: list[ChatMessage]) -> list[dict]:
    out = []
    for m in messages[-settings.max_history_messages:]:
        if m.role in ("user", "assistant") and m.content:
            out.append({"role": m.role, "content": m.content[:2000]})
    # A turn must end on a user message.
    if not out or out[-1]["role"] != "user":
        return []
    return out


def _event(obj: dict) -> str:
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


def _sse_single(obj: dict) -> StreamingResponse:
    async def one():
        yield _event(obj)
        yield _event({"type": "done"})
    return StreamingResponse(one(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


def _html(message: str) -> Response:
    return Response(
        f"<!doctype html><meta charset=utf-8><title>Демо</title>"
        f"<div style='font-family:system-ui;max-width:480px;margin:80px auto;"
        f"padding:24px;color:#2c3e50'>{message} "
        f"<a href='{settings.site_base_url}/site/mcp.html'>Назад</a></div>",
        media_type="text/html",
    )
