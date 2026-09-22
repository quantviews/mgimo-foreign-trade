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


@app.post("/demo/register")
async def register(body: RegisterIn):
    e = body.email.strip().lower()
    if not is_valid_email(e):
        return JSONResponse({"error": "Некорректный адрес почты."}, status_code=400)
    if not is_corporate(e):
        return JSONResponse(
            {"error": "Пожалуйста, используйте корпоративную (рабочую) почту. "
                      "Бесплатные почтовые сервисы не подходят для демо-доступа."},
            status_code=400,
        )
    token = await db.create_verification(e, (body.org or None))
    link = f"{settings.site_base_url}/demo/verify?token={token}"
    await email.send_verification(e, link)
    return {"ok": True, "message": "Проверьте почту: мы отправили ссылку для подтверждения."}


@app.get("/demo/verify")
async def verify(token: str):
    rec = await db.consume_verification(token)
    if rec is None:
        return _html("Ссылка недействительна или истекла. Запросите доступ заново.")
    user_id = await db.provision_user(rec["email"], rec["org"])
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

    async def gen():
        yield _event({"type": "start", "remaining": remaining})
        async for ev in run_agent(msgs):
            yield _event(ev)
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
