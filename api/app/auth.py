"""Token authentication.

Token is taken ONLY from headers (never a query param):
  - Authorization: Bearer <token>
  - HTTP Basic where the password is the token (username ignored) — needed for
    Excel Power Query.

Dev mode (no MGIMO_API_POSTGRES_DSN): a single static token from settings is accepted.
Prod mode: validate sha256(token) against the Postgres `api_tokens` table (TODO).
"""

from __future__ import annotations

import base64
import binascii
import hashlib

from fastapi import Header, HTTPException, Request, status

from .config import settings


def _extract_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, value = authorization.partition(" ")
    scheme = scheme.strip().lower()
    value = value.strip()
    if scheme == "bearer":
        return value or None
    if scheme == "basic":
        try:
            decoded = base64.b64decode(value).decode("utf-8", "replace")
        except (binascii.Error, ValueError):
            return None
        # username:password — the password carries the token.
        _, _, password = decoded.partition(":")
        return password or None
    return None


def _pilot_user(token_prefix: str) -> dict:
    return {
        "user_id": 0,
        "email": "dev@local",
        "plan": {"code": "pilot", "max_rows": settings.max_page_rows},
        "token_prefix": token_prefix,
        "token_id": None,
    }


async def authenticate(
    request: Request, authorization: str | None = Header(default=None)
) -> dict:
    token = _extract_token(authorization)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing token (Authorization: Bearer <token> or Basic).",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if settings.postgres_dsn:
        from . import store  # lazy: avoids importing asyncpg in dev mode

        user = await store.lookup_token(token)
        if user is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    else:
        if token != settings.dev_token:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
        user = _pilot_user(hashlib.sha256(token.encode()).hexdigest()[:8])

    # Expose the resolved user to the audit middleware.
    request.state.user = user

    # Quota / rate-limit enforcement (no-op without Redis or plan limits).
    from . import limits  # lazy

    try:
        await limits.enforce(user)
    except limits.RateLimited as e:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=e.detail,
            headers={"Retry-After": str(e.retry_after)},
        )
    return user
