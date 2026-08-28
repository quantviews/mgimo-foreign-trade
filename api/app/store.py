"""Postgres-backed store for tokens, users, plans and the audit log.

asyncpg is imported lazily so the service (and its unit tests) run in dev mode
without asyncpg installed. Tables live in schema `api` (set via search_path on the
pool). Pure token helpers below need no Postgres and are unit-tested.
"""

from __future__ import annotations

import hashlib
import json
import secrets

from .config import settings

_pool = None


# --- Pure helpers (no DB) ---------------------------------------------------

def generate_token() -> str:
    """Opaque API key. Shown to the user once; only its hash is stored."""
    return "mgt_" + secrets.token_urlsafe(24)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def token_prefix(token: str) -> str:
    """Non-secret prefix for display/lookup hints, e.g. 'mgt_ab12cd'."""
    return token[:10]


# --- Pool lifecycle ---------------------------------------------------------

async def init_pool():
    """Create the asyncpg pool if a DSN is configured. No-op in dev mode."""
    global _pool
    if not settings.postgres_dsn:
        return None
    if _pool is None:
        import asyncpg  # lazy: only needed in Postgres mode

        _pool = await asyncpg.create_pool(
            settings.postgres_dsn,
            min_size=1,
            max_size=10,
            server_settings={"search_path": "api"},
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def _require_pool():
    pool = await init_pool()
    if pool is None:
        raise RuntimeError("Postgres DSN not configured")
    return pool


# --- Queries ----------------------------------------------------------------

async def lookup_token(token: str) -> dict | None:
    """Validate a raw token; return a user/plan dict or None. Touches last_used_at."""
    pool = await _require_pool()
    row = await pool.fetchrow(
        """
        SELECT t.id AS token_id, u.id AS user_id, u.email,
               p.code AS plan_code, p.max_rows, p.scopes,
               p.monthly_quota, p.rate_limit_per_min
        FROM tokens t
        JOIN users u ON u.id = t.user_id AND u.active
        JOIN plans p ON p.id = u.plan_id AND p.active
        WHERE t.token_hash = $1
          AND t.revoked_at IS NULL
          AND (t.expires_at IS NULL OR t.expires_at > now())
        """,
        hash_token(token),
    )
    if row is None:
        return None
    await pool.execute("UPDATE tokens SET last_used_at = now() WHERE id = $1", row["token_id"])
    return {
        "user_id": row["user_id"],
        "email": row["email"],
        "token_id": row["token_id"],
        "plan": {
            "code": row["plan_code"],
            "max_rows": row["max_rows"],
            "scopes": list(row["scopes"]) if row["scopes"] else [],
            "monthly_quota": row["monthly_quota"],
            "rate_limit_per_min": row["rate_limit_per_min"],
        },
    }


async def create_user_with_token(
    email: str, org: str | None = None, plan_code: str = "pilot",
    superset_user_id: int | None = None,
) -> str:
    """Create/ensure a user and issue a fresh token. Returns the raw token (once)."""
    pool = await _require_pool()
    raw = generate_token()
    async with pool.acquire() as con:
        async with con.transaction():
            plan_id = await con.fetchval(
                "SELECT id FROM plans WHERE code = $1 AND active", plan_code
            )
            if plan_id is None:
                raise ValueError(f"Unknown or inactive plan: {plan_code}")
            user_id = await con.fetchval(
                """
                INSERT INTO users(email, org, plan_id, superset_user_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (email) DO UPDATE SET org = EXCLUDED.org
                RETURNING id
                """,
                email, org, plan_id, superset_user_id,
            )
            await con.execute(
                "INSERT INTO tokens(user_id, token_hash, prefix) VALUES ($1, $2, $3)",
                user_id, hash_token(raw), token_prefix(raw),
            )
    return raw


async def insert_audit(record: dict) -> None:
    pool = await _require_pool()
    await pool.execute(
        """
        INSERT INTO audit_log
            (user_id, token_id, endpoint, method, params, status,
             rows_returned, bytes, latency_ms, cost_units, ip)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11::inet)
        """,
        record.get("user_id"),
        record.get("token_id"),
        record["endpoint"],
        record["method"],
        json.dumps(record.get("params"), ensure_ascii=False),
        record["status"],
        record.get("rows_returned"),
        record.get("bytes"),
        record.get("latency_ms"),
        record.get("cost_units", 1),
        record.get("ip"),
    )


async def monthly_usage(user_id: int) -> int:
    """Request count for the current calendar month (for the cabinet / quotas)."""
    pool = await _require_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM audit_log "
        "WHERE user_id = $1 AND ts >= date_trunc('month', now())",
        user_id,
    )
