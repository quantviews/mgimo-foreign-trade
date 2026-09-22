"""Postgres store for demo access: users, per-user turns, email verifications.

Shares the API's Postgres and schema `api`. asyncpg pool with search_path=api.
"""

from __future__ import annotations

import secrets

from .config import settings

_pool = None


async def init_pool():
    global _pool
    if not settings.postgres_dsn:
        raise RuntimeError("MGIMO_DEMO_POSTGRES_DSN not configured")
    if _pool is None:
        import asyncpg

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
    return await init_pool()


# --- Email verification -----------------------------------------------------

async def create_verification(email: str, org: str | None) -> str:
    token = secrets.token_urlsafe(24)
    pool = await _require_pool()
    await pool.execute(
        "INSERT INTO email_verifications(token, email, org, expires_at) "
        "VALUES ($1, $2, $3, now() + make_interval(hours => $4))",
        token, email.lower(), org, settings.verify_ttl_hours,
    )
    return token


async def consume_verification(token: str) -> dict | None:
    """Return {email, org} and mark consumed, or None if invalid/expired/used."""
    pool = await _require_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            row = await con.fetchrow(
                "SELECT email, org FROM email_verifications "
                "WHERE token = $1 AND consumed_at IS NULL AND expires_at > now() "
                "FOR UPDATE",
                token,
            )
            if row is None:
                return None
            await con.execute(
                "UPDATE email_verifications SET consumed_at = now() WHERE token = $1",
                token,
            )
            return {"email": row["email"], "org": row["org"]}


# --- Provisioning + access --------------------------------------------------

async def user_exists(email: str) -> bool:
    """True if the address is already a known (active) user - used to let existing
    API/MCP users past the corporate-domain gate."""
    pool = await _require_pool()
    val = await pool.fetchval(
        "SELECT 1 FROM users WHERE email = $1 AND active", email.lower()
    )
    return val is not None


async def provision_user(email: str, org: str | None) -> int:
    """Upsert the user (kept on their existing plan; new ones go on 'demo') and
    ensure a demo_access row. Existing users get the higher turn limit. Returns id."""
    pool = await _require_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            demo_plan = await con.fetchval(
                "SELECT id FROM plans WHERE code = 'demo' AND active"
            )
            if demo_plan is None:
                raise RuntimeError("demo plan missing (run migration 004_demo.sql)")
            # Was this a known user before we upsert? Existing users (pilot etc.)
            # get the higher limit; brand-new demo signups get the small one.
            existed = await con.fetchval(
                "SELECT 1 FROM users WHERE email = $1", email.lower()
            )
            limit = (settings.existing_user_turn_limit if existed
                     else settings.turn_limit)
            user_id = await con.fetchval(
                """
                INSERT INTO users(email, org, plan_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (email) DO UPDATE
                    SET org = COALESCE(EXCLUDED.org, users.org)
                RETURNING id
                """,
                email.lower(), org, demo_plan,
            )
            await con.execute(
                "INSERT INTO demo_access(user_id, turn_limit) VALUES ($1, $2) "
                "ON CONFLICT (user_id) DO NOTHING",
                user_id, limit,
            )
    return user_id


async def get_access(user_id: int) -> dict | None:
    pool = await _require_pool()
    row = await pool.fetchrow(
        "SELECT turns_used, turn_limit FROM demo_access WHERE user_id = $1", user_id
    )
    return dict(row) if row else None


async def try_consume_turn(user_id: int) -> tuple[bool, int, str]:
    """Atomically reserve one turn against the per-user limit and the daily budget.

    Returns (ok, remaining, reason). reason in {ok, no_access, limit, daily}.
    """
    pool = await _require_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            row = await con.fetchrow(
                "SELECT turns_used, turn_limit FROM demo_access "
                "WHERE user_id = $1 FOR UPDATE",
                user_id,
            )
            if row is None:
                return (False, 0, "no_access")
            remaining = row["turn_limit"] - row["turns_used"]
            if remaining <= 0:
                return (False, 0, "limit")

            day_turns = await con.fetchval(
                "INSERT INTO demo_usage_daily(day, turns) VALUES (current_date, 0) "
                "ON CONFLICT (day) DO UPDATE SET turns = demo_usage_daily.turns "
                "RETURNING turns"
            )
            if day_turns >= settings.daily_budget:
                return (False, remaining, "daily")

            await con.execute(
                "UPDATE demo_access SET turns_used = turns_used + 1, updated_at = now() "
                "WHERE user_id = $1",
                user_id,
            )
            await con.execute(
                "UPDATE demo_usage_daily SET turns = turns + 1 WHERE day = current_date"
            )
            return (True, remaining - 1, "ok")
