"""PostgreSQL regression for login-email deduplication. All fixtures roll back.

Run with the demo's configured environment. Does not send email.
"""
import asyncio
from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import db


async def main():
    pool = await db.init_pool()
    original = db._require_pool
    try:
        async with pool.acquire() as con:
            tx = con.transaction()
            await tx.start()
            async def connection():
                return con
            db._require_pool = connection
            email = f"dedup-test-{uuid4()}@example.invalid"
            try:
                assert not await db.recent_verification(email, 10)
                await con.execute(
                    "INSERT INTO email_verifications(token,email,expires_at) "
                    "VALUES($1,$2,now()+interval '1 hour')", str(uuid4()), email)
                assert await db.recent_verification(email, 10)
                await con.execute("UPDATE email_verifications SET consumed_at=now() WHERE email=$1", email)
                assert not await db.recent_verification(email, 10)
                await con.execute("UPDATE email_verifications SET consumed_at=NULL, expires_at=now()-interval '1 second' WHERE email=$1", email)
                assert not await db.recent_verification(email, 10)
                await con.execute("UPDATE email_verifications SET expires_at=now()+interval '1 hour', created_at=now()-interval '11 minutes' WHERE email=$1", email)
                assert not await db.recent_verification(email, 10)
            finally:
                db._require_pool = original
                await tx.rollback()
        print('Verification dedup: absent, active, consumed, expired and old records OK; fixtures rolled back.')
    finally:
        await db.close_pool()


if __name__ == '__main__':
    asyncio.run(main())
