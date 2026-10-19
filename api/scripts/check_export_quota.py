"""Integration check against configured Postgres; uses and removes isolated test users."""
import asyncio
import sys
from pathlib import Path
from uuid import uuid4
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import store

async def check():
    pool = await store.init_pool()
    users = []
    try:
        for _ in range(2):
            uid = await pool.fetchval("INSERT INTO users(email,plan_id) VALUES($1,(SELECT id FROM plans WHERE code='pilot')) RETURNING id", f"quota-test-{uuid4()}@example.invalid")
            users.append(uid)
        uid, other = users
        async def reserve(who, rows=1000, size=100):
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval("SELECT api.reserve_export($1,$2,$3,$4)", uuid4(), who, rows, size)
        results = await asyncio.gather(*(reserve(uid) for _ in range(6)))
        assert results.count(0) == 5 and sum(v>0 for v in results) == 1, results
        assert await reserve(other) == 0
        assert await pool.fetchval("SELECT sum(row_count) FROM export_usage WHERE user_id=$1",uid) == 5000
        print("Concurrency and user isolation: OK")
        await pool.execute("UPDATE export_usage SET created_at=now()-interval '25 hours' WHERE user_id=$1",uid)
        assert await reserve(uid) == 0
        print("Rolling expiry: OK")
        await pool.execute("DELETE FROM export_usage WHERE user_id=$1",uid)
        for _ in range(10): assert await reserve(uid,1) == 0
        assert await reserve(uid,1) > 0
        print("File count cap: OK")
        await pool.execute("DELETE FROM export_usage WHERE user_id=$1",uid)
        for _ in range(4): assert await reserve(uid,1,5000000) == 0
        assert await reserve(uid,1,1) > 0
        print("Byte cap: OK")
        await pool.execute("DELETE FROM export_usage WHERE user_id=$1",uid)
        async with pool.acquire() as conn:
            tx=conn.transaction()
            await tx.start()
            assert await conn.fetchval("SELECT api.reserve_export($1,$2,$3,$4)",uuid4(),uid,1000,100) == 0
            await tx.rollback()
        assert await pool.fetchval("SELECT count(*) FROM export_usage WHERE user_id=$1",uid) == 0
        print("Rollback restores quota: OK")
    finally:
        for uid in users:
            await pool.execute("DELETE FROM export_usage WHERE user_id=$1",uid)
            await pool.execute("DELETE FROM users WHERE id=$1",uid)
        await store.close_pool()

if __name__ == '__main__': asyncio.run(check())
