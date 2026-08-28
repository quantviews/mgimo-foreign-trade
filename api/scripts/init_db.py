"""Apply the API schema to Postgres.

Usage:
    MGIMO_API_POSTGRES_DSN=postgres://user:pass@host/db python api/scripts/init_db.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.config import settings  # noqa: E402

MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "001_init.sql"


async def main() -> None:
    if not settings.postgres_dsn:
        raise SystemExit("Set MGIMO_API_POSTGRES_DSN first.")
    import asyncpg

    sql = MIGRATION.read_text(encoding="utf-8")
    con = await asyncpg.connect(settings.postgres_dsn)
    try:
        await con.execute(sql)
        print(f"Applied {MIGRATION.name}")
    finally:
        await con.close()


if __name__ == "__main__":
    asyncio.run(main())
