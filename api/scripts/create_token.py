"""Create/ensure a user and issue an API token (prints the raw token once).

Usage:
    MGIMO_API_POSTGRES_DSN=... python api/scripts/create_token.py user@org.ru [--org ORG] [--plan pilot]
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import store  # noqa: E402
from app.config import settings  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("email")
    parser.add_argument("--org", default=None)
    parser.add_argument("--plan", default="pilot")
    args = parser.parse_args()

    if not settings.postgres_dsn:
        raise SystemExit("Set MGIMO_API_POSTGRES_DSN first.")

    try:
        raw = await store.create_user_with_token(args.email, org=args.org, plan_code=args.plan)
        print("API token (store it now, shown once):")
        print(raw)
    finally:
        await store.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
