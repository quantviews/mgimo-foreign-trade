"""Initial setup on MGIMO 146. Never prints credentials. No data calculations.

Run as stp after extracting the source bundle to ~/mgimo-trade.
Requires Docker access, Python and the existing PostgreSQL container.
Refuses to overwrite service env files. Migrations have a checksum ledger.
"""
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
PG = "mgimo-superset-postgres-1"


def sql(query, database="postgres"):
    r = subprocess.run(["docker", "exec", "-i", PG, "psql", "-X", "-U", "superset",
                        "-d", database, "-At", "-v", "ON_ERROR_STOP=1"],
                       input=query, text=True, capture_output=True)
    if r.returncode:
        raise RuntimeError("PostgreSQL operation failed; inspect locally without exposing credentials")
    return r.stdout.strip()


def write_env(path, values):
    with path.open("x", encoding="utf-8") as f:
        os.chmod(path, 0o600)
        for k, v in values.items():
            # dotenv quoted literals preserve dollar characters in provider passwords.
            if "\n" in str(v) or "'" in str(v):
                raise ValueError("Unsupported env value")
            f.write(f"{k}='{v}'\n")


if __name__ == "__main__":
    if (HERE / "api.env").exists() or (HERE / "demo.env").exists():
        raise SystemExit("Service env exists. Do not run initial bootstrap again.")
    if sql("SELECT 1 FROM pg_database WHERE datname='tradeapi'"):
        raise SystemExit("tradeapi exists. Inspect before initial bootstrap.")
    password = secrets.token_urlsafe(40)
    sql(f"CREATE ROLE tradeapp LOGIN PASSWORD '{password}';")
    # CREATE DATABASE cannot run in the implicit transaction of a multi-command query.
    sql("CREATE DATABASE tradeapi OWNER tradeapp;")
    sql("CREATE TABLE public.deployment_migrations(name text primary key, sha256 text not null, applied_at timestamptz default now());", "tradeapi")
    for p in sorted((ROOT / "api/migrations").glob("*.sql")):
        sql("SET ROLE tradeapp;\n" + p.read_text(encoding="utf-8-sig"), "tradeapi")
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        sql(f"INSERT INTO public.deployment_migrations(name,sha256) VALUES ('{p.name}','{digest}');", "tradeapi")
        print("Applied", p.name)
    dsn = f"postgresql://tradeapp:{password}@127.0.0.1:5432/tradeapi"
    service_key = "mgt_" + secrets.token_urlsafe(32)
    digest = hashlib.sha256(service_key.encode()).hexdigest()
    sql("INSERT INTO api.users(email,plan_id) VALUES ('demo-service@nts.mgimo.ru',(SELECT id FROM api.plans WHERE code='demo'));"
        f"INSERT INTO api.tokens(user_id,token_hash,prefix) SELECT id,'{digest}','{service_key[:10]}' FROM api.users WHERE email='demo-service@nts.mgimo.ru';", "tradeapi")
    sql("INSERT INTO api.export_delegates(token_id) SELECT t.id FROM api.tokens t JOIN api.users u ON u.id=t.user_id WHERE u.email='demo-service@nts.mgimo.ru' AND t.revoked_at IS NULL ON CONFLICT DO NOTHING;", "tradeapi")
    write_env(HERE / "api.env", {"MGIMO_API_POSTGRES_DSN": dsn, "MGIMO_API_REDIS_URL": "redis://127.0.0.1:6379/2"})
    write_env(HERE / "demo.env", {
        "MGIMO_DEMO_POSTGRES_DSN": dsn, "MGIMO_DEMO_API_BASE": "http://127.0.0.1:8090",
        "MGIMO_DEMO_SERVICE_API_KEY": service_key,
        "MGIMO_DEMO_SESSION_SECRET": secrets.token_urlsafe(48),
        "MGIMO_DEMO_SITE_BASE_URL": "https://nts.mgimo.ru",
        "MGIMO_DEMO_DEEPSEEK_API_KEY": "", "MGIMO_DEMO_SMTP_HOST": "",
        "MGIMO_DEMO_SMTP_PASSWORD": "", "MGIMO_DEMO_NOTIFY_EMAIL": "",
    })
    write_env(HERE / "cabinet.env", {"TRADEAPI_DSN": dsn})
    print("Created tradeapi and private service configuration. Provider and SMTP credentials pending.")
