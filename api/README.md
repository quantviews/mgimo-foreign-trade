# MGIMO Foreign Trade API

Thin read-only FastAPI service over the DuckDB serving file
(`db/unified_trade_data.duckdb`). Phase 1 (MVP) — see
[../docs/api-mvp-phase1.md](../docs/api-mvp-phase1.md).

## Run (dev mode, no Postgres)

```bash
pip install -r api/requirements.txt
# from the repo root:
uvicorn app.main:app --app-dir api --reload
```

Dev mode accepts a single static token (`MGIMO_API_DEV_TOKEN`, default `dev-token`)
and logs audit records to stdout.

## Postgres mode (tokens + audit)

Set `MGIMO_API_POSTGRES_DSN` to use the Postgres-backed store (schema `api`,
same instance as Superset). Bootstrap:

```bash
export MGIMO_API_POSTGRES_DSN=postgres://user:pass@host:5432/superset
python api/scripts/init_db.py                     # apply migrations/001_init.sql
python api/scripts/create_token.py user@org.ru    # prints a raw token once
```

Then call with that token. Every request is written to `api.audit_log` with
`cost_units` (the metering/monitoring foundation); `/v1/meta` returns your
`requests_this_month`. Token validation, `last_used_at`, plans and per-user usage
all run through `app/store.py`. Self-registration and the cabinet page live in
Superset (later); `create_token.py` bootstraps users until then.

## Try it

```bash
# no auth
curl localhost:8000/health

# aggregated trade (Bearer)
curl -H "Authorization: Bearer dev-token" \
  "localhost:8000/v1/trade?strana=CN&napr=im&period_from=2024-01&group_by=tnved2,period"

# with names + kol (edizm required in group_by for kol)
curl -H "Authorization: Bearer dev-token" \
  "localhost:8000/v1/trade?group_by=tnved2,edizm,period&metrics=stoim,netto,kol&include=tnved2_name"
```

## Excel (Power Query)

Data → Get Data → From Web → the `/v1/trade?...&format=json` URL; auth = Basic,
username anything, password = your API token. JSON keeps types and UTF-8 (no CSV
delimiter/encoding pain).

## Config (env, prefix `MGIMO_API_`)

| Var | Default | Meaning |
|---|---|---|
| `DUCKDB_PATH` | `db/unified_trade_data.duckdb` | serving file (read-only) |
| `POSTGRES_DSN` | *(empty)* | empty = dev mode |
| `DEV_TOKEN` | `dev-token` | dev-mode static token |
| `MAX_PAGE_ROWS` | `100000` | hard page ceiling |
| `DEFAULT_PAGE_ROWS` | `10000` | default page size |

## Status

Done: read-only DuckDB, safe query builder (allowlist + params), `/health`,
`/v1/meta` (+ monthly usage), `/v1/reference/*`, `/v1/trade` (JSON/CSV),
Bearer+Basic auth (dev token or Postgres tokens), audit middleware (stdout or
Postgres `audit_log`), Postgres store (`app/store.py`) + migration + bootstrap
scripts, problem+json errors.
TODO: keyset cursor export, quota/rate-limit enforcement (Phase 2), OData
(Phase 3), Superset cabinet page + self-registration.
