-- API Phase 1 schema: plans, users, tokens, audit log.
-- Lives in schema `api` inside the same Postgres as Superset.
-- Idempotent: safe to re-run.

CREATE SCHEMA IF NOT EXISTS api;
SET search_path TO api;

CREATE TABLE IF NOT EXISTS plans (
    id                 SERIAL PRIMARY KEY,
    code               TEXT UNIQUE NOT NULL,          -- 'pilot','free','pro'
    name               TEXT NOT NULL,
    rate_limit_per_min INT,                           -- NULL = unlimited (Phase 2)
    monthly_quota      INT,                           -- NULL = unlimited (Phase 2)
    max_rows           INT NOT NULL DEFAULT 100000,   -- page ceiling
    scopes             TEXT[] NOT NULL DEFAULT '{trade:read}',
    active             BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO plans(code, name, max_rows, scopes)
VALUES ('pilot', 'Pilot (generous)', 1000000, '{trade:read,fizob:read}')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS users (
    id               SERIAL PRIMARY KEY,
    superset_user_id INT,                             -- ab_user.id (FAB), optional
    email            TEXT UNIQUE NOT NULL,
    org              TEXT,
    plan_id          INT NOT NULL REFERENCES plans(id),
    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tokens (
    id           SERIAL PRIMARY KEY,
    user_id      INT NOT NULL REFERENCES users(id),
    token_hash   TEXT NOT NULL,                       -- sha256(raw token)
    prefix       TEXT NOT NULL,                       -- e.g. 'mgt_ab12cd' for display
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_tokens_hash ON tokens(token_hash);

CREATE TABLE IF NOT EXISTS audit_log (
    id            BIGSERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_id       INT,
    token_id      INT,
    endpoint      TEXT NOT NULL,
    method        TEXT NOT NULL,
    params        JSONB,
    status        INT NOT NULL,
    rows_returned INT,
    bytes         INT,
    latency_ms    INT,
    cost_units    INT NOT NULL DEFAULT 1,             -- billing dimension
    ip            INET
);
CREATE INDEX IF NOT EXISTS ix_audit_user_ts ON audit_log(user_id, ts);
