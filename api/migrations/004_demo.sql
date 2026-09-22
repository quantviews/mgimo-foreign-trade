-- Demo AI-chat access: a restricted plan for tool calls, a per-user turn counter,
-- email verification tokens and a global daily backstop.
-- Same schema `api`, same Postgres. Idempotent: safe to re-run.

SET search_path TO api;

-- The demo chat calls the data API with ONE service key on this plan. A low
-- max_rows keeps the free chat from becoming a bulk-export channel.
INSERT INTO plans(code, name, max_rows, scopes)
VALUES ('demo', 'Demo (chat, limited)', 5000, '{trade:read,fizob:read}')
ON CONFLICT (code) DO NOTHING;

-- One row per verified demo user; "10 free turns" is enforced here, not on the
-- API key (one chat turn makes several API calls under the shared service key).
CREATE TABLE IF NOT EXISTS demo_access (
    user_id    INT PRIMARY KEY REFERENCES users(id),
    turns_used INT NOT NULL DEFAULT 0,
    turn_limit INT NOT NULL DEFAULT 10,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Magic-link tokens. Access is auto-granted by corporate domain, so we must
-- verify the person actually controls the address before provisioning.
CREATE TABLE IF NOT EXISTS email_verifications (
    token       TEXT PRIMARY KEY,
    email       TEXT NOT NULL,
    org         TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_email_verif_email ON email_verifications(email);

-- Global cost backstop: total demo turns per day, capped in the service.
CREATE TABLE IF NOT EXISTS demo_usage_daily (
    day   DATE PRIMARY KEY,
    turns INT NOT NULL DEFAULT 0
);
