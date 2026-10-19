CREATE TABLE IF NOT EXISTS api.demo_exports (
    id UUID PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES api.users(id),
    csv_content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '7 days'
);
CREATE INDEX IF NOT EXISTS ix_demo_exports_expiry ON api.demo_exports(expires_at);
