CREATE TABLE IF NOT EXISTS api.trade_exports (
    id UUID PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES api.users(id),
    csv_content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '7 days'
);
CREATE INDEX IF NOT EXISTS ix_trade_exports_expiry ON api.trade_exports(expires_at);
