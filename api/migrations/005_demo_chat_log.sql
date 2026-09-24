-- Log of demo chat questions (and answers) for later analysis.
-- Same schema `api`. Idempotent: safe to re-run.

SET search_path TO api;

CREATE TABLE IF NOT EXISTS demo_chat_log (
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_id    INT REFERENCES users(id),
    question   TEXT NOT NULL,
    answer     TEXT,
    tool_calls INT,
    status     TEXT
);
CREATE INDEX IF NOT EXISTS ix_demo_chat_ts ON demo_chat_log(ts);
