-- Views for the "API usage" Superset dashboard (admin-only).
-- Read-friendly denormalization of api.audit_log + users + plans.
-- Idempotent: safe to re-run.

SET search_path TO api;

-- Row-level log with email/plan and derived day/month/error columns.
CREATE OR REPLACE VIEW usage_log AS
SELECT
    a.id,
    a.ts,
    a.ts::date                          AS day,
    date_trunc('month', a.ts)::date     AS month,
    u.email,
    p.code                              AS plan,
    a.endpoint,
    a.method,
    a.status,
    (a.status >= 400)                   AS is_error,
    a.rows_returned,
    a.latency_ms,
    a.cost_units,
    a.ip::text                          AS ip,
    a.params
FROM audit_log a
LEFT JOIN users u ON u.id = a.user_id
LEFT JOIN plans p ON p.id = u.plan_id;

-- Pre-aggregated: per user per month (for quota/overview charts).
CREATE OR REPLACE VIEW usage_by_user_month AS
SELECT
    date_trunc('month', a.ts)::date                 AS month,
    u.email,
    p.code                                          AS plan,
    count(*)                                        AS requests,
    count(*) FILTER (WHERE a.status >= 400)         AS errors,
    coalesce(sum(a.rows_returned), 0)               AS rows_returned,
    coalesce(sum(a.cost_units), 0)                  AS cost_units,
    round(avg(a.latency_ms))                        AS avg_latency_ms,
    max(a.ts)                                        AS last_request
FROM audit_log a
LEFT JOIN users u ON u.id = a.user_id
LEFT JOIN plans p ON p.id = u.plan_id
GROUP BY 1, 2, 3;
