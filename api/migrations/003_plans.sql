-- Fixed tier limits (2026-08-28). Enforcement reads these from api.plans;
-- NULL means unlimited. Idempotent — safe to re-run; edit values and re-apply anytime.
--
--   plan  | rate/min | quota/month | max_rows  | notes
--   pilot |   120    |   NULL (∞)  | 1000000   | internal pilot: abuse guard, no quota
--   free  |    30    |    5000     |   50000   | future free tier
--   pro   |   120    |   100000    | 1000000   | paid

SET search_path TO api;

UPDATE plans
SET rate_limit_per_min = 120, monthly_quota = NULL, max_rows = 1000000
WHERE code = 'pilot';

INSERT INTO plans(code, name, max_rows, monthly_quota, rate_limit_per_min, scopes)
VALUES
    ('free', 'Free', 50000,   5000,  30,  '{trade:read}'),
    ('pro',  'Pro',  1000000, 100000, 120, '{trade:read,fizob:read}')
ON CONFLICT (code) DO UPDATE SET
    name               = EXCLUDED.name,
    max_rows           = EXCLUDED.max_rows,
    monthly_quota      = EXCLUDED.monthly_quota,
    rate_limit_per_min = EXCLUDED.rate_limit_per_min,
    scopes             = EXCLUDED.scopes;
