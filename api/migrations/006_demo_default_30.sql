-- Apply to new rows only; preserve existing personal limits and usage.
ALTER TABLE api.demo_access ALTER COLUMN turn_limit SET DEFAULT 30;
