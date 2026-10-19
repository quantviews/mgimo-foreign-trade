BEGIN;
ALTER TABLE api.trade_exports ADD COLUMN IF NOT EXISTS xlsx_content BYTEA;
ALTER TABLE api.trade_exports ALTER COLUMN csv_content DROP NOT NULL;
ALTER TABLE api.demo_exports ADD COLUMN IF NOT EXISTS xlsx_content BYTEA;
ALTER TABLE api.demo_exports ALTER COLUMN csv_content DROP NOT NULL;
COMMIT;
