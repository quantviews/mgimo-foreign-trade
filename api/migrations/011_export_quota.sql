BEGIN;
CREATE TABLE IF NOT EXISTS api.export_delegates (
 token_id INTEGER PRIMARY KEY REFERENCES api.tokens(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS api.export_usage (
 export_id UUID PRIMARY KEY,
 user_id INTEGER NOT NULL REFERENCES api.users(id),
 row_count INTEGER NOT NULL CHECK (row_count > 0),
 byte_count INTEGER NOT NULL CHECK (byte_count > 0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_export_usage_user_time ON api.export_usage(user_id, created_at);
-- Shared by API, MCP and the trusted demo service. Must run in the same
-- transaction as saving the export. Lock held until commit/rollback.
CREATE OR REPLACE FUNCTION api.reserve_export(p_id UUID, p_user INTEGER, p_rows INTEGER, p_bytes INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
 t TIMESTAMPTZ;
 used_rows BIGINT;
 used_bytes BIGINT;
 used_files BIGINT;
 event RECORD;
BEGIN
 IF p_rows <= 0 OR p_rows > 1000 OR p_bytes <= 0 OR p_bytes > 5000000 THEN
   RAISE EXCEPTION 'Invalid export size';
 END IF;
 PERFORM pg_advisory_xact_lock(172901, p_user);
 t := clock_timestamp();
 DELETE FROM api.export_usage WHERE user_id=p_user AND created_at <= t - interval '24 hours';
 SELECT coalesce(sum(row_count),0), coalesce(sum(byte_count),0), count(*)
 INTO used_rows, used_bytes, used_files FROM api.export_usage WHERE user_id=p_user;
 IF used_rows+p_rows>5000 OR used_bytes+p_bytes>20000000 OR used_files+1>10 THEN
   -- Find when enough earlier exports have aged out for this requested slice.
   FOR event IN SELECT * FROM api.export_usage WHERE user_id=p_user ORDER BY created_at LOOP
     used_rows := used_rows-event.row_count;
     used_bytes := used_bytes-event.byte_count;
     used_files := used_files-1;
     IF used_rows+p_rows<=5000 AND used_bytes+p_bytes<=20000000 AND used_files+1<=10 THEN
       RETURN greatest(1,ceil(extract(epoch FROM event.created_at+interval '24 hours'-t))::integer);
     END IF;
   END LOOP;
 END IF;
 INSERT INTO api.export_usage(export_id,user_id,row_count,byte_count,created_at)
 VALUES(p_id,p_user,p_rows,p_bytes,t);
 RETURN 0;
END $$;
COMMIT;
