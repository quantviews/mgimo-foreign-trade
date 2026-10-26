#!/usr/bin/env bash
# Publish locally computed artifacts only. No calculations on the VPS.
set -euo pipefail
db_new=/srv/duckdb/unified_trade_data.20261024.new
view_new=/srv/parquet/fizob_bulletin.20261024.new
backup=/srv/duckdb/backups/release-20261024-1800
test -s "$backup/unified_trade_data.duckdb"
test -s "$backup/fizob_bulletin.parquet"
printf '%s  %s\n' "$1" "$db_new" "$2" "$view_new" | sha256sum -c -
rollback() {
  cp "$backup/unified_trade_data.duckdb" /srv/duckdb/unified_trade_data.rollback
  cp "$backup/fizob_bulletin.parquet" /srv/parquet/fizob_bulletin.rollback
  mv /srv/duckdb/unified_trade_data.rollback /srv/duckdb/unified_trade_data.duckdb
  mv /srv/parquet/fizob_bulletin.rollback /srv/parquet/fizob_bulletin.parquet
  for service in api demo mcp; do
    docker tag "mgimo-trade-$service:pre-release-20261024" "mgimo-trade-$service"
  done
  docker compose -f /home/marcel/mgimo-foreign-trade/api/docker-compose.yml up -d --no-build --force-recreate trade-api
  docker compose -f /home/marcel/mgimo-foreign-trade/demo/docker-compose.yml up -d --no-build --force-recreate trade-demo
  docker compose -f /home/marcel/mgimo-foreign-trade/mcp-server/docker-compose.yml up -d --no-build --force-recreate trade-mcp
  docker restart superset-superset-1 superset-worker-1 >/dev/null
  echo 'Rolled back to saved release' >&2
}
trap rollback ERR
mv "$db_new" /srv/duckdb/unified_trade_data.duckdb
mv "$view_new" /srv/parquet/fizob_bulletin.parquet
chmod 664 /srv/duckdb/unified_trade_data.duckdb /srv/parquet/fizob_bulletin.parquet
docker compose -f /home/marcel/mgimo-foreign-trade/api/docker-compose.yml up -d --no-build --force-recreate trade-api
docker compose -f /home/marcel/mgimo-foreign-trade/demo/docker-compose.yml up -d --no-build --force-recreate trade-demo
docker compose -f /home/marcel/mgimo-foreign-trade/mcp-server/docker-compose.yml up -d --no-build --force-recreate trade-mcp
docker restart superset-superset-1 superset-worker-1 >/dev/null
docker exec -i trade-api python - <<'PY'
import duckdb
c=duckdb.connect('/data/unified_trade_data.duckdb',read_only=True)
assert str(c.execute("SELECT max(PERIOD) FROM unified_trade_data WHERE TYPE='fact'").fetchone()[0])[:10]=='2026-09-01'
assert c.execute("SELECT count(*) FROM fizob_index WHERE methodology_version!='fisher_annual_v2' OR (coverage<1 AND fizob IS NOT NULL)").fetchone()[0]==0
assert c.execute("SELECT count(*) FROM unified_trade_data WHERE STRANA='CN' AND PERIOD BETWEEN DATE '2026-07-01' AND DATE '2026-09-01' AND TYPE='pred'").fetchone()[0]==0
print('Published DB checks passed')
PY

docker exec -i superset-superset-1 python - <<'PY'
import redis
r=redis.Redis(host='redis',port=6379,db=1)
n=0
for key in r.scan_iter(match='superset_data_*',count=200):
    n+=r.delete(key)
print('Cleared chart result cache:',n)
PY
for attempt in $(seq 1 30); do
  if curl -fsS https://nts.mgimo.ru/api/health > /tmp/release-20261024-health.json; then
    if python3 -c "import json; x=json.load(open('/tmp/release-20261024-health.json')); assert x.get('status')=='ok'; assert '2026-09' in str(x)"; then
      trap - ERR
      cat /tmp/release-20261024-health.json
      exit 0
    fi
  fi
  sleep 2
done
false
