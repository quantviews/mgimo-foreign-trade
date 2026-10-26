"""Read-only publication gate for the isolated September release."""
import json
import sys
from pathlib import Path
import duckdb

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from orchestration.checks import run_sql_quality_checks
out=ROOT/'tmp/release-20261024'
db=out/'unified_trade_data.duckdb'
report=run_sql_quality_checks(db,require_fizob=True)
with duckdb.connect(str(db),read_only=True) as c:
    assert c.execute("SELECT count(*) FROM read_parquet(?)",[str(out/'fisher/confirmed_absence.parquet')]).fetchone()[0]==0
    assert c.execute("SELECT max(PERIOD) FROM unified_trade_data WHERE TYPE='fact'").fetchone()[0].isoformat()[:10]=='2026-09-01'
    manifest=json.loads((ROOT/'metadata/national_release_coverage.json').read_text(encoding='utf-8'))
    for r in manifest['releases']:
        n,v=c.execute("SELECT count(*),sum(STOIM) FROM unified_trade_data WHERE STRANA=? AND NAPR=? AND PERIOD=CAST(? AS DATE) AND SOURCE='national' AND TYPE='fact'",[r['country'],r['direction'],r['month']+'-01']).fetchone()
        assert n==r['rows'] and abs(v-r['stoim_usd'])<.01
        assert c.execute("SELECT count(*) FROM unified_trade_data WHERE STRANA=? AND NAPR=? AND PERIOD=CAST(? AS DATE) AND TYPE='pred'",[r['country'],r['direction'],r['month']+'-01']).fetchone()[0]==0
    assert c.execute("SELECT count(*) FROM fizob_index WHERE methodology_version!='fisher_annual_v2' OR TYPE!='fact'").fetchone()[0]==0
    assert c.execute("SELECT count(*) FROM fizob_index WHERE fizob IS NOT NULL AND (coverage<1 OR status NOT IN ('base','complete') OR base_period IS NULL OR NOT isfinite(fizob))").fetchone()[0]==0
    assert c.execute("SELECT count(*) FROM (SELECT STRANA,NAPR,tn_level,tn_code,PERIOD FROM fizob_index GROUP BY ALL HAVING count(*)>1)").fetchone()[0]==0
    focus=c.execute("SELECT STRANA,NAPR,tn_code,available_stoim_12 FROM fizob_index WHERE PERIOD=DATE '2026-09-01' AND STRANA IN ('CN','ALL') AND tn_level IN (0,2) AND tn_code IN ('0','27')").fetchall()
    assert len(focus)==8
    for country,direction,code,value in focus:
        raw=c.execute("SELECT sum(STOIM) FROM unified_trade_data WHERE TYPE='fact' AND PERIOD BETWEEN DATE '2025-10-01' AND DATE '2026-09-01' AND NAPR=? AND (?='ALL' OR STRANA=?) AND (?='0' OR TNVED2=?)",[direction,country,country,code,code]).fetchone()[0]
        assert abs(raw-value)<.01,(country,direction,code,raw,value)
    report['focus_cost_reconciled']=len(focus)
    assert c.execute("SELECT count(*) FROM unified_trade_data WHERE STRANA='CN' AND NAPR='ЭК' AND TNVED='2711210000' AND PERIOD BETWEEN DATE '2026-07-01' AND DATE '2026-09-01' AND TYPE='fact' AND (NETTO IS NOT NULL OR KOL IS NOT NULL)").fetchone()[0]==0
    for code in ['6107','8703']:
        assert c.execute("SELECT count(*) FROM unified_trade_data WHERE TNVED4=? AND TYPE='fact'",[code]).fetchone()[0]>0
    report['latest_facts']=c.execute("SELECT SOURCE,max(PERIOD),count(*) FROM unified_trade_data WHERE TYPE='fact' GROUP BY SOURCE").fetchall()
    report['latest_fizob']=c.execute("SELECT STRANA,NAPR,tn_code,idx,status,coverage FROM fizob_index_v WHERE PERIOD=DATE '2026-09-01' AND STRANA IN ('CN','ALL') AND tn_level IN (0,2) AND tn_code IN ('0','27') ORDER BY STRANA,NAPR,tn_code").fetchall()
    report['index_counts']=c.execute("SELECT count(*),count(idx) FROM fizob_index_v").fetchone()
    report['nowcast_rows']=c.execute("SELECT count(*) FROM unified_trade_data WHERE TYPE='pred'").fetchone()[0]
report['production_changed']=False
(out/'release-quality.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
print(json.dumps({k:report[k] for k in ['latest_facts','index_counts','nowcast_rows','latest_fizob']},ensure_ascii=False,default=str))
