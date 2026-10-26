"""Independent SQL checks of the local matched-basket experiment."""
import json
from pathlib import Path
import duckdb

out = Path('tmp/fisher-matched-basket-v2')
c = duckdb.connect()
c.read_parquet(str(out/'series.parquet')).create_view('links')
c.read_parquet(str(out/'summary.parquet')).create_view('summary')
assert c.execute('SELECT count(*) FROM links WHERE included AND (observed!=12 OR observed0!=12 OR source_count!=1 OR r<=0 OR NOT isfinite(r))').fetchone()[0] == 0
assert c.execute('SELECT count(*) FROM links WHERE included AND (v IS NULL OR v0 IS NULL)').fetchone()[0] == 0
assert c.execute('SELECT count(*) FROM summary').fetchone()[0] == 8
assert set(c.execute('SELECT DISTINCT NAPR FROM summary').fetchnumpy()['NAPR']) == {'ИМ','ЭК'}
assert c.execute('SELECT count(*) FROM summary WHERE series=0 OR q_link IS NULL OR p_link IS NULL OR NOT isfinite(q_link)').fetchone()[0] == 0
for row in c.execute('SELECT STRANA,NAPR,TNVED_G,A,B,C,D,q_link,p_link,imp FROM summary').fetchall():
    country,direction,code,*values = row
    a,b,cc,d,q,p,imp = values
    sums = c.execute('''SELECT sum(v0),sum(v),sum(v0*r),sum(v/r),
        sum(CASE WHEN imputed THEN v0+v ELSE 0 END)/sum(v0+v)
        FROM links WHERE included AND NAPR=? AND (?='ALL' OR STRANA=?)
        AND (?='0' OR starts_with(TNVED,?))''',[direction,country,country,code,code]).fetchone()
    assert all(abs(x-y)<max(.001,abs(y)*1e-12) for x,y in zip([a,b,cc,d,imp],sums))
    assert abs(q*p-b/a)<1e-10
    assert abs(q-((d/a)*(b/cc))**.5)<1e-10
    assert 0 <= imp <= 1
assert c.execute('''SELECT count(*) FROM summary WHERE current_coverage NOT BETWEEN 0 AND 1
    OR base_coverage NOT BETWEEN 0 AND 1''').fetchone()[0] == 0
c.execute('COPY summary TO ? (HEADER, DELIMITER \',\')',[str(out/'summary.csv')])
report = {'passed':True,'aggregate_rows':8,'scope':'matched single link, not chained or full-trade index',
          'checks':['complete windows','same source','positive finite prices','independent A/B/C/D and imputation share','Fisher identity','coverage bounds'],
          'production_changed':False}
(out/'verification.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps(report))
