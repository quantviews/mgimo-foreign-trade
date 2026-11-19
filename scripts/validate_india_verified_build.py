"""Compare local release facts with the rollback snapshot and reviewed registry."""
import argparse
import json
from pathlib import Path

import duckdb


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--baseline', required=True)
    p.add_argument('--output', required=True)
    args = p.parse_args()
    c = duckdb.connect(args.db, read_only=True)
    baseline = args.baseline.replace("'", "''")
    c.execute(f"ATTACH '{baseline}' AS before (READ_ONLY)")
    columns = [x[0] for x in c.execute('DESCRIBE before.unified_trade_data').fetchall()
               if x[0] not in ('STOIM', 'NETTO', 'NETTO_ESTIMATED', 'PHYSICAL_QUALITY', 'NETTO_SOURCE', 'VERIFIED_ADJUSTMENT')]
    names = ','.join('"'+x+'"' for x in columns)
    diff = c.execute(f"""SELECT count(*) FROM (
        (SELECT {names} FROM unified_trade_data WHERE TYPE='fact'
         EXCEPT ALL SELECT {names} FROM before.unified_trade_data WHERE TYPE='fact')
        UNION ALL
        (SELECT {names} FROM before.unified_trade_data WHERE TYPE='fact'
         EXCEPT ALL SELECT {names} FROM unified_trade_data WHERE TYPE='fact'))""").fetchone()[0]
    assert diff == 0, ('unexpected factual rows/fields', diff)
    changes = c.execute("""SELECT a.STRANA,a.NAPR,a.TNVED,a.PERIOD,a.STOIM,b.STOIM,a.NETTO,b.NETTO
      FROM unified_trade_data a JOIN before.unified_trade_data b
        USING(STRANA,NAPR,TNVED,PERIOD,TYPE,SOURCE)
      WHERE a.TYPE='fact' AND a.EDIZM IS NOT DISTINCT FROM b.EDIZM
        AND (a.STOIM IS DISTINCT FROM b.STOIM OR a.NETTO IS DISTINCT FROM b.NETTO)""").fetchall()
    actual = {}
    for country, direction, code, period, cost, oldcost, mass, oldmass in changes:
        assert country == 'IN'
        if cost != oldcost:
            actual[(direction,code,str(period),'STOIM')] = (oldcost,cost)
        if mass != oldmass:
            actual[(direction,code,str(period),'NETTO')] = (oldmass,mass)
    registry = json.loads((Path(__file__).resolve().parents[1] / 'metadata/india_verified_adjustments.json').read_text(encoding='utf-8'))
    expected = {(r['napr'],r['tnved'],r['period'],r['field']): (r['old'],r['value']) for r in registry['adjustments']}
    assert actual == expected, ('unreviewed/missing changes', actual.keys() ^ expected.keys())
    supplements = c.execute("""SELECT count(*),sum(NETTO),count(NETTO_ESTIMATED)
      FROM unified_trade_data WHERE NETTO_SOURCE='comtrade_tariffline_alt_qty_kg'""").fetchone()
    assert supplements == (25,3792337.0,0), supplements
    oil = c.execute("""SELECT sum(STOIM),sum(NETTO) FROM unified_trade_data
      WHERE STRANA='IN' AND NAPR='ЭК' AND TNVED4='2709' AND TYPE='fact'
      AND PERIOD BETWEEN DATE '2025-10-01' AND DATE '2026-09-01'""").fetchone()
    assert oil == (56243360000.0,86814535668.0), oil
    result = dict(verified_field_changes=len(actual),mass_supplements=supplements,
                  oil_current_12_months_usd_kg=oil,other_factual_fields_and_rows_unchanged=True,
                  zero_substitution=False,production_changed=False)
    Path(args.output).write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
