"""Build a separate local audit DB with confirmed mass patches and provenance."""
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def main():
    out=Path('tmp/india-comtrade-20261119')
    target=out/'candidate.duckdb'
    if target.exists():raise RuntimeError('Candidate already exists; never overwrite')
    expected={202403}|{y*100+m for y in (2025,2026) for m in range(1,13) if y==2025 or m<=8}
    for prefix in ['tariff-allcodes','tariff-consignment']:
        found={int(p.stem.split('-')[-1]) for p in out.glob(prefix+'-*.json')}
        assert found==expected, (prefix,expected-found)
    candidates=pd.read_csv(out/'physical-gap-candidates.csv',dtype={'TNVED':str})
    patches=candidates[candidates.passes_overlap_screen].copy()
    patches['PERIOD']=pd.to_datetime(patches.PERIOD)
    assert len(patches)>0 and not patches.duplicated(['NAPR','TNVED','PERIOD']).any()
    assert patches.ct_alt_mass_kg.gt(0).all() and np.isfinite(patches.ct_alt_mass_kg).all()
    original=duckdb.connect('db/unified_trade_data.duckdb',read_only=True)
    original.register('patches',patches)
    count=original.execute("""SELECT count(*) FROM unified_trade_data u JOIN patches p
        USING(NAPR,TNVED,PERIOD) WHERE u.STRANA='IN' AND u.TYPE='fact'
        AND u.SOURCE='national' AND u.NETTO IS NULL AND u.STOIM=p.STOIM""").fetchone()[0]
    assert count==len(patches)
    original.close()
    db=duckdb.connect(str(target))
    db.execute("SET threads=2")
    db.execute("SET memory_limit='3GB'")
    db.execute("ATTACH 'db/unified_trade_data.duckdb' AS original (READ_ONLY)")
    db.execute("CREATE TABLE unified_trade_data AS SELECT *,CASE WHEN NETTO IS NOT NULL THEN SOURCE END AS NETTO_SOURCE FROM original.unified_trade_data")
    db.register('patches',patches)
    db.execute('CREATE TABLE comtrade_mass_patches AS SELECT * FROM patches')
    db.execute("""UPDATE unified_trade_data u SET NETTO=p.ct_alt_mass_kg,
        NETTO_SOURCE='comtrade_tariffline_alt_qty_kg',NETTO_ESTIMATED=NULL
        FROM patches p WHERE u.STRANA='IN' AND u.TYPE='fact' AND u.SOURCE='national'
        AND u.NAPR=p.NAPR AND u.TNVED=p.TNVED AND u.PERIOD=p.PERIOD AND u.NETTO IS NULL""")
    columns=[r[0] for r in db.execute('DESCRIBE original.unified_trade_data').fetchall() if r[0] not in ('NETTO','NETTO_ESTIMATED')]
    names=','.join('"'+x+'"' for x in columns)
    # ALL semantics preserve duplicate counts, proving no changed values/quantities/rows.
    changed=db.execute(f"SELECT count(*) FROM ((SELECT {names} FROM unified_trade_data EXCEPT ALL SELECT {names} FROM original.unified_trade_data) UNION ALL (SELECT {names} FROM original.unified_trade_data EXCEPT ALL SELECT {names} FROM unified_trade_data))").fetchone()[0]
    assert changed==0
    assert db.execute("SELECT count(*) FROM unified_trade_data WHERE NETTO_SOURCE='comtrade_tariffline_alt_qty_kg'").fetchone()[0]==len(patches)
    db.execute('CHECKPOINT');db.close()
    patches.to_csv(out/'approved-mass-patches.csv',index=False)
    decision=dict(status='review_complete',approved_row_count=0,approved_mass_patch_count=len(patches),
        candidate_database=str(target),source_database_unchanged=True,
        rule='Exact national HS8 and geography; value agrees within USD 5000 rounding; at least 3 positive cost/physical overlaps; all cost overlaps within rounding and mass within 0.1%; positive alternate quantity explicitly kg agrees with netWgt; national mass NULL only',
        existing_rows_and_all_other_fields_preserved=True,zero_substitution=False,
        source_estimation_flag='Tariff response supplies no flag: NETTO_ESTIMATED remains unknown, not forced false')
    (out/'decisions.json').write_text(json.dumps(decision,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(decision,ensure_ascii=False))


if __name__=='__main__':main()
