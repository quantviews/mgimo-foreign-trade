"""Reconcile audited July/August originals into an isolated Fisher input database."""
import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from core.china_units import normalize_quantities
from core.edizm import load_common_edizm_mapping


def main():
    out = ROOT / 'tmp/china-revision-build'
    out.mkdir(exist_ok=False)
    audit = json.loads((ROOT / 'tmp/china-jul-aug-original-review/report.json').read_text())
    registry_path = ROOT / 'metadata/publication_registry.json'
    registry = json.loads(registry_path.read_text(encoding='utf-8'))
    (out / 'registry-before.json').write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding='utf-8')
    frames = []
    for item in audit:
        raw = ROOT / item['file']
        assert hashlib.sha256(raw.read_bytes()).hexdigest() == item['sha256']
        staged = ROOT / 'tmp/china-jul-aug-original-review/staged' / raw.parent.name / raw.name
        df = pd.read_csv(staged, dtype={'TNVED': str})
        assert len(df) == item['rows'] and abs(df.STOIM.sum() - item['value_usd']) < .01
        df = normalize_quantities(df, {}, load_common_edizm_mapping(ROOT))
        df['PERIOD'] = pd.to_datetime(df.PERIOD)
        df['TNVED'] = df.TNVED.str.ljust(10, '0')
        df['TYPE'], df['SOURCE'] = 'fact', 'national'
        df['KOL_ESTIMATED'], df['NETTO_ESTIMATED'] = False, False
        release = next(r for r in registry['releases'] if r['id'] == item['file'])
        release.setdefault('revision_history', []).append({k: release.get(k) for k in
            ('raw_sha256', 'expected_rows', 'expected_value_usd', 'evidence')})
        release.update(raw_sha256=item['sha256'], expected_rows=item['rows'],
            expected_value_usd=item['value_usd'], original_archive=item['archive'],
            evidence='User-supplied revised all-commodity Russia release; period, partner, rows and USD checked against archived original. Not yet published to serving database.',
            absence_review_reason='Original remarks retained. Absent commodity rows remain unknown; no explicit zero-trade evidence.',
            quantity_disclosure=[])
        for comment in item['comments']:
            code = comment['TNVED'].ljust(10, '0')
            rows = df[df.TNVED == code]
            assert len(rows) == 1 and rows.NETTO.isna().all() and rows.KOL.isna().all()
            release['quantity_disclosure'].append(dict(code=code, status='not_disclosed',
                fields=['NETTO', 'KOL'], value_status='published', evidence=comment['remarks']))
        for candidate in release.get('candidate_code_review', []):
            assert candidate['canonical_code'] not in set(df.TNVED)
            candidate.update(row_status='absent', absence_status='unknown_not_zero')
        frames.append(df)
    fresh = pd.concat(frames, ignore_index=True)
    assert not fresh.duplicated(['STRANA','NAPR','TNVED','PERIOD']).any()
    columns = ['STRANA','NAPR','TNVED','PERIOD','EDIZM','STOIM','NETTO','KOL','TYPE','SOURCE','KOL_ESTIMATED','NETTO_ESTIMATED']
    fresh = fresh[columns]
    fresh.to_parquet(out / 'china-revised.parquet', index=False)
    with duckdb.connect(str(out / 'input.duckdb')) as con:
        con.execute("ATTACH 'db/unified_trade_data.duckdb' AS previous (READ_ONLY)")
        con.register('fresh', fresh)
        names = ','.join(columns)
        con.execute(f'''CREATE TABLE unified_trade_data AS SELECT {names} FROM previous.unified_trade_data
            WHERE NOT (STRANA='CN' AND PERIOD IN (DATE '2026-07-01',DATE '2026-08-01')
            AND (SOURCE='national' OR TYPE='pred')) UNION ALL SELECT * FROM fresh''')
        assert con.execute("SELECT count(*) FROM unified_trade_data WHERE STRANA='CN' AND TNVED='2711210000' AND PERIOD>=DATE '2026-07-01' AND TYPE='fact' AND (NETTO IS NOT NULL OR KOL IS NOT NULL)").fetchone()[0] == 0
    registry_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('Revised rows:', len(fresh), '; isolated database:', out / 'input.duckdb')


if __name__ == '__main__':
    main()
