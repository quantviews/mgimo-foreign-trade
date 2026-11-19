import json
import hashlib
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from core.india_verified_adjustments import apply_india_verified_adjustments
from collectors.india_collector import save_india_data


def test_committed_evidence_matches_registry_hashes():
    root = Path(__file__).resolve().parents[1]
    registry = json.loads((root / 'metadata/india_verified_adjustments.json').read_text(encoding='utf-8'))
    for patch in registry['adjustments']:
        evidence = (root / patch['evidence']).resolve()
        assert evidence.is_relative_to(root)
        assert hashlib.sha256(evidence.read_bytes()).hexdigest() == patch['evidence_sha256']


def test_guarded_mass_and_idempotence(tmp_path):
    (tmp_path / 'metadata').mkdir()
    patch = dict(period='2025-05-01', napr='ИМ', tnved='2710195300', field='NETTO',
                 old=None, value=245499, expected_cost=140000, source='comtrade_tariffline_alt_qty_kg')
    (tmp_path / 'metadata/india_verified_adjustments.json').write_text(json.dumps({'adjustments': [patch]}))
    frame = pd.DataFrame([dict(STRANA='IN', SOURCE='national', TYPE='fact', PERIOD=pd.Timestamp('2025-05-01'),
                               NAPR='ИМ', TNVED='2710195300', NETTO=float('nan'), STOIM=140000, KOL=float('nan'))])
    result = apply_india_verified_adjustments(frame.copy(), tmp_path)
    assert result.NETTO.iloc[0] == 245499
    assert result.SOURCE.iloc[0] == 'national' and pd.isna(result.KOL.iloc[0])
    assert pd.isna(result.NETTO_ESTIMATED.iloc[0])
    pd.testing.assert_frame_equal(result, apply_india_verified_adjustments(result.copy(), tmp_path))
    frame.loc[0, 'NETTO'] = 100
    with pytest.raises(ValueError, match='source changed'):
        apply_india_verified_adjustments(frame, tmp_path)
    patch.update(evidence='metadata/evidence.json', evidence_sha256='0' * 64)
    (tmp_path / 'metadata/evidence.json').write_text('{}')
    (tmp_path / 'metadata/india_verified_adjustments.json').write_text(json.dumps({'adjustments': [patch]}))
    with pytest.raises(ValueError, match='evidence hash mismatch'):
        apply_india_verified_adjustments(frame, tmp_path)


def test_zero_direction_preserves_previous_and_valid_individual_zero(tmp_path):
    old = pd.DataFrame([dict(NAPR='ЭК', TNVED='27090010', STOIM=100, KOL=20),
                        dict(NAPR='ИМ', TNVED='27090010', STOIM=5, KOL=1)])
    old.to_csv(tmp_path / 'india_2024_03.csv', index=False)
    incoming = pd.DataFrame([dict(NAPR='ЭК', TNVED='27090010', STOIM=0, KOL=0),
                             dict(NAPR='ИМ', TNVED='27090010', STOIM=6, KOL=2),
                             dict(NAPR='ИМ', TNVED='27090090', STOIM=0, KOL=0)])
    save_india_data(incoming, 2024, 3, tmp_path)
    result = pd.read_csv(tmp_path / 'india_2024_03.csv')
    assert result.loc[result.NAPR.eq('ЭК'), 'STOIM'].tolist() == [100]
    assert result.loc[result.NAPR.eq('ИМ'), 'STOIM'].tolist() == [6, 0]
    assert len(list((tmp_path / 'quarantine').glob('*.csv'))) == 1
    assert len(list((tmp_path / 'revisions').iterdir())) == 1


def test_all_zero_new_month_not_written(tmp_path):
    frame = pd.DataFrame([dict(NAPR='ЭК', TNVED='27090010', STOIM=0, KOL=0)])
    save_india_data(frame, 2024, 3, tmp_path)
    assert not (tmp_path / 'india_2024_03.csv').exists()
