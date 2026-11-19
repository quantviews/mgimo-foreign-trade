"""Guard the shared dictionary used by database builds and bulletins."""
import json
from pathlib import Path

from core.reference_tables import load_hs4_labels

ROOT = Path(__file__).resolve().parents[1]


def test_site_fallback_matches_canonical_dictionary():
    canonical = json.loads((ROOT / 'metadata/hs4_labels.json').read_text(encoding='utf-8'))
    fallback = json.loads((ROOT / 'site/data/hs4_labels.json').read_text(encoding='utf-8'))
    assert fallback == canonical
    codes = [r['hs4'] for r in canonical]
    assert len(codes) == len(set(codes))
    assert all(len(c) == 4 and c.isdigit() for c in codes)


def test_database_loader_preserves_reviewed_group_scope():
    labels = load_hs4_labels(ROOT).set_index('TNVED4')
    blood = labels.loc['3002', 'TNVED4_NAME_SHORT'].lower()
    assert all(word in blood for word in ('вакцины', 'иммуно', 'крови'))
    assert labels.loc['2709', 'TNVED4_NAME_SHORT'] == 'Сырая нефть'
    assert 'чугун' in labels.loc['7201', 'TNVED4_NAME_SHORT'].lower()
    assert 'индей' not in labels.loc['2799', 'TNVED4_NAME_FULL'].lower()
    assert 'нефть сырая' in labels.loc['2709', 'TNVED4_NAME_FULL'].lower()
