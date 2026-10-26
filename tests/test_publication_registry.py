import hashlib
import json

import pandas as pd
import pytest

from core.publication_registry import confirmed_absence_rows, load_publication_registry


def fixture(tmp_path, **updates):
    raw = tmp_path / 'raw.csv'
    raw.write_text('checked publication', encoding='utf-8')
    release = dict(id='release', country='TR', direction='ЭК', month='2026-08',
        source='national', publication_status='confirmed_complete', download_status='complete',
        absence_semantics='zero_for_applicable_codes', evidence='source contract',
        applicability_evidence='active nomenclature', applicable_codes=['2709000000','2710000000'],
        excluded_prefixes=['2709'], raw_file='raw.csv', raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest())
    release.update(updates)
    path = tmp_path / 'registry.json'
    path.write_text(json.dumps(dict(schema_version=1, releases=[release])),encoding='utf-8')
    return path


def empty():
    return pd.DataFrame(columns=['STRANA','NAPR','TNVED','PERIOD'])


def test_only_applicable_disclosed_codes_become_zero(tmp_path):
    result = confirmed_absence_rows(empty(), fixture(tmp_path), tmp_path)
    assert result.TNVED.tolist() == ['2710000000']
    assert result.STOIM.tolist() == [0]
    assert result.KOL.isna().all() and result.NETTO.isna().all()


@pytest.mark.parametrize('changes', [dict(download_status='truncated'),dict(download_status='failed'),
    dict(publication_status='unknown'),dict(absence_semantics='unknown')])
def test_incomplete_publications_never_create_zeros(tmp_path, changes):
    assert confirmed_absence_rows(empty(), fixture(tmp_path, **changes), tmp_path).empty


def test_existing_observation_is_never_overwritten(tmp_path):
    obs = pd.DataFrame([dict(STRANA='TR',NAPR='ЭК',TNVED='2710000000',PERIOD='2026-08-01')])
    assert confirmed_absence_rows(obs, fixture(tmp_path), tmp_path).empty


def test_published_value_with_withheld_quantity_is_not_absent_trade(tmp_path):
    path = fixture(tmp_path, quantity_disclosure=[dict(code='2710000000',
        status='not_disclosed', fields=['NETTO','KOL'], value_status='published')])
    assert confirmed_absence_rows(empty(), path, tmp_path).empty


@pytest.mark.parametrize('changes', [dict(raw_sha256='bad'),dict(applicability_evidence=None)])
def test_changed_evidence_fails_closed(tmp_path, changes):
    with pytest.raises(ValueError):
        confirmed_absence_rows(empty(), fixture(tmp_path, **changes), tmp_path)


def test_legacy_comtrade_manifest_does_not_authorize_zeros(tmp_path):
    path = fixture(tmp_path, absence_semantics='unknown')
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'2026-08': {'reporters': {
        '792': {'checksum':'abc', 'present':False},
        '156': {'checksum':'def', 'download_status':'truncated'}}}}))
    registry = json.loads(path.read_text())
    registry['comtrade_manifests'] = ['manifest.json']
    path.write_text(json.dumps(registry))
    releases = load_publication_registry(path, tmp_path)
    assert releases[1]['download_status'] == 'unknown'
    assert releases[2]['download_status'] == 'truncated'
    assert confirmed_absence_rows(empty(), path, tmp_path).empty


def test_source_rule_requires_specific_absence_evidence(tmp_path):
    path = fixture(tmp_path)
    registry = json.loads(path.read_text())
    registry['source_rules'] = [dict(id='national_rule',source='national',country='TR',
        requires_release_specific_absence_evidence=True)]
    path.write_text(json.dumps(registry))
    with pytest.raises(ValueError,match='release-specific'):
        confirmed_absence_rows(empty(),path,tmp_path)
    registry['releases'][0]['absence_evidence'] = 'Synthetic explicit confirmation for these codes'
    path.write_text(json.dumps(registry))
    assert len(confirmed_absence_rows(empty(),path,tmp_path))==1


@pytest.mark.parametrize('source_flow, expected', [('M',0),('X',1)])
def test_known_detail_gap_blocks_only_matching_direction(tmp_path,source_flow,expected):
    path = fixture(tmp_path,source='comtrade',reporter_code='792')
    registry = json.loads(path.read_text())
    registry['detail_coverage_findings'] = [dict(source='comtrade',reporter_code='792',
        month='2026-08',flow=source_flow,commodity_scope='27')]
    path.write_text(json.dumps(registry))
    assert len(confirmed_absence_rows(empty(),path,tmp_path))==expected
