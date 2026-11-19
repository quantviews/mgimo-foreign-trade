import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'deploy/mgimo146'))
from prepare_package import prepare
from publish_remote import sha


def inputs(root):
    validation = root / 'checks'
    validation.mkdir()
    installed = []
    for name in ('db/unified_trade_data.duckdb', 'site/data/fizob_bulletin.parquet'):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'validated data')
        installed.append(dict(path=name, sha256=sha(path)))
    render = {}
    site = root / 'site/_site'
    site.mkdir(parents=True)
    (site / 'bulletin_comparison.html').write_text('summary')
    for country in ('china', 'india', 'turkey'):
        stem = site / 'country_bulletins' / country / f'bulletin_{country}_pdf'
        stem.parent.mkdir(parents=True)
        stem.with_suffix('.html').write_text('bulletin')
        stem.with_suffix('.pdf').write_bytes(b'reviewed PDF')
        render[country] = dict(sha256=sha(stem.with_suffix('.pdf')))
    for name, data in {
        'release-quality.json': dict(factual_rows=10, nowcast_rows=2, latest_facts=[['national', '2026-09-01', 10]]),
        'final-adjustments-check.json': dict(other_factual_fields_and_rows_unchanged=True, zero_substitution=False),
        'bulletin-render-check.json': render,
        'local-installed.json': installed,
    }.items():
        (validation / name).write_text(json.dumps(data))
    return validation


def test_package_roundtrip_and_no_secrets(tmp_path):
    validation = inputs(tmp_path)
    (tmp_path / '.env').write_text('not for release')
    out = tmp_path / 'package'
    prepare(tmp_path, out, validation, 'test-release')
    manifest = json.loads((out / 'manifest.json').read_text())
    assert manifest['rows'] == 12 and manifest['data_version'] == '2026-09'
    assert len(manifest['files']) == 9
    assert not any('.env' in x['path'] for x in manifest['files'])
    assert json.loads((out / 'package-check.json').read_text())['local_unpack_verified']


def test_changed_data_and_secret_assets_rejected(tmp_path):
    validation = inputs(tmp_path)
    db = tmp_path / 'db/unified_trade_data.duckdb'
    original = db.read_bytes()
    db.write_bytes(b'changed')
    with pytest.raises(ValueError, match='data changed'):
        prepare(tmp_path, tmp_path / 'bad', validation, 'test')
    db.write_bytes(original)
    libs = tmp_path / 'site/_site/site_libs'
    libs.mkdir()
    (libs / 'private.key').write_text('not for release')
    with pytest.raises(ValueError, match='Unexpected web asset'):
        prepare(tmp_path, tmp_path / 'bad', validation, 'test')
    assert not (tmp_path / 'bad').exists()
