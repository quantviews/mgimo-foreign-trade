import importlib.util
import io
import json
from pathlib import Path
import tarfile

import pytest

spec = importlib.util.spec_from_file_location(
    'publication146', Path(__file__).resolve().parents[1] / 'deploy/mgimo146/publish_remote.py')
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


@pytest.mark.parametrize('name', [
    '../etc/passwd', '/etc/passwd', 'web/site/../../secret',
    'web/site/demo.html', 'web/site/api.html', 'data/keys.env',
    'web/site/site_libs/../api.html', 'web\\site\\foo',
])
def test_rejects_unrelated_paths(name):
    with pytest.raises(ValueError):
        release.destination(name)


def fixture_files(tmp_path):
    stage = tmp_path / 'stage'
    root = tmp_path / 'target'
    names = ['data/unified_trade_data.duckdb', 'data/fizob_bulletin.parquet',
             'web/site/bulletin_comparison.html']
    records = []
    for name in names:
        src = stage / 'payload' / name
        src.parent.mkdir(parents=True, exist_ok=True)
        src.write_bytes(b'new')
        records.append({'path': name, 'sha256': release.sha(src), 'bytes': 3})
        dst = release.destination(name, root)
        if name.startswith('data/'):
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(b'old')
    (stage / 'payload/manifest.json').write_text(json.dumps({'files': records}))
    return stage, root, names


def test_backup_install_rollback_removes_only_new_files(tmp_path):
    stage, root, names = fixture_files(tmp_path)
    unrelated = root / 'web/site/demo.html'
    unrelated.parent.mkdir(parents=True, exist_ok=True)
    unrelated.write_text('keep')
    release.files_action('backup', stage, root)
    release.files_action('install', stage, root)
    assert all(release.destination(n, root).read_bytes() == b'new' for n in names)
    release.files_action('rollback', stage, root)
    assert all(release.destination(n, root).read_bytes() == b'old' for n in names[:2])
    assert not release.destination(names[2], root).exists()
    assert unrelated.read_text() == 'keep'


def test_damaged_backup_aborts_before_any_restore(tmp_path):
    stage, root, names = fixture_files(tmp_path)
    release.files_action('backup', stage, root)
    release.files_action('install', stage, root)
    (stage / 'backup' / names[1]).write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='Backup damaged'):
        release.files_action('rollback', stage, root)
    assert all(release.destination(n, root).read_bytes() == b'new' for n in names)


def test_changed_payload_aborts_before_any_install(tmp_path):
    stage, root, names = fixture_files(tmp_path)
    release.files_action('backup', stage, root)
    (stage / 'payload' / names[1]).write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='Staged file changed'):
        release.files_action('install', stage, root)
    assert all(release.destination(n, root).read_bytes() == b'old' for n in names[:2])


def test_tar_symlink_not_extracted(tmp_path):
    with tarfile.open(tmp_path / 'release.tar.gz', 'w:gz') as tar:
        member = tarfile.TarInfo('web/site/site_libs/link')
        member.type = tarfile.SYMTYPE
        member.linkname = '/etc'
        tar.addfile(member)
    with pytest.raises(ValueError, match='Unsupported'):
        release.unpack(tmp_path)


def test_package_without_country_html_rejected(tmp_path):
    stage, root, names = fixture_files(tmp_path)
    archive_stage = tmp_path / 'incoming'
    archive_stage.mkdir()
    with tarfile.open(archive_stage / 'release.tar.gz', 'w:gz') as tar:
        for name in names + ['manifest.json']:
            tar.add(stage / 'payload' / name, arcname=name)
    with pytest.raises(ValueError, match='Country bulletin HTML missing'):
        release.unpack(archive_stage)


def test_partial_install_can_be_rolled_back(tmp_path, monkeypatch):
    stage, root, names = fixture_files(tmp_path)
    release.files_action('backup', stage, root)
    original = release.atomic_copy
    calls = []
    def failing_copy(src, dst):
        calls.append(dst)
        if len(calls) == 2:
            raise OSError('simulated write failure')
        return original(src, dst)
    monkeypatch.setattr(release, 'atomic_copy', failing_copy)
    with pytest.raises(OSError):
        release.files_action('install', stage, root)
    monkeypatch.setattr(release, 'atomic_copy', original)
    release.files_action('rollback', stage, root)
    assert all(release.destination(n, root).read_bytes() == b'old' for n in names[:2])


@pytest.mark.parametrize('country', ['china', 'india', 'turkey'])
@pytest.mark.parametrize('prefix', ['', './', '../', '/site/', 'https://nts.mgimo.ru/site/'])
def test_navigation_links_keep_query_and_fragment(country, prefix):
    url = f'{prefix}country_bulletins/{country}/bulletin_{country}_pdf.qmd'
    body = f'<a href="{url}?lang=ru#chart">country</a>'.encode()
    fixed = release.repair_links(body)
    assert fixed == body.replace(b'.qmd?', b'.html?')
    assert release.repair_links(fixed) == fixed


def test_unrelated_links_and_text_untouched():
    body = (b'<a href="https://other.test/country_bulletins/india/bulletin_india_pdf.qmd">x</a>'
            b'<a href="demo.qmd">x</a> bulletin_india_pdf.qmd')
    assert release.repair_links(body) == body


def test_navigation_repaired_in_package_and_existing_pages_with_rollback(tmp_path):
    stage, root, names = fixture_files(tmp_path)
    old = b'<a href="country_bulletins/india/bulletin_india_pdf.qmd">India</a>'
    existing = root / 'web/site/index.html'
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(old)
    packaged = stage / 'payload' / names[2]
    packaged.write_bytes(old)
    manifest_path = stage / 'payload/manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['files'][2].update(sha256=release.sha(packaged), bytes=len(old))
    manifest_path.write_text(json.dumps(manifest))
    release.files_action('backup', stage, root)
    release.files_action('install', stage, root)
    assert existing.read_bytes() == release.repair_links(old)
    assert release.destination(names[2], root).read_bytes() == release.repair_links(old)
    expected = json.loads((stage / 'navigation-installed.json').read_text())
    assert len(expected) == 2
    release.files_action('rollback', stage, root)
    assert existing.read_bytes() == old
    assert not release.destination(names[2], root).exists()


def test_corrupt_navigation_backup_aborts_rollback_before_data_changes(tmp_path):
    stage, root, names = fixture_files(tmp_path)
    existing = root / 'web/site/index.html'
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(b'<a href="country_bulletins/china/bulletin_china_pdf.qmd">x</a>')
    release.files_action('backup', stage, root)
    release.files_action('install', stage, root)
    (stage / 'backup/navigation/site/index.html').write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='Backup damaged'):
        release.files_action('rollback', stage, root)
    assert release.destination(names[0], root).read_bytes() == b'new'
