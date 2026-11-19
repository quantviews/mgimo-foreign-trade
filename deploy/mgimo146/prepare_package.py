"""Package already validated local results. Never connects to a server."""
import argparse
import io
import json
from pathlib import Path
import tarfile

from publish_remote import destination, sha, unpack


def prepare(root, output, validation_dir, release):
    quality = json.loads((validation_dir / 'release-quality.json').read_text(encoding='utf-8'))
    corrections = json.loads((validation_dir / 'final-adjustments-check.json').read_text(encoding='utf-8'))
    render = json.loads((validation_dir / 'bulletin-render-check.json').read_text(encoding='utf-8'))
    installed = {x['path']: x['sha256'] for x in json.loads((validation_dir / 'local-installed.json').read_text())}
    assert corrections['other_factual_fields_and_rows_unchanged']
    assert not corrections['zero_substitution']
    sources = {
        'data/unified_trade_data.duckdb': root / 'db/unified_trade_data.duckdb',
        'data/fizob_bulletin.parquet': root / 'site/data/fizob_bulletin.parquet',
    }
    for path in sources.values():
        if sha(path) != installed[path.relative_to(root).as_posix()]:
            raise ValueError('Validated local data changed: ' + str(path))
    site = root / 'site/_site'
    pages = [site / 'bulletin_comparison.html']
    folders = [site / 'site_libs', site / 'bulletin_comparison_files']
    for country in ('china', 'india', 'turkey'):
        stem = site / 'country_bulletins' / country / f'bulletin_{country}_pdf'
        pages += [stem.with_suffix('.html'), stem.with_suffix('.pdf')]
        folders.append(stem.with_name(stem.name + '_files'))
        if sha(stem.with_suffix('.pdf')) != render[country]['sha256']:
            raise ValueError('Reviewed PDF changed: ' + country)
    assets = pages + [p for folder in folders for p in folder.rglob('*') if p.is_file()]
    allowed = {'.html', '.pdf', '.svg', '.png', '.jpg', '.jpeg', '.webp', '.gif',
               '.js', '.css', '.woff', '.woff2', '.ttf', '.eot', '.map', '.scss', '.txt'}
    for path in assets:
        if path.suffix.lower() not in allowed or path.is_symlink():
            raise ValueError('Unexpected web asset: ' + str(path))
        sources['web/site/' + path.relative_to(site).as_posix()] = path
    records = []
    for name, path in sorted(sources.items()):
        destination(name)
        records.append(dict(path=name, bytes=path.stat().st_size, sha256=sha(path)))
    manifest = dict(release=release, rows=quality['factual_rows'] + quality['nowcast_rows'],
                    data_version=max(str(x[1])[:7] for x in quality['latest_facts']), files=records)
    output.mkdir(parents=True, exist_ok=False)
    body = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
    (output / 'manifest.json').write_bytes(body)
    with tarfile.open(output / 'release.tar.gz', 'w:gz', compresslevel=1) as archive:
        info = tarfile.TarInfo('manifest.json')
        info.size = len(body)
        archive.addfile(info, io.BytesIO(body))
        for name, path in sorted(sources.items()):
            archive.add(path, arcname=name, recursive=False)
    # Exercise the same path/manifest/hash verification as the server, locally.
    verified = unpack(output)
    assert verified == manifest
    digest = sha(output / 'release.tar.gz')
    receipt = dict(release=release, files=len(records), bytes=(output / 'release.tar.gz').stat().st_size,
                   sha256=digest, local_unpack_verified=True, production_changed=False)
    (output / 'package-check.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print(json.dumps(receipt))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--validation-dir', type=Path, required=True)
    parser.add_argument('--release', required=True)
    args = parser.parse_args()
    prepare(Path(__file__).resolve().parents[2], args.output, args.validation_dir, args.release)
