"""Copy verified ready-made data and TLS files from /stage to mounted targets.

Run in a short-lived local Docker helper with /stage read-only and only the
required /target/duckdb, /target/parquet, /target/certs mounts writable.
Does not calculate data. Backups are managed separately by the operator.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil

stage = Path('/stage')
def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

manifest = json.loads((stage / 'data-manifest.json').read_text())
targets = {
    'db/unified_trade_data.duckdb': Path('/target/duckdb/unified_trade_data.duckdb'),
    'site/data/fizob_bulletin.parquet': Path('/target/parquet/fizob_bulletin.parquet'),
}
for name, target in targets.items():
    src = stage / Path(name).name
    digest = sha256(src)
    assert digest == manifest[name]['sha256'], name
    assert src.stat().st_size == manifest[name]['bytes'], name
    temp = target.with_name(target.name + '.new')
    shutil.copyfile(src, temp)
    assert sha256(temp) == digest
    os.chmod(temp, 0o644)
    os.replace(temp, target)
    print('Installed', target.name, digest)
for source, dest, mode in [('mgimo-fullchain.pem', 'mgimo-fullchain.pem', 0o644),
                           ('key.txt', 'mgimo.key', 0o600)]:
    target = Path('/target/certs') / dest
    temp = target.with_name(dest + '.new')
    shutil.copyfile(stage / source, temp)
    os.chmod(temp, mode)
    os.replace(temp, target)
print('TLS files installed; reload nginx separately after nginx -t.')
