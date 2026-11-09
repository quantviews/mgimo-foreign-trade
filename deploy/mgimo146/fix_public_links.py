"""Repair legacy public HTML links and prefixed Superset branding on MGIMO 146.

Run with python3 on 146. Originals are retained for rollback outside the web root.
"""
from datetime import datetime, timezone
from pathlib import Path
import shutil
import subprocess

root = Path('/opt/mgimo-landing/mgimo-landing')
config = Path('/opt/mgimo-landing/landing-nginx.conf')
backup = Path.home() / ('mgimo-public-links-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
backup.mkdir(mode=0o700)
changed = []

def update(path, data):
    original = path.read_bytes()
    if data == original:
        return
    saved = backup / path.relative_to('/opt/mgimo-landing')
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, saved)
    path.write_bytes(data)
    changed.append((path, saved))

try:
    for p in root.rglob('*.html'):
        data = p.read_bytes()
        update(p, data.replace(b'https://172.17.0.146', b'https://nts.mgimo.ru')
               .replace(b'http://172.17.0.146', b'https://nts.mgimo.ru'))
    data = config.read_text()
    anchor = '    location /branding/ { alias /usr/share/nginx/branding/; }'
    if '/superset/branding/' not in data:
        if anchor not in data:
            raise RuntimeError('Expected branding location not found')
        data = data.replace(anchor, anchor + '\n'
            '    location ^~ /superset/branding/ { alias /usr/share/nginx/branding/; }\n'
            '    location ^~ /superset/static/branding/ { alias /usr/share/nginx/branding/; }')
        update(config, data.encode())
    subprocess.run(['docker', 'exec', 'mgimo-landing-landing-1', 'nginx', '-t'], check=True)
    subprocess.run(['docker', 'exec', 'mgimo-landing-landing-1', 'nginx', '-s', 'reload'], check=True)
except Exception:
    for p, saved in reversed(changed):
        shutil.copyfile(saved, p)
    raise

print('Rollback originals:', backup)
for p, _ in changed:
    print('Updated:', p)
