"""Data-only deployment on prod-146. No builds, migrations or calculations.

Host orchestrator uses the existing Superset image for narrowly mounted file
operations, so stp needs Docker access but no sudo password. Keep the stage
directory: it contains verified originals and the manual rollback command.
"""
import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
import uuid


# Only local bulletin hrefs, preserving query strings, fragments and quoting.
BULLETIN_HREF = re.compile(
    rb'''(\bhref\s*=\s*["'])(?P<url>(?:(?:\.\./|\./)*|/site/|https://nts\.mgimo\.ru/site/)country_bulletins/(?P<country>china|india|turkey)/bulletin_(?P=country)_pdf)\.qmd(?=[?#"'])''',
    re.IGNORECASE)


def repair_links(body):
    return BULLETIN_HREF.sub(rb'\1\g<url>.html', body)


def navigation_target(name, root):
    relative(name)
    if not name.startswith('site/') or not name.endswith('.html'):
        raise ValueError('Unexpected navigation page')
    target = root / 'web' / name
    safe_target(target)
    return target


def backup_navigation(stage, root):
    records = []
    for path in sorted((root / 'web/site').rglob('*.html')):
        safe_target(path)
        original = path.read_bytes()
        if repair_links(original) == original:
            continue
        name = path.relative_to(root / 'web').as_posix()
        copy = stage / 'backup/navigation' / name
        copy.parent.mkdir(parents=True, exist_ok=True)
        copy.write_bytes(original)
        records.append({'path': name, 'sha256': sha(copy)})
    (stage / 'backup/navigation.json').write_text(json.dumps(records))


def install_navigation(stage, root, manifest):
    names = {x['path'] for x in json.loads((stage / 'backup/navigation.json').read_text())}
    names.update(x['path'].removeprefix('web/') for x in manifest['files']
                 if x['path'].startswith('web/') and x['path'].endswith('.html'))
    expected = []
    for name in sorted(names):
        target = navigation_target(name, root)
        body = target.read_bytes()
        fixed = repair_links(body)
        if fixed != body:
            prepared = stage / 'navigation' / name
            prepared.parent.mkdir(parents=True, exist_ok=True)
            prepared.write_bytes(fixed)
            atomic_copy(prepared, target)
        expected.append({'path': name, 'sha256': sha(target)})
    # Used by the host's HTTPS checks, including pages outside the data package.
    output = stage / 'navigation-installed.json'
    output.write_text(json.dumps(expected))
    output.chmod(0o644)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def relative(name):
    p = PurePosixPath(name)
    if p.is_absolute() or '..' in p.parts or '\\' in name or str(p) != name:
        raise ValueError('Unsafe member path: ' + name)
    return p


def destination(name, root=Path('/target')):
    relative(name)
    if name == 'data/unified_trade_data.duckdb':
        return root / 'duckdb/unified_trade_data.duckdb'
    if name == 'data/fizob_bulletin.parquet':
        return root / 'parquet/fizob_bulletin.parquet'
    allowed = ('web/site/site_libs/', 'web/site/bulletin_comparison_files/')
    allowed += tuple('web/site/country_bulletins/' + c + '/bulletin_' + c + '_pdf_files/'
                     for c in ('china', 'india', 'turkey'))
    exact = {'web/site/bulletin_comparison.html'}
    exact.update('web/site/country_bulletins/' + c + '/bulletin_' + c + '_pdf.' + ext
                 for c in ('china', 'india', 'turkey') for ext in ('html', 'pdf'))
    if name in exact or name.startswith(allowed):
        return root / 'web' / name.removeprefix('web/')
    raise ValueError('Unexpected deployment file: ' + name)


def unpack(stage):
    payload = stage / 'payload'
    payload.mkdir()
    with tarfile.open(stage / 'release.tar.gz') as archive:
        seen = set()
        for member in archive.getmembers():
            relative(member.name)
            if not member.isfile() or member.name in seen:
                raise ValueError('Unsupported or duplicate archive member')
            seen.add(member.name)
            if member.name != 'manifest.json':
                destination(member.name)
            target = payload / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as src, target.open('wb') as dst:
                shutil.copyfileobj(src, dst)
    manifest = json.loads((payload / 'manifest.json').read_text())
    names = [x['path'] for x in manifest['files']]
    if len(names) != len(set(names)) or seen != set(names) | {'manifest.json'}:
        raise ValueError('Manifest does not match archive')
    if not {'data/unified_trade_data.duckdb', 'data/fizob_bulletin.parquet'} <= set(names):
        raise ValueError('Data pair missing')
    bulletins = {'web/site/country_bulletins/' + c + '/bulletin_' + c + '_pdf.html'
                 for c in ('china', 'india', 'turkey')}
    if not bulletins <= set(names):
        raise ValueError('Country bulletin HTML missing; navigation would be broken')
    for x in manifest['files']:
        p = payload / x['path']
        if sha(p) != x['sha256'] or p.stat().st_size != x['bytes']:
            raise ValueError('Payload checksum mismatch: ' + x['path'])
    return manifest


def safe_target(path):
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('Symlink in target path: ' + str(path))


def atomic_copy(src, dst):
    safe_target(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    temp = dst.with_name(dst.name + '.release-' + uuid.uuid4().hex)
    # Exclusive create protects against a previous interrupted install/symlink.
    try:
        with temp.open('xb') as f, src.open('rb') as source:
            shutil.copyfileobj(source, f)
            f.flush()
            os.fsync(f.fileno())
        if sha(temp) != sha(src):
            raise ValueError('Copy checksum mismatch')
        os.chmod(temp, 0o644)
        os.replace(temp, dst)
    finally:
        if temp.exists():
            temp.unlink()


def files_action(action, stage, root=Path('/target')):
    manifest = json.loads((stage / 'payload/manifest.json').read_text())
    backup = stage / 'backup'
    if action == 'backup':
        backup.mkdir(mode=0o700)
        originals = []
        for x in manifest['files']:
            dst = destination(x['path'], root)
            safe_target(dst)
            record = {'path': x['path'], 'existed': dst.exists()}
            if dst.exists():
                src = backup / x['path']
                src.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dst, src)
                record['sha256'] = sha(src)
                if sha(dst) != record['sha256']:
                    raise ValueError('Backup mismatch')
            elif x['path'].startswith('data/'):
                raise ValueError('Existing production data missing')
            originals.append(record)
        (backup / 'manifest.json').write_text(json.dumps(originals, indent=2))
        backup_navigation(stage, root)
    elif action == 'install':
        for x in manifest['files']:
            src = stage / 'payload' / x['path']
            if sha(src) != x['sha256']:
                raise ValueError('Staged file changed')
        for x in manifest['files']:
            atomic_copy(stage / 'payload' / x['path'], destination(x['path'], root))
        install_navigation(stage, root, manifest)
    elif action == 'rollback':
        originals = json.loads((backup / 'manifest.json').read_text())
        navigation_file = backup / 'navigation.json'
        navigation = json.loads(navigation_file.read_text()) if navigation_file.exists() else []
        for x in navigation:
            navigation_target(x['path'], root)
            if sha(backup / 'navigation' / x['path']) != x['sha256']:
                raise ValueError('Backup damaged; navigation rollback aborted before writes')
        for x in originals:
            destination(x['path'], root)
            if x['existed'] and sha(backup / x['path']) != x['sha256']:
                raise ValueError('Backup damaged; rollback aborted before writes')
        for x in originals:
            dst = destination(x['path'], root)
            safe_target(dst)
            if x['existed']:
                atomic_copy(backup / x['path'], dst)
            elif dst.exists():
                dst.unlink()
        for x in navigation:
            atomic_copy(backup / 'navigation' / x['path'], navigation_target(x['path'], root))
    else:
        raise ValueError(action)


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


def health():
    last = None
    for _ in range(30):
        try:
            for port, path in ((8090, '/health'), (8091, '/demo/health'), (8088, '/superset/health')):
                with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=5) as r:
                    if r.status != 200:
                        raise RuntimeError('Health check failed')
            return
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError('Service health checks failed') from last


def host(action, stage, digest=None):
    if capture('hostname') != 'prod-146':
        raise RuntimeError('Wrong host; expected prod-146')
    stage = stage.resolve()
    if stage.parent != Path('/home/stp') or not stage.name.startswith('mgimo-release-'):
        raise ValueError('Unexpected stage directory')
    # Prevent concurrent publication or rollback on this host.
    import fcntl
    lock = open('/home/stp/.mgimo-publication.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if action == 'publish':
        if sha(stage / 'release.tar.gz') != digest:
            raise ValueError('Archive checksum mismatch')
        # Conservative allowance: extraction + old copies + atomic temp files.
        with tarfile.open(stage / 'release.tar.gz') as archive:
            size = sum(x.size for x in archive.getmembers())
        if shutil.disk_usage('/srv').free < size * 4 + 1024**3:
            raise RuntimeError('Insufficient space for staging and rollback')
        manifest = unpack(stage)
        containers = json.loads(capture('docker', 'inspect', *capture('docker', 'ps', '-aq').split()))
        selected = [c for c in containers if c['Name'].lstrip('/') in ('trade-api','trade-demo','trade-mcp') or
                    (c['Config'].get('Labels', {}).get('com.docker.compose.project') == 'mgimo-superset' and
                     c['Config'].get('Labels', {}).get('com.docker.compose.service') in ('superset','worker','beat'))]
        if len(selected) != 6 or not all(c['State']['Running'] for c in selected):
            raise RuntimeError('Expected six running consumers; inspect server before proceeding')
        superset = next(c for c in selected if c['Config'].get('Labels', {}).get('com.docker.compose.service') == 'superset')
        state = {'containers': [c['Name'].lstrip('/') for c in selected], 'image': superset['Image'],
                 'superset': superset['Name'].lstrip('/')}
        (stage / 'state.json').write_text(json.dumps(state, indent=2))
    elif action == 'rollback':
        state = json.loads((stage / 'state.json').read_text())
        manifest = json.loads((stage / 'payload/manifest.json').read_text())
    else:
        raise ValueError(action)
    def helper(mode):
        run('docker','run','--rm','--network','none','--user','0','--entrypoint','python',
            '-v',f'{stage}:/stage','-v','/srv/duckdb:/target/duckdb',
            '-v','/srv/parquet:/target/parquet','-v','/opt/mgimo-landing/mgimo-landing:/target/web',
            state['image'],'/stage/publish_remote.py','files',mode)
    def start():
        run('docker','start',*state['containers'])
    def clear_cache():
        run('docker','exec',state['superset'],'python','-c',
            "import redis; r=redis.Redis(host='127.0.0.1',port=6379,db=1); "
            "n=sum(r.delete(k) for k in r.scan_iter(match='superset_data_*',count=200)); print('Chart cache cleared:',n)")
    backed_up = False
    changed = False
    can_start = True
    try:
        run('docker','stop','--time','45',*state['containers'])
        if action == 'rollback':
            helper('rollback')
        else:
            helper('backup')
            backed_up = True
            changed = True
            helper('install')
        start()
        clear_cache()
        health()
        if action == 'publish':
            # Reuse authenticated API/XLSX/MCP checks without copying any secrets.
            smoke = (stage / 'smoke.py').read_text()
            smoke = smoke.replace('HERE = Path(__file__).resolve().parent',
                                  "HERE = Path('/home/stp/mgimo-trade/deploy/mgimo146')")
            smoke = smoke.replace("== 7399486", '== ' + str(manifest['rows']))
            smoke = smoke.replace("(HERE / 'smoke-export.xlsx')", "(Path(__file__).resolve().parent / 'smoke-export.xlsx')")
            smoke = smoke.replace("(HERE / 'smoke-export.json')", "(Path(__file__).resolve().parent / 'smoke-export.json')")
            (stage / 'check.py').write_text(smoke)
            run('python3', str(stage / 'check.py'))
            navigation = json.loads((stage / 'navigation-installed.json').read_text())
            expected_html = {x['path']: x['sha256'] for x in navigation}
            for x in manifest['files']:
                if x['path'].startswith('web/') and x['path'].endswith(('.html','.pdf')):
                    url = 'https://nts.mgimo.ru/' + x['path'].removeprefix('web/')
                    body = subprocess.check_output(['curl','-fsS','--max-time','30','--resolve','nts.mgimo.ru:443:127.0.0.1',url])
                    expected = expected_html.get(x['path'].removeprefix('web/'), x['sha256'])
                    if hashlib.sha256(body).hexdigest() != expected:
                        raise RuntimeError('HTTPS content differs: ' + url)
            for x in navigation:
                url = 'https://nts.mgimo.ru/' + x['path']
                body = subprocess.check_output(['curl','-fsS','--max-time','30','--resolve','nts.mgimo.ru:443:127.0.0.1',url])
                if hashlib.sha256(body).hexdigest() != x['sha256'] or repair_links(body) != body:
                    raise RuntimeError('HTTPS navigation differs: ' + url)
        (stage / 'result.json').write_text(json.dumps({'status':action + '_ok','release':manifest['release']},indent=2))
        print(action.upper() + ' OK. Backup and logs: ' + str(stage), flush=True)
    except BaseException:
        if backed_up and changed:
            can_start = False
            run('docker','stop','--time','45',*state['containers'])
            helper('rollback')
            can_start = True
            start()
            clear_cache()
            health()
            print('Previous files restored and health checked', flush=True)
        elif action == 'rollback':
            can_start = False
        raise
    finally:
        if can_start:
            start()


if __name__ == '__main__':
    if sys.argv[1] == 'files':
        files_action(sys.argv[2], Path('/stage'))
    else:
        host(sys.argv[1], Path(sys.argv[2]), sys.argv[3] if len(sys.argv)>3 else None)
