"""Connect the existing Superset cabinet to the new API DB, without printing secrets."""
from pathlib import Path
import shutil
import yaml
from bootstrap import HERE, ROOT

values = {}
for line in (HERE / 'cabinet.env').read_text().splitlines():
    k, v = line.split('=', 1)
    values[k] = v.strip("'")
compose = Path('/opt/mgimo-superset/docker-compose.yml')
cfg = yaml.safe_load(compose.read_text())
for name, service in cfg['services'].items():
    if 'superset' in service.get('image', ''):
        env = service.setdefault('environment', {})
        if isinstance(env, list):
            env = dict(item.split('=', 1) for item in env)
            service['environment'] = env
        env.update(values)
compose.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding='utf-8')
compose.chmod(0o600)
for p in (ROOT / 'superset_cabinet').rglob('*'):
    if p.is_file() and '__pycache__' not in p.parts:
        target = Path('/opt/mgimo-superset/superset_cabinet') / p.relative_to(ROOT / 'superset_cabinet')
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(p, target)

print('Superset cabinet configured. User keys must be issued through the cabinet.')
