"""Explicitly confirmed national releases supersede the entire monthly forecast slice."""
import json
import math
import hashlib
from pathlib import Path
import pandas as pd


def drop_confirmed_national_nowcast(frame, project_root, logger):
    manifest = Path(project_root) / 'metadata/national_release_coverage.json'
    if not manifest.exists():
        return frame
    records = json.loads(manifest.read_text(encoding='utf-8'))['releases']
    periods = pd.to_datetime(frame['PERIOD']).dt.strftime('%Y-%m')
    remove = pd.Series(False, index=frame.index)
    for record in records:
        if record.get('status') != 'confirmed_complete':
            continue
        raw = Path(project_root) / record['raw_file']
        if hashlib.sha256(raw.read_bytes()).hexdigest() != record['raw_sha256']:
            raise ValueError(f"National release changed: reconfirm coverage for {record['raw_file']}")
        scope = (frame['STRANA'] == record['country']) & (frame['NAPR'] == record['direction']) & (periods == record['month'])
        facts = frame[scope & (frame['TYPE'] == 'fact') & (frame['SOURCE'] == 'national')]
        if facts.empty and not scope.any():
            continue  # Country/month excluded from this build.
        if len(facts) != record['rows'] or not math.isclose(float(facts['STOIM'].sum()),record['stoim_usd'],rel_tol=0,abs_tol=0.01):
            raise ValueError(f"Incomplete national slice: {record['country']} {record['month']} {record['direction']}")
        remove |= scope & (frame['TYPE'] == 'pred')
    logger.info('Confirmed national releases: removing %s residual nowcast rows', int(remove.sum()))
    return frame.loc[~remove].copy()
