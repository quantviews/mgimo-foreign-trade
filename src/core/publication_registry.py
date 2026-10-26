"""Evidence-backed permissions for absent observations. Unknown never means zero."""
import hashlib
import json
from pathlib import Path

import pandas as pd

KEYS = ['STRANA', 'NAPR', 'TNVED', 'PERIOD']


def load_publication_registry(registry_path, root):
    """Combine curated evidence with download manifests without upgrading legacy claims."""
    registry = json.loads(Path(registry_path).read_text(encoding='utf-8'))
    if registry.get('schema_version') != 1:
        raise ValueError('Unsupported publication registry version')
    releases = list(registry['releases'])
    for reference in registry.get('comtrade_manifests', []):
        path = (Path(root) / reference).resolve()
        if not path.is_relative_to(Path(root).resolve()):
            raise ValueError('Manifest must be inside project')
        manifest = json.loads(path.read_text(encoding='utf-8'))
        for month, snapshot in manifest.items():
            for reporter, item in snapshot.get('reporters', {}).items():
                releases.append(dict(id=f'comtrade:{month}:{reporter}',source='comtrade',
                    reporter_code=reporter,partner_code='643',month=month,direction='X,M',
                    publication_status=item.get('publication_status','available'),
                    download_status=item.get('download_status','unknown'),
                    absence_semantics='unknown', checksum=item.get('checksum'),
                    last_released=item.get('lastReleased'),raw_file=f'{path.parent.relative_to(root).as_posix()}/{month}.parquet'))
    rules = {rule['id']: rule for rule in registry.get('source_rules', [])}
    for release in releases:
        rule_id = release.get('source_rule_id')
        if rule_id:
            if rule_id not in rules:
                raise ValueError('Unknown publication source rule')
            matches = [rules[rule_id]]
        else:
            matches = [rule for rule in rules.values() if rule['source'] == release['source']
                       and (not rule.get('country') or rule['country'] == release.get('country'))]
        for rule in matches:
            if rule['source'] != release['source'] or (rule.get('country') and rule['country'] != release.get('country')):
                raise ValueError('Source rule scope does not match publication')
            if rule.get('requires_release_specific_absence_evidence'):
                release['requires_absence_evidence'] = True
        source_flow = release.get('flow') or {'ЭК':'M','ИМ':'X'}.get(release.get('direction'), release.get('direction',''))
        release['unreconciled_scopes'] = [finding['commodity_scope']
            for finding in registry.get('detail_coverage_findings', [])
            if finding['source'] == release['source']
            and finding['month'] == release['month']
            and finding['reporter_code'] == release.get('reporter_code')
            and finding['flow'] in source_flow.split(',')]
    return releases


def confirmed_absence_rows(observations, registry_path, root):
    releases = load_publication_registry(registry_path, root)
    existing = observations.copy()
    existing['PERIOD'] = pd.to_datetime(existing.PERIOD)
    existing['TNVED'] = existing.TNVED.astype(str)
    # Even a prediction or another source's observation prevents synthetic replacement.
    occupied = set(existing[KEYS].itertuples(index=False, name=None))
    rows = []
    for release in releases:
        if not (release.get('publication_status') == 'confirmed_complete'
                and release.get('download_status') == 'complete'
                and release.get('absence_semantics') == 'zero_for_applicable_codes'):
            continue
        if not release.get('evidence') or not release.get('applicability_evidence'):
            raise ValueError('Zero inference requires publication and code applicability evidence')
        if release.get('requires_absence_evidence') and not release.get('absence_evidence'):
            raise ValueError('Source rule requires release-specific evidence of absent-code semantics')
        if release.get('source') not in {'national', 'comtrade'}:
            raise ValueError('Only statistical publications can establish factual absence')
        raw = (Path(root) / release['raw_file']).resolve()
        if not raw.is_relative_to(Path(root).resolve()):
            raise ValueError('Raw evidence must be inside project')
        if hashlib.sha256(raw.read_bytes()).hexdigest() != release['raw_sha256']:
            raise ValueError('Publication evidence hash mismatch')
        period = pd.Timestamp(release['month'] + '-01')
        excluded = release.get('excluded_prefixes', [])
        gaps = release.get('unreconciled_scopes', [])
        # A published value with withheld quantity is an observation, never
        # evidence of absent trade, even if the caller omitted that row.
        disclosed_values = {item['code'] for item in release.get('quantity_disclosure', [])
                            if item.get('value_status') == 'published'}
        for code in release['applicable_codes']:
            if not isinstance(code, str) or len(code) != 10 or not code.isdigit():
                raise ValueError('Applicable codes must be explicit canonical 10-digit codes')
            if code in disclosed_values or 'TOTAL' in gaps or any(code.startswith(prefix) for prefix in excluded + gaps):
                continue
            key = (release['country'], release['direction'], code, period)
            if key in occupied:
                continue
            occupied.add(key)
            rows.append(dict(zip(KEYS, key), STOIM=0., NETTO=None, KOL=None,
                             EDIZM=None, TYPE='fact', SOURCE=release['source'],
                             OBSERVATION_STATUS='confirmed_absence', RELEASE_ID=release['id']))
    return pd.DataFrame(rows, columns=KEYS + ['STOIM','NETTO','KOL','EDIZM','TYPE','SOURCE',
                                             'OBSERVATION_STATUS','RELEASE_ID'])
