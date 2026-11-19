"""Apply reviewed, value-guarded national revisions and supplemental mass only."""
import json
import hashlib
import logging
from pathlib import Path

import pandas as pd


def apply_india_verified_adjustments(df, project_root):
    registry = Path(project_root) / 'metadata/india_verified_adjustments.json'
    if not registry.exists():
        return df
    scope = df.STRANA.eq('IN') & df.SOURCE.eq('national') & df.TYPE.eq('fact')
    if not scope.any():
        return df
    for name in ('NETTO_SOURCE', 'VERIFIED_ADJUSTMENT'):
        if name not in df:
            df[name] = pd.Series(None, index=df.index, dtype=object)
    applied = 0
    for patch in json.loads(registry.read_text(encoding='utf-8'))['adjustments']:
        if 'evidence' in patch:
            evidence = Path(project_root) / patch['evidence']
            if hashlib.sha256(evidence.read_bytes()).hexdigest() != patch['evidence_sha256']:
                raise ValueError('India adjustment evidence hash mismatch')
        mask = scope & df.NAPR.eq(patch['napr']) & df.TNVED.eq(patch['tnved']) & df.PERIOD.eq(pd.Timestamp(patch['period']))
        ids = df.index[mask]
        if len(ids) == 0:
            continue  # An intentionally restricted build need not contain this month.
        if len(ids) != 1:
            raise ValueError(f'Ambiguous India adjustment target: {patch}')
        i = ids[0]
        current = df.at[i, patch['field']]
        already = pd.notna(current) and abs(float(current) - patch['value']) < .01
        expected = pd.isna(current) if patch['old'] is None else pd.notna(current) and abs(float(current) - patch['old']) < .01
        if not already and not expected:
            raise ValueError(f'India source changed; review adjustment {patch["period"]} {patch["tnved"]} {patch["field"]}')
        if 'expected_cost' in patch and abs(float(df.at[i, 'STOIM']) - patch['expected_cost']) >= .01:
            raise ValueError('India supplemental mass: national cost changed; recheck source match')
        df.at[i, patch['field']] = patch['value']
        tag = patch['source'] + ':' + patch['field']
        tags = [] if pd.isna(df.at[i, 'VERIFIED_ADJUSTMENT']) else df.at[i, 'VERIFIED_ADJUSTMENT'].split(';')
        df.at[i, 'VERIFIED_ADJUSTMENT'] = ';'.join(sorted(set(tags + [tag])))
        if patch['field'] == 'NETTO':
            df.at[i, 'NETTO_SOURCE'] = patch['source']
            if patch['source'].startswith('comtrade'):
                df.at[i, 'NETTO_ESTIMATED'] = pd.NA
                df.at[i, 'PHYSICAL_QUALITY'] = 'Масса дополнена по Comtrade: количество в кг; признак оценки источником не раскрыт'
        applied += not already
    for name in ('NETTO_SOURCE', 'VERIFIED_ADJUSTMENT'):
        df[name] = df[name].astype('string')
    logging.info('Applied %s reviewed India field adjustments', applied)
    return df
