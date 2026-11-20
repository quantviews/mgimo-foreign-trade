"""Versioned, evidence-backed analytical code families; never rewrites source data."""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REGISTRY = ROOT / 'metadata/code_correspondence/2026-11-20.v1.json'


def validate(registry: dict) -> None:
    def require(condition, message):
        if not condition:
            raise ValueError(message)

    require(registry['schema_version'] == 1, 'Unsupported schema version')
    require(bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}\.v\d+', registry['version'])), 'Invalid version')
    date.fromisoformat(registry['reviewed_on'])
    require(registry['policy']['missing_to_zero'] is False, 'Zero imputation forbidden')
    require(registry['policy']['source_rows_immutable'] is True, 'Source rows must be immutable')
    require(registry['policy']['production_enabled'] is False, 'Registry is local-audit only')
    ids, occupied = set(), set()
    for entry in registry['entries']:
        eid = entry['id']
        require(eid not in ids, 'Duplicate entry ID: ' + eid)
        ids.add(eid)
        require(entry['revision'] >= 1, 'Invalid revision')
        scope = entry['scope']
        require(bool(re.fullmatch('[A-Z]{2}', scope['country'])), 'Invalid country')
        require(scope['direction'] in ('ИМ', 'ЭК'), 'Direction must be relative to Russia')
        require(scope['type'] == 'fact' and bool(scope['source']), 'Only scoped facts supported')
        require(entry['relation'] in ('split', 'merge', 'recode', 'observed_family'), 'Invalid relation')
        require(entry['use'] in ('local_complete_family', 'requires_monthly_reconciliation', 'review_only'), 'Invalid use')
        require(entry['statistical_valid_from'] is None or entry['statistical_valid_to'] is None
                or entry['statistical_valid_from'] <= entry['statistical_valid_to'], 'Reversed validity')
        for field in ('legal_effective_date', 'statistical_valid_from', 'statistical_valid_to', 'first_observed_successor'):
            if entry[field] is not None:
                date.fromisoformat(entry[field])
        before, after = entry['predecessors'], entry['successors']
        codes = before + after + entry['observed_members']
        require(bool(codes) and len(codes) == len(set(codes)), 'Empty or repeated codes')
        require(all(re.fullmatch(r'\d{8}', c) for c in codes), 'Expected native eight-digit codes')
        family = entry['comparable']
        require(family['level'] in (2, 4, 6) and len(family['code']) == family['level'], 'Invalid comparable level')
        require(all(c.startswith(family['code']) for c in codes), 'Codes outside comparable heading')
        require(family['extent'] in ('whole_heading', 'subset'), 'Explicit family extent required')
        require(bool(re.fullmatch(r'\d{10}', entry['analysis_key'])), 'Invalid internal key')
        require(entry['analysis_key'].startswith(family['code']), 'Key outside family')
        if entry['relation'] == 'split':
            require(len(before) == 1 and len(after) > 1, 'Invalid split')
        elif entry['relation'] == 'merge':
            require(len(before) > 1 and len(after) == 1, 'Invalid merge')
        elif entry['relation'] == 'recode':
            require(len(before) == len(after) == 1, 'Invalid recode')
        else:
            require(not before and not after and entry['use'] != 'local_complete_family', 'Observed family is not a succession')
        if entry['use'] == 'local_complete_family':
            require(entry['relation'] == 'split' and entry['relationship_confirmed'], 'Unsupported executable rule')
            require(entry['first_observed_successor'] is not None, 'Missing observed boundary')
        require(bool(entry['evidence']), 'Missing evidence')
        for evidence in entry['evidence']:
            require(bool(evidence['locator']) and bool(evidence.get('url') or evidence.get('path')), 'Missing evidence locator')
            if evidence.get('sha256'):
                require(bool(re.fullmatch('[a-f0-9]{64}', evidence['sha256'])), 'Invalid evidence hash')
        # Conservatively reject ambiguous families, even when dates seem disjoint.
        for code in codes:
            key = (scope['country'], scope['direction'], scope['source'], scope['type'], code)
            require(key not in occupied, 'Overlapping scoped code: ' + code)
            occupied.add(key)


def load_registry(path: Path = DEFAULT_REGISTRY) -> dict:
    registry = json.loads(Path(path).read_text(encoding='utf-8'))
    validate(registry)
    return registry


def local_split_rules(registry: dict, country: str, direction: str, source: str) -> dict:
    """Return candidates only; callers MUST check every monthly representation."""
    validate(registry)
    result = {}
    for entry in registry['entries']:
        if entry['use'] != 'local_complete_family' or entry['scope'] != {
            'country': country, 'direction': direction, 'source': source, 'type': 'fact'
        }:
            continue
        result[entry['analysis_key']] = {
            'id': entry['id'], 'revision': entry['revision'],
            'children': [c.ljust(10, '0') for c in entry['successors']],
            'legal_effective_date': entry['legal_effective_date'],
            'first_observed_successor': entry['first_observed_successor'],
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    registry = load_registry(args.registry)
    payload = {'version': registry['version'],
               'sha256': hashlib.sha256(args.registry.read_bytes()).hexdigest(),
               'rules': local_split_rules(registry, 'IN', 'ИМ', 'national')}
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(text + '\n', encoding='utf-8')
    else:
        print(text)


if __name__ == '__main__':
    main()
