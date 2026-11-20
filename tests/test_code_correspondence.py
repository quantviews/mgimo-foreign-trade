import copy
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('code_correspondence', ROOT / 'src/core/code_correspondence.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_pinned_rules_preserve_checked_scenario():
    r = module.load_registry()
    rules = module.local_split_rules(r, 'IN', 'ИМ', 'national')
    assert {k: v['children'] for k, v in rules.items()} == {
        '2924190000': ['2924191000', '2924199000'],
        '2933490000': ['2933491000', '2933499000'],
    }
    assert all(x['first_observed_successor'] == '2025-04-01' for x in rules.values())
    assert all(x['legal_effective_date'] == '2025-05-01' for x in rules.values())


@pytest.mark.parametrize('country,direction,source', [('CN','ИМ','national'), ('IN','ЭК','national'), ('IN','ИМ','comtrade')])
def test_no_cross_scope_rules(country, direction, source):
    assert module.local_split_rules(module.load_registry(), country, direction, source) == {}


def test_oil_is_not_a_native_succession():
    oil = module.load_registry()['entries'][-1]
    assert oil['relation'] == 'observed_family'
    assert oil['statistical_valid_from'] is oil['statistical_valid_to'] is None
    assert oil['use'] == 'requires_monthly_reconciliation'


def test_fungicide_family_includes_all_legal_branches():
    e = next(x for x in module.load_registry()['entries'] if x['id'] == 'IN-2025-38089290')
    assert e['successors'] == ['38089280', '38089291', '38089299']
    assert e['comparable']['extent'] == 'subset'
    assert e['use'] == 'review_only'


@pytest.mark.parametrize('mutation', ['duplicate', 'overlap', 'wrong_heading', 'no_evidence', 'oil_executable', 'bad_date', 'zeros'])
def test_invalid_registry_rejected(mutation):
    r = copy.deepcopy(module.load_registry())
    if mutation in ('duplicate', 'overlap'):
        duplicate = copy.deepcopy(r['entries'][0])
        if mutation == 'overlap':
            duplicate['id'] += '-conflict'
        r['entries'].append(duplicate)
    elif mutation == 'wrong_heading':
        r['entries'][0]['successors'][0] = '27090010'
    elif mutation == 'no_evidence':
        r['entries'][0]['evidence'] = []
    elif mutation == 'oil_executable':
        r['entries'][-1]['use'] = 'local_complete_family'
    elif mutation == 'zeros':
        r['policy']['missing_to_zero'] = True
    else:
        r['entries'][0]['statistical_valid_from'] = '2025-99-01'
    with pytest.raises(ValueError):
        module.validate(r)
