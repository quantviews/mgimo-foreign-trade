import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from audit_india_oil_family import reconcile


def local(period, value):
    return dict(period=period, local_usd=value, positive_rows=1)


def report(period, value, ytd, invalid=False):
    return dict(period=period, monthly_usd=value, ytd_usd=ytd,
                requested_year=int(period[:4]), file='national-report.html',
                all_zero_response=invalid)


def test_broken_zero_response_is_not_zero_trade():
    source = [local('2024-01-01', 100000)]
    before = copy.deepcopy(source)
    result = reconcile(source, [report('2024-01-01', 0, 0, invalid=True)])
    assert result[0]['status'] == 'invalid_all_zero_source_response'
    assert result[0]['local_usd'] == 100000
    assert source == before


def test_exact_cumulative_corroborates_one_existing_month_only():
    source = [local('2024-01-01', 100000), local('2024-02-01', 200000),
              local('2024-03-01', 300000), local('2024-04-01', 400000)]
    reports = [report('2024-01-01', 100000, 100000),
               report('2024-02-01', 200000, 300000),
               report('2024-03-01', 0, 0, invalid=True),
               report('2024-04-01', 400000, 1000000)]
    result = reconcile(source, reports)
    assert result[2]['status'] == 'verified_exact_cumulative'
    assert result[2]['local_usd'] == 300000
    assert result[2]['cumulative_period'] == '2024-04-01'
    reports[1]['all_zero_response'] = True
    result = reconcile(source, reports)
    assert not result[1]['status'].startswith('verified_')
    assert not result[2]['status'].startswith('verified_')


def test_cumulative_disagreement_does_not_rewrite_source_value():
    source = [local('2024-01-01', 100000), local('2024-02-01', 200000)]
    result = reconcile(source, [report('2024-01-01', 0, 0, invalid=True),
                                report('2024-02-01', 200000, 310000)])
    assert result[0]['status'] == 'invalid_all_zero_source_response'
    assert result[0]['local_usd'] == 100000


def test_missing_month_is_not_created_by_annual_total():
    source = [local('2024-02-01', 200000)]
    result = reconcile(source, [report('2024-02-01', 200000, 300000)])
    assert len(result) == 1
    assert result[0]['period'] == '2024-02-01'


def test_material_monthly_disagreement_remains_unresolved():
    result = reconcile([local('2024-01-01', 100000)],
                       [report('2024-01-01', 150000, 150000)])
    assert result[0]['status'] == 'unresolved_value_disagreement'
