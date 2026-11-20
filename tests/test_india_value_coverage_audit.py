import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from validate_india_joint_audit import coverage_table


def frame():
    return pd.DataFrame([dict(STRANA='IN', NAPR='ЭК', TNVED='2709000000', PERIOD=p,
                              STOIM=100.0, SOURCE='national')
                         for p in pd.date_range('2025-01-01','2026-09-01',freq='MS')])


def compute(data):
    with duckdb.connect() as con:
        con.register('raw',data)
        result, blockers = coverage_table(con,'raw',pd.Timestamp('2026-09-01'))
    return result[(result.STRANA=='IN') & (result.TNVED_G=='0')].iloc[0], blockers


def test_value_weighted_coverage_keeps_incomplete_flow_in_denominator():
    complete = frame()
    partial = frame()
    partial['TNVED'] = '2710122100'
    partial = partial[~partial.PERIOD.between('2026-01-01','2026-03-01')].copy()
    partial.loc[partial.PERIOD.dt.year.eq(2025),'STOIM'] = 0
    data = pd.concat([complete,partial],ignore_index=True)
    before = data.copy(deep=True)
    result, blockers = compute(data)
    assert result.current_available_usd == 1800
    assert result.matched_current_usd == 1200
    assert result.matched_current_share == pytest.approx(2/3)
    assert result.matched_base_share == 1
    assert blockers.iloc[0].missing_current_cells == 3
    assert blockers.iloc[0].missing_base_cells == 0
    pd.testing.assert_frame_equal(before,data)


def test_complete_months_with_source_change_are_not_comparable():
    data = frame()
    data.loc[data.PERIOD.ge('2025-10-01'),'SOURCE'] = 'comtrade'
    result, _ = compute(data)
    assert result.current_available_usd == 1200
    assert result.matched_current_share == 0
    assert result.matched_base_share == 0


def test_unknown_value_is_not_counted_as_an_observed_zero():
    data = frame()
    data.loc[data.PERIOD.eq('2026-03-01'),'STOIM'] = float('nan')
    result, blockers = compute(data)
    assert result.current_available_usd == 1100
    assert result.matched_current_share == 0
    assert blockers.iloc[0].missing_current_cells == 1
    assert data.STOIM.isna().sum() == 1
