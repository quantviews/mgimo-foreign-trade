"""Unit tests for the safe query builder (no DB needed)."""

import pytest

from app.query import QueryError, build_trade_query


def _build(**kw):
    base = dict(
        filters={}, group_by=[], metrics=[], include=[],
        period_from=None, period_to=None, order_by=None, limit=100, offset=0,
    )
    base.update(kw)
    return build_trade_query(**base)


def test_aggregation_default_metrics():
    sql, params, meta = _build(group_by=["strana", "period"])
    assert "GROUP BY STRANA, PERIOD" in sql
    assert "SUM(STOIM) AS stoim" in sql and "SUM(NETTO) AS netto" in sql
    assert "SUM(KOL)" not in sql  # kol is opt-in
    assert meta["table"] == "unified_trade_data"


def test_kol_requires_edizm_in_group_by():
    with pytest.raises(QueryError):
        _build(group_by=["strana"], metrics=["stoim", "kol"])
    # OK when edizm is grouped
    sql, _, _ = _build(group_by=["strana", "edizm"], metrics=["kol"])
    assert "SUM(KOL) AS kol" in sql


def test_napr_alias_mapping():
    sql, params, _ = _build(filters={"napr": ["im", "ex"]})
    assert "NAPR IN (?, ?)" in sql
    assert params == ["ИМ", "ЭК"]


def test_strana_uppercased_and_parameterized():
    sql, params, _ = _build(filters={"strana": ["cn", "de"]})
    assert "STRANA IN (?, ?)" in sql
    assert params == ["CN", "DE"]


def test_unknown_dimension_rejected():
    with pytest.raises(QueryError):
        _build(group_by=["drop_table"])
    with pytest.raises(QueryError):
        _build(metrics=["evil"])
    with pytest.raises(QueryError):
        _build(include=["secret"])


def test_include_uses_enriched_view_and_requires_dim():
    with pytest.raises(QueryError):  # tnved2_name needs tnved2 in group_by
        _build(group_by=["strana"], include=["tnved2_name"])
    sql, _, meta = _build(group_by=["tnved2"], include=["tnved2_name"])
    assert meta["table"] == "unified_trade_data_enriched"
    assert "TNVED2_NAME AS tnved2_name" in sql


def test_raw_mode_selects_base_columns():
    sql, _, meta = _build(filters={"strana": ["CN"]})
    assert sql.startswith("SELECT NAPR, STRANA, TNVED,")
    assert "GROUP BY" not in sql
    assert meta["table"] == "unified_trade_data"


def test_period_parsing():
    sql, params, _ = _build(period_from="2024-01", period_to="2024-06-15")
    assert "PERIOD >= ?" in sql and "PERIOD <= ?" in sql
    assert params == ["2024-01-01", "2024-06-15"]
    with pytest.raises(QueryError):
        _build(period_from="2024")


def test_limit_fetches_one_extra():
    sql, _, meta = _build(group_by=["period"], limit=50)
    assert "LIMIT 51 OFFSET 0" in sql
    assert meta["page_rows"] == 50
