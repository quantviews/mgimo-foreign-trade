"""OData feed: $filter parser + entity set (unit + smoke against real DuckDB)."""

import xml.etree.ElementTree as ET

import pytest
from fastapi.testclient import TestClient

from app import odata
from app.main import app

H = {"Authorization": "Bearer dev-token"}


# --- $filter parser ---------------------------------------------------------

def test_filter_single_eq():
    sql, params = odata.filter_to_sql("STRANA eq 'CN'")
    assert sql == "STRANA = ?"
    assert params == ["CN"]


def test_filter_and_with_date():
    sql, params = odata.filter_to_sql("STRANA eq 'CN' and PERIOD ge 2024-01-01")
    assert sql == "(STRANA = ? AND PERIOD >= ?)"
    assert params == ["CN", "2024-01-01"]


def test_filter_parentheses_and_or():
    sql, params = odata.filter_to_sql("(NAPR eq 'ИМ' or NAPR eq 'ЭК') and STRANA eq 'DE'")
    assert params == ["ИМ", "ЭК", "DE"]
    assert "OR" in sql and "AND" in sql


def test_filter_rejects_unknown_field_and_op():
    with pytest.raises(odata.ODataError):
        odata.filter_to_sql("EVIL eq 'x'")
    with pytest.raises(odata.ODataError):
        odata.filter_to_sql("STRANA zz 'x'")


def test_filter_number_literal():
    sql, params = odata.filter_to_sql("STOIM gt 1000")
    assert sql == "STOIM > ?" and params == [1000]


def test_build_query_select_orderby_paging():
    sql, params, cols = odata.build_trade_query(
        filter_expr="STRANA eq 'CN'", select="STRANA,PERIOD,STOIM",
        orderby="PERIOD desc", top=100, skip=200,
    )
    assert cols == ["STRANA", "PERIOD", "STOIM"]
    assert "ORDER BY PERIOD DESC" in sql
    assert "LIMIT 101 OFFSET 200" in sql
    assert "WHERE STRANA = ?" in sql and params == ["CN"]


def test_metadata_is_valid_xml_with_entityset():
    root = ET.fromstring(odata.metadata_xml())
    assert root.tag.endswith("Edmx")
    xml = odata.metadata_xml()
    assert "trade" in xml and "fizob" in xml  # both entity sets described


def test_build_fizob_query_derived_and_filter():
    sql, params, cols = odata.build_query(
        odata.FIZOB, filter_expr="STRANA eq 'CN' and tn_level eq 2",
        select="STRANA,tn_name,idx", orderby=None, top=10, skip=0,
    )
    assert cols == ["STRANA", "tn_name", "idx"]
    assert "AS tn_name" in sql  # derived CASE property is aliased
    assert "FROM fizob_enriched" in sql
    assert params == ["CN", 2]

    with pytest.raises(odata.ODataError):  # trade-only field is rejected on fizob
        odata.build_query(odata.FIZOB, filter_expr="TNVED eq '01'",
                          select=None, orderby=None, top=10, skip=0)


# --- smoke against the real DuckDB -----------------------------------------

def test_service_and_metadata():
    with TestClient(app) as c:
        assert c.get("/odata/").status_code == 401  # auth required
        r = c.get("/odata/", headers=H)
        assert r.status_code == 200
        assert r.json()["value"][0]["name"] == "trade"
        m = c.get("/odata/$metadata", headers=H)
        assert m.status_code == 200 and "EntitySet" in m.text


def test_entityset_top_filter_count():
    with TestClient(app) as c:
        r = c.get("/odata/trade", headers=H, params={"$top": 3})
        j = r.json()
        assert r.status_code == 200 and len(j["value"]) == 3
        assert "Id" in j["value"][0] and "STOIM" in j["value"][0]

        r = c.get("/odata/trade", headers=H, params={"$filter": "STRANA eq 'CN'", "$top": 5})
        assert all(row["STRANA"] == "CN" for row in r.json()["value"])

        r = c.get("/odata/trade", headers=H, params={"$top": 2, "$count": "true"})
        assert r.json()["@odata.count"] > 0
        assert "@odata.nextLink" in r.json()


def test_service_lists_fizob():
    with TestClient(app) as c:
        names = {e["name"] for e in c.get("/odata/", headers=H).json()["value"]}
        assert {"trade", "fizob"} <= names


def test_fizob_entityset_filter_and_derived_name():
    with TestClient(app) as c:
        r = c.get("/odata/fizob", headers=H,
                  params={"$filter": "STRANA eq 'CN' and tn_level eq 2", "$top": 3})
        assert r.status_code == 200
        j = r.json()
        assert len(j["value"]) <= 3
        if j["value"]:
            row = j["value"][0]
            assert row["STRANA"] == "CN" and row["tn_level"] == 2
            assert "idx" in row and "tn_name" in row and "Id" in row
