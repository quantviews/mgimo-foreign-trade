"""/v1/fizob smoke tests against the real DuckDB."""

from fastapi.testclient import TestClient

from app.main import app

H = {"Authorization": "Bearer dev-token"}


def test_fizob_requires_auth():
    with TestClient(app) as c:
        assert c.get("/v1/fizob").status_code == 401


def test_fizob_filtered():
    with TestClient(app) as c:
        r = c.get("/v1/fizob", headers=H,
                  params={"strana": "CN", "napr": "im", "tn_level": 2, "limit": 3})
        assert r.status_code == 200
        j = r.json()
        assert len(j["data"]) <= 3
        if j["data"]:
            row = j["data"][0]
            assert row["STRANA"] == "CN" and row["NAPR"] == "ИМ" and row["tn_level"] == 2
            assert "idx" in row and "fizob_bp" in row and "tn_name" in row


def test_fizob_bad_napr():
    with TestClient(app) as c:
        assert c.get("/v1/fizob", headers=H, params={"napr": "xx"}).status_code == 400
