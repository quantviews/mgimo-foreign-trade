import base64

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.auth import authenticate
from app.config import settings
from app import limits


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "postgres_dsn", "")
    monkeypatch.setattr(settings, "dev_token", "swagger-test-token")

    async def no_limits(user):
        pass

    monkeypatch.setattr(limits, "enforce", no_limits)
    app = FastAPI()

    @app.get("/protected")
    async def protected(user=Depends(authenticate)):
        return {"authenticated": True}

    with TestClient(app) as client:
        yield client


def test_swagger_declares_bearer_instead_of_header_parameter(client):
    schema = client.get("/openapi.json").json()
    assert schema["components"]["securitySchemes"]["BearerAuth"]["scheme"] == "bearer"
    operation = schema["paths"]["/protected"]["get"]
    assert operation["security"] == [{"BearerAuth": []}]
    assert not any(p["name"].lower() == "authorization" for p in operation.get("parameters", []))


@pytest.mark.parametrize("scheme", ["Bearer", "Basic"])
def test_bearer_and_excel_basic_still_authenticate(client, scheme):
    value = "swagger-test-token"
    if scheme == "Basic":
        value = base64.b64encode(("excel:" + value).encode()).decode()
    assert client.get("/protected", headers={"Authorization": f"{scheme} {value}"}).status_code == 200


@pytest.mark.parametrize("value", [None, "Bearer wrong", "Bearer", "swagger-test-token"])
def test_missing_or_invalid_key_is_rejected(client, value):
    headers = {} if value is None else {"Authorization": value}
    assert client.get("/protected", headers=headers).status_code == 401
