"""Smoke test: the BFF starts and responds to /health."""
from fastapi.testclient import TestClient

from customer_bff.main import app


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
