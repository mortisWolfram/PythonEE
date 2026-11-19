"""Smoke tests for /health and /ready without a running core lifespan."""

from fastapi.testclient import TestClient

from main import app


def test_health_returns_ok() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_503_when_container_missing() -> None:
    # No `with TestClient(...)` → lifespan not executed → no container.
    # /ready must degrade to 503, not 500.
    client = TestClient(app)
    response = client.get("/ready")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["reason"] == "container not initialized"
