"""Tests for the API endpoints using the FastAPI test client."""
import pytest
from fastapi.testclient import TestClient

from ap_executor.main import app


@pytest.fixture
def client():
    """Create a test client that doesn't start the lifespan (no Celery)."""
    # Override lifespan to avoid Celery startup in tests
    app.router.lifespan_context = None  # type: ignore
    return TestClient(app, raise_server_exceptions=False)


SAMPLE_AP = {
    "nodes": [
        {
            "id": "db-1",
            "labels": ["Relational_Database"],
            "properties": {"name": "testdb"},
        },
        {
            "id": "tbl-1",
            "labels": ["Table"],
            "properties": {"name": "public.users"},
        },
        {
            "id": "op-1",
            "labels": ["Operator", "SQL_Operator"],
            "properties": {"name": "Select", "query": "SELECT 1"},
        },
    ],
    "edges": [
        {"from": "op-1", "to": "tbl-1", "labels": ["input"]},
        {"from": "tbl-1", "to": "db-1", "labels": ["contain"]},
    ],
}


def test_root(client):
    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "AP Executor"


def test_health(client):
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_execute_missing_db_node(client):
    """An AP with no Relational_Database node should return 422."""
    resp = client.post("/api/v1/execute", json={"nodes": [], "edges": []})
    assert resp.status_code == 422
