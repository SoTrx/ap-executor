"""Tests for the API endpoints using the FastAPI test client."""
from dataclasses import dataclass
from typing import Any, List, Optional

import pytest
from dapr.ext.workflow import WorkflowStatus
from fastapi.testclient import TestClient

from ap_executor.di import get_workflow_client
from ap_executor.main import app
from ap_executor.services.executor.execution import ExecutionResult, ExecutionStatus


@dataclass
class FakeFailureDetails:
    message: str


@dataclass
class FakeWorkflowState:
    runtime_status: WorkflowStatus
    serialized_output: Optional[str] = None
    failure_details: Optional[FakeFailureDetails] = None


class FakeWorkflowClient:
    """Stand-in for DaprWorkflowClient — no real Dapr sidecar needed in tests."""

    def __init__(self, state: FakeWorkflowState, instance_id: str = "fake-instance-id"):
        self._state = state
        self._instance_id = instance_id
        self.scheduled_inputs: List[Any] = []

    async def schedule_new_workflow(self, workflow, *, input=None, **kwargs):
        self.scheduled_inputs.append(input)
        return self._instance_id

    async def wait_for_workflow_completion(self, instance_id, **kwargs):
        return self._state

    async def get_workflow_state(self, instance_id, **kwargs):
        return self._state


@pytest.fixture
def client():
    """Create a test client that doesn't start the lifespan (no Dapr sidecar)."""
    app.router.lifespan_context = None  # type: ignore
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def _override_workflow_client(fake: FakeWorkflowClient) -> None:
    app.dependency_overrides[get_workflow_client] = lambda: fake


DB_ID = "11111111-1111-4111-8111-111111111111"
TABLE_ID = "22222222-2222-4222-8222-222222222222"
OPERATOR_ID = "33333333-3333-4333-8333-333333333333"
AP_ID = "44444444-4444-4444-8444-444444444444"

SAMPLE_AP = {
    "nodes": [
        {
            "id": DB_ID,
            "labels": ["RelationalDatabase"],
            "properties": {"name": "testdb"},
        },
        {
            "id": TABLE_ID,
            "labels": ["Table"],
            "properties": {"name": "public.users"},
        },
        {
            "id": OPERATOR_ID,
            "labels": ["Operator", "SQL_Operator"],
            "properties": {"name": "Select", "query": "SELECT 1"},
        },
        {
            "id": AP_ID,
            "labels": ["Analytical_Pattern"],
            "properties": {"name": "Test AP"},
        },
    ],
    "edges": [
        {"from": AP_ID, "to": OPERATOR_ID, "labels": ["consist_of"]},
        {"from": DB_ID, "to": OPERATOR_ID, "labels": ["input"]},
        {"from": DB_ID, "to": TABLE_ID, "labels": ["containedIn"]},
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
    """An AP with no root Analytical_Pattern node fails AnalyticalPattern's own
    structural validation and should return 422 before the workflow client is
    ever touched."""
    _override_workflow_client(FakeWorkflowClient(
        FakeWorkflowState(runtime_status=WorkflowStatus.RUNNING)))
    resp = client.post("/api/v1/aps/execute",
                       json={"ap": {"nodes": [], "edges": []}})
    assert resp.status_code == 422


def test_execute_sync_success(client):
    result = ExecutionResult(status=ExecutionStatus.SUCCESS, operators=[])
    state = FakeWorkflowState(
        runtime_status=WorkflowStatus.COMPLETED, serialized_output=result.model_dump_json()
    )
    fake = FakeWorkflowClient(state)
    _override_workflow_client(fake)

    resp = client.post(
        "/api/v1/aps/execute",
        json={"ap": SAMPLE_AP, "state": {OPERATOR_ID: {"nl": "hi"}}},
    )

    assert resp.status_code == 200
    assert resp.json()["status"] == "success"
    # The instance's parameters reach the scheduled workflow input.
    assert fake.scheduled_inputs[0].state == {
        OPERATOR_ID: {"nl": "hi"}}


def test_execute_sync_workflow_failure_returns_500(client):
    state = FakeWorkflowState(
        runtime_status=WorkflowStatus.FAILED,
        failure_details=FakeFailureDetails(message="operator unresolvable"),
    )
    _override_workflow_client(FakeWorkflowClient(state))

    resp = client.post("/api/v1/aps/execute", json={"ap": SAMPLE_AP})

    assert resp.status_code == 500
    assert resp.json()["detail"] == "operator unresolvable"


def test_execute_async_returns_task_id(client):
    state = FakeWorkflowState(runtime_status=WorkflowStatus.RUNNING)
    _override_workflow_client(FakeWorkflowClient(state, instance_id="wf-123"))

    resp = client.post("/api/v1/aps/execute/async", json={"ap": SAMPLE_AP})

    assert resp.status_code == 202
    assert resp.json() == {"task_id": "wf-123", "status": "pending"}


def test_get_task_status_running(client):
    state = FakeWorkflowState(runtime_status=WorkflowStatus.RUNNING)
    _override_workflow_client(FakeWorkflowClient(state))

    resp = client.get("/api/v1/aps/execute/async/wf-123")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"


def test_get_task_status_success(client):
    result = ExecutionResult(status=ExecutionStatus.SUCCESS, operators=[])
    state = FakeWorkflowState(
        runtime_status=WorkflowStatus.COMPLETED, serialized_output=result.model_dump_json()
    )
    _override_workflow_client(FakeWorkflowClient(state))

    resp = client.get("/api/v1/aps/execute/async/wf-123")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["result"]["status"] == "success"
