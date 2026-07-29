"""Integration test for the AP-instance execution workflow.

Drives the real ``ap_execution_workflow`` orchestration generator against
dummy operators (real FastAPI apps) discovered through a dummy in-memory
Consul, all wired over a host-dispatching ASGI transport (no sockets). This
exercises the full slice end-to-end: execution ordering, per-operator input
resolution from the AP instance's ``state`` + upstream outputs,
Consul discovery, manifest fetch, strategy invocation, and result threading
between operators.

The orchestrator (``ap_execution.ap_execution_workflow``) calls the
production ``execute_operator_activity`` / ``poll_operator_activity``
directly by module-level name; those are Dapr-activity-decorated and can't be
invoked outside a running Dapr sidecar. So the two names are monkeypatched
(inside the ``ap_execution`` module, where the orchestrator looks them up) to
thin single-arg wrappers around the *real* activity bodies (reached via
``.__wrapped__``, same idiom as below for the workflow itself), with their
``http_client_factory``/``operator_resolver_factory`` DI seams pointed at the
dummy cluster transport — no activity-body duplication needed.
"""
import inspect
import json
from pathlib import Path
from typing import Dict

import httpx
import pytest
from fastapi import FastAPI

import ap_executor.workflows.ap_execution as ap_execution_module
from ap_executor.domain.ap_instance import ApInstance
from ap_executor.services.executor.execution import ExecutionStatus, OperatorStatus
from ap_executor.services.operator_resolver import OperatorResolver
from ap_executor.services.operator_resolver.manifest import HttpManifestRetriever
from ap_executor.services.operator_resolver.registry.consul_registry import (
    ConsulRegistryClient,
)
from ap_executor.workflows.ap_execution import (
    ap_execution_workflow as _decorated_workflow,
)
from ap_executor.workflows.operator_execution import (
    execute_operator_activity,
    poll_operator_activity,
)

# ``@wfr.workflow`` wraps the orchestrator into a Dapr-internal callable that
# can't be invoked directly outside a running Dapr sidecar (its own __call__
# takes no arguments — Dapr injects ctx/input through its own machinery).
# ``functools.wraps`` keeps the original generator function reachable via
# ``__wrapped__``, which is what actually needs driving here.
ap_execution_workflow = _decorated_workflow.__wrapped__

FIXTURES = Path(__file__).parent.parent / "fixtures"
CONSUL_ADDR = "http://consul:8500"

TEXT_TO_SQL_ID = "1de6e343-6952-4361-a17f-e4a9f1eaeae2"
SQL_PROVENANCE_ID = "68281dc0-9bb6-4caa-8bf8-b7d0054f1729"

TEXT_TO_SQL_MANIFEST = {
    "manifest_version": "0.1.0", "operator": "Text to SQL", "version": "1.0.0",
    "execution": {"mode": "sync", "protocol": "http", "endpoint": "/execute"},
    "inputs": [{"name": "nl", "type": "string", "required": True, "default": ""}],
    "outputs": [{"name": "query", "type": "string", "required": True, "default": ""}],
}

SQL_PROVENANCE_MANIFEST = {
    "manifest_version": "0.1.0", "operator": "SQL Provenance", "version": "1.0.0",
    "execution": {"mode": "sync", "protocol": "http", "endpoint": "/execute"},
    "inputs": [{"name": "sql", "type": "string", "required": True, "default": ""}],
    "outputs": [{"name": "provenance", "type": "string", "required": True, "default": ""}],
}


# --- dummy cluster (Consul + host-dispatched operator apps) -------------------

class _ClusterTransport(httpx.AsyncBaseTransport):
    """Dispatches each request to the dummy app registered under its URL host."""

    def __init__(self, apps: Dict[str, FastAPI]):
        self._transports = {host: httpx.ASGITransport(
            app=app) for host, app in apps.items()}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self._transports[request.url.host].handle_async_request(request)


def _make_consul_app(registry: Dict[str, str]) -> FastAPI:
    """Dummy Consul health API: maps a service slug to a healthy instance host."""
    app = FastAPI()

    @app.get("/v1/health/service/{service_name}")
    async def health(service_name: str) -> list:
        host = registry.get(service_name)
        if host is None:
            return []
        return [{
            "Service": {"ID": f"{service_name}-1", "Service": service_name,
                        "Address": host, "Port": 8080, "Meta": {"version": "1.0.0"}},
            "Node": {"Address": host},
        }]

    return app


# --- test doubles for the two Dapr activities, backed by the dummy cluster ----

def _make_activities(transport: httpx.AsyncBaseTransport):
    """Thin single-arg wrappers around the real activity bodies (via
    ``.__wrapped__``), with their DI seams pointed at the dummy cluster
    transport — matching how ``_FakeWorkflowContext.call_activity`` below
    invokes them (single positional arg, no Dapr ``WorkflowActivityContext``).
    """
    real_execute = execute_operator_activity.__wrapped__
    real_poll = poll_operator_activity.__wrapped__

    async def execute(inp) -> dict:
        return await real_execute(
            None, inp,
            http_client_factory=lambda: httpx.AsyncClient(transport=transport),
            operator_resolver_factory=lambda http: OperatorResolver(
                ConsulRegistryClient(
                    http, CONSUL_ADDR), HttpManifestRetriever(http)
            ),
        )

    async def poll(handle) -> dict:
        return await real_poll(
            None, handle,
            http_client_factory=lambda: httpx.AsyncClient(transport=transport),
        )

    return execute, poll


# --- a fake orchestration context that actually runs the activities -----------

class _FakeWorkflowContext:
    """Minimal DaprWorkflowContext stand-in that resolves activities/timers inline."""
    instance_id = "int-executor-test"

    def call_activity(self, activity, *, input, retry_policy=None):
        return activity(input)

    def create_timer(self, _interval):
        async def _noop():
            return None
        return _noop()


async def _drive(generator):
    """Run an orchestration generator to completion, awaiting whatever it yields."""
    to_send = None
    try:
        while True:
            yielded = generator.send(to_send)
            to_send = await yielded if inspect.isawaitable(yielded) else yielded
    except StopIteration as stop:
        return stop.value


def _load_instance(name: str, parameters: dict) -> ApInstance:
    ap_json = json.loads((FIXTURES / name).read_text())
    return ApInstance.model_validate({"ap": ap_json, "state": parameters})


@pytest.fixture
def patch_activities(monkeypatch):
    """Monkeypatch the orchestrator's activity lookups to the dummy-cluster doubles."""
    def _apply(transport: httpx.AsyncBaseTransport):
        execute_activity, poll_activity = _make_activities(transport)
        monkeypatch.setattr(ap_execution_module,
                            "execute_operator_activity", execute_activity)
        monkeypatch.setattr(ap_execution_module,
                            "poll_operator_activity", poll_activity)

    return _apply


@pytest.mark.asyncio
async def test_ap_instance_two_operator_chain(operator_apps, patch_activities):
    """An AP instance's parameter flows into op1, whose output feeds op2 via the graph mapping."""
    text_to_sql = operator_apps.sync(
        TEXT_TO_SQL_MANIFEST, lambda inp: {"query": f"SELECT -- {inp['nl']}"}
    )
    sql_provenance = operator_apps.sync(
        SQL_PROVENANCE_MANIFEST, lambda inp: {
            "provenance": f"lineage({inp['sql']})"}
    )
    cluster = _ClusterTransport({
        "consul": _make_consul_app({"text-to-sql": "text-to-sql", "sql-provenance": "sql-provenance"}),
        "text-to-sql": text_to_sql,
        "sql-provenance": sql_provenance,
    })
    patch_activities(cluster)

    instance = _load_instance("composed/01_02.json",
                              {TEXT_TO_SQL_ID: {"nl": "show me all users"}})
    result = await _drive(ap_execution_workflow(_FakeWorkflowContext(), instance))

    assert result.status == ExecutionStatus.SUCCESS
    by_id = {op.operator_id: op for op in result.operators}
    assert by_id[TEXT_TO_SQL_ID].status == OperatorStatus.SUCCESS
    assert by_id[TEXT_TO_SQL_ID].result == {
        "query": "SELECT -- show me all users"}
    # op2's `sql` input was wired from op1's `query` output (query -> sql rename).
    assert by_id[SQL_PROVENANCE_ID].result == {
        "provenance": "lineage(SELECT -- show me all users)"}


@pytest.mark.asyncio
async def test_ap_instance_async_operator_polls_through_orchestrator(operator_apps, patch_activities):
    """A single async operator is started and polled to completion by the orchestrator."""
    ap_id = "b1000000-0000-4000-8000-000000000001"
    op_id = "b1000000-0000-4000-8000-000000000002"
    manifest = {
        "manifest_version": "0.1.0", "operator": "Async Job", "version": "1.0.0",
        "execution": {"mode": "async", "protocol": "http",
                      "start_endpoint": "/jobs", "poll_endpoint": "/jobs/{id}"},
        "inputs": [{"name": "payload", "type": "string", "required": True, "default": ""}],
        "outputs": [{"name": "rows", "type": "number", "required": True, "default": 0}],
    }
    async_op = operator_apps.async_(manifest, {"rows": 42}, running_polls=1)
    cluster = _ClusterTransport({
        "consul": _make_consul_app({"async-job": "async-job"}),
        "async-job": async_op,
    })
    patch_activities(cluster)

    ap_json = {
        "nodes": [
            {"id": ap_id, "labels": ["Analytical_Pattern"],
             "properties": {"name": "Async AP", "process": "job", "publishedDate": "2026-01-01"}},
            {"id": op_id, "labels": ["Async_Operator", "Operator"],
             "properties": {"name": "Async Job", "version": "1.0.0", "step": 1,
                            "inputs": [{"name": "payload", "type": "string", "required": True, "default": ""}],
                            "outputs": [{"name": "rows", "type": "number", "required": True, "default": 0}]}},
        ],
        "edges": [{"from": ap_id, "to": op_id, "labels": ["consist_of"]}],
    }
    instance = ApInstance.model_validate(
        {"ap": ap_json, "state": {op_id: {"payload": "go"}}})
    result = await _drive(ap_execution_workflow(_FakeWorkflowContext(), instance))

    assert result.status == ExecutionStatus.SUCCESS
    assert result.operators[0].status == OperatorStatus.SUCCESS
    assert result.operators[0].result == {"rows": 42}
    assert result.operators[0].execution_mode == "async"
