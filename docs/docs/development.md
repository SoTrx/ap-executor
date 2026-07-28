# Development Guide

## Setup

### With Dev Container (Recommended)

Open in VS Code with the Dev Containers extension. Consul and a Dapr sidecar are pre-configured.

### Local Setup

Requirements: Python 3.14, `uv` package manager

```bash
# Install dependencies
uv sync --all-groups

# Configure environment
cp .env.example .env
# Edit .env if you need to override any defaults

# Run service
uv run ap_executor/main.py
```

---

## Running Tests

```bash
pytest tests/
```

Tests run entirely against mocks (`httpx.MockTransport` for Consul/manifest/operator HTTP calls, a fake Dapr workflow client for the API layer) — no external services needed.

### End-to-end tests

`tests/e2e/` exercises the full stack for real — a real Consul instance, real Dapr Workflow infrastructure, and real HTTP calls to `ap_executor` and three self-sufficient `magic_operator` instances, each paired with a plain Consul client agent (official `hashicorp/consul` image, no custom registration code — see `docs/docs/operators.md`), all brought up via `e2e/docker-compose.yml`. These tests are marked `@pytest.mark.e2e` and are **excluded from the default `pytest tests/` run** (`uv run pytest -m "not e2e"`, as CI's `tests.yml` job does) — they need Docker and take much longer than the rest of the suite.

To run them locally:

```bash
docker compose -f e2e/docker-compose.yml up -d --build
uv run python e2e/wait_for_e2e_stack.py
uv run pytest tests/e2e -m e2e
docker compose -f e2e/docker-compose.yml down -v
```

They run in a separate CI job (`.github/workflows/e2e-tests.yml`) alongside the fast `tests.yml` job.

---

## Project Structure

```
ap_executor/
├── api/v1/
│   ├── analytical_patterns/
│   │   ├── routes.py       # wires /aps/execute, /aps/execute/async, /aps/execute/async/{task_id}
│   │   ├── exec_sync.py    # POST /execute (blocks on workflow completion)
│   │   └── exec_async.py   # POST /execute/async + GET /execute/async/{task_id}
│   ├── health.py           # /health, /ready endpoints
│   └── routes.py           # aggregates the above under /api/v1
├── domain/
│   └── ap_instance.py      # ApInstance – iter_operators() (topo order), resolve_operator_input_values()
├── services/
│   ├── operator_resolver/
│   │   ├── operator_resolver.py   # OperatorResolver – registry + manifest fetch -> ResolvedOperator
│   │   ├── resolved_operator.py   # ResolvedOperator dataclass
│   │   ├── factory.py             # default_operator_resolver() – shared prod construction (di.py + operator_execution.py)
│   │   ├── errors.py              # UnsupportedOperatorError
│   │   ├── manifest/              # OperatorManifest schema, HttpManifestRetriever
│   │   └── registry/              # OperatorRegistry protocol, ConsulRegistryClient, ServiceInstance
│   └── executor/
│       ├── execution.py           # OperatorInvocationInput, OperatorResult, ExecutionResult, statuses
│       ├── execution_handle.py    # ExecutionHandle – in-flight/completed operator invocation state
│       ├── errors.py              # OperatorExecutionError
│       └── strategies/            # OperatorExecutionStrategy protocol, http_sync.py, http_async_polling.py, factory.py
├── workflows/
│   ├── ap_execution.py       # Dapr workflow orchestrator (must stay I/O-free)
│   ├── operator_execution.py # Dapr activities – all I/O happens here
│   └── runtime.py            # module-level WorkflowRuntime instance (wfr) + dapr-ext-workflow alias patch
├── di.py                   # FastAPI lifespan – starts the Dapr WorkflowRuntime, exposes DaprWorkflowClient + OperatorResolver
└── main.py                 # FastAPI application entry point
```

See [Architecture](architecture.md) for how these pieces interact, and [Registering an Operator](operators.md) for the operator-side contract (manifest + Consul registration).
