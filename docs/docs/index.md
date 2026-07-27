# AP Executor API

[![License](https://img.shields.io/github/license/datagems-eosc/ap-executor)](https://img.shields.io/github/license/datagems-eosc/ap-executor)

This is the documentation site for the AP Executor service. The service provides a RESTful API for orchestrating execution of the operators defined in an **Analytical Pattern (AP)** graph, dispatching each operator to a Consul-discovered operator microservice via Dapr Workflow.

## What is the AP Executor?

The AP Executor takes an Analytical Pattern as input, parses the operator graph, resolves the execution order, and runs each operator step by step against its own externally-deployed implementation.

It supports:
- **Synchronous execution** (`POST /aps/execute`) — schedules a Dapr workflow instance and blocks until it completes (up to `SYNC_EXECUTION_TIMEOUT_SECONDS`), returning the full result inline
- **Asynchronous execution** (`POST /aps/execute/async`) — schedules the same workflow and returns its instance id as `task_id` for polling

## Quick Links

- [Configuration](configuration.md) - How to configure the service
- [Architecture](architecture.md) - Technical architecture details
- [Registering an Operator](operators.md) - How operator execution works and how to register a new operator
- [Usage](usage.md) - API usage guide

## Working with Analytical Patterns (AP)

The service processes **Analytical Patterns (AP)** in PG-JSON format — a graph structure with nodes and edges. One `Analytical_Pattern` root node is connected via `consist_of` edges to one or more `Operator` nodes; `follows` edges between operators express ordering.

### Example AP Structure

```json
{
  "nodes": [
    {
      "id": "0a79a9c7-76f3-4f96-be42-e6818793f182",
      "labels": ["Analytical_Pattern"],
      "properties": {
        "name": "Text to SQL AP",
        "description": "Takes a natural-language query and translates it into SQL.",
        "process": "query"
      }
    },
    {
      "id": "1de6e343-6952-4361-a17f-e4a9f1eaeae2",
      "labels": ["Text_To_SQL_Operator", "Operator"],
      "properties": {
        "name": "Text to SQL",
        "version": "1.0.0",
        "inputs": [{"name": "nl", "type": "string", "required": true}],
        "outputs": [{"name": "query", "type": "string", "required": true}]
      }
    }
  ],
  "edges": [
    {"from": "0a79a9c7-76f3-4f96-be42-e6818793f182", "to": "1de6e343-6952-4361-a17f-e4a9f1eaeae2", "labels": ["consist_of"]}
  ]
}
```

This is a trimmed-down version of [`fixtures/01_ap_nl_to_sql.json`](https://github.com/datagems-eosc/ap-executor/blob/master/fixtures/01_ap_nl_to_sql.json) — see the `fixtures/` directory for more complete, runnable examples, including multi-operator chains.

### Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/aps/execute` | Execute an AP synchronously |
| `POST` | `/api/v1/aps/execute/async` | Execute an AP asynchronously (returns `task_id`) |
| `GET`  | `/api/v1/aps/execute/async/{task_id}` | Poll for async execution result |
| `GET`  | `/api/v1/health` | Liveness check |
| `GET`  | `/api/v1/ready` | Readiness check (Consul + Dapr sidecar) |

## Getting Started

The best solution is to use the provided `.devcontainer` configuration. Consul and a Dapr sidecar are pre-configured.

```bash
# Requirements: Python 3.14, uv
uv sync --all-groups
cp .env.example .env
# Fill in any variables you need to override in .env
uv run ap_executor/main.py
```

The API will be available at `http://localhost:5000/api/v1`

### Interactive Documentation

- **Swagger UI**: http://localhost:5000/docs
- **ReDoc**: http://localhost:5000/redoc
