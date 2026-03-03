# AP Executor API

[![License](https://img.shields.io/github/license/datagems-eosc/ap-executor)](https://img.shields.io/github/license/datagems-eosc/ap-executor)

This is the documentation site for the AP Executor service. The service provides a RESTful API for executing the operators defined in an **Analytical Pattern (AP)** against PostgreSQL databases.

## What is the AP Executor?

The AP Executor takes an Analytical Pattern as input, parses the operator graph, resolves the execution order, and runs each operator step by step against the target database.

It supports:
- **Synchronous execution** (`POST /execute`) — runs all operators and returns the full result inline
- **Asynchronous execution** (`POST /execute/async`) — dispatches to a Celery worker and returns a `task_id` for polling

## Quick Links

- [Configuration](configuration.md) - How to configure the service
- [Architecture](architecture.md) - Technical architecture details
- [Usage](usage.md) - API usage guide

## Working with Analytical Patterns (AP)

The service processes **Analytical Patterns (AP)** in PG-JSON format — a graph structure with nodes and edges representing database operations.

### Example AP Structure

```json
{
  "nodes": [
    {
      "id": "db-node-id",
      "labels": ["Relational_Database"],
      "properties": {
        "contentUrl": "postgresql://user:pass@host/db",
        "name": "mydb"
      }
    },
    {
      "id": "table-node-id",
      "labels": ["Table"],
      "properties": {"name": "public.students"}
    },
    {
      "id": "query-node-id",
      "labels": ["Operator", "SQL_Operator"],
      "properties": {
        "name": "Select students",
        "query": "SELECT name FROM students WHERE grade > 80"
      }
    }
  ],
  "edges": [
    {"from": "query-node-id", "to": "table-node-id", "labels": ["input"]},
    {"from": "table-node-id", "to": "db-node-id", "labels": ["contain"]}
  ]
}
```

### Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/execute` | Execute an AP synchronously |
| `POST` | `/api/v1/execute/async` | Execute an AP asynchronously (returns `task_id`) |
| `GET`  | `/api/v1/execute/async/{task_id}` | Poll for async execution result |
| `GET`  | `/api/v1/health` | Liveness check |
| `GET`  | `/api/v1/ready` | Readiness check (DB + Redis) |

## Getting Started

The best solution is to use the provided `.devcontainer` configuration. PostgreSQL and Redis are pre-configured.

```bash
# Requirements: Python 3.14, uv
uv sync --all-groups
cp .env.example .env
# Fill in the required variables in .env
uv run ap_executor/main.py
```

The API will be available at `http://localhost:5000/api/v1`

### Interactive Documentation

- **Swagger UI**: http://localhost:5000/docs
- **ReDoc**: http://localhost:5000/redoc
