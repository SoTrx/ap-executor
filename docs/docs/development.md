# Development Guide

## Setup

### With Dev Container (Recommended)

Open in VS Code with the Dev Containers extension. PostgreSQL and Redis are pre-configured.

### Local Setup

Requirements: Python 3.14, `uv` package manager, PostgreSQL

```bash
# Install dependencies
uv sync --all-groups

# Configure environment
cp .env.example .env
# Edit .env with your database connection details

# Run service
uv run ap_executor/main.py
```

---

## Running Tests

```bash
pytest tests/
```

Tests use `testcontainers` to spin up a PostgreSQL instance automatically — no manual setup needed.

---

## Running a standalone Celery worker

For scale-out or to run the worker separately from the API:

```bash
docker run --rm \
  --env-file .env \
  ap-executor:prod \
  uv run celery -A ap_executor.celery_app:celery_app worker --loglevel=info
```

---

## Project Structure

```
ap_executor/
├── api/v1/              # API endpoints
│   ├── execution/
│   │   ├── sync.py      # POST /execute (synchronous)
│   │   └── async_exec.py # POST /execute/async (asynchronous)
│   ├── dependencies/    # FastAPI dependencies (AP parser)
│   └── health.py        # Health check endpoints
├── tasks/               # Celery tasks
├── services/            # Business logic (executor service)
├── types/               # Type definitions (PG-JSON, execution results)
├── errors/              # Custom exceptions
├── di.py                # Dependency injection & DB connection
├── celery_app.py        # Celery application
└── main.py              # FastAPI application entry point
```
