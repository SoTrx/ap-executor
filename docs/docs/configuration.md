# Configuration

This document describes how to configure the AP Executor service for different environments.

## Environment Variables

The service requires the following environment variables to connect to PostgreSQL databases:

### Required Variables

- `POSTGRES_USER`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password  
- `POSTGRES_HOST`: Hostname or IP address of the primary PostgreSQL server

### Optional Variables

- `POSTGRES_PORT`: Port for the primary PostgreSQL server (default: `5432`)
- `POSTGRES_TIMESCALE_HOST`: Hostname for the Timescale/secondary PostgreSQL server
- `POSTGRES_TIMESCALE_PORT`: Port for the Timescale server (default: `5433`)
- `ROOT_PATH`: Root path for the API when behind a reverse proxy (default: `""`)
- `REDIS_BROKER_URI`: Redis URL for Celery broker and result backend (default: `redis://redis:6379/0`)
- `USE_EMBEDDED_CELERY_WORKER`: Start a Celery worker inside the FastAPI process (default: `true`)

### Database Connection Behavior

The service supports a dual-database architecture:

1. **Primary Connection**: The service first attempts to connect to the database specified in the Analytical Pattern on the primary PostgreSQL server (`POSTGRES_HOST:POSTGRES_PORT`)
2. **Fallback Connection**: If the database doesn't exist on the primary server, the service automatically falls back to the Timescale server (`POSTGRES_TIMESCALE_HOST:POSTGRES_TIMESCALE_PORT`)
3. **Error Handling**: If the database doesn't exist on either server, a `DatabaseNotFoundError` is raised

## Testing Configuration

Tests automatically use testcontainers to spin up a PostgreSQL instance. No manual configuration needed for running tests:

```bash
pytest tests/
```
