import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable

from dotenv import load_dotenv
from fastapi import FastAPI
from psycopg import AsyncConnection, OperationalError
from psycopg_pool import AsyncConnectionPool

from ap_executor.errors.exceptions import DatabaseNotFoundError

load_dotenv()
logger = logging.getLogger(__name__)
REDIS_BROKER_URI: str | None = os.getenv("REDIS_BROKER_URI") or None
# True when a Redis broker is configured and async execution is available.
REDIS_ENABLED: bool = REDIS_BROKER_URI is not None
# When True (default), the FastAPI process spawns a Celery worker in a daemon
# thread so no separate worker process is needed. Set to 'false' when running
# dedicated standalone workers (e.g. via Docker) to avoid double-processing.
USE_EMBEDDED_CELERY_WORKER = (
    REDIS_ENABLED
    and os.getenv("USE_EMBEDDED_CELERY_WORKER", "true").lower() == "true"
)


def _start_celery_worker() -> threading.Thread:
    """Start an embedded Celery worker in a daemon thread."""
    from ap_executor.celery_app import celery_app as _celery

    def _run_worker():
        _celery.worker_main(
            ["worker", "--loglevel=info", "--concurrency=2", "-P", "solo"]
        )

    t = threading.Thread(target=_run_worker, daemon=True, name="celery-worker")
    t.start()
    logger.info("Embedded Celery worker started in daemon thread")
    return t


@asynccontextmanager
async def container_lifespan(_: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    Conditionally starts an embedded Celery worker in a daemon thread based on
    the ``USE_EMBEDDED_CELERY_WORKER`` environment variable (default: ``true``).
    Set it to ``false`` when running dedicated standalone workers so the API
    process does not also consume tasks.
    """
    if not REDIS_ENABLED:
        logger.info("Redis not configured – async execution disabled")
    elif USE_EMBEDDED_CELERY_WORKER:
        _start_celery_worker()
    else:
        logger.info(
            "Embedded Celery worker disabled (USE_EMBEDDED_CELERY_WORKER=false)")
    yield
    if USE_EMBEDDED_CELERY_WORKER:
        logger.info("Shutting down embedded Celery worker")


@asynccontextmanager
async def get_dynamic_db_conn(connection_string: str) -> AsyncGenerator[AsyncConnection, None]:
    """
    Validates the connection string by opening a direct connection first, then creates a
    temporary database connection pool, yields a connection, and closes the pool afterwards.
    This ensures the connection pool is cleaned up after AP processing completes.

    Raises OperationalError immediately if the database does not exist, rather than
    letting the pool silently retry in the background.

    Args:
        connection_string: PostgreSQL connection string from AP
    """
    # Validate eagerly: raises OperationalError immediately if the DB doesn't exist
    check_conn = await AsyncConnection.connect(connection_string)
    await check_conn.close()

    pool = AsyncConnectionPool(
        conninfo=connection_string,
        min_size=1,
        max_size=5,
        open=False,
    )

    try:
        await pool.open()
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            yield conn
    finally:
        await pool.close()


def get_db_connection_for_ap(db_name: str) -> Callable[[], AsyncGenerator[AsyncConnection, None]]:
    """
    Factory function that returns a dependency providing an async database connection
    for the given ``db_name``.  Tries the primary PostgreSQL instance first, then falls
    back to the Timescale/secondary instance.

    Args:
        db_name: Database name extracted from the AP

    Returns:
        An async generator factory yielding an ``AsyncConnection``

    Raises:
        DatabaseNotFoundError: If the database doesn't exist on either instance
    """

    async def _provide_connection() -> AsyncGenerator[AsyncConnection, None]:
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        postgres_host = os.getenv("POSTGRES_HOST")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        timescale_host = os.getenv("POSTGRES_TIMESCALE_HOST")
        timescale_port = os.getenv("POSTGRES_TIMESCALE_PORT", "5433")

        if not all([user, password, postgres_host]):
            raise ValueError(
                "Missing required environment variables: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST"
            )

        # Try Postgres instance first
        postgres_connection_string = f"postgresql://{user}:{password}@{postgres_host}:{postgres_port}/{db_name}"
        try:
            async with get_dynamic_db_conn(postgres_connection_string) as conn:
                yield conn
                return
        except OperationalError:
            pass

        # Try Timescale instance
        if timescale_host:
            timescale_connection_string = f"postgresql://{user}:{password}@{timescale_host}:{timescale_port}/{db_name}"
            try:
                async with get_dynamic_db_conn(timescale_connection_string) as conn:
                    yield conn
                    return
            except OperationalError:
                pass

        raise DatabaseNotFoundError(db_name)

    return _provide_connection
