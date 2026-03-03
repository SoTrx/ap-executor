"""
Celery task that executes an Analytical Pattern asynchronously.
"""
import asyncio
import logging
from typing import List, Optional

from ap_executor.celery_app import celery_app
from ap_executor.di import get_db_connection_for_ap
from ap_executor.models.pg_json import PgJson
from ap_executor.services.executor import ExecutorService

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async coroutine from a synchronous (Celery) context."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        pass
    return asyncio.run(coro)


async def _execute_ap_async(
    ap_dict: dict,
    db_name: str,
    schema_name: str,
    ap_name: Optional[str] = None,
) -> dict:
    """Core async logic shared by the Celery task."""
    ap = PgJson.model_validate(ap_dict)
    connection_factory = get_db_connection_for_ap(db_name)

    async for conn in connection_factory():
        service = ExecutorService(conn)
        result = await service.execute_ap(ap, db_name, schema_name, ap_name)
        return result.model_dump(mode="json")

    return {"status": "error", "error": "No database connection could be established"}


@celery_app.task(bind=True, name="ap_executor.tasks.execute.execute_ap_task")
def execute_ap_task(
    self,
    ap_dict: dict,
    db_name: str,
    schema_name: str,
    ap_name: Optional[str] = None,
) -> dict:
    """Celery task that executes an AP.

    Args:
        ap_dict: Serialised PgJson AP.
        db_name: Target database name.
        schema_name: Target schema.
        ap_name: Optional human-readable AP name.

    Returns:
        Serialised ``ExecutionResult``.
    """
    logger.info("Celery task: executing AP '%s' on db '%s'", ap_name, db_name)
    return _run_async(_execute_ap_async(ap_dict, db_name, schema_name, ap_name))
