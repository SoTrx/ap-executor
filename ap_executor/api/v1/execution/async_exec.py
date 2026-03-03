"""
POST /execute/async – asynchronous AP execution via Celery.

Dispatches the AP execution to a Celery worker and returns a ``task_id``
immediately so the caller can poll for results.
"""
from logging import getLogger
from typing import Any, Optional

from fastapi import Response, status
from pydantic import BaseModel

from ap_executor.api.v1.dependencies.ap_parser import (
    ApName,
    DatabaseName,
    SchemaName,
)
from ap_executor.di import REDIS_ENABLED
from ap_executor.types.pg_json import PgJson

logger = getLogger(__name__)


class AsyncExecutionTaskResponse(BaseModel):
    """Response returned when an async execution is dispatched."""
    task_id: str
    status: str = "pending"


class TaskStatusResponse(BaseModel):
    """Response returned when polling for a task result."""
    task_id: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None


def execute_ap_async(
    ap: PgJson,
    db_name: DatabaseName,
    schema_name: SchemaName,
    ap_name: ApName,
    response: Response,
) -> AsyncExecutionTaskResponse:
    """Dispatch AP execution to a Celery worker.

    Returns HTTP 202 with a ``task_id`` that can be polled via
    ``GET /execute/async/{task_id}``.
    """
    if not REDIS_ENABLED:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Async execution is not available: REDIS_BROKER_URI is not configured.",
        )

    from ap_executor.tasks.execute import execute_ap_task

    logger.info("Async execution dispatched for AP '%s' on db '%s'",
                ap_name, db_name)

    task = execute_ap_task.delay(
        ap_dict=ap.model_dump(mode="json", by_alias=True),
        db_name=db_name,
        schema_name=schema_name,
        ap_name=ap_name,
    )

    response.status_code = status.HTTP_202_ACCEPTED
    return AsyncExecutionTaskResponse(task_id=task.id, status="pending")


def get_task_status(task_id: str) -> TaskStatusResponse:
    """Poll the status / result of a previously dispatched execution task."""
    if not REDIS_ENABLED:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Async execution is not available: REDIS_BROKER_URI is not configured.",
        )

    from ap_executor.celery_app import celery_app

    result = celery_app.AsyncResult(task_id)

    if result.state == "PENDING":
        return TaskStatusResponse(task_id=task_id, status="pending")
    elif result.state == "STARTED":
        return TaskStatusResponse(task_id=task_id, status="running")
    elif result.state == "SUCCESS":
        return TaskStatusResponse(task_id=task_id, status="success", result=result.result)
    elif result.state == "FAILURE":
        return TaskStatusResponse(task_id=task_id, status="error", error=str(result.result))
    else:
        return TaskStatusResponse(task_id=task_id, status=result.state.lower())
