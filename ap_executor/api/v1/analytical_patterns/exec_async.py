"""
POST /execute/async – asynchronous AP execution via a Dapr workflow.

Schedules a Dapr workflow instance and returns its ``instance_id`` (as
``task_id``) immediately so the caller can poll for results.
"""
from json import loads
from logging import getLogger
from typing import Annotated, Any, Optional

from dapr.ext.workflow import WorkflowStatus
from dapr.ext.workflow.aio import DaprWorkflowClient
from fastapi import Depends, Response, status
from pydantic import BaseModel

from ap_executor.di import get_workflow_client
from ap_executor.domain.ap_instance import ApInstance
from ap_executor.workflows.ap_execution import ap_execution_workflow

logger = getLogger(__name__)

WorkflowClientDep = Annotated[DaprWorkflowClient, Depends(get_workflow_client)]

_STATUS_MAP = {
    WorkflowStatus.PENDING: "pending",
    WorkflowStatus.RUNNING: "running",
    WorkflowStatus.COMPLETED: "success",
    WorkflowStatus.FAILED: "error",
    WorkflowStatus.TERMINATED: "error",
    WorkflowStatus.SUSPENDED: "suspended",
    WorkflowStatus.STALLED: "stalled",
    WorkflowStatus.UNKNOWN: "unknown",
}


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


async def execute_ap_async(
    instance: ApInstance,
    client: WorkflowClientDep,
    response: Response,
) -> AsyncExecutionTaskResponse:
    """Schedule AP instance execution as a Dapr workflow instance.

    Returns HTTP 202 with a ``task_id`` (the workflow instance id) that can
    be polled via ``GET /execute/async/{task_id}``.
    """
    logger.info("Async execution dispatched")

    instance_id = await client.schedule_new_workflow(workflow=ap_execution_workflow, input=instance)

    response.status_code = status.HTTP_202_ACCEPTED
    return AsyncExecutionTaskResponse(task_id=instance_id, status="pending")


async def get_task_status(task_id: str, client: WorkflowClientDep) -> TaskStatusResponse:
    """Poll the status / result of a previously dispatched execution's workflow instance."""

    state = await client.get_workflow_state(task_id)
    if state is None:
        return TaskStatusResponse(task_id=task_id, status="not_found")

    task_status = _STATUS_MAP.get(state.runtime_status, "unknown")
    match state.runtime_status:
        case WorkflowStatus.COMPLETED:
            result = None
            if state.serialized_output:
                result = loads(state.serialized_output)
            return TaskStatusResponse(task_id=task_id, status=task_status, result=result)
        case WorkflowStatus.FAILED | WorkflowStatus.TERMINATED:
            error = state.failure_details.message if state.failure_details else "Execution failed"
            return TaskStatusResponse(task_id=task_id, status=task_status, error=error)
        case _:
            return TaskStatusResponse(task_id=task_id, status=task_status)
