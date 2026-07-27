"""
POST /execute – synchronous AP execution.

Schedules a Dapr workflow instance and blocks until it completes (up to
``SYNC_EXECUTION_TIMEOUT_SECONDS``), returning the full ``ExecutionResult``
inline. For executions that may run long, use ``POST /execute/async``
instead.
"""
import os
from logging import getLogger
from typing import Annotated

from dapr.ext.workflow import WorkflowStatus
from dapr.ext.workflow.aio import DaprWorkflowClient
from fastapi import Depends, HTTPException, status

from ap_executor.di import get_workflow_client
from ap_executor.domain.ap_instance import ApInstance
from ap_executor.services.executor.execution import ExecutionResult
from ap_executor.workflows.ap_execution import ap_execution_workflow

logger = getLogger(__name__)

SYNC_EXECUTION_TIMEOUT_SECONDS = int(
    os.getenv("SYNC_EXECUTION_TIMEOUT_SECONDS", "60"))

WorkflowClientDep = Annotated[DaprWorkflowClient, Depends(get_workflow_client)]


async def execute_ap_sync(
    instance: ApInstance,
    client: WorkflowClientDep,
) -> ExecutionResult:
    """Execute the AP instance synchronously and return the result immediately.

    The AP is scheduled as a Dapr workflow instance; each operator becomes
    one activity episode, resolved via the Consul registry and invoked
    through its manifest-declared adapter with the instance's parameters.

    Returns:
        ``ExecutionResult`` with per-operator outcomes.
    """
    logger.info("Synchronous execution requested")

    instance_id = await client.schedule_new_workflow(workflow=ap_execution_workflow, input=instance)

    try:
        state = await client.wait_for_workflow_completion(
            instance_id, timeout_in_seconds=SYNC_EXECUTION_TIMEOUT_SECONDS
        )
    except TimeoutError:
        raise HTTPException(
            status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Execution still running; poll GET /api/v1/execute/async/{instance_id}",
        )

    if state is None or state.runtime_status != WorkflowStatus.COMPLETED:
        detail = "Execution failed"
        if state is not None and state.failure_details is not None:
            detail = state.failure_details.message
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail)

    return ExecutionResult.model_validate_json(state.serialized_output)
