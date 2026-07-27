"""The AP execution orchestrator – a thin Dapr entrypoint delegating to the executor service.

Determinism note: this module must never perform I/O directly; the actual
per-operator execution logic lives in ``ap_executor.services.executor``.
"""
from datetime import timedelta
from logging import getLogger
from typing import List

from dapr.ext.workflow import DaprWorkflowContext, RetryPolicy

from ap_executor.domain.ap_instance import ApInstance
from ap_executor.services.executor.execution import (
    ExecutionResult,
    ExecutionStatus,
    OperatorInvocationInput,
    OperatorResult,
    OperatorStatus,
)
from ap_executor.services.executor.execution_handle import ExecutionHandle
from ap_executor.workflows.operator_execution import (
    execute_operator_activity,
    poll_operator_activity,
)
from ap_executor.workflows.runtime import wfr

DEFAULT_RETRY_POLICY = RetryPolicy(
    first_retry_interval=timedelta(seconds=2),
    max_number_of_attempts=3,
    backoff_coefficient=2,
)
POLL_INTERVAL = timedelta(seconds=5)

logger = getLogger(__name__)


@wfr.workflow(name="ap_execution_workflow")
def ap_execution_workflow(ctx: DaprWorkflowContext, instance: ApInstance):
    """Orchestrates one AP instance execution by delegating to the configured executor."""

    ap_id = str(instance.ap.root.id)
    logger.info("AP with ID %s execution workflow started", ap_id)

    operators: List[OperatorResult] = []

    try:
        # For each operator (in execution order)...
        for operator in instance.iter_operators():
            op_id = str(operator.id)
            op_name = (operator.properties or {}).get("name", op_id)
            op_inputs = instance.resolve_operator_input_values(op_id)

            activity_input = OperatorInvocationInput(
                operator_id=op_id,
                operator_name=op_name,
                operator_version=(operator.properties or {}).get("version"),
                operator_labels=operator.labels,
                inputs=op_inputs
            )

            # Start the operator execution...
            raw = yield ctx.call_activity(
                execute_operator_activity,
                input=activity_input,
                retry_policy=DEFAULT_RETRY_POLICY
            )
            handle = ExecutionHandle.model_validate(raw)

            # And poll until it completes (or fails)
            while not handle.done:
                yield ctx.create_timer(POLL_INTERVAL)
                raw = yield ctx.call_activity(
                    poll_operator_activity,
                    input=handle,
                    retry_policy=DEFAULT_RETRY_POLICY
                )
                handle = ExecutionHandle.model_validate(raw)

            # Once the operator is done, add its output in the state
            # so it can propagate to any downstream operators
            if isinstance(handle.output, dict):
                instance.state.parameters[str(operator.id)] = handle.output

            operators.append(
                OperatorResult(
                    operator_id=str(operator.id),
                    operator_name=op_name,
                    operator_labels=operator.labels,
                    operator_version=(
                        operator.properties or {}).get("version"),
                    status=OperatorStatus.SUCCESS if handle.success else OperatorStatus.ERROR,
                    result=handle.output,
                    error=handle.error,
                    service_instance=handle.service_instance,
                    execution_mode=handle.execution_mode,
                )
            )
    except Exception as e:
        operators.append(OperatorResult(
            operator_id=str(operator.id),
            operator_name=op_name,
            operator_labels=operator.labels,
            operator_version=activity_input.operator_version,
            status=OperatorStatus.ERROR,
            error=str(e),
        ))
    return ExecutionResult(
        status=_aggregate_status(operators),
        operators=operators,
        workflow_instance_id=ctx.instance_id,
    )


def _aggregate_status(operators: List[OperatorResult]) -> ExecutionStatus:
    if not operators:
        return ExecutionStatus.SUCCESS
    if all(o.status == OperatorStatus.ERROR for o in operators):
        return ExecutionStatus.ERROR
    if any(o.status == OperatorStatus.ERROR for o in operators):
        return ExecutionStatus.PARTIAL_SUCCESS
    return ExecutionStatus.SUCCESS
