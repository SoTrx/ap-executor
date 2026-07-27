"""Dapr Workflow activities – the only place operator discovery/invocation I/O happens.

Orchestrator functions must be deterministic (no direct I/O), so the Consul
lookup, manifest fetch, and the operator's own HTTP call all live here.
Doing the registry lookup fresh on every activity attempt also means a
Dapr-driven retry re-resolves the operator via Consul, self-healing if the
previously-picked instance died mid-attempt.
"""
from typing import Any, Callable, Dict

import httpx
from dapr.ext.workflow import WorkflowActivityContext

from ap_executor.services.executor.errors import OperatorExecutionError
from ap_executor.services.executor.execution import OperatorInvocationInput
from ap_executor.services.executor.execution_handle import ExecutionHandle
from ap_executor.services.executor.strategies import OperatorExecutionStrategy
from ap_executor.services.executor.strategies.factory import ExecutionStrategyFactory
from ap_executor.services.operator_resolver import OperatorResolver
from ap_executor.services.operator_resolver.factory import default_operator_resolver
from ap_executor.services.operator_resolver.manifest import OperatorManifest
from ap_executor.workflows.runtime import wfr


def _filter_inputs(raw_inputs: Dict[str, Any], manifest: OperatorManifest) -> Dict[str, Any]:
    """Keep only the keys the manifest declares, applying defaults / raising on missing-required."""
    filtered: Dict[str, Any] = {}
    for spec in manifest.inputs:
        if raw_inputs.get(spec.name) is not None:
            filtered[spec.name] = raw_inputs[spec.name]
        elif spec.required and spec.default is None:
            raise OperatorExecutionError(
                manifest.operator, f"missing required input '{spec.name}'")
        else:
            filtered[spec.name] = spec.default
    return filtered


@wfr.activity(name="execute_operator")
async def execute_operator_activity(
    ctx: WorkflowActivityContext,
    inp: OperatorInvocationInput,
    *,
    http_client_factory: Callable[[], httpx.AsyncClient] = httpx.AsyncClient,
    operator_resolver_factory: Callable[
        [httpx.AsyncClient], OperatorResolver] = default_operator_resolver,
    execution_strategy_factory: Callable[[
        str, str], OperatorExecutionStrategy] = ExecutionStrategyFactory.create,
) -> dict:
    """
    Resolve an operator via Consul, fetch its manifest, and start its invocation.
    Parameters:
        ctx: Dapr workflow activity context (unused)
        inp: OperatorInvocationInput containing operator name, version, and inputs
        http_client_factory: Factory for an httpx.AsyncClient (default: httpx.AsyncClient)
        operator_resolver_factory: Factory for an OperatorResolver (default: Consul + HTTP manifest fetch)
        execution_strategy_factory: Factory for an OperatorExecutionStrategy (default: ExecutionStrategyFactory.create)
    Returns:
        ExecutionHandle as a dict, to be passed back to the orchestrator for polling
    """
    async with http_client_factory() as http:
        # First, find out if the operator is registered and healthy, and fetch its manifest
        resolved = await (
            operator_resolver_factory(http)
            .resolve(inp.operator_name, inp.operator_version)
        )

        # If healthy instance are found, filter the inputs to only those the manifest declares
        # and find the way the operator wants to be invoked (execution mode + protocol)
        payload = _filter_inputs(inp.inputs, resolved.manifest)
        execution_strategy = execution_strategy_factory(
            resolved.manifest.execution.mode,
            resolved.manifest.execution.protocol
        )

        # Start the operator invocation and return an execution handle to the orchestrator
        handle = await execution_strategy.start(http, resolved.instance, resolved.manifest, payload)
        handle.execution_mode = resolved.manifest.execution.mode
        handle.execution_protocol = resolved.manifest.execution.protocol
        handle.service_instance = f"{resolved.instance.address}:{resolved.instance.port}"

        return handle.model_dump(mode="json")


@wfr.activity(name="poll_operator")
async def poll_operator_activity(
    ctx: WorkflowActivityContext,
    handle: ExecutionHandle,
    *,
    http_client_factory: Callable[[], httpx.AsyncClient] = httpx.AsyncClient,
    execution_strategy_factory: Callable[[
        str, str], OperatorExecutionStrategy] = ExecutionStrategyFactory.create,
) -> dict:
    """
    Check the status of a previously started async operator invocation.
    Parameters:
        ctx: Dapr workflow activity context (unused)
        handle: ExecutionHandle containing the operator invocation details
        http_client_factory: Factory for an httpx.AsyncClient (default: httpx.AsyncClient)
        execution_strategy_factory: Factory for an OperatorExecutionStrategy (default: ExecutionStrategyFactory.create)
    Returns:
        Polling result as a dict, to be passed back to the orchestrator
    """

    async with http_client_factory() as http:
        # Use the execution mode + protocol from the handle to get the right
        # strategy to poll the operator
        strategy = execution_strategy_factory(
            handle.execution_mode,
            handle.execution_protocol
        )
        result = await strategy.poll(http, handle)

        return result.model_dump(mode="json")
