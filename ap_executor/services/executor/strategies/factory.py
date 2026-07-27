from ap_executor.services.operator_resolver.errors import UnsupportedOperatorError
from ap_executor.services.executor.strategies.http_async_polling import (
    HttpAsyncPollingExecutionStrategy,
)
from ap_executor.services.executor.strategies.http_sync import HttpSyncExecutionStrategy
from ap_executor.services.executor.strategies.strategy import OperatorExecutionStrategy


class ExecutionStrategyFactory:
    """Selects an execution strategy based on manifest execution mode/protocol."""

    @staticmethod
    def create(execution_mode: str, execution_protocol: str) -> OperatorExecutionStrategy:
        """Return the execution strategy matching a manifest's declared execution mode/protocol.

        Raises:
            UnsupportedOperatorError: If no strategy is registered for this mode/protocol pair.
        """
        match (execution_mode, execution_protocol):
            case ("sync", "http"):
                return HttpSyncExecutionStrategy()
            case ("async", "http"):
                return HttpAsyncPollingExecutionStrategy()
            case _:
                raise UnsupportedOperatorError(
                    f"execution mode '{execution_mode}/{execution_protocol}'"
                )
