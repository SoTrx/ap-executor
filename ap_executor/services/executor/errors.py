"""Exceptions raised by the executor service."""


class OperatorExecutionError(Exception):
    """
    Raised when an operator fails during execution.
    """

    def __init__(self, operator_name: str, detail: str = ""):
        self.operator_name = operator_name
        self.message = f"Operator '{operator_name}' failed: {detail}" if detail else f"Operator '{operator_name}' failed"
        super().__init__(self.message)
