"""Custom exception classes for the AP Executor."""


class DatabaseNotFoundError(Exception):
    """
    Raised when a database cannot be found on either
    the Postgres or Timescale instance.
    """

    def __init__(self, db_name: str):
        self.message = f"Database '{db_name}' not found on either Postgres or Timescale instance"
        super().__init__(self.message)


class OperatorExecutionError(Exception):
    """
    Raised when an operator fails during execution.
    """

    def __init__(self, operator_name: str, detail: str = ""):
        self.operator_name = operator_name
        self.message = f"Operator '{operator_name}' failed: {detail}" if detail else f"Operator '{operator_name}' failed"
        super().__init__(self.message)


class UnsupportedOperatorError(Exception):
    """
    Raised when the AP contains an operator type that is not supported.
    """

    def __init__(self, operator_label: str):
        self.message = f"Unsupported operator type: '{operator_label}'"
        super().__init__(self.message)
