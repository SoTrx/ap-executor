"""Exceptions raised by the magic operator."""
from typing import List


class MagicOperatorConfigError(Exception):
    """
    Raised when the magic operator's environment-variable configuration is
    missing or invalid.
    """

    def __init__(self, detail: str):
        self.message = f"Invalid magic operator configuration: {detail}"
        super().__init__(self.message)


class InputValidationFailed(Exception):
    """
    Raised when an incoming request is missing one or more required inputs
    declared in the operator's own `MAGIC_OPERATOR_INPUTS` config.
    """

    def __init__(self, missing: List[str]):
        self.missing = missing
        self.message = f"missing required input(s): {', '.join(missing)}"
        super().__init__(self.message)
