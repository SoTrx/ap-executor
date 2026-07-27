"""Exceptions raised by the operator resolver service."""


class UnsupportedOperatorError(Exception):
    """
    Raised when the AP contains an operator type that is not supported.
    """

    def __init__(self, operator_label: str):
        self.message = f"Unsupported operator type: '{operator_label}'"
        super().__init__(self.message)
