"""Execution model types for the AP Executor."""
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class OperatorInvocationInput(BaseModel):
    """Everything a single operator activity needs to resolve and invoke an operator."""
    operator_id: str
    operator_name: str
    operator_version: Optional[str] = None
    operator_labels: List[str]
    inputs: Dict[str, Any]


class OperatorStatus(str, Enum):
    """Status of a single operator execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"


class OperatorResult(BaseModel):
    """Result of executing a single AP operator."""
    operator_id: str
    operator_name: str
    operator_labels: List[str]
    operator_version: Optional[str] = None
    status: OperatorStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    service_instance: Optional[str] = None
    execution_mode: Optional[str] = None


class ExecutionStatus(str, Enum):
    """Overall status of the AP execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    ERROR = "error"


class ExecutionResult(BaseModel):
    """Full result of executing an Analytical Pattern."""
    status: ExecutionStatus
    operators: List[OperatorResult] = []
    error: Optional[str] = None
    workflow_instance_id: Optional[str] = None

    @property
    def summary(self) -> Dict[str, int]:
        """Return a count of operator statuses."""
        counts: Dict[str, int] = {}
        for op in self.operators:
            counts[op.status.value] = counts.get(op.status.value, 0) + 1
        return counts
