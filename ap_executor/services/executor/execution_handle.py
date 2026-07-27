"""Execution-handle model for tracking a single operator invocation."""
from typing import Any, Dict, Optional

from pydantic import BaseModel


class ExecutionHandle(BaseModel):
    """Tracks the in-flight/completed state of a single operator invocation."""
    # Wether the operator invocation has completed (successfully or not)
    done: bool
    # Whether the operator invocation completed successfully (if done)
    success: bool = False
    # The operator's output (if done and successful)
    output: Optional[Dict[str, Any]] = None
    # The operator's error message (if done and failed)
    error: Optional[str] = None

    # The operator's poll endpoint (if the operator is async). Ex: "/poll"
    poll_endpoint: Optional[str] = None
    # The operator's execution mode (if known, "sync" or "async")
    execution_mode: Optional[str] = None
    # The operator's execution protocol (if known, "http" or "grpc")
    execution_protocol: Optional[str] = None
    # The operator's service instance (if known"). Ex: "http://localhost:8080"
    service_instance: Optional[str] = None
