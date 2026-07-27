from typing import Annotated, Any, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class OperatorIOSpec(BaseModel):
    """A single declared operator input or output."""
    name: str
    type: str
    required: bool = True
    default: Optional[Any] = None


class OperatorExecutionSyncSpec(BaseModel):
    mode: Literal["sync"]
    protocol: Literal["http"]
    endpoint: str


class OperatorExecutionAsyncSpec(BaseModel):
    mode: Literal["async"]
    protocol: Literal["http"]
    start_endpoint: str
    poll_endpoint: str


OperatorExecutionSpec = Annotated[
    Union[OperatorExecutionSyncSpec, OperatorExecutionAsyncSpec],
    Field(discriminator="mode"),
]


class OperatorManifest(BaseModel):
    """The `/.well-known/operator.json` contract exposed by an operator's sidecar."""
    manifest_version: str
    operator: str
    version: str
    execution: OperatorExecutionSpec
    inputs: List[OperatorIOSpec] = []
    outputs: List[OperatorIOSpec] = []
