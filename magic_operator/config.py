"""Magic operator configuration: the operator's own declaration (name,
version, inputs, execution mode, prompt template) lives in a single mounted
YAML file (``MAGIC_OPERATOR_CONFIG_PATH``) -- portable as-is to a Kubernetes
``ConfigMap`` volume mount. Deployment/runtime settings (LLM provider
credentials, bind host/port) stay plain env vars. Also defines the fixed
HTTP paths this operator serves in each execution mode -- these back both
the routes it registers AND the manifest it self-serves at
``GET /.well-known/operator.yaml`` (see `app.py`), so the two can never
drift apart.
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Literal, Mapping, Optional

import yaml
from pydantic import BaseModel, ValidationError

from .errors import MagicOperatorConfigError

SYNC_EXECUTE_PATH = "/execute"
ASYNC_START_PATH = "/jobs"
ASYNC_POLL_PATH = "/jobs/{job_id}"  # FastAPI route param; the manifest's poll_endpoint literal is "/jobs/{id}"

_VALID_EXECUTION_MODES = {"sync_http", "async_http"}


class InputSpec(BaseModel):
    """Same shape as `OperatorIOSpec` (ap_executor/services/operator_resolver/manifest/manifest.py)."""
    name: str
    type: str
    required: bool = True
    default: Optional[Any] = None


class OperatorDeclaration(BaseModel):
    """The shape of the file `MAGIC_OPERATOR_CONFIG_PATH` points at."""
    name: str = "Magic Operator"
    version: str
    manifest_version: str = "0.1.0"
    execution_mode: Literal["sync_http", "async_http"] = "sync_http"
    output_name: str = "response"
    prompt_template: str
    inputs: List[InputSpec] = []


@dataclass(frozen=True)
class MagicOperatorConfig:
    operator_name: str
    version: str
    manifest_version: str
    inputs: List[InputSpec]
    output_name: str
    prompt_template: str
    execution_mode: Literal["sync_http", "async_http"]
    llm_provider: Literal["mock", "litellm"]
    llm_model: Optional[str]
    llm_api_base: Optional[str]
    llm_api_key: Optional[str]
    llm_timeout_seconds: float
    async_poll_cycles: int
    bind_host: str
    bind_port: int


def _load_declaration(path: str) -> OperatorDeclaration:
    try:
        raw_text = Path(path).read_text()
    except OSError as exc:
        raise MagicOperatorConfigError(f"could not read MAGIC_OPERATOR_CONFIG_PATH '{path}': {exc}") from exc

    try:
        raw = yaml.safe_load(raw_text) or {}
    except yaml.YAMLError as exc:
        raise MagicOperatorConfigError(f"MAGIC_OPERATOR_CONFIG_PATH '{path}' is not valid YAML: {exc}") from exc

    execution_mode = raw.get("execution_mode", "sync_http")
    if execution_mode == "messaging":
        raise MagicOperatorConfigError(
            "execution_mode: messaging is reserved but unsupported: the AP Executor's "
            "ExecutionStrategyFactory only supports protocol='http' today, so a "
            "messaging-based operator could never actually be invoked (see docs/docs/operators.md)."
        )
    if execution_mode not in _VALID_EXECUTION_MODES:
        raise MagicOperatorConfigError(
            f"execution_mode must be one of {sorted(_VALID_EXECUTION_MODES)}, got '{execution_mode}'"
        )

    try:
        return OperatorDeclaration.model_validate(raw)
    except ValidationError as exc:
        raise MagicOperatorConfigError(f"config at '{path}' failed validation: {exc}") from exc


def load_config(env: Optional[Mapping[str, str]] = None) -> MagicOperatorConfig:
    e = env if env is not None else os.environ

    config_path = e.get("MAGIC_OPERATOR_CONFIG_PATH")
    if not config_path:
        raise MagicOperatorConfigError("missing required environment variable 'MAGIC_OPERATOR_CONFIG_PATH'")
    declaration = _load_declaration(config_path)

    llm_provider = e.get("MAGIC_OPERATOR_LLM_PROVIDER", "mock")
    if llm_provider not in ("mock", "litellm"):
        raise MagicOperatorConfigError(f"MAGIC_OPERATOR_LLM_PROVIDER must be 'mock' or 'litellm', got '{llm_provider}'")

    llm_model = e.get("MAGIC_OPERATOR_LLM_MODEL")
    if llm_provider == "litellm" and not llm_model:
        raise MagicOperatorConfigError("MAGIC_OPERATOR_LLM_MODEL is required when MAGIC_OPERATOR_LLM_PROVIDER=litellm")

    return MagicOperatorConfig(
        operator_name=declaration.name,
        version=declaration.version,
        manifest_version=declaration.manifest_version,
        inputs=declaration.inputs,
        output_name=declaration.output_name,
        prompt_template=declaration.prompt_template,
        execution_mode=declaration.execution_mode,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_base=e.get("MAGIC_OPERATOR_LLM_API_BASE"),
        llm_api_key=e.get("MAGIC_OPERATOR_LLM_API_KEY"),
        llm_timeout_seconds=float(e.get("MAGIC_OPERATOR_LLM_TIMEOUT_SECONDS", "30")),
        async_poll_cycles=int(e.get("MAGIC_OPERATOR_ASYNC_POLL_CYCLES", "1")),
        bind_host=e.get("MAGIC_OPERATOR_HOST", "0.0.0.0"),
        bind_port=int(e.get("MAGIC_OPERATOR_PORT", "9000")),
    )
