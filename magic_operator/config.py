"""Magic operator configuration: read from plain env vars, once, at process
startup (mirrors `sidecar/config.py`'s fail-fast `os.getenv` convention). Also
defines the fixed HTTP paths this operator serves in each execution mode --
these MUST match whatever the paired sidecar's `OPERATOR_EXECUTION` env var
declares for the same operator instance (see `docker-compose.e2e.yml`).
"""
import json
import os
from dataclasses import dataclass
from typing import Any, List, Literal, Mapping, Optional

from pydantic import BaseModel

from .errors import MagicOperatorConfigError

SYNC_EXECUTE_PATH = "/execute"
ASYNC_START_PATH = "/jobs"
ASYNC_POLL_PATH = "/jobs/{job_id}"  # FastAPI route param; the manifest's poll_endpoint literal is "/jobs/{id}"

_VALID_EXECUTION_MODES = {"sync_http", "async_http"}


class InputSpec(BaseModel):
    """Same shape as sidecar's `OperatorIOSpec` (services/operator_resolver/manifest/manifest.py)."""
    name: str
    type: str
    required: bool = True
    default: Optional[Any] = None


@dataclass(frozen=True)
class MagicOperatorConfig:
    operator_name: str
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


def load_config(env: Optional[Mapping[str, str]] = None) -> MagicOperatorConfig:
    e = env if env is not None else os.environ

    def parse_json(name: str, raw: str) -> object:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MagicOperatorConfigError(f"'{name}' is not valid JSON: {exc}") from exc

    execution_mode = e.get("MAGIC_OPERATOR_EXECUTION_MODE", "sync_http")
    if execution_mode == "messaging":
        raise MagicOperatorConfigError(
            "MAGIC_OPERATOR_EXECUTION_MODE=messaging is reserved but unsupported: the AP "
            "Executor's ExecutionStrategyFactory only supports protocol='http' today, so a "
            "messaging-based operator could never actually be invoked (see docs/docs/operators.md)."
        )
    if execution_mode not in _VALID_EXECUTION_MODES:
        raise MagicOperatorConfigError(
            f"MAGIC_OPERATOR_EXECUTION_MODE must be one of {sorted(_VALID_EXECUTION_MODES)}, got '{execution_mode}'"
        )

    llm_provider = e.get("MAGIC_OPERATOR_LLM_PROVIDER", "mock")
    if llm_provider not in ("mock", "litellm"):
        raise MagicOperatorConfigError(f"MAGIC_OPERATOR_LLM_PROVIDER must be 'mock' or 'litellm', got '{llm_provider}'")

    llm_model = e.get("MAGIC_OPERATOR_LLM_MODEL")
    if llm_provider == "litellm" and not llm_model:
        raise MagicOperatorConfigError("MAGIC_OPERATOR_LLM_MODEL is required when MAGIC_OPERATOR_LLM_PROVIDER=litellm")

    prompt_template = e.get("MAGIC_OPERATOR_PROMPT_TEMPLATE")
    if not prompt_template:
        raise MagicOperatorConfigError("missing required environment variable 'MAGIC_OPERATOR_PROMPT_TEMPLATE'")

    raw_inputs = parse_json("MAGIC_OPERATOR_INPUTS", e.get("MAGIC_OPERATOR_INPUTS", "[]"))
    inputs = [InputSpec.model_validate(i) for i in raw_inputs]

    return MagicOperatorConfig(
        operator_name=e.get("MAGIC_OPERATOR_NAME", "Magic Operator"),
        inputs=inputs,
        output_name=e.get("MAGIC_OPERATOR_OUTPUT_NAME", "response"),
        prompt_template=prompt_template,
        execution_mode=execution_mode,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_api_base=e.get("MAGIC_OPERATOR_LLM_API_BASE"),
        llm_api_key=e.get("MAGIC_OPERATOR_LLM_API_KEY"),
        llm_timeout_seconds=float(e.get("MAGIC_OPERATOR_LLM_TIMEOUT_SECONDS", "30")),
        async_poll_cycles=int(e.get("MAGIC_OPERATOR_ASYNC_POLL_CYCLES", "1")),
        bind_host=e.get("MAGIC_OPERATOR_HOST", "0.0.0.0"),
        bind_port=int(e.get("MAGIC_OPERATOR_PORT", "9000")),
    )
