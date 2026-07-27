"""Unit tests for magic_operator/config.py and the routes create_app exposes
for each execution mode -- pure in-process, no Docker/network needed.

The route-constant assertions here are a cheap guard against
SYNC_EXECUTE_PATH/ASYNC_START_PATH/ASYNC_POLL_PATH drifting away from what
docker-compose.e2e.yml's OPERATOR_EXECUTION env vars declare for the paired
sidecar -- a rename here would be caught without needing Docker.
"""
import pytest

from magic_operator.app import create_app
from magic_operator.config import ASYNC_START_PATH, SYNC_EXECUTE_PATH, load_config
from magic_operator.errors import MagicOperatorConfigError


def base_env(**overrides: str):
    env = {
        "MAGIC_OPERATOR_PROMPT_TEMPLATE": "Summarize: {topic}",
        "MAGIC_OPERATOR_INPUTS": '[{"name":"topic","type":"string","required":true}]',
    }
    env.update(overrides)
    return env


def test_missing_prompt_template_raises():
    with pytest.raises(MagicOperatorConfigError):
        load_config({})


def test_messaging_mode_rejected():
    with pytest.raises(MagicOperatorConfigError):
        load_config(base_env(MAGIC_OPERATOR_EXECUTION_MODE="messaging"))


def test_unknown_execution_mode_rejected():
    with pytest.raises(MagicOperatorConfigError):
        load_config(base_env(MAGIC_OPERATOR_EXECUTION_MODE="carrier_pigeon"))


def test_litellm_provider_requires_model():
    with pytest.raises(MagicOperatorConfigError):
        load_config(base_env(MAGIC_OPERATOR_LLM_PROVIDER="litellm"))


def test_defaults_applied():
    config = load_config(base_env())
    assert config.execution_mode == "sync_http"
    assert config.llm_provider == "mock"
    assert config.output_name == "response"
    assert config.async_poll_cycles == 1
    assert config.bind_host == "0.0.0.0"
    assert config.bind_port == 9000


def test_sync_http_app_exposes_execute_route():
    config = load_config(base_env(MAGIC_OPERATOR_EXECUTION_MODE="sync_http"))
    app = create_app(config)
    paths = {route.path for route in app.routes}
    assert SYNC_EXECUTE_PATH in paths
    assert ASYNC_START_PATH not in paths


def test_async_http_app_exposes_job_routes():
    config = load_config(base_env(MAGIC_OPERATOR_EXECUTION_MODE="async_http"))
    app = create_app(config)
    paths = {route.path for route in app.routes}
    assert ASYNC_START_PATH in paths
    assert SYNC_EXECUTE_PATH not in paths
    assert any(p.startswith("/jobs/") for p in paths)
