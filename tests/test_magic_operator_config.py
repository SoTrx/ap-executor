"""Unit tests for magic_operator/config.py and the routes/manifest create_app
exposes for each execution mode -- pure in-process, no Docker/network needed.

The route-constant and manifest-content assertions here are a cheap guard
against SYNC_EXECUTE_PATH/ASYNC_START_PATH/ASYNC_POLL_PATH drifting away
from what the self-served manifest declares -- a rename here would be
caught without needing Docker.
"""
import pytest
import yaml
from fastapi.testclient import TestClient

from magic_operator.app import create_app
from magic_operator.config import ASYNC_START_PATH, SYNC_EXECUTE_PATH, load_config
from magic_operator.errors import MagicOperatorConfigError

DEFAULT_DECLARATION_YAML = """\
version: "1.0.0"
prompt_template: "Summarize: {topic}"
inputs:
  - name: topic
    type: string
    required: true
"""


def write_declaration(tmp_path, content: str = DEFAULT_DECLARATION_YAML, filename: str = "config.yaml") -> str:
    path = tmp_path / filename
    path.write_text(content)
    return str(path)


def base_env(tmp_path, **overrides: str):
    env = {"MAGIC_OPERATOR_CONFIG_PATH": write_declaration(tmp_path)}
    env.update(overrides)
    return env


def test_missing_config_path_raises():
    with pytest.raises(MagicOperatorConfigError):
        load_config({})


def test_config_path_pointing_at_nonexistent_file_raises(tmp_path):
    with pytest.raises(MagicOperatorConfigError):
        load_config({"MAGIC_OPERATOR_CONFIG_PATH": str(tmp_path / "does-not-exist.yaml")})


def test_declaration_invalid_yaml_raises(tmp_path):
    path = write_declaration(tmp_path, content="{not: valid: yaml: [", filename="bad.yaml")
    with pytest.raises(MagicOperatorConfigError):
        load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})


def test_declaration_missing_prompt_template_raises(tmp_path):
    path = write_declaration(tmp_path, content="name: Magic Operator\nversion: \"1.0.0\"\n", filename="incomplete.yaml")
    with pytest.raises(MagicOperatorConfigError):
        load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})


def test_declaration_missing_version_raises(tmp_path):
    path = write_declaration(
        tmp_path, content='prompt_template: "Summarize: {topic}"\n', filename="no-version.yaml"
    )
    with pytest.raises(MagicOperatorConfigError):
        load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})


def test_messaging_mode_rejected(tmp_path):
    path = write_declaration(tmp_path, content=DEFAULT_DECLARATION_YAML + "execution_mode: messaging\n")
    with pytest.raises(MagicOperatorConfigError):
        load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})


def test_unknown_execution_mode_rejected(tmp_path):
    path = write_declaration(tmp_path, content=DEFAULT_DECLARATION_YAML + "execution_mode: carrier_pigeon\n")
    with pytest.raises(MagicOperatorConfigError):
        load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})


def test_litellm_provider_requires_model(tmp_path):
    with pytest.raises(MagicOperatorConfigError):
        load_config(base_env(tmp_path, MAGIC_OPERATOR_LLM_PROVIDER="litellm"))


def test_defaults_applied(tmp_path):
    config = load_config(base_env(tmp_path))
    assert config.execution_mode == "sync_http"
    assert config.llm_provider == "mock"
    assert config.output_name == "response"
    assert config.manifest_version == "0.1.0"
    assert config.async_poll_cycles == 1
    assert config.bind_host == "0.0.0.0"
    assert config.bind_port == 9000


def test_declaration_fields_are_read(tmp_path):
    declaration_yaml = """\
name: Magic Echo A
version: "1.0.0"
manifest_version: "0.2.0"
execution_mode: sync_http
output_name: summary
prompt_template: "Summarize the topic: {topic}"
inputs:
  - name: topic
    type: string
    required: true
"""
    path = write_declaration(tmp_path, content=declaration_yaml)
    config = load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})
    assert config.operator_name == "Magic Echo A"
    assert config.version == "1.0.0"
    assert config.manifest_version == "0.2.0"
    assert config.output_name == "summary"
    assert config.prompt_template == "Summarize the topic: {topic}"
    assert [i.name for i in config.inputs] == ["topic"]


def test_sync_http_app_exposes_execute_route(tmp_path):
    config = load_config(base_env(tmp_path))
    app = create_app(config)
    paths = {route.path for route in app.routes}
    assert SYNC_EXECUTE_PATH in paths
    assert ASYNC_START_PATH not in paths


def test_async_http_app_exposes_job_routes(tmp_path):
    path = write_declaration(tmp_path, content=DEFAULT_DECLARATION_YAML + "execution_mode: async_http\n")
    config = load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})
    app = create_app(config)
    paths = {route.path for route in app.routes}
    assert ASYNC_START_PATH in paths
    assert SYNC_EXECUTE_PATH not in paths
    assert any(p.startswith("/jobs/") for p in paths)


def test_sync_http_manifest_self_served(tmp_path):
    config = load_config(base_env(tmp_path))
    app = create_app(config)
    with TestClient(app) as client:
        resp = client.get("/.well-known/operator.yaml")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/yaml")
    body = yaml.safe_load(resp.text)
    assert body["operator"] == config.operator_name
    assert body["version"] == config.version
    assert body["execution"] == {"mode": "sync", "protocol": "http", "endpoint": SYNC_EXECUTE_PATH}
    assert body["inputs"] == [{"name": "topic", "type": "string", "required": True, "default": None}]
    assert body["outputs"] == [{"name": config.output_name, "type": "string", "required": True, "default": None}]


def test_async_http_manifest_self_served(tmp_path):
    path = write_declaration(tmp_path, content=DEFAULT_DECLARATION_YAML + "execution_mode: async_http\n")
    config = load_config({"MAGIC_OPERATOR_CONFIG_PATH": path})
    app = create_app(config)
    with TestClient(app) as client:
        resp = client.get("/.well-known/operator.yaml")
    assert resp.status_code == 200
    body = yaml.safe_load(resp.text)
    assert body["execution"] == {
        "mode": "async",
        "protocol": "http",
        "start_endpoint": ASYNC_START_PATH,
        "poll_endpoint": "/jobs/{id}",
    }
