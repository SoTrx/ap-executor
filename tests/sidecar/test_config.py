"""Unit tests for sidecar/config.py's env-var + mounted-manifest-file loading
and validation."""
import pytest

from conftest import base_env, write_manifest
from sidecar.config import load_config
from sidecar.errors import SidecarConfigError


@pytest.mark.parametrize("missing", [
    "SIDECAR_SERVICE_NAME",
    "SIDECAR_ADVERTISE_ADDRESS",
    "UPSTREAM_PORT",
    "OPERATOR_MANIFEST_PATH",
])
def test_missing_required_var_raises(tmp_path, missing):
    env = base_env(tmp_path)
    del env[missing]
    with pytest.raises(SidecarConfigError):
        load_config(env)


def test_manifest_path_pointing_at_nonexistent_file_raises(tmp_path):
    env = base_env(tmp_path, OPERATOR_MANIFEST_PATH=str(tmp_path / "does-not-exist.yaml"))
    with pytest.raises(SidecarConfigError):
        load_config(env)


def test_manifest_invalid_yaml_raises(tmp_path):
    path = write_manifest(tmp_path, content="{not: valid: yaml: [", filename="bad.yaml")
    env = base_env(tmp_path, OPERATOR_MANIFEST_PATH=path)
    with pytest.raises(SidecarConfigError):
        load_config(env)


def test_manifest_missing_required_fields_raises(tmp_path):
    path = write_manifest(tmp_path, content="operator: Text to SQL\n", filename="incomplete.yaml")
    env = base_env(tmp_path, OPERATOR_MANIFEST_PATH=path)
    with pytest.raises(SidecarConfigError):
        load_config(env)


def test_defaults_applied(tmp_path):
    config = load_config(base_env(tmp_path))
    assert config.consul_addr == "http://localhost:8500"
    assert config.bind_host == "0.0.0.0"
    assert config.bind_port == 8000
    assert config.health_check_path == "/health"
    assert config.health_check_interval == "10s"
    assert config.health_check_timeout == "5s"
    assert config.deregister_after == "1m"
    assert config.upstream_base_url == "http://localhost:9000"


def test_advertise_port_defaults_to_bind_port(tmp_path):
    config = load_config(base_env(tmp_path, SIDECAR_PORT="9999"))
    assert config.advertise_port == 9999


def test_advertise_port_overrides_bind_port(tmp_path):
    config = load_config(base_env(tmp_path, SIDECAR_PORT="8000", SIDECAR_ADVERTISE_PORT="18000"))
    assert config.bind_port == 8000
    assert config.advertise_port == 18000


def test_service_name_is_slugified(tmp_path):
    config = load_config(base_env(tmp_path, SIDECAR_SERVICE_NAME="Text to SQL"))
    assert config.service_slug == "text-to-sql"


def test_service_id_default_derivation(tmp_path):
    config = load_config(base_env(
        tmp_path,
        SIDECAR_SERVICE_NAME="Text to SQL",
        SIDECAR_ADVERTISE_ADDRESS="10.0.0.9",
        SIDECAR_PORT="8000",
    ))
    assert config.service_id == "text-to-sql-10.0.0.9-8000"


def test_service_id_can_be_overridden(tmp_path):
    config = load_config(base_env(tmp_path, SIDECAR_SERVICE_ID="custom-id"))
    assert config.service_id == "custom-id"


def test_manifest_inputs_outputs_parsed(tmp_path):
    manifest_yaml = """\
manifest_version: "0.1.0"
operator: Text to SQL
version: "1.0.0"
execution:
  mode: sync
  protocol: http
  endpoint: /execute
inputs:
  - name: nl
    type: string
    required: true
outputs:
  - name: query
    type: string
    required: true
"""
    path = write_manifest(tmp_path, content=manifest_yaml)
    config = load_config(base_env(tmp_path, OPERATOR_MANIFEST_PATH=path))
    assert [i.name for i in config.manifest.inputs] == ["nl"]
    assert [o.name for o in config.manifest.outputs] == ["query"]


def test_consul_check_url_targets_own_advertise_address(tmp_path):
    config = load_config(base_env(
        tmp_path,
        SIDECAR_ADVERTISE_ADDRESS="10.0.0.9",
        SIDECAR_ADVERTISE_PORT="8000",
    ))
    assert config.consul_check_url == "http://10.0.0.9:8000/health"
