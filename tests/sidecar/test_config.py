"""Unit tests for sidecar/config.py's env-var loading and validation."""
import pytest

from conftest import base_env
from sidecar.config import load_config
from sidecar.errors import SidecarConfigError


@pytest.mark.parametrize("missing", [
    "SIDECAR_SERVICE_NAME",
    "SIDECAR_ADVERTISE_ADDRESS",
    "UPSTREAM_PORT",
    "OPERATOR_NAME",
    "OPERATOR_VERSION",
    "OPERATOR_EXECUTION",
])
def test_missing_required_var_raises(missing):
    env = base_env()
    del env[missing]
    with pytest.raises(SidecarConfigError):
        load_config(env)


def test_malformed_execution_json_raises():
    with pytest.raises(SidecarConfigError):
        load_config(base_env(OPERATOR_EXECUTION="not json"))


def test_execution_json_missing_manifest_fields_raises():
    with pytest.raises(SidecarConfigError):
        load_config(base_env(OPERATOR_EXECUTION='{"protocol":"http","endpoint":"/execute"}'))


def test_defaults_applied():
    config = load_config(base_env())
    assert config.consul_addr == "http://localhost:8500"
    assert config.bind_host == "0.0.0.0"
    assert config.bind_port == 8000
    assert config.health_check_path == "/health"
    assert config.health_check_interval == "10s"
    assert config.health_check_timeout == "5s"
    assert config.deregister_after == "1m"
    assert config.upstream_base_url == "http://localhost:9000"


def test_advertise_port_defaults_to_bind_port():
    config = load_config(base_env(SIDECAR_PORT="9999"))
    assert config.advertise_port == 9999


def test_advertise_port_overrides_bind_port():
    config = load_config(base_env(SIDECAR_PORT="8000", SIDECAR_ADVERTISE_PORT="18000"))
    assert config.bind_port == 8000
    assert config.advertise_port == 18000


def test_service_name_is_slugified():
    config = load_config(base_env(SIDECAR_SERVICE_NAME="Text to SQL"))
    assert config.service_slug == "text-to-sql"


def test_service_id_default_derivation():
    config = load_config(base_env(
        SIDECAR_SERVICE_NAME="Text to SQL",
        SIDECAR_ADVERTISE_ADDRESS="10.0.0.9",
        SIDECAR_PORT="8000",
    ))
    assert config.service_id == "text-to-sql-10.0.0.9-8000"


def test_service_id_can_be_overridden():
    config = load_config(base_env(SIDECAR_SERVICE_ID="custom-id"))
    assert config.service_id == "custom-id"


def test_manifest_inputs_outputs_parsed():
    config = load_config(base_env(
        OPERATOR_INPUTS='[{"name":"nl","type":"string","required":true}]',
        OPERATOR_OUTPUTS='[{"name":"query","type":"string","required":true}]',
    ))
    assert [i.name for i in config.manifest.inputs] == ["nl"]
    assert [o.name for o in config.manifest.outputs] == ["query"]


def test_consul_check_url_targets_own_advertise_address():
    config = load_config(base_env(
        SIDECAR_ADVERTISE_ADDRESS="10.0.0.9",
        SIDECAR_ADVERTISE_PORT="8000",
    ))
    assert config.consul_check_url == "http://10.0.0.9:8000/health"
