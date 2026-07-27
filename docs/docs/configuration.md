# Configuration

This document describes how to configure the AP Executor service for different environments.

## Environment Variables

All variables have working defaults matching the underlying SDKs' own defaults, so nothing is strictly required to boot the service locally.

| Variable | Default | Description |
|---|---|---|
| `CONSUL_HTTP_ADDR` | `http://localhost:8500` | Consul agent address used for operator service discovery |
| `DAPR_RUNTIME_HOST` | `localhost` | Dapr sidecar host |
| `DAPR_GRPC_PORT` | `50001` | Dapr sidecar gRPC port |
| `DAPR_HTTP_PORT` | `3500` | Dapr sidecar HTTP port |
| `SYNC_EXECUTION_TIMEOUT_SECONDS` | `60` | How long `POST /aps/execute` blocks before returning 504 and pointing the caller at the async endpoint instead |
| `ROOT_PATH` | `""` | API root path when behind a reverse proxy |

Copy `.env.example` to `.env` and adjust as needed for your environment (e.g. pointing `CONSUL_HTTP_ADDR` at a real Consul cluster in production).

## Operator Sidecar

`sidecar/` (see [Registering an Operator](operators.md#4-pairing-an-operator-with-the-reference-sidecar)) is a separate process paired **1:1 with one operator instance** — not with the executor. Run it with `uv run python sidecar/main.py`. Its own env vars:

| Variable | Default | Description |
|---|---|---|
| `CONSUL_HTTP_ADDR` | `http://localhost:8500` | Consul agent the sidecar registers/deregisters against |
| `SIDECAR_SERVICE_NAME` | *(required)* | Raw operator name; slugified into the Consul service name — must match the AP node's slugified `properties.name` |
| `SIDECAR_SERVICE_ID` | `{slug}-{advertise_address}-{advertise_port}` | Consul service instance ID |
| `SIDECAR_HOST` | `0.0.0.0` | Bind host for the sidecar's own HTTP server |
| `SIDECAR_PORT` | `8000` | Bind port for the sidecar's own HTTP server |
| `SIDECAR_ADVERTISE_ADDRESS` | *(required)* | Address Consul hands the executor to reach this sidecar |
| `SIDECAR_ADVERTISE_PORT` | = `SIDECAR_PORT` | Port Consul hands out (differs from bind port under container port remapping) |
| `SIDECAR_HEALTH_CHECK_PATH` | `/health` | Path Consul's HTTP check hits on the sidecar's own address; falls through the catch-all proxy to the upstream's `/health` |
| `SIDECAR_HEALTH_CHECK_INTERVAL` | `10s` | Consul `Check.Interval` |
| `SIDECAR_HEALTH_CHECK_TIMEOUT` | `5s` | Consul `Check.Timeout` |
| `SIDECAR_DEREGISTER_AFTER` | `1m` | Consul `Check.DeregisterCriticalServiceAfter` |
| `SIDECAR_PROXY_TIMEOUT_SECONDS` | `30` | httpx client timeout for proxied calls to the upstream |
| `SIDECAR_CONSUL_TIMEOUT_SECONDS` | `10` | httpx client timeout for the register/deregister calls |
| `UPSTREAM_HOST` | `localhost` | Real operator backend host |
| `UPSTREAM_PORT` | *(required)* | Real operator backend port |
| `OPERATOR_MANIFEST_VERSION` | `0.1.0` | Manifest `manifest_version` |
| `OPERATOR_NAME` | *(required)* | Manifest `operator` (descriptive) |
| `OPERATOR_VERSION` | *(required)* | Manifest `version`, and Consul `Meta.version` |
| `OPERATOR_EXECUTION` | *(required)* | JSON blob → manifest `execution` (discriminated union) |
| `OPERATOR_INPUTS` | `[]` | JSON blob → manifest `inputs` |
| `OPERATOR_OUTPUTS` | `[]` | JSON blob → manifest `outputs` |

## Testing Configuration

Tests run entirely against mocks — an `httpx.MockTransport` for HTTP calls (Consul, manifests, operator invocations) and a fake Dapr workflow client for the API layer. No external services (Consul, Dapr sidecar, or otherwise) are needed to run the suite:

```bash
pytest tests/
```
