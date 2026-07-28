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

`sidecar/` (see [Registering an Operator](operators.md#4-pairing-an-operator-with-the-reference-sidecar)) is a separate process paired **1:1 with one operator instance** — not with the executor. Run it with `uv run python sidecar/main.py`.

The operator's own manifest content (`manifest_version`, `operator`, `version`, `execution`, `inputs`, `outputs`) is **not** decomposed into env vars — it's read from a single mounted YAML file that validates directly as an `OperatorManifest`, given via `OPERATOR_MANIFEST_PATH`. This keeps the manifest one portable artifact (a plain YAML file — a Docker Compose bind mount today, a Kubernetes `ConfigMap` volume mount tomorrow, no code change either way) instead of several JSON-blob env vars, and sidesteps shell/YAML quoting hazards entirely. See `manifests/magic-echo-a/manifest.yaml` for an example. Everything else — deployment topology, Consul registration, health-check timing — stays a plain env var, since it varies per deployment and isn't part of "what this operator is":

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
| `OPERATOR_MANIFEST_PATH` | *(required)* | Path to a mounted YAML file that validates as `OperatorManifest` |

## Magic Operator (reference/test)

`magic_operator/` (see [`magic_operator/` — a fuller reference/test operator](operators.md#magic_operator--a-fuller-referencetest-operator)) is a separate process, paired 1:1 with a `sidecar/` instance just like a real operator. Run it with `uv run python magic_operator/main.py`.

Like the sidecar, the operator's own declaration (name, inputs, execution mode, prompt template) is read from a single mounted YAML file given via `MAGIC_OPERATOR_CONFIG_PATH` — see `manifests/magic-echo-a/magic-operator.yaml` for an example. Its `execution_mode` must stay consistent with the paired sidecar manifest's `execution` field for the same operator instance (`sync_http` ↔ a manifest with `endpoint`; `async_http` ↔ one with `start_endpoint`/`poll_endpoint`) — see the comments in each `manifests/<operator>/` directory. LLM provider credentials and runtime settings stay plain env vars (never put `MAGIC_OPERATOR_LLM_API_KEY` in the mounted file — it's a secret, not manifest content):

| Variable | Default | Description |
|---|---|---|
| `MAGIC_OPERATOR_CONFIG_PATH` | *(required)* | Path to a mounted YAML file with `name`, `execution_mode`, `output_name`, `prompt_template`, `inputs` |
| `MAGIC_OPERATOR_LLM_PROVIDER` | `mock` | `mock` (deterministic, no network — the CI default) or `litellm` (real LLM call, requires the `llm` extra) |
| `MAGIC_OPERATOR_LLM_MODEL` | — | required when `MAGIC_OPERATOR_LLM_PROVIDER=litellm` |
| `MAGIC_OPERATOR_LLM_API_BASE` | *(unset)* | passed to `litellm.acompletion` |
| `MAGIC_OPERATOR_LLM_API_KEY` | *(unset)* | omitted from the call entirely when unset — a secret, keep it out of the mounted config file |
| `MAGIC_OPERATOR_LLM_TIMEOUT_SECONDS` | `30` | |
| `MAGIC_OPERATOR_ASYNC_POLL_CYCLES` | `1` | number of `GET /jobs/{id}` calls that report `"running"` before `"done"` (no real delay — a counter, kept fast for CI) |
| `MAGIC_OPERATOR_HOST` | `0.0.0.0` | bind host |
| `MAGIC_OPERATOR_PORT` | `9000` | bind port — this is what a paired sidecar's `UPSTREAM_PORT` points at |

## Testing Configuration

Tests run entirely against mocks — an `httpx.MockTransport` for HTTP calls (Consul, manifests, operator invocations) and a fake Dapr workflow client for the API layer. No external services (Consul, Dapr sidecar, or otherwise) are needed to run the suite:

```bash
pytest tests/
```
