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

## Magic Operator (reference/test)

`magic_operator/` (see [Registering an Operator](operators.md#4-registering-an-operator-with-consul)) is a self-sufficient reference operator — no sidecar of any kind fronts it. It serves its own manifest, health check, and execute/start/poll endpoints all from one process. Run it with `uv run python magic_operator/main.py`.

Its declaration (name, version, inputs, execution mode, prompt template — everything needed to both self-serve its manifest and behave correctly) is read from a single mounted YAML file given via `MAGIC_OPERATOR_CONFIG_PATH` — see `e2e/declarations/magic-echo-a.yaml` for an example. Note this declaration is **not** an `OperatorManifest` itself (`ap_executor/services/operator_resolver/manifest/manifest.py`) — it's `magic_operator`'s own input config (`OperatorDeclaration` in `magic_operator/config.py`), which `app.py` transforms into a real `OperatorManifest` at startup (`output_name` becomes the single entry in `outputs`; `prompt_template` is consumed internally and never appears in the served manifest). This keeps the operator's own contract one portable artifact (a Docker Compose bind mount today, a Kubernetes `ConfigMap` volume mount tomorrow) instead of several JSON-blob env vars. Because the manifest it serves is built from the same `execution_mode`/route constants it registers its routes with, there's no separate file to keep in sync. LLM provider credentials and runtime settings stay plain env vars (never put `LLM_API_KEY` in the mounted file — it's a secret, not manifest content):

| Variable | Default | Description |
|---|---|---|
| `MAGIC_OPERATOR_CONFIG_PATH` | *(required)* | Path to a mounted YAML file with `name`, `version`, `manifest_version`, `execution_mode`, `output_name`, `prompt_template`, `inputs` |
| `MAGIC_OPERATOR_LLM_PROVIDER` | `mock` | `mock` (deterministic, no network — the CI default) or `litellm` (real LLM call, requires the `llm` extra) |
| `LLM_API_MODEL` | — | required when `MAGIC_OPERATOR_LLM_PROVIDER=litellm` — same variable name as [ap-management](https://github.com/datagems-eosc/ap-management)'s `LLM` client |
| `LLM_API_BASE` | *(unset)* | passed to `litellm.acompletion` |
| `LLM_API_KEY` | *(unset)* | omitted from the call entirely when unset — a secret, keep it out of the mounted config file |
| `LLM_SSL_VERIFY` | `true` | set to `false` for self-signed LLM endpoint certificates |
| `MAGIC_OPERATOR_LLM_TIMEOUT_SECONDS` | `30` | |
| `MAGIC_OPERATOR_ASYNC_POLL_CYCLES` | `1` | number of `GET /jobs/{id}` calls that report `"running"` before `"done"` (no real delay — a counter, kept fast for CI) |
| `MAGIC_OPERATOR_HOST` | `0.0.0.0` | bind host |
| `MAGIC_OPERATOR_PORT` | `9000` | bind port — the same address Consul registers (a Consul client agent alongside it in Compose, or Kubernetes Service Sync in production; see `operators.md`) |

## Testing Configuration

Tests run entirely against mocks — an `httpx.MockTransport` for HTTP calls (Consul, manifests, operator invocations) and a fake Dapr workflow client for the API layer. No external services (Consul, Dapr sidecar, or otherwise) are needed to run the suite:

```bash
pytest tests/
```
