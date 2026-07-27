# Registering an Operator

An **operator** is an independently-deployed microservice. The AP Executor never bundles or hosts operator code — it discovers a healthy instance via Consul, fetches a small self-description manifest from it, and calls it over HTTP. This page covers everything an operator implementation must do to be discoverable and invokable, and how it gets referenced from an AP graph.

## How an operator gets called (end to end)

For each `Operator` node in the AP graph, in topological order (`ApInstance.iter_operators()`, `domain/ap_instance.py`), the workflow orchestrator (`workflows/ap_execution.py::ap_execution_workflow`) does the following, entirely inside the Dapr activity `execute_operator_activity` (`workflows/operator_execution.py`) so the orchestrator itself stays I/O-free:

1. **Resolve inputs** — `instance.resolve_operator_input_values(operator_id)` merges the caller-supplied parameters for this operator with any values wired in from upstream operators via the graph's `input`/`output` edges (see [Wiring into an AP graph](#3-wiring-into-an-ap-graph)).
2. **Resolve the operator** — `OperatorResolver.resolve(operator_name, operator_version)` (`services/operator_resolver/operator_resolver.py`):
   - looks up a healthy instance in Consul by service name (`ConsulRegistryClient.resolve_operator`, `services/operator_resolver/registry/consul_registry.py`);
   - fetches that instance's manifest at `GET {base_url}/.well-known/operator.json` (`HttpManifestRetriever`, `services/operator_resolver/manifest/http_manifest_retriever.py`).
3. **Filter inputs to the manifest's declared contract** — `_filter_inputs` (`workflows/operator_execution.py`) drops any resolved input the manifest doesn't declare, fills in declared defaults, and raises `OperatorExecutionError` (`services/executor/errors.py`) if a required input (no default) is still missing. This is the only validation the executor does on inputs before calling the operator.
4. **Dispatch via the manifest's declared execution mode** — `ExecutionStrategyFactory.create(mode, protocol)` (`services/executor/strategies/factory.py`) picks `HttpSyncExecutionStrategy` or `HttpAsyncPollingExecutionStrategy`, which calls the operator over HTTP.
5. **Poll until done** (async operators only) — the orchestrator itself owns the polling cadence (`POLL_INTERVAL = 5s`, `workflows/ap_execution.py`), calling the `poll_operator_activity` activity on a Dapr timer until the handle reports `done`. This is intentional: polling from inside the orchestrator (rather than blocking one activity call for the operator's whole duration) means a slow operator never ties up a worker slot, and each poll re-resolves nothing — it just re-checks the same job via the handle it already has.
6. **Propagate the result** — on completion, the operator's output dict is written into `instance.state.parameters[operator_id]`, making it available to any downstream operator resolved later in the same run, and an `OperatorResult` (success or error) is appended to the execution's result list.

None of this is operator-specific configuration on the executor side — an operator becomes callable purely by satisfying the contract below.

## 1. Register the service instance in Consul

The executor only **reads** Consul (`ConsulRegistryClient`, `services/operator_resolver/registry/consul_registry.py`) — every operator implementation is expected to **self-register** (e.g. via its own sidecar or startup hook) as a Consul service that passes its health check.

The service name Consul is queried under is derived from the AP graph's `Operator` node `properties.name` (e.g. `"Text to SQL"`), slugified by `OperatorRegistry.normalize_op_name`:

```python
_SLUG_RE = re.compile(r"[^a-z0-9]+")
_SLUG_RE.sub("-", name.strip().lower()).strip("-")
# "Text to SQL" -> "text-to-sql"
```

So an operator whose AP node is named `"Text to SQL"` must register in Consul as service `text-to-sql`. This is the load-bearing name — the manifest's own `operator` field (see below) is descriptive only (used in error messages), it is **not** looked up.

If the AP graph pins a `version` on the operator node, resolution filters Consul results with `Service.Meta.version == "<version>"` — so register that version in the instance's `Service.Meta`. Without a pinned version, the resolver picks at random among all healthy instances for that service name (no "latest wins" logic).

## 2. Serve the manifest

Every operator instance must serve its manifest at:

```
GET {base_url}/.well-known/operator.json
```

returning JSON matching `OperatorManifest` (`services/operator_resolver/manifest/manifest.py`):

```python
class OperatorIOSpec(BaseModel):
    name: str
    type: str
    required: bool = True
    default: Optional[Any] = None

class OperatorManifest(BaseModel):
    manifest_version: str
    operator: str
    version: str
    execution: OperatorExecutionSpec       # see below — discriminated on "mode"
    inputs: List[OperatorIOSpec] = []
    outputs: List[OperatorIOSpec] = []
```

`inputs`/`outputs` here describe the operator's own contract (used for input filtering/defaults, see step 3 above); they should match — but are validated independently from — the `inputs`/`outputs` declared on the operator's node in the AP graph itself (used for dataflow wiring between operators, see below).

### Execution modes

`execution` is a discriminated union on `mode`:

**`sync`** — a single call that returns the result immediately:

```json
{"mode": "sync", "protocol": "http", "endpoint": "/execute"}
```

The executor does `POST {base_url}{endpoint}` with the filtered inputs as the JSON body. A non-2xx response fails the operator (`OperatorExecutionError`); a 2xx response's JSON body **is** the operator's output dict.

**`async`** — a start+poll job pattern:

```json
{
  "mode": "async",
  "protocol": "http",
  "start_endpoint": "/jobs",
  "poll_endpoint": "/jobs/{id}"
}
```

- `POST {base_url}{start_endpoint}` with the filtered inputs; the response body must be JSON with an `"id"` field (the job id).
- The executor then polls `GET {base_url}{poll_endpoint.format(id=job_id)}` (i.e. `poll_endpoint` must contain a literal `{id}` placeholder) on its own `POLL_INTERVAL` cadence, not the operator's.
- Each poll response is JSON with a `"status"` field:

  | `status` | Meaning | Also read from the response |
  |---|---|---|
  | `done`, `completed`, `success` | terminal success | `"result"` → becomes the operator's output dict |
  | `error`, `failed` | terminal failure | `"error"` → becomes the operator result's error message |
  | anything else (e.g. `"running"`, `"pending"`) | still running | *(ignored, polled again after `POLL_INTERVAL`)* |

Currently only `protocol: "http"` is supported for either mode (`ExecutionStrategyFactory.create` raises `UnsupportedOperatorError`, `services/operator_resolver/errors.py`, for anything else) — this is where a new protocol/strategy pair would be registered if one is ever needed.

## 3. Wiring into an AP graph

An operator is referenced from an AP graph as a node:

```json
{
  "id": "1de6e343-6952-4361-a17f-e4a9f1eaeae2",
  "labels": ["Text_To_SQL_Operator", "Operator"],
  "properties": {
    "name": "Text to SQL",
    "version": "1.0.0",
    "inputs": [{"name": "nl", "type": "string", "required": true}],
    "outputs": [{"name": "query", "type": "string", "required": true}]
  }
}
```

- `labels` must include `"Operator"` — that's what `ApInstance.iter_operators()` filters on. The more specific label (`Text_To_SQL_Operator` here) is carried through into `OperatorResult.operator_labels` but otherwise unused by the executor.
- `properties.name` is the operator name resolved against Consul (see step 1) — this is the field that actually has to match a registered service.
- `properties.version`, if present, pins the Consul lookup to that `Service.Meta.version`.
- `properties.inputs`/`outputs` describe the operator's contract at the graph level, for documentation and for the dataflow edges below to reference by name.

It must be connected to the AP root node by a `consist_of` edge, and — for a multi-operator AP — ordered relative to other operators with `follows` edges, and wired to its data sources with `input`/`output` edges:

```json
{"from": "<ap-id>", "to": "<operator-id>", "labels": ["consist_of"]},
{"from": "<downstream-op-id>", "to": "<upstream-op-id>", "labels": ["follows"]}
```

A downstream operator's input can come from either:

- **A caller-supplied parameter** — placed directly in `state.parameters[operator_id]` on the `ApInstance` at execution time; no edge needed.
- **Another operator's output** — an `output` edge from the producing operator to an intermediate `ResultType` node, and an `input` edge from that `ResultType` node to the consuming operator, each carrying a `properties.mapping` of `{target_expr: source_expr}` (e.g. `{"to['inputs']['sql']": "from['outputs']['query']"}`) — see `fixtures/composed/06_07.json` for a full worked example. Values wired in this way override a same-named caller parameter.

## Minimal reference implementation

Any HTTP framework works — the only requirements are the manifest endpoint and whatever `execution` declares. A minimal sync operator in FastAPI:

```python
from fastapi import FastAPI

app = FastAPI()

MANIFEST = {
    "manifest_version": "0.1.0",
    "operator": "Text to SQL",
    "version": "1.0.0",
    "execution": {"mode": "sync", "protocol": "http", "endpoint": "/execute"},
    "inputs": [{"name": "nl", "type": "string", "required": True}],
    "outputs": [{"name": "query", "type": "string", "required": True}],
}

@app.get("/.well-known/operator.json")
async def manifest():
    return MANIFEST

@app.post("/execute")
async def execute(payload: dict):
    return {"query": f"SELECT * FROM students WHERE {payload['nl']}"}
```

...and an async one:

```python
JOBS: dict[str, dict] = {}

@app.get("/.well-known/operator.json")
async def manifest():
    return {
        "manifest_version": "0.1.0",
        "operator": "Long Running Op",
        "version": "1.0.0",
        "execution": {
            "mode": "async", "protocol": "http",
            "start_endpoint": "/jobs", "poll_endpoint": "/jobs/{id}",
        },
        "inputs": [...],
        "outputs": [...],
    }

@app.post("/jobs")
async def start(payload: dict):
    job_id = "job-1"
    JOBS[job_id] = {"status": "running"}
    background_task(job_id, payload)  # flips JOBS[job_id] to {"status": "done", "result": {...}}
    return {"id": job_id}

@app.get("/jobs/{job_id}")
async def poll(job_id: str):
    return JOBS[job_id]
```

(`tests/conftest.py`'s `_make_sync_operator_app` / `_make_async_operator_app` build exactly these shapes as dummy in-process apps for the integration tests — a useful reference for the exact request/response cycle the executor expects.)

Finally, register the running instance in Consul under the slugified operator name (step 1) with a passing health check, and the AP Executor will be able to discover, resolve, and invoke it — no code change or configuration on the executor side is needed to add a new operator.

## Checklist

- [ ] Service registers in Consul as `<slug of the AP node's "name" property>`, with a passing health check (and `Service.Meta.version` if the AP pins a version).
- [ ] `GET /.well-known/operator.json` returns a valid `OperatorManifest` (`manifest_version`, `operator`, `version`, `execution`, `inputs`, `outputs`).
- [ ] `execution.mode`/`protocol` is `sync`/`http` or `async`/`http` (the only two currently supported).
- [ ] Sync: `execution.endpoint` returns the output dict directly on 2xx.
- [ ] Async: `execution.start_endpoint` returns `{"id": ...}`; `execution.poll_endpoint` contains a literal `{id}` placeholder and returns `{"status": ..., "result": ...}` / `{"status": ..., "error": ...}` using the status vocabulary above.
- [ ] The AP graph's `Operator` node declares matching `inputs`/`outputs` and is wired in with `consist_of`/`follows`/`input`/`output` edges as needed.
