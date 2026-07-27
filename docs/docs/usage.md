# Usage Guide

## Synchronous Execution

Execute an AP and get the result immediately:

```bash
curl -X POST http://localhost:5000/api/v1/aps/execute \
  -H "Content-Type: application/json" \
  -d @fixtures/01_ap_nl_to_sql.json
```

**Response (HTTP 200):**
```json
{
  "status": "success",
  "operators": [
    {
      "operator_id": "1de6e343-6952-4361-a17f-e4a9f1eaeae2",
      "operator_name": "Text to SQL",
      "operator_labels": ["Text_To_SQL_Operator", "Operator"],
      "operator_version": "1.0.0",
      "status": "success",
      "result": {"query": "SELECT * FROM students"},
      "service_instance": "10.0.0.4:8080",
      "execution_mode": "sync"
    }
  ],
  "workflow_instance_id": "..."
}
```

If the workflow takes longer than `SYNC_EXECUTION_TIMEOUT_SECONDS`, this returns **HTTP 504** with a message pointing you at `GET /aps/execute/async/{instance_id}` instead of blocking further. If the workflow fails outright, it returns **HTTP 500** with the failure detail.

---

## Asynchronous Execution

### 1. Dispatch an execution task

```bash
curl -X POST http://localhost:5000/api/v1/aps/execute/async \
  -H "Content-Type: application/json" \
  -d @fixtures/06_ap_two_op_chain.json
```

**Response (HTTP 202):**
```json
{ "task_id": "abc123", "status": "pending" }
```

`task_id` is the underlying Dapr workflow instance id.

### 2. Poll for the result

```bash
curl http://localhost:5000/api/v1/aps/execute/async/abc123
```

**Response while running:**
```json
{ "task_id": "abc123", "status": "running" }
```

**Response when complete:**
```json
{
  "task_id": "abc123",
  "status": "success",
  "result": {
    "status": "success",
    "operators": [...],
    "workflow_instance_id": "abc123"
  }
}
```

`status` follows the workflow's lifecycle: `pending`, `running`, `success`, `error`, `suspended`, `stalled`, `unknown`, or `not_found` (unknown `task_id`). On `error`, the response includes an `error` message instead of `result`.

---

## More examples

`fixtures/` contains progressively more complex sample APs (single operator, independent operators, chained operators, cross-operator dependencies, provenance consumers), and `fixtures/composed/` splices several of them together into larger multi-operator graphs — useful for exercising the execution-order logic manually.
