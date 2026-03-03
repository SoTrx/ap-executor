# Usage Guide

## Synchronous Execution

Execute an AP and get the result immediately:

```bash
curl -X POST http://localhost:5000/api/v1/execute \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query_mathe.json
```

**Response (HTTP 200):**
```json
{
  "ap_name": "Query Dataset AP",
  "database_name": "mathe",
  "schema_name": "mathe",
  "status": "success",
  "operators": [
    {
      "operator_id": "...",
      "operator_name": "Annotate Dataset Operator",
      "operator_labels": ["Operator", "Provenance_Annotate_Dataset_Operator"],
      "status": "skipped"
    },
    {
      "operator_id": "...",
      "operator_name": "Provenance Query Operator",
      "operator_labels": ["Operator", "Provenance_SQL_Operator"],
      "status": "success",
      "result": [...],
      "rows_affected": 5
    }
  ]
}
```

---

## Asynchronous Execution

### 1. Dispatch an execution task

```bash
curl -X POST http://localhost:5000/api/v1/execute/async \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query_mathe.json
```

**Response (HTTP 202):**
```json
{ "task_id": "abc123", "status": "pending" }
```

### 2. Poll for the result

```bash
curl http://localhost:5000/api/v1/execute/async/abc123
```

**Response when complete:**
```json
{
  "task_id": "abc123",
  "status": "success",
  "result": {
    "ap_name": "Query Dataset AP",
    "database_name": "mathe",
    "schema_name": "mathe",
    "status": "success",
    "operators": [...]
  }
}
```
