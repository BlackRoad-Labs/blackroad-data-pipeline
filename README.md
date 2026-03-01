# blackroad-data-pipeline

![CI](https://github.com/BlackRoad-Labs/blackroad-data-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-proprietary-red)

Production-grade ETL data pipeline framework for the [BlackRoad OS](https://blackroad.dev) platform. Build, schedule, and monitor data pipelines with a dependency-graph execution engine, multi-format sources & sinks, and first-class Stripe and npm ecosystem integration.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Installation](#3-installation)
4. [Quick Start](#4-quick-start)
5. [CLI Reference](#5-cli-reference)
6. [Sources](#6-sources)
7. [Transforms](#7-transforms)
8. [Sinks](#8-sinks)
9. [Schema Validation](#9-schema-validation)
10. [Configuration](#10-configuration)
11. [Stripe Integration](#11-stripe-integration)
12. [npm Ecosystem](#12-npm-ecosystem)
13. [Testing](#13-testing)
14. [License](#14-license)

---

## 1. Overview

`blackroad-data-pipeline` is the core ETL engine of the **BlackRoad OS** platform. It provides:

- **Pluggable sources** — CSV, JSON, SQLite, and inline data
- **Composable transforms** — filter, map, aggregate, join, sort, deduplicate, select, rename
- **Flexible sinks** — stdout, JSON, CSV, SQLite
- **Dependency-graph execution** — transforms declare explicit dependencies so stages run in the correct order
- **Schema validation** — infer and validate column types before a pipeline runs
- **Run history** — every execution is recorded with row counts, duration, and error details

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      BlackRoad OS Platform                  │
│                                                             │
│  ┌──────────┐    ┌───────────────┐    ┌──────────────────┐  │
│  │  Sources │───▶│  Transforms   │───▶│      Sinks       │  │
│  │          │    │ (dependency   │    │                  │  │
│  │ CSV      │    │  graph order) │    │ stdout           │  │
│  │ JSON     │    │               │    │ JSON file        │  │
│  │ SQLite   │    │ filter        │    │ CSV file         │  │
│  │ inline   │    │ map           │    │ SQLite           │  │
│  └──────────┘    │ aggregate     │    └──────────────────┘  │
│                  │ join          │                           │
│                  │ sort          │    ┌──────────────────┐   │
│                  │ deduplicate   │    │   Run History    │   │
│                  │ select        │    │  (SQLite store)  │   │
│                  │ rename        │    └──────────────────┘   │
│                  └───────────────┘                           │
└─────────────────────────────────────────────────────────────┘
```

All pipeline metadata (definitions, run logs, schemas) is stored in a local **SQLite** database (`~/.blackroad/pipelines.db` by default), making the framework fully self-contained with no external dependencies.

---

## 3. Installation

**Requirements:** Python 3.11 or later.

```bash
# Clone the repository
git clone https://github.com/BlackRoad-Labs/blackroad-data-pipeline.git
cd blackroad-data-pipeline

# (Optional) create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# No additional pip dependencies are required —
# the framework uses only the Python standard library.
```

> **Environment variable** — set `PIPELINE_DB` to override the default database path:
> ```bash
> export PIPELINE_DB=/data/blackroad/pipelines.db
> ```

---

## 4. Quick Start

```python
from main import PipelineService, init_db

init_db()
svc = PipelineService()

# 1. Create a pipeline
pipeline = svc.create_pipeline("Sales Report", description="Daily sales ETL")

# 2. Add an inline data source
svc.add_source(pipeline.id, "raw_sales", "inline", {
    "data": [
        {"region": "west", "revenue": "1200", "rep": "Alice"},
        {"region": "east", "revenue": "800",  "rep": "Bob"},
        {"region": "west", "revenue": "950",  "rep": "Carol"},
    ]
})

# 3. Add a transform — keep only west-region rows
svc.add_transform(pipeline.id, "filter_west", "filter",
                  {"field": "region", "op": "eq", "value": "west"})

# 4. Add a JSON sink
svc.add_sink(pipeline.id, "report", "json", {"path": "sales_report.json"})

# 5. Run
result = svc.run_pipeline(pipeline.id)
print(result)
# {"run_id": "...", "status": "success", "rows_read": 3, "rows_written": 2, "duration_ms": 1}
```

---

## 5. CLI Reference

```bash
python main.py <command> [options]
```

| Command | Description |
|---|---|
| `create "<name>" [--description "<desc>"]` | Create a new pipeline |
| `list` | List all pipelines with status |
| `run <pipeline-id>` | Execute a pipeline |
| `runs <pipeline-id>` | Show the run history for a pipeline |
| `validate <source-id>` | Infer and validate the schema of a data source |

### Examples

```bash
# Create
python main.py create "Customer ETL" --description "Nightly customer sync"

# List all pipelines
python main.py list

# Run a specific pipeline
python main.py run 3f2a1b4c-...

# Inspect run history
python main.py runs 3f2a1b4c-...

# Validate a source schema
python main.py validate 7e9d0c2a-...
```

---

## 6. Sources

Sources define where data is read from. Add them with `PipelineService.add_source(pipeline_id, name, source_type, config)`.

| `source_type` | Required config keys | Description |
|---|---|---|
| `csv` | `path` | Read a delimited text file. Optional: `delimiter` (default `,`) |
| `json` | `path` | Read a JSON file (array or single object) |
| `sqlite` | `database`, `table` | Read a full table. Optional: `query` (overrides `table`) |
| `inline` | `data` | Pass a Python list of dicts directly (ideal for testing) |

### CSV example

```python
svc.add_source(pipeline.id, "orders", "csv", {
    "path": "/data/orders.csv",
    "delimiter": ","
})
```

### SQLite example

```python
svc.add_source(pipeline.id, "users", "sqlite", {
    "database": "/data/app.db",
    "query": "SELECT id, email FROM users WHERE active = 1"
})
```

---

## 7. Transforms

Transforms are applied in `order_index` order. Add them with `PipelineService.add_transform(pipeline_id, name, transform_type, config, depends_on, order_index)`.

### `filter`

Keep rows matching a condition.

| Config key | Values | Description |
|---|---|---|
| `field` | string | Column name |
| `op` | `eq` `ne` `gt` `lt` `contains` `notnull` | Comparison operator |
| `value` | any | Value to compare against |

```python
svc.add_transform(pipeline.id, "active_only", "filter",
                  {"field": "status", "op": "eq", "value": "active"})
```

### `map`

Apply a type-cast or string operation to a column.

| Config key | Values |
|---|---|
| `field` | Column name |
| `operation` | `str` `int` `float` `upper` `lower` `strip` |

```python
svc.add_transform(pipeline.id, "normalise_email", "map",
                  {"field": "email", "operation": "lower"})
```

### `aggregate`

Group rows and compute a summary statistic.

| Config key | Values |
|---|---|
| `group_by` | List of column names |
| `field` | Column to aggregate |
| `function` | `count` `sum` `avg` `max` `min` |

```python
svc.add_transform(pipeline.id, "revenue_by_region", "aggregate", {
    "group_by": ["region"],
    "field": "revenue",
    "function": "sum"
})
```

### `join`

Inner-join the working dataset with a named source.

| Config key | Description |
|---|---|
| `right` | Name of the source to join with |
| `left_key` | Join key in the left dataset |
| `right_key` | Join key in the right dataset |

```python
svc.add_transform(pipeline.id, "enrich", "join", {
    "right": "products",
    "left_key": "product_id",
    "right_key": "id"
})
```

### `sort`

| Config key | Description |
|---|---|
| `field` | Column to sort by |
| `descending` | `true` / `false` (default `false`) |

### `deduplicate`

| Config key | Description |
|---|---|
| `keys` | List of columns that form the unique key (defaults to all columns) |

### `select`

Keep only specific columns.

| Config key | Description |
|---|---|
| `fields` | List of column names to keep |

### `rename`

Rename columns.

| Config key | Description |
|---|---|
| `mapping` | Dict of `{"old_name": "new_name", ...}` |

---

## 8. Sinks

Sinks define where processed data is written. Add them with `PipelineService.add_sink(pipeline_id, name, sink_type, config)`.

| `sink_type` | Required config keys | Description |
|---|---|---|
| `stdout` | _(none)_ | Pretty-print JSON to the terminal |
| `json` | `path` | Write a JSON array to a file |
| `csv` | `path` | Write a CSV file |
| `sqlite` | `database`, `table` | Append rows to a SQLite table (created if absent) |

### JSON sink example

```python
svc.add_sink(pipeline.id, "export", "json", {"path": "/output/result.json"})
```

### SQLite sink example

```python
svc.add_sink(pipeline.id, "warehouse", "sqlite", {
    "database": "/data/warehouse.db",
    "table": "processed_orders"
})
```

---

## 9. Schema Validation

Before running a pipeline you can infer the schema of any source:

```bash
python main.py validate <source-id>
```

```json
{
  "valid": true,
  "rows": 1500,
  "schema": {
    "id":     ["str"],
    "amount": ["str"],
    "region": ["str"]
  }
}
```

Programmatically:

```python
report = svc.validate_schema(source_id)
if not report["valid"]:
    raise RuntimeError(report["error"])
```

---

## 10. Configuration

| Environment variable | Default | Description |
|---|---|---|
| `PIPELINE_DB` | `~/.blackroad/pipelines.db` | Path to the SQLite metadata database |

The database is created automatically on first use. All tables use WAL journal mode for concurrent read access.

---

## 11. Stripe Integration

`blackroad-data-pipeline` is designed to run inside the **BlackRoad OS** platform, which uses **Stripe** for subscription billing and usage metering.

### How pipeline runs are metered

Each successful pipeline execution emits a usage record to Stripe Billing via the BlackRoad OS backend. The metering dimension is **rows written per run**.

| Stripe object | Purpose |
|---|---|
| `SubscriptionItem` + `usage_record` | Per-run row metering |
| `Product` / `Price` (metered) | `blackroad_pipeline_rows` |
| `Customer` | Mapped 1-to-1 to a BlackRoad OS organisation |

### Connecting your Stripe account

Configuration is managed through the BlackRoad OS dashboard — no Stripe keys are stored in this repository. To enable billing:

1. Log in to [blackroad.dev](https://blackroad.dev)
2. Navigate to **Settings → Billing**
3. Connect your Stripe account or activate a BlackRoad OS subscription plan

### Webhook events consumed

| Stripe event | Effect |
|---|---|
| `customer.subscription.deleted` | Pipeline execution is suspended |
| `customer.subscription.updated` | Quota limits are re-evaluated |
| `invoice.payment_failed` | Warning email + grace-period enforcement |

---

## 12. npm Ecosystem

The BlackRoad OS platform exposes a JavaScript/TypeScript SDK (published to npm) that wraps this Python backend via a REST API. The SDK is the recommended integration path for Node.js applications.

### Install the SDK

```bash
npm install @blackroad/pipeline-sdk
# or
yarn add @blackroad/pipeline-sdk
```

### Create and run a pipeline from Node.js

```typescript
import { BlackRoadPipeline } from "@blackroad/pipeline-sdk";

const client = new BlackRoadPipeline({ apiKey: process.env.BLACKROAD_API_KEY });

const pipeline = await client.pipelines.create({
  name: "Node ETL Example",
  sources: [{ name: "data", type: "inline", config: { data: [{ x: 1 }, { x: 2 }] } }],
  sinks:   [{ name: "out",  type: "json",   config: { path: "output.json" } }],
});

const run = await client.pipelines.run(pipeline.id);
console.log(run); // { status: "success", rows_read: 2, rows_written: 2 }
```

> **Note:** The `@blackroad/pipeline-sdk` package and its REST API are maintained in separate repositories within the [BlackRoad-Labs](https://github.com/BlackRoad-Labs) organisation.

---

## 13. Testing

### Unit tests

Run the full unit-test suite (Python 3.11+):

```bash
python -m pytest test_data_pipeline.py -v
```

The test matrix runs automatically on every push and pull request via GitHub Actions (see `.github/workflows/ci.yml`) against **Python 3.11** and **3.12**.

### End-to-end (E2E) tests

E2E tests exercise a complete pipeline execution cycle — from source read through transform to sink write — using temporary files and an isolated in-memory database.

```bash
# Run only E2E-style integration tests (TestPipelineService)
python -m pytest test_data_pipeline.py -v -k "TestPipelineService"
```

Key E2E scenarios covered:

| Test | Source | Transform | Sink |
|---|---|---|---|
| `test_run_pipeline_inline` | inline | _(none)_ | stdout |
| `test_run_pipeline_with_transform` | inline | filter | stdout |
| `test_run_pipeline_csv` | CSV file | _(none)_ | JSON file |

### CI badge

![CI](https://github.com/BlackRoad-Labs/blackroad-data-pipeline/actions/workflows/ci.yml/badge.svg)

All tests must pass before merging to `main`.

---

## 14. License

Copyright © 2024–2026 **BlackRoad OS, Inc.** All rights reserved.
Founder, CEO & Sole Stockholder: Alexa Louise Amundson.

This software is proprietary and confidential. Unauthorised copying, modification, distribution, or use of this software, in whole or in part, is strictly prohibited without the express written consent of BlackRoad OS, Inc.

See the [`LICENSE`](LICENSE) file for full terms.
