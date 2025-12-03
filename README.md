# DXT — The YAML-first Data Move Tool
*Pronounced: "dee-ex-tee"*

> **DXT is a lightweight Extract–Load tool for building simple, reliable batch data movement pipelines.**  
> Write a single YAML file that defines an extract and a load step, and run it anywhere with a clean, fast CLI.

DXT gives data engineers a predictable, extensible, Git-native way to move data between databases, APIs, files, and cloud warehouses — without operating a control plane or writing yet another custom Python script.

---

## 🚀 Features

- **YAML-first pipelines** — define your extract and load declaratively  
- **CLI-first design** — run pipelines from the command line or any orchestrator  
- **Extensible** — create custom extractors/loaders with minimal Python  
- **Portable** — works anywhere Python runs (local, cron, Airflow, Prefect, GitHub Actions, on-prem)  
- **Incremental loads** — simple watermark & key-based incremental patterns  
- **Load strategies** — append, truncate-insert, merge (planned)  
- **Zero platform overhead** — no UI, no scheduler, no metadata DB  
- **Engineer-friendly** — CLI-first, Git-native, orchestration-agnostic  

If you’ve ever written a one-off Python script to copy data from A to B, DXT replaces that with a clean YAML pipeline and a battle-tested execution engine.

---

## Design Philosophy

DXT aims to occupy the gap between:

“I’ll write another custom Python script…”
and
“Let’s deploy Airbyte / a managed ELT platform.”

Principles:

- Simplicity over magic
- Declarative over imperative
- Small surface area
- No servers
- Do one thing well: move data

If dbt is the “data build tool”, DXT is the data move tool.

## 📦 Installation

```bash
pip install dxt

# For PostgreSQL support
pip install dxt[postgres]
```

## 🚀 Quick Start

### 1. Create a pipeline YAML file

```yaml
# my_pipeline.yaml
version: 1
name: my_first_pipeline
description: "Move customer data from source to target"

source:
  connection: "postgresql://${DB_USER}:${DB_PASS}@localhost:5432/source_db"

target:
  connection: "postgresql://${DB_USER}:${DB_PASS}@localhost:5432/target_db"

buffer:
  format: parquet
  compression: snappy

streams:
  # Simple table extraction with shorthand
  - id: customers
    source: public.customers
    target: analytics.customers
    extract:
      mode: full
      batch_size: 10000
    load:
      mode: append
      batch_size: 10000

  # SQL query with explicit configuration
  - id: active_orders
    source:
      type: sql
      sql: |
        SELECT o.*, c.name as customer_name
        FROM public.orders o
        JOIN public.customers c ON o.customer_id = c.id
        WHERE o.status = 'active'
    target: analytics.active_orders
    extract:
      mode: full
    load:
      mode: replace
```

### 2. Run the pipeline

```bash
# Validate the pipeline
dxt validate my_pipeline.yaml

# Run the pipeline
dxt run my_pipeline.yaml

# Run with options
dxt run my_pipeline.yaml --verbose
dxt run my_pipeline.yaml --select customers
dxt run my_pipeline.yaml --dry-run
```

## 📖 Pipeline Configuration

### Source and Target References

DXT supports flexible source and target references with smart type inference:

**Shorthand (recommended for simple cases):**
```yaml
streams:
  - id: orders
    source: public.orders      # Inferred as database relation
    target: warehouse.orders
```

**Explicit SQL queries:**
```yaml
streams:
  - id: filtered_data
    source:
      type: sql
      sql: "SELECT * FROM orders WHERE created_at > '2024-01-01'"
    target: staging.filtered_data
```

**Type inference rules:**
- Starts with `SELECT` or `WITH` → SQL query
- Contains `/` or file extension → File path
- Starts with `http://` → HTTP endpoint
- Default → Database relation (table/view)

### Environment Variables

Use `${VAR_NAME}` or `${VAR_NAME:-default}` syntax for environment variables:

```yaml
source:
  connection: "postgresql://${DB_USER}:${DB_PASS}@${DB_HOST:-localhost}:5432/${DB_NAME}"
```

### Extract Modes

- `full`: Extract all records (default)
- `incremental`: Extract only new/updated records (requires watermark field)

### Load Modes

- `append`: Append records to target (default)
- `replace`: Truncate target before loading
- `upsert`: Update existing records, insert new ones (requires upsert_keys)

## 🤝 Contributing

DXT is designed to be open, simple, and community-friendly.

Ways to contribute:

- Create a new adapter (database, file, API, cloud service)
- Improve docs and examples
- Add validator rules for YAML schema
- Build testing fixtures
- Submit ideas for features

PRs are welcome!

## 📄 License

MIT License.
