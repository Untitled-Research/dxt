# Test Fixtures - Pipeline YAML Files

This directory contains YAML pipeline configurations used for testing DXT's operator loading system.

## Recommended Starting Point

**[`pipeline_example_new_structure.yaml`](pipeline_example_new_structure.yaml)** - Complete example showing the natural pipeline structure
  - Demonstrates clean separation of concerns with top-level `extract:` and `load:` keys
  - Shows pipeline-level defaults and stream-level overrides
  - Includes multiple load strategies (replace, append, upsert)
  - **Start here** to understand DXT's YAML structure

## Files

### Basic Examples

- **`pipeline_with_defaults.yaml`** - Uses default operators (no explicit specification)
  - Demonstrates smart defaults based on connection protocol
  - PostgresExtractor and PostgresLoader are inferred automatically

- **`dvdrental_to_dxt.yaml`** - Original example showing basic pipeline structure
  - Uses default operators
  - Good baseline for testing standard functionality

- **`dvdrental_sql_query.yaml`** - Example using SQL queries as source
  - Uses default operators
  - Tests SQL source type handling

### Operator Specification Examples

- **`pipeline_with_copy_loader.yaml`** - Uses PostgreSQL COPY loader variant
  - Demonstrates operator override at stream level
  - Uses shorter form: `dxt.operators.postgres.PostgresCopyLoader`
  - Shows loader-specific configuration (`copy_format`, `delimiter`)

- **`pipeline_with_pipeline_defaults.yaml`** - Pipeline-level defaults with stream overrides
  - Shows how to set defaults for all streams
  - Demonstrates per-stream overrides
  - Tests config merging (pipeline defaults + stream config)

### Error Handling Examples

- **`pipeline_with_invalid_operator.yaml`** - Non-existent operator specification
  - Tests graceful error handling
  - Should produce descriptive error message
  - Used to verify dynamic loading fails safely

## Pipeline Structure

DXT pipelines use a clear, phase-oriented structure with top-level keys for each pipeline component:

```yaml
version: 1
name: my_pipeline

source:
  connection: "postgresql://..."

target:
  connection: "postgresql://..."

extract:                                    # Pipeline-level extract defaults
  extractor: dxt.operators.sql.SQLExtractor
  batch_size: 10000

load:                                       # Pipeline-level load defaults
  loader: dxt.operators.sql.SQLLoader
  batch_size: 5000

buffer:
  format: parquet
  compression: snappy

streams:
  - id: my_stream
    source: public.table1
    target: staging.table1
```

## Operator Specification Formats

DXT supports two formats for specifying operators:

### 1. Package Re-export (Shorter, Recommended)
```yaml
extractor: dxt.operators.sql.SQLExtractor
loader: dxt.operators.postgres.PostgresCopyLoader
```

### 2. Full Module Path
```yaml
extractor: dxt.operators.sql.extractor.SQLExtractor
loader: dxt.operators.postgres.copy_loader.PostgresCopyLoader
```

Both work because operators are re-exported in package `__init__.py` files.

## Running Tests

These fixtures are used by:
- `tests/test_operator_loading.py` - Unit tests for operator resolution
- Future integration tests for end-to-end pipeline execution

## Adding New Fixtures

When adding new test fixtures:
1. Use descriptive names indicating what they test
2. Include comments explaining the test scenario
3. Document any expected errors or special behaviors
4. Update this README
