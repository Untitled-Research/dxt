# DXT Examples

This directory contains example scripts demonstrating how to use DXT programmatically.

## Available Examples

### [postgres_example.py](postgres_example.py)

End-to-end PostgreSQL example demonstrating:
- **Full load (replace mode)** - Complete table replacement
- **Filtered load with criteria** - Loading subset of data based on WHERE clause
- **Upsert load mode** - Insert or update based on key columns

**Prerequisites**:
- PostgreSQL running (provided by devcontainer)
- Database connection available at `postgresql+psycopg://postgres:postgres@postgres:5432/postgres`

**Usage**:
```bash
python examples/postgres_example.py
```

The script will:
1. Create test tables with sample data
2. Execute three different pipeline patterns
3. Verify results and display statistics

## YAML Pipeline Examples

YAML pipeline examples have been moved to [tests/fixtures/](../tests/fixtures/) where they serve as test artifacts. See [tests/fixtures/README.md](../tests/fixtures/README.md) for documentation.

## Test Suites

Test suites have been moved to the [tests/](../tests/) directory:
- [test_core_framework.py](../tests/test_core_framework.py) - Core framework validation tests
- [test_operator_loading.py](../tests/test_operator_loading.py) - Dynamic operator loading tests
- [test_sqlite_operators.py](../tests/test_sqlite_operators.py) - SQLite operator tests
