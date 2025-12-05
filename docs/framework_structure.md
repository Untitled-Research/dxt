# DXT Core Framework Structure

## Overview

This document describes the implemented core framework structure and organization.

## Directory Structure

```
dxt/
├── __init__.py                     # Package exports for user-facing API
├── cli.py                          # Typer CLI entry point (existing)
├── exceptions.py                   # Custom exception hierarchy
│
├── models/                         # Pydantic models (YAML schemas)
│   ├── __init__.py                 # Export all models
│   ├── field.py                    # Field, SourceField, TargetField, DXTType
│   ├── stream.py                   # Stream, ExtractConfig, LoadConfig
│   ├── pipeline.py                 # Pipeline, ConnectionConfig, BufferConfig
│   └── results.py                  # ExtractResult, LoadResult, RunResult
│
├── core/                           # Abstract base classes
│   ├── __init__.py                 # Export core ABCs
│   ├── connector.py                # Base Connector ABC
│   ├── extractor.py                # Base Extractor ABC
│   ├── loader.py                   # Base Loader ABC
│   ├── buffer.py                   # Base Buffer ABC
│   └── type_mapper.py              # Base TypeMapper ABC
│
└── buffers/                        # Buffer implementations
    ├── __init__.py                 # Export buffer types
    ├── memory.py                   # MemoryBuffer
    └── parquet.py                  # ParquetBuffer
```

## Module Descriptions

### `dxt/exceptions.py`

Custom exception hierarchy for DXT:
- `DXTError` - Base exception
- `ConfigurationError` - Invalid configuration
- `ConnectionError` - Connection failures
- `ConnectorError` - Connector operation failures
- `ExtractorError` - Extraction failures
- `LoaderError` - Load failures
- `BufferError` - Buffer operation failures
- `ValidationError` - Validation failures
- `SchemaError` - Schema operation failures
- `TypeMappingError` - Type conversion failures
- `PipelineExecutionError` - Pipeline execution failures
- `SelectorError` - Selector parsing/matching failures

### `dxt/models/` - User-Facing Data Models

#### `field.py`
- `DXTType` - Enum of internal types (INT64, STRING, TIMESTAMP, etc.)
- `SourceField` - Source-side field configuration
- `TargetField` - Target-side field configuration
- `Field` - Complete field metadata with progressive complexity support

#### `stream.py`
- `ExtractConfig` - Extract phase configuration (mode, watermark, batch size)
- `LoadConfig` - Load phase configuration (mode, upsert keys, batch size)
- `Stream` - Dataset to transfer with source, target, and config

#### `pipeline.py`
- `ConnectionConfig` - Connection configuration
- `BufferConfig` - Buffer configuration (format, compression, persistence)
- `Pipeline` - Complete pipeline with source, target, streams

#### `results.py`
- `ExtractResult` - Extraction metrics and metadata
- `LoadResult` - Load metrics and metadata
- `StreamResult` - Combined extract + load result
- `RunResult` - Complete pipeline run result

### `dxt/core/` - Abstract Base Classes

#### `connector.py`
- `Connector` - Base class for managing connections
  - `connect()` - Establish connection
  - `disconnect()` - Close connection
  - `test_connection()` - Test connectivity
  - `get_schema()` - Get field definitions
  - `execute_query()` - Execute queries
  - Context manager support

#### `extractor.py`
- `Extractor` - Base class for extracting data
  - `extract()` - Extract records to buffer
  - `build_query()` - Build extraction query
  - `get_stream_fields()` - Get source schema
  - `validate_stream()` - Validate stream config

#### `loader.py`
- `Loader` - Base class for loading data
  - `load()` - Load records from buffer
  - `create_target_stream()` - Create target table/collection
  - `get_max_watermark()` - Get max watermark for incremental
  - `_load_append()` - Append mode implementation
  - `_load_replace()` - Replace mode implementation
  - `_load_upsert()` - Upsert mode implementation

#### `buffer.py`
- `Buffer` - Base class for temporary storage
  - `write_batch()` - Write records to temp location
  - `finalize()` - Close file handles, flush buffers
  - `commit()` - Atomically make writes visible
  - `rollback()` - Discard writes
  - `read_batches()` - Read records in batches
  - `exists()` - Check if data exists
  - `cleanup()` - Remove buffer and release resources
  - Transactional semantics: write → finalize → commit → cleanup

#### `type_mapper.py`
- `TypeMapper` - Base class for type conversions
  - `from_source()` - Convert source type to DXT type
  - `to_target()` - Convert DXT type to target type
  - `normalize_source_type()` - Normalize type strings
  - `get_python_type()` - Get Python type for DXT type

### `dxt/buffers/` - Buffer Implementations

#### `memory.py`
- `MemoryBuffer` - In-memory storage
  - No serialization
  - Suitable for small datasets (< 100K records)
  - Testing and development
  - Not persistent

#### `parquet.py`
- `ParquetBuffer` - Parquet-based storage (default)
  - Type-preserving via PyArrow
  - Excellent compression (5-10x vs CSV)
  - Columnar format
  - Industry standard
  - Suitable for production

## Import Patterns

### User API (External)

```python
# Import from dxt package root
from dxt import (
    # Models
    Field, DXTType, Stream, Pipeline,
    ExtractConfig, LoadConfig,

    # Results
    ExtractResult, LoadResult, RunResult,

    # Buffers
    MemoryBuffer, ParquetBuffer,

    # For custom operators
    Connector, Extractor, Loader, Buffer, TypeMapper,
)
```

### Internal Imports (Within DXT)

```python
# Core ABCs
from dxt.core import Connector, Extractor, Loader

# Models
from dxt.models import Stream, Field, ExtractConfig

# Exceptions
from dxt.exceptions import ExtractorError, BufferError
```

## Design Principles

### 1. Separation of Concerns
- **models/** - Pure data structures (Pydantic)
- **core/** - Abstract interfaces and contracts
- **buffers/** - Concrete buffer implementations
- **operators/** - (Future) System-specific implementations

### 2. Progressive Complexity
- Simple cases require minimal configuration
- Advanced features opt-in via config
- Field mapping: string shorthand → full object

### 3. Type Safety
- Pydantic for all models
- Type hints everywhere
- Clear validation errors

### 4. Composition Over Inheritance
- Extractors and Loaders compose Connectors
- Enables connector reuse
- Cleaner separation

### 5. Stateless Design
- No external state files
- Watermark queried from target at runtime
- Simpler architecture

### 6. Iterator/Generator Pattern
- Extract returns generators
- Buffer reads in batches
- Never load full dataset into memory

## Next Steps

### Phase 2: PostgreSQL Operator
1. Create `dxt/operators/postgres/` package
2. Implement PostgresConnector (SQLAlchemy)
3. Implement PostgresExtractor
4. Implement PostgresLoader
5. Implement PostgresTypeMapper

### Phase 3: Pipeline Runner
1. Create `dxt/core/pipeline_runner.py`
2. Orchestrate extract → buffer → load flow
3. Handle errors and rollbacks
4. Integrate with CLI

### Phase 4: Utilities
1. Create `dxt/utils/` package
2. Implement logging setup (structlog)
3. Implement YAML parser
4. Implement environment variable substitution

## Dependencies

Core dependencies added to `pyproject.toml`:
- `sqlalchemy>=2.0.0` - SQL database support
- `pyarrow>=14.0.0` - Parquet buffer support
- `rich>=13.0.0` - CLI output and progress bars
- `structlog>=24.0.0` - Structured logging

## Testing

A test script is available at [examples/test_core_framework.py](../examples/test_core_framework.py) that demonstrates:
- Field model creation
- MemoryBuffer write/read cycle
- Stream configuration
- Pipeline configuration

Run with:
```bash
python examples/test_core_framework.py
```

## Status

✅ **Phase 1 Complete**: Core framework and primitives implemented
- All base ABCs defined
- All Pydantic models implemented
- MemoryBuffer and ParquetBuffer working
- Exception hierarchy established
- Dependencies configured
- Test suite passing
