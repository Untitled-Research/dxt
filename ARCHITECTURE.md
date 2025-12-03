# DXT Architecture Design v3

## Overview

DXT is built around a simple, extensible, **stateless** architecture with well-defined primitives that enable reliable batch data movement between systems.

**Core Principle:** No external state - all state is derived from the data systems themselves (source/target).

**Nomenclature:** DXT uses generic terminology that works across SQL databases, NoSQL systems, REST APIs, and files:
- **Stream**: A dataset to transfer (SQL table, MongoDB collection, API endpoint, CSV file)
- **Record**: A single data item (SQL row, NoSQL document, JSON object, CSV line)
- **Field**: A property in a record (SQL column, JSON key, CSV column)
- **Buffer**: Temporary storage between extract and load (memory or disk)

## Core Primitives

### 1. Connector (Connection Management)

Manages connection to a data system. Separated from extraction/loading logic for reusability.

```python
class Connector(ABC):
    """Base class for managing connections to data systems."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data system."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data system."""

    @abstractmethod
    def test_connection(self) -> bool:
        """Test connectivity."""

    @abstractmethod
    def get_schema(self, ref: str) -> Schema:
        """Get schema for a table/view/resource."""

    @abstractmethod
    def execute_query(self, query: str) -> list[dict]:
        """Execute a query and return results."""

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()
```

**Key Design Points:**
- Context manager for connection lifecycle
- Reusable by both Extractor and Loader
- Configuration via Pydantic models
- System-agnostic interface

### 2. Extractor

Reads data from a source system and writes records to a Buffer. Composes a Connector.

```python
class Extractor(ABC):
    """Extract records from a source system to a buffer."""

    def __init__(self, connector: Connector, config: dict):
        self.connector = connector
        self.config = config

    @abstractmethod
    def extract(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> ExtractResult:
        """
        Extract records from source to buffer.

        The Stream.extract contains the extract mode and parameters.
        For incremental extraction, the extractor should:
        1. Query target (via separate loader) for max watermark if needed
        2. Build appropriate query with WHERE clause
        3. Stream records to buffer in batches

        Handles different stream kinds:
        - kind="table": Build SELECT query from table reference, apply criteria if present
        - kind="query": Use source_ref as literal SQL query
        - kind="view": Same as table
        - kind="file": Read from file path
        - kind="api": Call API endpoint
        """

    def get_stream_fields(self, stream_ref: str) -> list[Field]:
        """Get field definitions from source system."""
        return self.connector.get_stream_fields(stream_ref)

    def build_query(self, stream: Stream, watermark_value: Optional[Any] = None) -> str:
        """
        Build extraction query based on stream kind and configuration.

        For kind="table":
          - Start with SELECT * FROM {source_ref}
          - Apply criteria WHERE clauses if present
          - Add watermark filter if incremental
          - Apply system-specific optimizations

        For kind="query":
          - Return source_ref as-is (literal SQL)
          - Watermark filtering not supported (use WHERE in query)

        For kind="view":
          - Same as table
        """
```

**Extract Strategies (defined in ExtractConfig):**
- `full`: Complete stream extraction
- `incremental`: Based on watermark field (timestamp/sequence/id)

**Stream Kind Handling:**
- `table`: Build SELECT query from table name, apply criteria filters
- `query`: Use source_ref as literal SQL (no criteria or watermark applied)
- `view`: Treat as table
- `file`: Read from file path (CSV, JSON, Parquet, etc.)
- `api`: Call REST/GraphQL endpoint

**Design Principles:**
- Composition over inheritance (uses Connector)
- Generator/iterator pattern for memory efficiency
- Configurable batch sizes
- Watermark field drives incremental logic
- Stream kind determines query building approach

### 3. Loader

Reads records from a Buffer and writes to a target system. Composes a Connector.

```python
class Loader(ABC):
    """Load records from a buffer to a target system."""

    def __init__(self, connector: Connector, config: dict):
        self.connector = connector
        self.config = config

    @abstractmethod
    def load(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> LoadResult:
        """
        Load records from buffer to target.

        The Stream.load contains the load mode and parameters.
        Responsibilities:
        1. Optionally create/alter target stream
        2. Execute appropriate load mode
        3. Use bulk load methods where available
        4. Return detailed results
        """

    def get_stream_fields(self, stream_ref: str) -> Optional[list[Field]]:
        """Get field definitions from target system (None if doesn't exist)."""
        return self.connector.get_stream_fields(stream_ref)

    def create_target_stream(self, stream_ref: str, fields: list[Field]) -> None:
        """Create target stream/table/collection from field definitions."""
        # Implementation uses connector's SQL builder or native API

    def get_max_watermark(self, stream_ref: str, watermark_field: str) -> Optional[Any]:
        """Query target for max watermark value (for incremental loads)."""
        query = f"SELECT MAX({watermark_field}) FROM {stream_ref}"
        result = self.connector.execute_query(query)
        return result[0][0] if result and result[0][0] else None
```

**Load Modes (defined in LoadConfig):**
- `append`: Insert new records (default)
- `replace`: Truncate stream then insert
- `upsert`: Update/insert based on upsert_keys

**Stream Creation Convention:**
- Target is **always created if it doesn't exist** (using stream.fields)
- Stream creation uses connector's capabilities (SQL builder or native API)
- Schema validation is strict by default (fail if schemas don't match)

**Design Principles:**
- Composition over inheritance (uses Connector)
- Bulk loading preferred (COPY, BULK INSERT, cloud-native methods)
- Transaction support with rollback
- Stream operations configurable per stream

### 4. Buffer & Data Flow

Temporary storage between extract and load operations with transactional semantics.

#### Buffer Interface

```python
from abc import ABC, abstractmethod
from typing import Iterator, Optional
from pathlib import Path

class Buffer(ABC):
    """Abstract base class for all buffer implementations."""

    def __init__(
        self,
        fields: list[Field],
        location: Optional[Path] = None,
        **kwargs
    ):
        """
        Initialize buffer with schema.

        Args:
            fields: List of Field definitions (schema is required)
            location: Path for persistent buffers, None for memory
            **kwargs: Buffer-specific options
        """
        self.fields = fields
        self.location = location
        self._finalized = False

    @abstractmethod
    def write_batch(self, records: list[dict]) -> None:
        """
        Write a batch of records to temporary location.

        Args:
            records: List of dicts with field.id as keys, Python native values
                    (int, float, Decimal, str, bool, datetime, date, bytes)

        Raises:
            BufferError: If write fails
        """

    @abstractmethod
    def finalize(self) -> None:
        """
        Finalize writing - close file handles, flush buffers.
        Must be called after all writes before reading.
        """

    @abstractmethod
    def commit(self) -> None:
        """
        Atomically make writes visible (rename temp → final).
        For persistent buffers: atomic filesystem rename.
        For memory buffers: no-op.
        """

    @abstractmethod
    def rollback(self) -> None:
        """
        Discard all writes (delete temp files).
        Called on extraction failure to prevent partial data.
        """

    @abstractmethod
    def read_batches(self, batch_size: int = 10000) -> Iterator[list[dict]]:
        """
        Read records in batches as an iterator.

        Args:
            batch_size: Number of records per batch

        Yields:
            Lists of dicts with field.id as keys, Python native values
            Types are preserved from write (int stays int, datetime stays datetime)

        Raises:
            BufferError: If read fails or not finalized/committed
        """

    @abstractmethod
    def exists(self) -> bool:
        """Check if buffered data exists (committed)."""

    @abstractmethod
    def cleanup(self) -> None:
        """Remove buffered data and release resources."""

    @property
    @abstractmethod
    def record_count(self) -> int:
        """Number of records in buffer."""

    @property
    def is_persistent(self) -> bool:
        """Whether buffer persists to disk."""
        return self.location is not None
```

#### Buffer Lifecycle

```python
# Extraction phase
try:
    for batch in extractor.extract_batches(stream):
        buffer.write_batch(batch)  # Write to temp location
    buffer.finalize()  # Close file handles
    buffer.commit()    # Atomic rename temp → final
except Exception as e:
    buffer.rollback()  # Delete temp files
    raise

# Load phase
try:
    for batch in buffer.read_batches(10000):
        loader.load_batch(batch)
    buffer.cleanup()  # Remove buffer after successful load
except Exception as e:
    # Buffer remains for debugging/retry
    raise
```

#### Supported Buffer Types

**1. ParquetBuffer (Recommended Default)**
```python
class ParquetBuffer(Buffer):
    """Parquet buffer - columnar, compressed, typed via PyArrow."""

    def __init__(self, fields, location, compression="snappy", **kwargs):
        super().__init__(fields, location, **kwargs)
        self.compression = compression
        self.schema = self._build_arrow_schema(fields)
        self.temp_location = location.with_suffix('.tmp')
        self.final_location = location

    def write_batch(self, records: list[dict]) -> None:
        """Python dicts → Arrow Table → Parquet (temp file)."""
        table = pa.Table.from_pylist(records, schema=self.schema)
        self.writer.write_table(table)

    def commit(self) -> None:
        """Atomic rename: temp.parquet.tmp → temp.parquet."""
        self.temp_location.rename(self.final_location)

    def read_batches(self, batch_size) -> Iterator[list[dict]]:
        """Parquet → Arrow Table → Python dicts (types preserved)."""
        for batch in pq.ParquetFile(self.final_location).iter_batches(batch_size):
            yield batch.to_pylist()
```

**Why Parquet?**
- ✅ Native Python ↔ Arrow conversion (C++ optimized, near zero-copy)
- ✅ SQLAlchemy compatibility (same Python types: int, Decimal, datetime, str, bool)
- ✅ Type preservation (int64 stays int64, not string)
- ✅ Excellent compression (5-10x smaller than CSV)
- ✅ Columnar format (fast for analytics)
- ✅ Schema embedded in metadata
- ✅ Industry standard (works with DuckDB, Polars, Pandas, Spark)

**2. MemoryBuffer**
```python
class MemoryBuffer(Buffer):
    """In-memory buffer - no persistence, Python objects as-is."""

    def __init__(self, fields, **kwargs):
        super().__init__(fields, location=None, **kwargs)
        self._records = []

    def write_batch(self, records: list[dict]) -> None:
        """Append to in-memory list (no serialization)."""
        self._records.extend(records)

    def commit(self) -> None:
        """No-op for memory buffer."""
        pass

    def read_batches(self, batch_size) -> Iterator[list[dict]]:
        """Yield slices (no deserialization)."""
        for i in range(0, len(self._records), batch_size):
            yield self._records[i:i+batch_size]
```

**When to use:**
- Small datasets (< 100K records)
- Testing/development
- Single run (no separate extract/load)

**3. FlatFileBuffer (CSV/TSV)**
```python
class FlatFileBuffer(Buffer):
    """Flat file buffer with delimiter (CSV, TSV, etc)."""

    def __init__(self, fields, location, delimiter=",", **kwargs):
        super().__init__(fields, location, **kwargs)
        self.delimiter = delimiter

    def write_batch(self, records: list[dict]) -> None:
        """Python objects → string serialization → CSV."""
        # datetime → ISO 8601, Decimal → str, bool → "true"/"false"

    def read_batches(self, batch_size) -> Iterator[list[dict]]:
        """CSV strings → type coercion → Python objects."""
        # Uses field.dtype for type conversion
```

**When to use:**
- Human readability required
- Integration with legacy systems
- Simple debugging

**Trade-offs:**
- ❌ Everything serialized to strings (type info lost in format)
- ❌ Larger file size
- ⚠️ Deserialization overhead on read

**4. JSONLBuffer (JSON Lines)**
```python
class JSONLBuffer(Buffer):
    """JSON Lines buffer - one JSON object per line."""

    def write_batch(self, records: list[dict]) -> None:
        """Python dicts → JSON serialization → one line per record."""
        # datetime → ISO 8601, Decimal → str

    def read_batches(self, batch_size) -> Iterator[list[dict]]:
        """JSON lines → Python objects with type coercion."""
        # Uses field.dtype for temporal and decimal types
```

**When to use:**
- Semi-structured data
- JSON-native systems
- Simple streaming format

#### Type System Integration

**Records always use Python native types:**
```python
# What flows through the buffer (Python objects)
record = {
    "customer_id": 12345,              # Python int
    "email": "user@example.com",       # Python str
    "created_at": datetime(2025, 1, 1) # Python datetime
    "is_active": True,                 # Python bool
    "balance": Decimal("99.95"),       # Python Decimal
}
```

**Type preservation by buffer:**
- **Parquet**: ✅ Perfect (Arrow schema preserves all types)
- **Memory**: ✅ Perfect (no serialization)
- **CSV**: ⚠️ Lossy (serialized to strings, type-coerced on read)
- **JSONL**: ⚠️ Partial (JSON types preserved, temporal/decimal coerced)

**Type conversion flow:**
```python
# EXTRACT
postgres_result = conn.execute("SELECT customer_id, balance, created_at ...")
# SQLAlchemy automatically converts:
#   PostgreSQL BIGINT → Python int
#   PostgreSQL NUMERIC(10,2) → Python Decimal
#   PostgreSQL TIMESTAMP → Python datetime

record = dict(row._mapping)  # Native Python types

# BUFFER WRITE
buffer.write_batch([record])
# Parquet: Arrow handles Python → Arrow type conversion (optimized)
# CSV: Manual serialization to strings
# Memory: No conversion (stores Python objects as-is)

# BUFFER READ
batch = next(buffer.read_batches(10000))
# Parquet: Arrow handles Arrow → Python type conversion (optimized)
# CSV: Manual parsing and type coercion using field.dtype
# Memory: No conversion (returns Python objects as-is)

# LOAD
loader.load_batch(batch)
# SQLAlchemy/native SDK automatically converts:
#   Python int → Snowflake NUMBER(19,0)
#   Python Decimal → Snowflake NUMBER(10,2)
#   Python datetime → Snowflake TIMESTAMP_NTZ
```

**Design Principles:**
- Buffer always requires schema (list[Field])
- Records are Python dicts with field.id keys and native Python values
- Type conversion only at boundaries (source/target), not in buffer
- Parquet default for best performance and type preservation
- Iterator pattern throughout (memory efficient)
- Transactional semantics via write → finalize → commit → cleanup
- Separate extract/load only works with persistent buffers

### 5. Stream Model

**Stream** represents a dataset to transfer with its extract and load configuration. This model maps directly to the YAML structure.

```python
from dataclasses import dataclass, field
from typing import Optional, Any, List

@dataclass
class Stream:
    """A stream of records to extract and load (maps directly to YAML)."""

    # Identity
    id: str  # Unique identifier within pipeline
    source: str  # Source reference: table name, query, file path, API endpoint
    target: str  # Target reference: table/collection/file
    kind: str = "table"  # "table", "query", "view", "file", "api"
    tags: List[str] = field(default_factory=list)  # For selector filtering

    # Query building (for kind="table" only)
    criteria: Optional[dict[str, Any]] = None  # {"where": "status = 'active'"}

    # Schema (optional, can auto-detect)
    fields: Optional[List["Field"]] = None

    # Extract configuration
    extract: "ExtractConfig" = field(default_factory=lambda: ExtractConfig())

    # Load configuration
    load: "LoadConfig" = field(default_factory=lambda: LoadConfig())


@dataclass
class ExtractConfig:
    """Extract phase configuration."""

    mode: str = "full"  # "full" | "incremental"
    batch_size: int = 10000

    # Incremental extraction
    watermark_field: Optional[str] = None
    watermark_type: Optional[str] = None  # "timestamp" | "integer" | "date"

    # System-specific options (API keys, pagination, file encoding, etc.)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class LoadConfig:
    """Load phase configuration."""

    mode: str = "append"  # "append" | "replace" | "upsert"
    batch_size: int = 10000

    # Keys for upsert mode
    upsert_keys: List[str] = field(default_factory=list)  # Required for upsert

    # System-specific options (Snowflake stage, BigQuery dataset, etc.)
    options: dict[str, Any] = field(default_factory=dict)
```

**Design Principles:**
- Stream model maps directly to YAML (no internal DTOs)
- Configuration organized by phase: `extract:` and `load:` sections
- Both use `mode` field for consistency (extract.mode, load.mode)
- Field mapping derived from Field definitions (not configured separately)
- `options` dict for system-specific configuration
- Progressive disclosure: defaults work for simple cases
- Tags enable selector filtering

**Conventions (implicit behavior):**
- Target table is **always created if missing** (using stream.fields)
- Schema validation is **strict by default** (fail if schemas don't match)
- Schema evolution deferred to v2 (users can manually ALTER TABLE if needed)
- Extract and load modes are **orthogonal** (any combination is valid)

### 6. Field & Type System

Field definition for a record property with progressive complexity support.

#### Field Model

```python
from typing import Union, Optional, Any
from dataclasses import dataclass, field

@dataclass
class Field:
    """Field metadata for a stream."""

    # Identity (required, immutable)
    id: str  # Internal identifier - used in buffer, logs, CLI references
             # This is the immutable logical name for this field

    # Internal type (used in buffer) - DXT internal type
    dtype: Union[DXTType, str]  # e.g., DXTType.INT64, DXTType.TIMESTAMP, "int64"

    # Source mapping (optional)
    # Can be: str (column name), dict (full config), or None (defaults to id)
    source: Optional[Union[str, "SourceField"]] = None

    # Target mapping (optional)
    # Can be: str (column name), dict (full config), or None (defaults to id)
    target: Optional[Union[str, "TargetField"]] = None

    # Type metadata
    nullable: bool = True
    precision: Optional[int] = None  # For DECIMAL types
    scale: Optional[int] = None      # For DECIMAL types

    # Extensibility
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceField:
    """Source-side field configuration."""

    expression: str  # Column name or SQL expression
    dtype: Optional[str] = None  # Source system's native type (e.g., "INTEGER", "VARCHAR(255)")


@dataclass
class TargetField:
    """Target-side field configuration."""

    name: str  # Target column name
    dtype: Optional[str] = None  # Target system's native type (e.g., "BIGINT", "timestamp_ntz(0)")
    nullable: Optional[bool] = None  # Override field-level nullable if specified
```

#### DXT Type System

DXT uses a well-defined internal type system (inspired by Apache Arrow/Parquet):

```python
from enum import Enum

class DXTType(str, Enum):
    """DXT internal data types - used in buffer and for type mapping."""

    # Numeric
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    DECIMAL = "decimal"  # Arbitrary precision

    # String
    STRING = "string"

    # Boolean
    BOOL = "bool"

    # Temporal
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"

    # Binary
    BINARY = "binary"

    # Complex (JSON)
    JSON = "json"

    # Collections (for NoSQL - future)
    ARRAY = "array"
    OBJECT = "object"
```

**Type Flow:**
```
Source System Types (PostgreSQL INTEGER, Snowflake NUMBER, etc.)
         ↓ [TypeMapper.from_source]
DXT Internal Types (int64, float64, timestamp, string, etc.)
         ↓ [Buffer storage with schema]
DXT Internal Types (preserved from buffer)
         ↓ [TypeMapper.to_target]
Target System Types (Snowflake BIGINT, MongoDB BSON types, etc.)
```

**Design Principles:**
- `id` is the immutable internal identifier (used in buffer, logs, CLI)
- `dtype` is the DXT internal type (how data flows through the system)
- `source` and `target` are optional mappings for source/target systems
- Progressive disclosure: simple cases need only `id`, complex cases use full config
- Type conversion happens at boundaries (extract and load), not in buffer
- Buffer always stores typed Python objects (int, Decimal, datetime, str, bool)

#### Progressive Complexity Examples

**Level 1: Simplest - Just ID**
```yaml
fields:
  - id: customer_id
  - id: email
  - id: created_at
```
- Source column: same as `id`
- Target column: same as `id`
- Types: inferred from source, used as-is for target

**Level 2: Specify Internal Type**
```yaml
fields:
  - id: order_total
    dtype: decimal
    precision: 10
    scale: 2
```
- Explicit DXT internal type with precision

**Level 3: Simple String Mapping**
```yaml
fields:
  # Source rename
  - id: billing_date
    source: "[Billing Date]"

  # Target rename
  - id: customer_email
    target: email

  # Both
  - id: order_total
    source: "Total Amount"
    target: total
```
- String shorthand for simple renames

**Level 4: Full Configuration**
```yaml
fields:
  # Source with type documentation
  - id: billing_date
    dtype: timestamp
    source:
      expression: "[Billing Date]"
      dtype: DATETIME  # PostgreSQL source type
    target:
      name: bill_date
      dtype: timestamp_ntz(0)  # Snowflake target type

  # SQL expression as source
  - id: order_year
    dtype: int64
    source:
      expression: "YEAR(order_date)"
      dtype: INTEGER
    target:
      name: year
      dtype: INT

  # Framework-generated field (no source)
  - id: load_timestamp
    dtype: timestamp_tz
    target:
      name: loaded_at
      dtype: timestamp_ntz(0)
    metadata:
      generated_by: dxt
      generator: current_timestamp
```

**Python Usage:**
```python
# Simple
Field(id="customer_id", dtype=DXTType.INT64)

# With source/target mapping
Field(
    id="billing_date",
    dtype=DXTType.TIMESTAMP,
    source=SourceField(expression="[Billing Date]", dtype="DATETIME"),
    target=TargetField(name="bill_date", dtype="timestamp_ntz(0)")
)

# Framework-generated
Field(
    id="load_timestamp",
    dtype=DXTType.TIMESTAMP_TZ,
    target=TargetField(name="loaded_at", dtype="timestamp_tz"),
    metadata={"generated_by": "dxt"}
)
```

### 7. Pipeline

Declarative YAML definition of a data movement job.

**Structure:**
```yaml
# pipeline.yaml
version: 1
name: postgres_to_snowflake
description: "Move sales data from Postgres to Snowflake"

source:
  connection: postgres_prod  # References dxt_project.yaml

target:
  connection: snowflake_warehouse  # References dxt_project.yaml

buffer:
  format: parquet           # Optional, defaults from project
  compression: snappy       # Optional
  persist: true             # Persist to disk (enables separate extract/load)

streams:
  # Table source with criteria and field definitions
  - id: orders
    kind: table
    source: public.orders
    target: analytics.orders
    tags: [critical, daily]
    criteria:
      where: "status = 'active'"
    fields:
      # Simple field (same name everywhere)
      - id: order_id
        dtype: int64

      # Field with source rename
      - id: order_date
        dtype: timestamp
        source: created_at

      # Framework-generated field (no source)
      - id: load_time
        dtype: timestamp_tz
        target:
          name: loaded_at
          dtype: timestamp_ntz(0)
        metadata:
          generated_by: dxt

    extract:
      mode: incremental
      watermark_field: order_date
      watermark_type: timestamp
      batch_size: 10000

    load:
      mode: upsert
      upsert_keys: [order_id]
      batch_size: 5000

  # Query source (custom SQL)
  - id: order_summary
    kind: query
    source: "SELECT order_id, SUM(total) as total FROM orders GROUP BY order_id"
    target: analytics.order_summary
    tags: [aggregated, daily]
    fields:
      - id: order_id
        dtype: int64
      - id: total
        dtype: decimal
        precision: 10
        scale: 2

    extract:
      mode: full

    load:
      mode: replace

  # Simple table with minimal config (infer fields, use defaults)
  - id: customers
    kind: table
    source: public.customers
    target: analytics.customers
    tags: [reference, daily]

    load:
      mode: replace

  # API source with system-specific options
  - id: api_events
    kind: api
    source: /api/v1/events
    target: events.raw
    tags: [api, hourly]

    extract:
      mode: incremental
      watermark_field: event_timestamp
      watermark_type: timestamp
      batch_size: 1000
      options:
        api_key: ${API_KEY}
        timeout: 30
        retry_attempts: 3

    load:
      mode: append
      options:
        mongodb_collection: raw_events
```

**Key Design:**
- `streams` instead of `items` (aligns with nomenclature)
- `buffer` instead of `stage`
- Stream `kind` field differentiates table, query, view, file, api sources
- `criteria` for building queries from table references (WHERE clauses)
- Field `id` is immutable internal identifier
- Field `source` and `target` support progressive complexity (string or object)
- `watermark_field` instead of `watermark_column`
- `upsert_keys` for upsert mode (replaces merge_keys/primary_keys)
- Field-level source/target mapping with type conversions
- Framework-generated fields (no source, just target)
- Works across SQL, NoSQL, APIs, and files
- Extract and load blocks are explicit and clear
- Conventions: always create target, strict schema validation by default

### 7.1 Type Mapping

Each operator implements TypeMapper for converting between system-specific types and DXT internal types.

```python
class TypeMapper(ABC):
    """Base class for converting between system types and DXT internal types."""

    @abstractmethod
    def from_source(self, source_type: str) -> DXTType:
        """Convert source system type to DXT internal type.

        Example:
            PostgreSQL "INTEGER" -> DXTType.INT32
            PostgreSQL "BIGINT" -> DXTType.INT64
            PostgreSQL "TIMESTAMP" -> DXTType.TIMESTAMP
        """

    @abstractmethod
    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        """Convert DXT internal type to target system type.

        Args:
            dxt_type: DXT internal type
            target_hint: User-specified target type (from Field.target.dtype)

        Returns:
            Target system type string

        Example:
            DXTType.INT64, hint="BIGINT" -> "BIGINT"
            DXTType.INT64, hint=None -> "BIGINT" (default mapping)
            DXTType.TIMESTAMP, hint="timestamp_ntz(0)" -> "timestamp_ntz(0)"
        """


class PostgresTypeMapper(TypeMapper):
    """PostgreSQL type conversions."""

    SOURCE_TO_DXT = {
        "smallint": DXTType.INT16,
        "integer": DXTType.INT32,
        "bigint": DXTType.INT64,
        "real": DXTType.FLOAT32,
        "double precision": DXTType.FLOAT64,
        "numeric": DXTType.DECIMAL,
        "varchar": DXTType.STRING,
        "text": DXTType.STRING,
        "boolean": DXTType.BOOL,
        "date": DXTType.DATE,
        "timestamp": DXTType.TIMESTAMP,
        "timestamptz": DXTType.TIMESTAMP_TZ,
        "bytea": DXTType.BINARY,
        "jsonb": DXTType.JSON,
    }

    DXT_TO_TARGET = {
        DXTType.INT16: "SMALLINT",
        DXTType.INT32: "INTEGER",
        DXTType.INT64: "BIGINT",
        DXTType.FLOAT32: "REAL",
        DXTType.FLOAT64: "DOUBLE PRECISION",
        DXTType.DECIMAL: "NUMERIC",
        DXTType.STRING: "TEXT",
        DXTType.BOOL: "BOOLEAN",
        DXTType.DATE: "DATE",
        DXTType.TIMESTAMP: "TIMESTAMP",
        DXTType.TIMESTAMP_TZ: "TIMESTAMPTZ",
        DXTType.BINARY: "BYTEA",
        DXTType.JSON: "JSONB",
    }

    def from_source(self, source_type: str) -> DXTType:
        normalized = source_type.lower().split('(')[0].strip()
        return self.SOURCE_TO_DXT.get(normalized, DXTType.STRING)

    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        if target_hint:
            return target_hint  # User specified exact type
        return self.DXT_TO_TARGET.get(dxt_type, "TEXT")
```

**Usage:**
```python
# During extraction
source_type = "BIGINT"
dxt_type = postgres_mapper.from_source(source_type)  # DXTType.INT64

# During load
if field.target and field.target.dtype:
    target_type = field.target.dtype  # User-specified: "NUMBER(19,0)"
else:
    target_type = snowflake_mapper.to_target(dxt_type)  # Default: "BIGINT"
```

### 8. Project Configuration

Global configuration file for reusable connections and settings.

**Structure:**
```yaml
# dxt_project.yaml (or connections.yaml)
version: 1
name: my_data_project

connections:
  postgres_prod:
    type: postgres
    host: ${POSTGRES_HOST}
    port: ${POSTGRES_PORT}
    database: ${POSTGRES_DB}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}  # Must come from environment

  snowflake_warehouse:
    type: snowflake
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS

buffer:
  default_format: parquet
  default_compression: snappy
  default_location: ./.dxt/buffer
  persist: true             # Persist by default
  cleanup_on_success: true
  retain_on_failure: true

defaults:
  extract:
    batch_size: 10000
    mode: full
  load:
    batch_size: 10000
    mode: append

logging:
  level: INFO
  format: json
  file: ./.dxt/logs/dxt.log
```

**Connection String Support:**

Connections can be specified as either references or inline strings:

```yaml
# Pipeline with connection reference
source:
  connection: postgres_prod  # References dxt_project.yaml

# Pipeline with inline connection string (less secure, but convenient)
source:
  connection: postgresql://localhost:5432/mydb
```

## CLI Command Structure

### Subcommand Organization

```bash
dxt run <pipeline.yaml>         # Extract + Load (default)
dxt extract <pipeline.yaml>     # Extract only (to buffer)
dxt load <pipeline.yaml>        # Load only (from buffer)
dxt list <pipeline.yaml>        # List streams in pipeline
dxt validate <pipeline.yaml>    # Validate pipeline config
dxt init [name]                 # Initialize project or pipeline
dxt test <pipeline.yaml>        # Test connections
dxt discover <connection>       # Discover available streams in a data system
```

### Selector Syntax

Filter streams to process (inspired by dbt):

```bash
# By exact ID
dxt run pipeline.yaml --select orders

# By multiple IDs
dxt run pipeline.yaml --select orders,customers

# By glob pattern
dxt run pipeline.yaml --select "sales_*"

# By tag
dxt run pipeline.yaml --select tag:critical

# Multiple tags (AND logic)
dxt run pipeline.yaml --select tag:critical+daily

# Multiple tags (OR logic)
dxt run pipeline.yaml --select tag:critical,tag:daily

# Exclude pattern
dxt run pipeline.yaml --select "all" --exclude "*_test"

# All streams (default if no selector)
dxt run pipeline.yaml
```

### Common Options

```bash
--dry-run              # Validate and plan without executing
--select <selector>    # Stream selector expression
--exclude <selector>   # Exclude streams matching expression
--threads <n>          # Parallel execution (future)
--verbose, -v          # Verbose logging
--quiet, -q            # Minimal output (JSON results only)
--log-file <path>      # Custom log file location
```

## Directory Structure

**Revised organization:**

```
dxt/
├── __init__.py
├── cli.py                      # Typer CLI entry point
├── core/
│   ├── __init__.py
│   ├── connector.py            # Base Connector class
│   ├── extractor.py            # Base Extractor class
│   ├── loader.py               # Base Loader class
│   ├── buffer.py               # Base Buffer class
│   ├── models.py               # Stream, ExtractConfig, LoadConfig, Field
│   ├── pipeline.py             # Pipeline model & executor
│   ├── selector.py             # Selector parsing/matching
│   ├── results.py              # ExtractResult, LoadResult, ExecutionResult
│   └── sql_builder.py          # Base SQLBuilder class (for SQL systems)
├── operators/                  # Grouped by data system
│   ├── __init__.py
│   ├── postgres/
│   │   ├── __init__.py
│   │   ├── connector.py        # PostgresConnector (uses SQLAlchemy)
│   │   ├── extractor.py        # PostgresExtractor
│   │   ├── loader.py           # PostgresLoader
│   │   └── sql_builder.py      # PostgresSQLBuilder
│   ├── csv/
│   │   ├── __init__.py
│   │   ├── extractor.py        # CSVExtractor (file-based, no connector)
│   │   └── loader.py           # CSVLoader
│   └── ... (more operators: mysql, snowflake, etc.)
├── buffers/
│   ├── __init__.py
│   ├── parquet.py              # ParquetBuffer (default, disk-backed)
│   ├── csv.py                  # CSVBuffer (disk-backed)
│   ├── jsonl.py                # JSONLBuffer (disk-backed)
│   └── memory.py               # MemoryBuffer (in-memory only)
├── utils/
│   ├── __init__.py
│   ├── logging.py              # Structured logging setup
│   ├── env.py                  # Environment variable substitution
│   ├── validation.py           # YAML/schema validation
│   └── progress.py             # Rich progress bars
└── exceptions.py               # Custom exceptions
```

**Key Organization:**
- `operators/` groups by system (postgres, csv, etc.) instead of role
- Each operator dir contains its connector, extractor, loader, and SQL builder
- `core/models.py` consolidates Stream, ExtractConfig, LoadConfig, Field
- `core/buffer.py` base class for temporary storage
- `buffers/` contains buffer implementations (parquet, csv, jsonl, memory)
- `core/results.py` for all result types
- `core/sql_builder.py` base class for SQL generation

## Key Architecture Decisions

### 1. Stateless by Design

**Decision:** No external state storage (no state files, no tracking database).

**Rationale:**
- Simpler architecture, fewer moving parts
- All state derived from data systems themselves
- For incremental loads: query target for max watermark value at runtime
- Avoids state synchronization issues and drift

**Implementation:**
```python
# Before extracting incrementally, query target
max_watermark = loader.get_max_watermark(stream.target_ref, "updated_at")
# Use max_watermark to build WHERE clause for extraction
```

### 2. SQL Builder Strategy

**Decision:** Hybrid approach - SQLAlchemy foundation + custom builders.

**For SQL databases (Postgres, MySQL, SQLite):**
- Use SQLAlchemy for connection management and schema reflection
- Custom `SQLBuilder` classes for dialect-specific operations (MERGE, COPY, etc.)

**For cloud warehouses (Snowflake, BigQuery, Redshift):**
- Native SDKs for connections
- Custom `SQLBuilder` for all SQL generation

**Rationale:**
- SQLAlchemy solves 80% of the problem for standard SQL databases
- Custom builders handle edge cases and optimize performance
- Each operator owns its SQL generation logic

**Example:**
```python
class PostgresSQLBuilder(SQLBuilder):
    def build_upsert(self, target, stage, upsert_keys, columns):
        # PostgreSQL-specific INSERT ... ON CONFLICT
        ...

class SnowflakeSQLBuilder(SQLBuilder):
    def build_upsert(self, target, stage, upsert_keys, columns):
        # Snowflake MERGE statement
        ...
```

### 3. Schema Management

**Decision:** Runtime schema checking with strict validation by default.

**Flow:**
1. Extract phase: Get source schema
2. Load phase: Get target schema (may be None)
3. If target doesn't exist, create it (using stream.fields)
4. Validate target schema matches stream.fields (strict validation)
5. Proceed with load

**Note:** Schema evolution deferred to v2. Users can manually ALTER TABLE if needed.

### 4. Incremental Load Support

**Decision:** Watermark column metadata drives incremental extraction.

**Configuration:**
```yaml
extract:
  mode: incremental
  watermark_field: updated_at
  watermark_type: timestamp  # or int, date, etc.
```

**Process:**
1. Loader queries target: `SELECT MAX(updated_at) FROM target_table`
2. Result passed to Extractor
3. Extractor builds query: `SELECT * FROM source WHERE updated_at > <max_value>`
4. Extract and load new/updated rows

**Rationale:**
- Simple and stateless
- Works for most incremental use cases
- Clear configuration in YAML

### 5. CLI Organization

**Decision:** Flat subcommand structure with rich output.

```bash
dxt run <pipeline.yaml>      # Main command
dxt extract <pipeline.yaml>  # Separate phases
dxt load <pipeline.yaml>
dxt list <pipeline.yaml>     # Utilities
dxt validate <pipeline.yaml>
dxt init [name]
```

**Output:**
- Use `rich` for progress bars, tables, colored output
- Autocompletion via Typer (built-in)
- Structured logs to file (JSON)
- Human-readable to console

### 6. Stage Format

**Decision:** Parquet as default, support CSV and JSONL.

**Rationale:**
- Parquet: columnar, compressed, preserves types, handles large datasets
- CSV: simple fallback, universal compatibility
- JSONL: semi-structured data support

**Configuration:**
```yaml
stage:
  format: parquet  # Default
  compression: snappy
```

### 7. Memory & Performance

**Strategies:**
- **Streaming:** Generator/iterator pattern throughout
- **Batching:** Configurable batch size (default 10,000 rows)
- **No full loads:** Never load entire dataset into memory
- **Bulk operations:** Use native bulk load (COPY, BULK INSERT) where available

**Example flow:**
```python
# Extract streams to stage in batches
for batch in extractor.extract_batches(item):
    stage.write_batch(batch)

# Stage streams to loader in batches
for batch in stage.read_batch(batch_size=10000):
    loader.load_batch(batch)
```

### 8. Operator Extensibility

**Decision:** Built-in operators in `dxt/operators/`, plugin support for future.

**Phase 1:** Built-in operators
- Group by system (postgres/, csv/, etc.)
- Each system has connector, extractor, loader, sql_builder

**Phase 2:** Plugin architecture via entry points
```python
# External package: dxt-bigquery
[project.entry-points."dxt.operators"]
bigquery = "dxt_bigquery.operators"
```

### 9. Multiple Operator Variants

**Decision:** Support multiple implementations per system via type suffix.

**Example:**
```yaml
source:
  type: postgres       # Standard implementation
  # OR
  type: postgres_bulk  # Optimized for bulk operations
```

**Rationale:**
- Some systems have multiple optimal approaches
- Users can choose based on use case
- Shared base code, different strategies

### 10. Logging Strategy

**Decision:** Structured logging with separate concerns.

**Three output channels:**
1. **Logs** (to file): JSON structured logs via `structlog`
2. **Progress** (to console): Real-time updates via `rich.Progress`
3. **Results** (to stdout): JSON output for scripting (with `--quiet`)

**Example:**
```python
# Structured logging
logger.info("extract_started", item_id="orders", mode="incremental")

# Progress bar
with Progress() as progress:
    task = progress.add_task("Extracting orders", total=total_rows)
    ...

# Result output (--quiet mode)
print(json.dumps(pipeline_result.dict()))
```

### 11. Discovery Command

**Decision:** Add `dxt discover` command for schema introspection.

**Purpose:** Automatically discover available streams (tables, views, collections) in a data system and their schemas.

**Usage:**
```bash
# Discover all streams in a connection
dxt discover postgres_prod

# Discover specific schema/database
dxt discover postgres_prod --schema public

# Output as YAML for pipeline generation
dxt discover postgres_prod --format yaml > discovered.yaml
```

**Output:**
```yaml
connection: postgres_prod
discovered_at: "2025-12-02T10:30:00Z"
streams:
  - id: customers
    kind: table
    source: public.customers
    fields:
      - name: customer_id
        dtype: int64
        nullable: false
      - name: email
        dtype: varchar
        nullable: true
      - name: created_at
        dtype: timestamp
        nullable: false

  - id: orders
    kind: table
    source: public.orders
    fields:
      - name: order_id
        dtype: int64
        nullable: false
      - name: customer_id
        dtype: int64
        nullable: true
      - name: total
        dtype: numeric
        nullable: true
```

**Rationale:**
- Inspired by Airbyte's discovery protocol
- Reduces manual YAML writing for large schemas
- Enables automated pipeline generation
- Useful for schema validation and documentation

## Implementation Priorities

### Phase 1: Core Foundation (MVP)
1. **Core models**: Stream, ExtractConfig, LoadConfig, Field, Result classes
2. **Base classes**: Connector, Extractor, Loader, Buffer, SQLBuilder
3. **Buffer implementations**: ParquetBuffer (default), MemoryBuffer
4. **Pipeline**: YAML parser, validator, executor
5. **CLI**: Basic commands (run, validate, list, init)
6. **PostgreSQL operator**: First complete implementation (connector, extractor, loader)
7. **Utils**: Logging, environment substitution, basic validation

**Goal:** End-to-end pipeline: Postgres → Parquet buffer → Postgres (full refresh)

### Phase 2: Incremental & Merge
1. **Incremental extraction**: Watermark-based mode
2. **Upsert load mode**: UPSERT implementation
3. **Schema management**: Auto-create target tables (already default convention)
4. **Selector**: Basic selector parsing and filtering
5. **CLI enhancement**: `--select`, `--dry-run` options

**Goal:** Production-ready for basic data movement use cases

### Phase 3: More Operators
1. **CSV operator**: File-based extract and load
2. **MySQL operator**: Using SQLAlchemy + custom SQL builder
3. **Generic SQL operator**: Fallback for other SQL databases
4. **Rich output**: Progress bars, colored logs, tables

**Goal:** Support multiple source/target systems

### Phase 4: Advanced Features
1. **Parallel execution**: Multi-threaded stream processing
2. **Schema evolution**: Auto-evolve, flexible modes
3. **Column mapping**: Simple rename transformations
4. **Remote buffers**: S3-compatible storage
5. **Error handling**: Retry logic, fail_fast vs continue

**Goal:** Enterprise-ready features

### Phase 5: Extended Ecosystem
1. **Cloud warehouse operators**: Snowflake, BigQuery, Redshift
2. **Plugin system**: Entry point discovery
3. **Advanced transformations**: Type casting, computed columns
4. **Monitoring**: Metrics, observability hooks

**Goal:** Comprehensive data movement platform

## Design Principles Summary

1. **Stateless**: No external state - derive from data systems
2. **Simplicity**: Start with simplest implementation, add complexity as needed
3. **Composition**: Favor composition over inheritance (Connector pattern)
4. **Type Safety**: Pydantic models for all configuration and results
5. **Memory Efficiency**: Streaming/generators throughout, never full loads
6. **Observability**: Structured logging, progress tracking, detailed results
7. **Extensibility**: Clear interfaces, plugin architecture for future
8. **Unix Philosophy**: Do one thing well (move data), compose with other tools
9. **Developer Experience**: Clear CLI, helpful errors, autocompletion
10. **Performance**: Bulk operations, batching, parallel execution where beneficial

## Open Questions for SQL Builder

**Challenge:** Multiple SQL dialects with different syntax.

**Options to explore:**

1. **Pure custom builders** (current proposal)
   - Pros: Full control, no dependencies
   - Cons: Lots of code, many dialects to support

2. **SQLAlchemy Core expressions**
   - Pros: Dialect abstraction built-in
   - Cons: Learning curve, may not support all edge cases

3. **Hybrid approach** (recommended)
   - Use SQLAlchemy Core for standard operations (SELECT, INSERT, CREATE TABLE)
   - Custom builders only for dialect-specific optimizations (MERGE, COPY, etc.)
   - Best of both worlds

**Example hybrid:**
```python
from sqlalchemy import select, insert, table, column

class PostgresLoader(Loader):
    def load_append(self, item, stage):
        # Use SQLAlchemy for standard INSERT
        tbl = table(item.target_ref)
        stmt = insert(tbl).values(...)
        self.connector.execute(stmt)

    def load_merge(self, item, stage):
        # Custom SQL for PostgreSQL-specific ON CONFLICT
        sql = self.sql_builder.build_merge(...)
        self.connector.execute(sql)
```

**Decision needed:** How much to lean on SQLAlchemy vs. custom builders?
