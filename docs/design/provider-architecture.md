# DXT Provider Architecture

> Design document for DXT's multi-category provider system

## Overview

This document defines DXT's architecture for supporting diverse data systems beyond relational databases. It introduces a category-based provider model that enables consistent interfaces across different data paradigms while allowing system-specific optimizations.

## Goals

1. **Support common data systems** — relational databases, document stores, file/object storage, web APIs, key-value stores
2. **Consistent abstractions** — unified pipeline model (extract → buffer → load) regardless of source/target
3. **Extensibility** — contributors can add new providers without modifying core framework
4. **Simplicity** — minimal concepts, predictable behavior

## Terminology

| Term | Definition |
|------|------------|
| **Category** | A family of data systems with shared data model and access patterns |
| **Provider** | A package that adds support for a data system (e.g., postgres, mongodb, s3) |
| **Connector** | Class managing connections to a system (part of a provider) |
| **Extractor** | Class extracting data from a system (part of a provider) |
| **Loader** | Class loading data into a system (part of a provider) |
| **TypeMapper** | Class converting types between source/target and DXT internal types |

### Provider vs Operator

| Concept | What it is | Example |
|---------|------------|---------|
| **Provider** | Package for a distinct system (organizational) | `dxt/providers/postgres/` |
| **Operator** | Class that does work (functional) | `PostgresExtractor`, `SQLiteLoader` |

```
Provider (package)           ← organizational unit
├── Connector (operator)     ← manages connections
├── Extractor (operator)     ← reads data
├── Loader (operator)        ← writes data
└── TypeMapper               ← type conversion helper
```

### For Contributors

To add support for a new system (e.g., MySQL):

1. **Create a provider package**: `dxt/providers/mysql/`
2. **Implement required classes**:
   - `MySQLConnector` — extends appropriate base (e.g., `RelationalConnector`)
   - `MySQLExtractor` — extends `RelationalExtractor`
   - `MySQLLoader` — extends `RelationalLoader`
   - `MySQLTypeMapper` — extends `TypeMapper`
3. **Export in `__init__.py`**:
   ```python
   from dxt.providers.mysql.connector import MySQLConnector
   from dxt.providers.mysql.extractor import MySQLExtractor
   from dxt.providers.mysql.loader import MySQLLoader

   __all__ = ["MySQLConnector", "MySQLExtractor", "MySQLLoader"]
   ```

Users reference providers in YAML:
```yaml
source:
  extractor: dxt.providers.mysql.MySQLExtractor
  # or just: dxt.providers.mysql.MySQLExtractor
```

## Categories

DXT organizes providers into categories based on their data model and access patterns:

| Category | Aliases | Description |
|----------|---------|-------------|
| `relational` | `rdb`, `sql` | Tabular data via SQL interface |
| `document` | `docdb`, `doc` | Document/collection-based stores |
| `filestore` | `fs`, `file` | File and object storage systems |
| `webapi` | `http`, `api` | HTTP-based web services |
| `keyvalue` | `kv` | Key-value stores |
| `graph` | — | Graph databases (future) |

### Why Categories?

Categories provide:

1. **Configuration shape** — each category has a distinct connection config model
2. **Method contracts** — each category defines appropriate connector methods
3. **Extraction/load patterns** — each category has specific data access patterns
4. **Validation** — framework can validate config against category schema

---

## Category Definitions

### 1. Relational

Systems that expose tabular data (rows and columns) via SQL interface.

**Providers**: `postgres`, `mysql`, `sqlite`, `duckdb`, `snowflake`, `bigquery`, `redshift`, `clickhouse`, `trino`

**Connection Config**:
```python
class RelationalConnectionConfig(BaseModel):
    url: str                          # SQLAlchemy URL
    pool_size: int = 5
    pool_timeout: int = 30
    echo: bool = False
    options: dict[str, Any] = {}
```

**Connector Interface**:
```python
class RelationalConnector(Connector):
    def execute(self, query: str, params: dict = None) -> Iterator[dict]:
        """Execute query and yield rows."""

    def execute_ddl(self, statement: str) -> None:
        """Execute DDL statement."""

    def get_schema(self, table_ref: str) -> list[Field]:
        """Get table schema."""

    def table_exists(self, table_ref: str) -> bool:
        """Check if table exists."""

    def list_tables(self, schema: str = None) -> Iterator[str]:
        """List tables in schema."""
```

**Source Types**:
| Type | Description | Example |
|------|-------------|---------|
| `relation` | Table or view reference | `public.users` |
| `sql` | Raw SQL query | `SELECT * FROM users WHERE active = true` |
| `function` | Table-valued function | `get_active_users(2024)` |

**YAML Example**:
```yaml
source:
  provider: postgres
  url: "postgresql://${PGUSER}:${PGPASS}@${PGHOST}:5432/mydb"

streams:
  - id: users
    source:
      type: relation
      relation: public.users
    target: public.users

  - id: active_orders
    source:
      type: sql
      sql: |
        SELECT o.*, c.name as customer_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'active'
    target: public.active_orders
```

---

### 2. Document

Systems that store data as documents (typically JSON-like) in collections.

**Providers**: `mongodb`, `firestore`, `dynamodb`, `couchdb`, `cosmosdb`

**Connection Config**:
```python
class DocumentConnectionConfig(BaseModel):
    hosts: list[str]                  # ["host1:27017", "host2:27017"]
    database: str
    auth: Optional[DocumentAuthConfig] = None
    replica_set: Optional[str] = None
    tls: bool = False
    tls_ca_file: Optional[str] = None
    options: dict[str, Any] = {}

class DocumentAuthConfig(BaseModel):
    username: str
    password: str
    auth_source: str = "admin"
    mechanism: str = "SCRAM-SHA-256"
```

**Connector Interface**:
```python
class DocumentConnector(Connector):
    def find(
        self,
        collection: str,
        filter: dict = None,
        projection: dict = None,
        sort: list[tuple[str, int]] = None,
        limit: int = None,
        skip: int = None
    ) -> Iterator[dict]:
        """Query documents from collection."""

    def aggregate(
        self,
        collection: str,
        pipeline: list[dict]
    ) -> Iterator[dict]:
        """Run aggregation pipeline."""

    def insert_many(self, collection: str, documents: list[dict]) -> int:
        """Insert documents, return count."""

    def update_many(self, collection: str, filter: dict, update: dict) -> int:
        """Update matching documents, return count."""

    def delete_many(self, collection: str, filter: dict) -> int:
        """Delete matching documents, return count."""

    def list_collections(self) -> Iterator[str]:
        """List collections in database."""

    def collection_exists(self, collection: str) -> bool:
        """Check if collection exists."""

    def infer_schema(self, collection: str, sample_size: int = 100) -> list[Field]:
        """Infer schema from document sample."""
```

**Source Types**:
| Type | Description | Example |
|------|-------------|---------|
| `collection` | Full collection | `users` |
| `query` | Find with filter/projection | `{"status": "active"}` |
| `aggregate` | Aggregation pipeline | `[{"$match": ...}, {"$group": ...}]` |

**YAML Example**:
```yaml
source:
  provider: mongodb
  hosts: ["mongo1:27017", "mongo2:27017"]
  database: myapp
  auth:
    username: ${MONGO_USER}
    password: ${MONGO_PASS}
  replica_set: rs0

streams:
  - id: users
    source:
      type: collection
      collection: users
    target: public.users

  - id: active_users
    source:
      type: query
      collection: users
      filter: {"status": "active"}
      projection: {"password": 0, "_id": 0}
    target: public.active_users

  - id: order_totals
    source:
      type: aggregate
      collection: orders
      pipeline:
        - {"$match": {"status": "completed"}}
        - {"$group": {"_id": "$user_id", "total": {"$sum": "$amount"}}}
    target: public.user_order_totals
```

---

### 3. Filestore

Systems that store data as files or objects with path-based access.

**Providers**: `local`, `s3`, `gcs`, `azure`, `sftp`, `ftp`

**Connection Config**:
```python
class FilestoreConnectionConfig(BaseModel):
    path: str                         # "s3://bucket/prefix" or "/local/path"
    protocol: Optional[str] = None    # file, s3, gs, az, sftp, ftp (inferred from path)
    region: Optional[str] = None
    endpoint_url: Optional[str] = None  # For MinIO, R2, etc.
    credentials: Optional[FilestoreCredentials] = None
    options: dict[str, Any] = {}

class FilestoreCredentials(BaseModel):
    # AWS S3
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_profile: Optional[str] = None

    # GCS
    gcp_service_account_json: Optional[str] = None

    # Azure
    azure_connection_string: Optional[str] = None
    azure_account_key: Optional[str] = None
    azure_sas_token: Optional[str] = None

    # SFTP/FTP
    username: Optional[str] = None
    password: Optional[str] = None
    private_key: Optional[str] = None
    private_key_passphrase: Optional[str] = None
```

**Connector Interface**:
```python
class FilestoreConnector(Connector):
    def list_files(
        self,
        pattern: str = "*",
        recursive: bool = True
    ) -> Iterator[FileInfo]:
        """List files matching pattern."""

    def open_read(self, path: str) -> BinaryIO:
        """Open file for reading."""

    def open_write(self, path: str) -> BinaryIO:
        """Open file for writing."""

    def exists(self, path: str) -> bool:
        """Check if file exists."""

    def delete(self, path: str) -> None:
        """Delete file."""

    def get_info(self, path: str) -> FileInfo:
        """Get file metadata."""

    def makedirs(self, path: str) -> None:
        """Create directory/prefix."""

@dataclass
class FileInfo:
    path: str
    size: int
    modified: datetime
    format: Optional[str] = None
```

**Source Types**:
| Type | Description | Example |
|------|-------------|---------|
| `file` | Single file | `data/users.parquet` |
| `glob` | Pattern match | `data/**/*.parquet` |
| `directory` | All files in directory | `data/2024/` |

**YAML Example**:
```yaml
source:
  provider: s3
  path: s3://my-bucket/data/
  region: us-east-1
  credentials:
    aws_profile: production

streams:
  - id: users
    source:
      type: file
      file: users.parquet
    target: public.users

  - id: events
    source:
      type: glob
      pattern: "events/**/*.parquet"
    target: public.events

  - id: legacy
    source:
      type: file
      file: legacy.csv
      options:
        format: csv
        delimiter: ";"
        encoding: latin-1
    target: public.legacy
```

---

### 4. WebAPI

HTTP-based web services including REST APIs and GraphQL.

**Providers**: `rest`, `graphql`, and specialized providers like `stripe`, `salesforce`, `github`

**Connection Config**:
```python
class WebAPIConnectionConfig(BaseModel):
    base_url: str                     # "https://api.example.com/v1"
    auth: Optional[WebAPIAuthConfig] = None
    headers: dict[str, str] = {}
    rate_limit: Optional[RateLimitConfig] = None
    timeout: int = 30
    retry: RetryConfig = RetryConfig()
    options: dict[str, Any] = {}

class WebAPIAuthConfig(BaseModel):
    type: str                         # "api_key", "bearer", "basic", "oauth2"

    # API Key / Bearer
    token: Optional[str] = None
    header_name: str = "Authorization"
    prefix: str = "Bearer"

    # Basic
    username: Optional[str] = None
    password: Optional[str] = None

    # OAuth2
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_url: Optional[str] = None
    scopes: list[str] = []
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None

class RateLimitConfig(BaseModel):
    requests_per_second: float = 10
    burst: int = 1
    retry_on_429: bool = True

class RetryConfig(BaseModel):
    max_retries: int = 3
    backoff_factor: float = 0.5
    retry_statuses: list[int] = [429, 500, 502, 503, 504]

class PaginationConfig(BaseModel):
    type: str                         # "offset", "cursor", "page", "link"

    # Offset-based
    limit_param: str = "limit"
    offset_param: str = "offset"
    page_size: int = 100

    # Page-based
    page_param: str = "page"

    # Cursor-based
    cursor_param: str = "cursor"
    cursor_path: str = "meta.next_cursor"

    # Link header (RFC 5988)
    link_rel: str = "next"

    # Response parsing
    results_path: str = "data"
    total_path: Optional[str] = "meta.total"
```

**Connector Interface**:
```python
class WebAPIConnector(Connector):
    def request(
        self,
        method: str,
        endpoint: str,
        params: dict = None,
        body: dict = None,
        headers: dict = None
    ) -> Response:
        """Make HTTP request."""

    def get(self, endpoint: str, params: dict = None) -> Response:
        """GET request."""

    def post(self, endpoint: str, body: dict = None) -> Response:
        """POST request."""

    def paginate(
        self,
        endpoint: str,
        params: dict = None,
        pagination: PaginationConfig = None
    ) -> Iterator[dict]:
        """Paginate through results."""

@dataclass
class Response:
    status_code: int
    headers: dict[str, str]
    body: Any
```

**Source Types**:
| Type | Description | Example |
|------|-------------|---------|
| `endpoint` | Single endpoint | `/users` |
| `paginated` | Paginated endpoint | `/users` with pagination config |

**YAML Example**:
```yaml
source:
  provider: rest
  base_url: https://api.example.com/v1
  auth:
    type: bearer
    token: ${API_TOKEN}
  rate_limit:
    requests_per_second: 5

streams:
  - id: users
    source:
      type: endpoint
      endpoint: /users
    target: public.users

  - id: orders
    source:
      type: paginated
      endpoint: /orders
      params:
        status: completed
      pagination:
        type: cursor
        cursor_path: meta.next_cursor
        results_path: data
    target: public.orders
```

---

### 5. KeyValue

Key-value stores and caches.

**Providers**: `redis`, `memcached`, `etcd`

**Connection Config**:
```python
class KeyValueConnectionConfig(BaseModel):
    host: str
    port: int
    database: int = 0                 # Redis DB index
    password: Optional[str] = None
    username: Optional[str] = None    # Redis 6+ ACL
    cluster: bool = False
    cluster_nodes: list[str] = []
    tls: bool = False
    tls_ca_file: Optional[str] = None
    options: dict[str, Any] = {}
```

**Connector Interface**:
```python
class KeyValueConnector(Connector):
    def scan(self, pattern: str = "*", count: int = 100) -> Iterator[str]:
        """Scan keys matching pattern."""

    def get(self, key: str) -> Optional[Any]:
        """Get value by key."""

    def mget(self, keys: list[str]) -> list[Any]:
        """Get multiple values."""

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value."""

    def mset(self, mapping: dict[str, Any]) -> None:
        """Set multiple values."""

    def delete(self, *keys: str) -> int:
        """Delete keys, return count deleted."""

    def exists(self, key: str) -> bool:
        """Check if key exists."""

    # Hash operations
    def hgetall(self, key: str) -> dict:
        """Get all hash fields."""

    def hset(self, key: str, mapping: dict) -> None:
        """Set hash fields."""

    def hscan(self, key: str, pattern: str = "*", count: int = 100) -> Iterator[tuple[str, Any]]:
        """Scan hash fields."""
```

**Source Types**:
| Type | Description | Example |
|------|-------------|---------|
| `keys` | Keys matching pattern | `user:*` |
| `hash` | Hash key(s) | `user:123` |
| `list` | List key | `queue:jobs` |
| `set` | Set key | `tags:active` |

**YAML Example**:
```yaml
source:
  provider: redis
  host: redis.example.com
  port: 6379
  password: ${REDIS_PASSWORD}

streams:
  - id: users
    source:
      type: keys
      pattern: "user:*"
      value_type: hash
    target: public.users

  - id: config
    source:
      type: hash
      key: "app:config"
    target: public.app_config
```

---

### 6. Graph (Future)

Graph databases with node/edge data models.

**Providers**: `neo4j`, `neptune`, `tigergraph`, `arangodb`

**Connection Config**:
```python
class GraphConnectionConfig(BaseModel):
    uri: str                          # "bolt://host:7687"
    database: str = "neo4j"
    username: Optional[str] = None
    password: Optional[str] = None
    options: dict[str, Any] = {}
```

**Connector Interface**:
```python
class GraphConnector(Connector):
    def query(self, cypher: str, params: dict = None) -> Iterator[dict]:
        """Execute Cypher/Gremlin query."""

    def list_labels(self) -> Iterator[str]:
        """List node labels."""

    def list_relationship_types(self) -> Iterator[str]:
        """List relationship types."""
```

**Source Types**:
| Type | Description | Example |
|------|-------------|---------|
| `cypher` | Cypher query | `MATCH (u:User) RETURN u` |
| `nodes` | All nodes of label | `User` |
| `edges` | All relationships of type | `FOLLOWS` |

---

## Buffer Architecture

### Design Principles

1. **Buffer always persists** — No streaming/memory-only mode. All data is staged to disk.
2. **Buffer is the single source of truth** — Format, location, and serialization config live in buffer.
3. **Extractors yield, loaders consume** — Extractor writes to buffer, loader reads from buffer.
4. **Buffer handles all serialization** — Extractors/loaders don't deal with format conversion.

### Why Always Persist?

- **Debugging**: Can inspect intermediate data between extract and load
- **Reliability**: Extract and load are independent steps; can retry load without re-extracting
- **Simplicity**: One code path, no special cases for streaming
- **Decoupling**: Extractor completes before loader starts

### Data Unit Types

Extractors yield ONE of these (each extractor class implements one):

| Data Unit | Description | Examples |
|-----------|-------------|----------|
| **Records** | Rows/documents as dicts | SQL SELECT, MongoDB find, API response |
| **Files** | Binary file streams | S3 objects, SFTP files |

Different extractor classes for different modes:
- `PostgresExtractor` → yields records (SELECT)
- `PostgresCopyExtractor` → yields file (COPY TO)
- `S3Extractor` → yields files (download objects)
- `S3SelectExtractor` → yields records (S3 Select query)

### Buffer Formats

| Format | Description | Use Case |
|--------|-------------|----------|
| `parquet` | Columnar, typed, compressed | Default for structured data |
| `csv` | Delimited text | Legacy compatibility, bulk loaders |
| `jsonl` | Newline-delimited JSON | Documents, nested data |
| `passthrough` | No parsing, preserve original | File-to-file transfers |

### Buffer Interface

```python
class Buffer:
    """Staging buffer for pipeline data.

    The buffer is the intermediary between extractor and loader.
    It handles all persistence and format conversion.
    """

    def __init__(
        self,
        location: str = ".dxt/staging",
        format: str = "parquet",
        compression: Optional[str] = None,  # snappy, gzip, zstd
        **format_options                     # CSV: delimiter, quoting, etc.
    ):
        self.location = location
        self.format = format
        self.compression = compression
        self.format_options = format_options
        self._schema: Optional[list[Field]] = None
        self._staged_files: list[StagedFile] = []

    # === Extractor writes to buffer ===

    def write_records(
        self,
        records: Iterator[dict],
        schema: list[Field]
    ) -> None:
        """Serialize records to staged file(s).

        Buffer handles:
        - Batching records into appropriately sized files
        - Serialization to configured format (parquet, csv, jsonl)
        - Compression if configured
        """
        pass

    def write_file(
        self,
        file_info: FileInfo,
        stream: BinaryIO
    ) -> None:
        """Stage a file as-is (passthrough mode).

        Buffer handles:
        - Streaming to disk without loading into memory
        - Preserving original filename and metadata
        """
        pass

    # === Loader reads from buffer ===

    def read_records(self) -> Iterator[dict]:
        """Parse staged files and yield records.

        Buffer handles:
        - Reading staged files
        - Deserializing from format (parquet, csv, jsonl)
        - Decompression if needed
        """
        pass

    def get_staged_files(self) -> list[StagedFile]:
        """Get list of staged files for direct access.

        Used by loaders that work with files directly:
        - Snowflake COPY (PUT file, then COPY INTO)
        - S3 upload
        - PostgreSQL COPY FROM
        """
        pass

    # === Metadata ===

    @property
    def schema(self) -> Optional[list[Field]]:
        """Schema of staged data (None for passthrough)."""
        return self._schema

    @property
    def row_count(self) -> Optional[int]:
        """Total rows across all staged files (None for passthrough)."""
        pass

    @property
    def format(self) -> str:
        """Staged format: parquet, csv, jsonl, or passthrough."""
        pass

    # === Lifecycle ===

    def cleanup(self) -> None:
        """Remove all staged files."""
        pass


@dataclass
class StagedFile:
    """Metadata about a staged file."""
    path: str                           # Full path to staged file
    format: str                         # parquet, csv, jsonl, passthrough
    compression: Optional[str]          # snappy, gzip, zstd, None
    size_bytes: int                     # File size
    row_count: Optional[int]            # Row count (None for passthrough)
    schema: Optional[list[Field]]       # Schema (None for passthrough)
    original_name: Optional[str]        # Original filename (for passthrough)
    metadata: dict[str, Any]            # Additional metadata
```

### CSV Format Options

```python
@dataclass
class CSVFormatOptions:
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: Optional[str] = None
    doublequote: bool = True
    lineterminator: str = "\n"
    header: bool = True
    encoding: str = "utf-8"
    null_value: str = ""
```

### How Extractor and Loader Use Buffer

```python
# Extractor writes to buffer
class PostgresExtractor(RelationalExtractor):
    def extract(self, buffer: Buffer) -> None:
        schema = self._get_schema()
        records = self._execute_query()
        buffer.write_records(records, schema)  # Buffer handles serialization

# Loader that needs records
class PostgresLoader(RelationalLoader):
    def load(self, buffer: Buffer) -> int:
        count = 0
        for record in buffer.read_records():  # Buffer handles deserialization
            self._insert(record)
            count += 1
        return count

# Loader that needs files (bulk load)
class SnowflakeCopyLoader(RelationalLoader):
    def load(self, buffer: Buffer) -> int:
        count = 0
        for staged_file in buffer.get_staged_files():
            # PUT file to Snowflake stage, then COPY INTO
            self._put_file(staged_file.path)
            count += self._copy_into(staged_file)
        return count

# File-to-file transfer
class S3Loader(FilestoreLoader):
    def load(self, buffer: Buffer) -> int:
        count = 0
        for staged_file in buffer.get_staged_files():
            self._upload(staged_file.path, staged_file.original_name)
            count += 1
        return count
```

### Buffer Format Selection Guide

| Source | Target | Recommended Format | Notes |
|--------|--------|-------------------|-------|
| PostgreSQL | SQLite | `parquet` | Type-safe, efficient |
| PostgreSQL | Snowflake | `parquet` or `csv` | Snowflake COPY supports both |
| PostgreSQL | S3 | `parquet` | Columnar storage |
| MongoDB | PostgreSQL | `parquet` | Flatten documents |
| S3 (parquet) | PostgreSQL | `parquet` | Parse and insert |
| S3 (any) | Azure Blob | `passthrough` | Preserve original |
| API | PostgreSQL | `parquet` | Structured response |
| API | S3 | `jsonl` | Preserve nesting |

### YAML Configuration

```yaml
buffer:
  location: .dxt/staging        # Where to stage files
  format: parquet               # parquet, csv, jsonl, passthrough
  compression: snappy           # snappy, gzip, zstd (optional)

  # CSV-specific options (only when format: csv)
  csv:
    delimiter: ","
    quotechar: '"'
    header: true
    encoding: utf-8
    null_value: ""

  # Lifecycle
  cleanup_on_success: true      # Remove staging after successful load
  retain_on_failure: true       # Keep staging for debugging on failure
```

---

## Directory Structure

```
dxt/
├── core/
│   ├── connector.py          # Base Connector ABC
│   ├── extractor.py          # Base Extractor ABC
│   ├── loader.py             # Base Loader ABC
│   ├── buffer.py             # FileBuffer implementation
│   └── type_mapper.py        # Base TypeMapper ABC
├── models/
│   ├── field.py              # Field, DXTType
│   ├── stream.py             # Stream, ExtractConfig, LoadConfig
│   ├── pipeline.py           # Pipeline, ConnectionConfig
│   ├── source.py             # SourceConfig
│   └── results.py            # ExtractResult, LoadResult, etc.
└── providers/
    ├── base/
    │   ├── __init__.py
    │   ├── relational.py     # RelationalConnector, RelationalExtractor, etc.
    │   ├── document.py       # DocumentConnector, DocumentExtractor, etc.
    │   ├── filestore.py      # FilestoreConnector, FilestoreExtractor, etc.
    │   ├── webapi.py         # WebAPIConnector, WebAPIExtractor, etc.
    │   ├── keyvalue.py       # KeyValueConnector, KeyValueExtractor, etc.
    │   └── graph.py          # GraphConnector, GraphExtractor, etc.
    ├── postgres/
    │   ├── __init__.py
    │   ├── connector.py
    │   ├── extractor.py
    │   ├── loader.py
    │   └── type_mapper.py
    ├── mysql/
    ├── sqlite/
    ├── mongodb/
    ├── s3/
    ├── local/
    ├── rest/
    ├── redis/
    └── ...
```

---

## YAML Schema

### Pipeline Structure

```yaml
version: 1
name: pipeline_name
description: Optional description

# Source connection
source:
  provider: postgres           # Provider name
  # ... provider-specific config

# Target connection
target:
  provider: s3
  # ... provider-specific config

# Buffer configuration
buffer:
  format: parquet              # parquet, csv, jsonl, binary
  compression: snappy          # snappy, gzip, zstd, none
  location: .dxt/buffer        # Staging directory
  cleanup_on_success: true
  retain_on_failure: true

# Pipeline-level defaults
extract:
  batch_size: 10000

load:
  batch_size: 10000

# Streams
streams:
  - id: stream_name
    source:
      type: relation           # Source type (category-specific)
      # ... source config
    target:
      type: relation           # Target type (category-specific)
      # ... target config
    tags: [tag1, tag2]
    extract:
      mode: full               # full, incremental
      batch_size: 5000
      watermark_field: updated_at
    load:
      mode: append             # append, replace, upsert
      batch_size: 5000
      upsert_keys: [id]
```

---

## Provider Resolution

**No registry required.** Pipeline YAML specifies concrete provider classes directly. Python's import system handles discovery.

### How It Works

```yaml
# Pipeline specifies full class path
streams:
  - id: orders
    extractor:
      class: dxt.providers.postgres.PostgresExtractor
      config:
        host: localhost
        database: mydb
    loader:
      class: dxt.providers.sqlite.SQLiteLoader
      config:
        path: ./output.db
```

### Pipeline Executor Resolution

```python
def load_operator(class_path: str) -> type:
    """Import and return operator class from dotted path."""
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

# Validation via inheritance
extractor_class = load_operator("dxt.providers.postgres.PostgresExtractor")
assert issubclass(extractor_class, Extractor)
```

### Why No Registry?

1. **Explicit is better** — YAML shows exactly what code runs
2. **No magic** — Standard Python imports, easy to debug
3. **User extensibility** — Users define their own providers anywhere
4. **Simplicity** — Less framework code to maintain

### Future: Operator Discovery (CLI Feature)

A future CLI command could discover available operators by scanning for classes that inherit from base classes:

```bash
dxt operators list                      # List all available operators
dxt operators list --category=relational  # Filter by category
```

This is a convenience feature, not required for runtime.

---

## Implementation Phases

### Phase 1: Core Refactoring
- Restructure to `providers/base/` and `providers/<name>/`
- Rename existing SQL classes to use `Relational` prefix
- Update `FileBuffer` to support `binary` format
- Update pipeline executor for provider-based resolution

### Phase 2: Filestore Category
- Implement `FilestoreConnector` base class
- Implement `local` provider
- Implement `s3` provider
- Test filestore → filestore (binary) and filestore → relational pipelines

### Phase 3: Document Category
- Implement `DocumentConnector` base class
- Implement `mongodb` provider
- Test document → relational pipelines

### Phase 4: WebAPI Category
- Implement `WebAPIConnector` base class
- Implement `rest` provider with pagination support
- Test API → relational pipelines

### Phase 5: KeyValue Category
- Implement `KeyValueConnector` base class
- Implement `redis` provider
- Test keyvalue → relational pipelines

---

## Design Decisions

Decisions made during architecture design:

### Buffer Always Persists (No Streaming Mode)

**Decision:** All buffers persist data to staging. No memory-only or streaming mode.

**Rationale:**
- Simplifies implementation (one code path)
- Enables debugging (can inspect staged data)
- Enables retry (can re-run load without re-extracting)
- Decouples extract and load phases

**Trade-off:** Small overhead for simple pipelines, but benefits outweigh costs.

### No Provider Registry

**Decision:** No central registry. YAML specifies full class paths directly.

**Rationale:**
- Explicit is better than implicit
- Standard Python imports, easy to debug
- Users can define providers anywhere
- Less framework code

**Future:** CLI discovery command can scan for operators by inheritance.

### Extractors Yield One Data Type

**Decision:** Each extractor class yields either records OR files, never both.

**Rationale:**
- Clear contracts
- Different extraction modes = different classes
- e.g., `PostgresExtractor` (records) vs `PostgresCopyExtractor` (files)

### Buffer Owns Serialization

**Decision:** Buffer handles all format conversion. Extractors/loaders don't serialize.

**Rationale:**
- DRY — serialization code in one place
- Single source of truth for format config
- Extractors focus on data access, loaders focus on loading

## Open Questions

1. **Type mapping across categories** — How do we handle type conversion when source and target are different categories (e.g., MongoDB document → Postgres table)? Likely handled by buffer during serialization to parquet (which has a type system).

2. **Schema inference for files** — When loading CSV/JSON files, how is schema determined? Options: infer from file, user-provided in YAML, or fail if not provided.

3. **Large file partitioning** — How does buffer partition large extractions into multiple files? By row count, by size, or configurable?

4. **Cloud staging** — Should buffer support staging directly to S3/GCS for cloud-to-cloud transfers? (Start with local filesystem, add later if needed.)
