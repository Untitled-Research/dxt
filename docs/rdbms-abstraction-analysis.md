# RDBMS Operator Abstraction Analysis

## Overview

After implementing both PostgreSQL and SQLite operators, clear patterns have emerged that suggest opportunities for abstraction to reduce code duplication and improve maintainability.

## Common Patterns Identified

### 1. **Connector Similarities** (99% identical)

Both `PostgresConnector` and `SQLiteConnector` share almost identical structure:

**Identical functionality:**
- SQLAlchemy-based connection management
- Connection string building from config
- `connect()`, `disconnect()`, `test_connection()`
- `get_schema()` using SQLAlchemy inspector
- `execute_query()` and `execute_statement()`
- `table_exists()` checking

**Minor differences:**
- PostgreSQL: `_parse_table_ref()` handles schema.table references
- SQLite: No schema concept (single namespace)
- Connection string format differs slightly

**Abstraction Opportunity:** Create `SQLAlchemyConnector` base class

```python
class SQLAlchemyConnector(Connector):
    """Base connector for SQLAlchemy-based databases."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.engine: Optional[Engine] = None
        self.type_mapper = self._create_type_mapper()

    @abstractmethod
    def _create_type_mapper(self) -> TypeMapper:
        """Create database-specific type mapper."""
        pass

    @abstractmethod
    def _build_connection_string(self) -> str:
        """Build database-specific connection string."""
        pass

    # Common implementations of connect(), disconnect(), get_schema(), etc.
    # that work for all SQLAlchemy databases
```

### 2. **Extractor Similarities** (~95% identical)

Both extractors have nearly identical logic:

**Identical functionality:**
- `extract()` method structure (timing, error handling, watermark logic)
- `build_query()` with support for relation/sql/function source types
- `_extract_batches()` using SQLAlchemy streaming
- `validate_stream()` checking for required fields

**Minor differences:**
- Query building for timestamps: PostgreSQL uses native timestamps, SQLite uses ISO8601 text
- Table reference parsing (schema.table vs table only)

**Abstraction Opportunity:** Create `SQLAlchemyExtractor` base class

```python
class SQLAlchemyExtractor(Extractor):
    """Base extractor for SQLAlchemy-based databases."""

    def extract(self, stream: Stream, buffer: Buffer) -> ExtractResult:
        # Common implementation - works for all SQL databases
        pass

    def _extract_batches(self, query: str, buffer: Buffer, batch_size: int) -> int:
        # Common implementation using SQLAlchemy streaming
        pass

    def build_query(self, stream: Stream, watermark_value: Optional[Any] = None) -> Optional[str]:
        # Common SQL query building logic
        # Can be overridden for database-specific SQL dialects
        pass

    @abstractmethod
    def _format_watermark_filter(self, field: str, value: Any, type: str) -> str:
        """Format watermark filter for specific database SQL dialect."""
        pass
```

### 3. **Loader Similarities** (~90% identical)

Loaders share substantial common code:

**Identical functionality:**
- `load()` method structure (timing, error handling, mode dispatching)
- `_load_append()` - INSERT logic
- `_load_replace()` - DELETE/TRUNCATE + INSERT logic
- `create_target_stream()` - DDL generation from fields

**Key differences:**
- PostgreSQL: Uses `TRUNCATE` for replace mode
- SQLite: Uses `DELETE` (no TRUNCATE command)
- PostgreSQL: Uses `ON CONFLICT` for upsert
- SQLite: Uses `INSERT OR REPLACE` for upsert
- Table reference formatting (schema.table vs table)

**Abstraction Opportunity:** Create `SQLAlchemyLoader` base class

```python
class SQLAlchemyLoader(Loader):
    """Base loader for SQLAlchemy-based databases."""

    def load(self, stream: Stream, buffer: Buffer) -> LoadResult:
        # Common implementation
        pass

    def create_target_stream(self, stream: Stream) -> None:
        # Common DDL generation
        # Delegates to _format_table_name()
        pass

    def _load_append(self, stream: Stream, buffer: Buffer) -> LoadResult:
        # Common INSERT logic
        pass

    @abstractmethod
    def _truncate_table(self, table_name: str) -> str:
        """Get SQL to truncate/delete table contents."""
        # PostgreSQL: "TRUNCATE TABLE {table}"
        # SQLite: "DELETE FROM {table}"
        pass

    @abstractmethod
    def _build_upsert_sql(self, table: str, fields: list[Field], upsert_keys: list[str]) -> str:
        """Build database-specific upsert SQL."""
        # PostgreSQL: INSERT ... ON CONFLICT
        # SQLite: INSERT OR REPLACE
        pass

    @abstractmethod
    def _format_table_name(self, table_ref: str) -> str:
        """Format table name with schema if applicable."""
        pass
```

### 4. **Type Mapper Similarities** (~80% identical)

Type mappers have similar structure but different mappings:

**Identical functionality:**
- `from_source()` - normalize and map source type to DXTType
- `to_target()` - map DXTType to target, with target_hint support
- `normalize_source_type()` - lowercase, strip params
- `get_ddl_type()` - add precision/scale for DECIMAL

**Key differences:**
- Different type name mappings (though conceptually similar)
- SQLite has simpler type system (INTEGER, TEXT, REAL, BLOB)
- PostgreSQL has richer type system (multiple numeric types, timestamps with/without timezone, etc.)

**Abstraction Opportunity:** Keep separate but use consistent patterns

Type mappers should remain database-specific as the mappings are fundamentally different. However, we should ensure they follow consistent patterns and interfaces defined in the base `TypeMapper` class.

## Proposed Abstraction Hierarchy

```
Connector
├── SQLAlchemyConnector (new abstract base)
│   ├── PostgresConnector
│   ├── SQLiteConnector
│   └── MySQLConnector (future)
│
Extractor
├── SQLAlchemyExtractor (new abstract base)
│   ├── PostgresExtractor
│   ├── SQLiteExtractor
│   └── MySQLExtractor (future)
│
Loader
├── SQLAlchemyLoader (new abstract base)
│   ├── PostgresLoader
│   ├── SQLiteLoader
│   └── MySQLLoader (future)
│
TypeMapper (existing abstract base)
├── PostgresTypeMapper
├── SQLiteTypeMapper
└── MySQLTypeMapper (future)
```

## Benefits of Abstraction

1. **Reduced Code Duplication**:
   - Eliminate ~200 lines of duplicated code per database
   - Current: ~600 lines per RDBMS implementation
   - With abstraction: ~150-200 lines per RDBMS + 300 lines shared base classes

2. **Easier Maintenance**:
   - Bug fixes in one place benefit all RDBMS operators
   - Common patterns enforced across all databases

3. **Faster New Database Support**:
   - Adding MySQL/MariaDB would require only ~150 lines
   - Most logic inherited from base classes

4. **Better Testing**:
   - Test base classes once with thorough test coverage
   - Database-specific tests focus on dialect differences only

5. **Consistency**:
   - All RDBMS operators behave identically
   - Same error handling, logging, metrics

## Implementation Strategy

### Phase 1: Extract Common SQLAlchemy Connector
1. Create `dxt/core/sql_alchemy_connector.py` with base class
2. Refactor `PostgresConnector` to inherit from it
3. Refactor `SQLiteConnector` to inherit from it
4. Ensure all tests still pass

### Phase 2: Extract Common SQLAlchemy Extractor
1. Create `dxt/core/sql_alchemy_extractor.py` with base class
2. Move common `extract()`, `build_query()`, `_extract_batches()` to base
3. Refactor database-specific extractors to inherit
4. Keep database-specific query formatting as abstract methods

### Phase 3: Extract Common SQLAlchemy Loader
1. Create `dxt/core/sql_alchemy_loader.py` with base class
2. Move common `load()`, `_load_append()`, `create_target_stream()` to base
3. Refactor database-specific loaders to inherit
4. Keep SQL dialect differences as abstract methods

### Phase 4: Standardize Type Mappers
1. Ensure all type mappers follow same patterns
2. Add comprehensive tests for type mappings
3. Document mapping decisions (e.g., why UINT64 → NUMERIC(20,0) in PostgreSQL)

## Risks and Considerations

1. **Over-abstraction**: Be careful not to create abstraction that's too rigid
   - Some databases have unique features (PostgreSQL's COPY, JSONB operators, etc.)
   - Leave room for database-specific optimizations

2. **Breaking Changes**: Refactoring could introduce bugs
   - Comprehensive test suite already in place (good!)
   - Refactor incrementally with tests at each step

3. **Complexity for Simple Cases**: Base classes add indirection
   - Balance: shared base classes vs. simple database-specific implementations
   - Keep base classes focused and not too complex

## Next Steps

1. **Decision**: Confirm abstraction strategy with team
2. **Priority**: Which abstraction to implement first? (Recommend: Connector → Extractor → Loader)
3. **Testing**: Ensure current test coverage is sufficient before refactoring
4. **Documentation**: Update architecture docs to reflect new abstraction layers

## Conclusion

The similarities between PostgreSQL and SQLite operators are striking - roughly 90%+ of the code is identical or nearly identical. This presents a clear opportunity to:
- **Reduce code by 60-70%** through shared base classes
- **Accelerate new database support** from days to hours
- **Improve quality** through shared, well-tested base implementations

The abstraction should be implemented incrementally, starting with the Connector (highest similarity), then Extractor, then Loader. Type mappers should remain database-specific but follow consistent patterns.
