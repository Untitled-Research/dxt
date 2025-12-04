"""Relational database provider base classes.

This module provides base classes for relational database providers
(PostgreSQL, MySQL, SQLite, Snowflake, BigQuery, etc.) using SQLAlchemy.

The relational category covers systems that:
- Expose tabular data (rows and columns)
- Use SQL or SQL-like query interface
- Have introspectable schemas

Classes:
    RelationalConnector: Base connector for relational databases
    RelationalExtractor: Base extractor for relational databases
    RelationalLoader: Base loader for relational databases
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import Any, Iterator, Optional

try:
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from dxt.core.buffer import Buffer
from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.core.type_mapper import TypeMapper
from dxt.exceptions import (
    ConnectionError,
    ConnectorError,
    ExtractorError,
    LoaderError,
    SchemaError,
    ValidationError,
)
from dxt.models.field import DXTType, Field
from dxt.models.results import ExtractResult, LoadResult
from dxt.models.stream import Stream


# =============================================================================
# Relational Connector
# =============================================================================


class RelationalConnector(Connector):
    """Base class for relational database connectors using SQLAlchemy.

    Provides common functionality for SQL-based data systems including:
    - SQLAlchemy engine management
    - Connection lifecycle (connect, disconnect, test)
    - Schema introspection via SQLAlchemy inspector
    - Query execution with result streaming
    - DDL execution with transaction handling
    - Table existence checking

    Subclasses must implement:
    - _build_connection_string(): Database-specific connection string
    - _parse_table_ref(): Database-specific table reference parsing
    - _get_type_mapper(): Return database-specific type mapper
    - _get_database_name(): Return database name for error messages

    Example:
        >>> class PostgresConnector(RelationalConnector):
        ...     def _build_connection_string(self) -> str:
        ...         return f"postgresql://{self.config['host']}/{self.config['database']}"
        ...
        ...     def _parse_table_ref(self, ref: str) -> tuple[Optional[str], str]:
        ...         parts = ref.split(".")
        ...         return (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])
        ...
        ...     def _get_type_mapper(self) -> TypeMapper:
        ...         return PostgresTypeMapper()
        ...
        ...     def _get_database_name(self) -> str:
        ...         return "PostgreSQL"
    """

    # Category identifier
    category: str = "relational"

    def __init__(self, config: dict[str, Any]):
        """Initialize relational connector.

        Args:
            config: Connection configuration. Common keys:
                - url: SQLAlchemy connection URL (alternative to individual params)
                - host, port, database, username, password: Connection params
                - echo: Enable SQL logging (default: False)
                - pool_size: Connection pool size (default: 5)

        Raises:
            ConnectorError: If SQLAlchemy is not installed
        """
        if not SQLALCHEMY_AVAILABLE:
            db_name = self._get_database_name()
            raise ConnectorError(
                f"{db_name}Connector requires SQLAlchemy. "
                "Install with: pip install sqlalchemy"
            )

        super().__init__(config)
        self.engine: Optional[Engine] = None
        self.type_mapper: TypeMapper = self._get_type_mapper()

    @abstractmethod
    def _build_connection_string(self) -> str:
        """Build database-specific connection string from config.

        Returns:
            SQLAlchemy connection string (e.g., "postgresql://...", "sqlite:///...")

        Raises:
            ConnectorError: If required config is missing or invalid
        """
        pass

    @abstractmethod
    def _parse_table_ref(self, ref: str) -> tuple[Optional[str], str]:
        """Parse table reference into schema and table name.

        Args:
            ref: Table reference string (e.g., "public.orders", "orders")

        Returns:
            Tuple of (schema_name, table_name).
            schema_name may be None for databases without schema support.
        """
        pass

    @abstractmethod
    def _get_type_mapper(self) -> TypeMapper:
        """Get database-specific type mapper.

        Returns:
            TypeMapper instance for this database
        """
        pass

    @abstractmethod
    def _get_database_name(self) -> str:
        """Get database name for error messages.

        Returns:
            Human-readable database name (e.g., "PostgreSQL", "SQLite")
        """
        pass

    def connect(self) -> None:
        """Establish connection to the database.

        Creates a SQLAlchemy engine and tests the connection.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection_string = self._build_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                echo=self.config.get("echo", False),
            )
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.connection = self.engine
        except Exception as e:
            db_name = self._get_database_name()
            raise ConnectionError(f"Failed to connect to {db_name}: {e}") from e

    def disconnect(self) -> None:
        """Close connection to the database.

        Disposes the SQLAlchemy engine and clears connection references.
        Safe to call even if already disconnected.
        """
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.connection = None

    def test_connection(self) -> bool:
        """Test connectivity to the database.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.is_connected:
                self.connect()
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def get_schema(self, ref: str) -> list[Field]:
        """Get schema (field definitions) for a table.

        Uses SQLAlchemy inspector to introspect table metadata.

        Args:
            ref: Table reference (format depends on database)

        Returns:
            List of Field objects representing the schema

        Raises:
            SchemaError: If not connected, table doesn't exist, or introspection fails
        """
        if not self.is_connected:
            raise SchemaError("Not connected to database")

        try:
            schema_name, table_name = self._parse_table_ref(ref)
            inspector = inspect(self.engine)

            if not inspector.has_table(table_name, schema=schema_name):
                raise SchemaError(f"Table does not exist: {ref}")

            columns = inspector.get_columns(table_name, schema=schema_name)

            fields = []
            for col in columns:
                db_type = str(col["type"])
                dxt_type = self.type_mapper.from_source(db_type)

                field = Field(
                    id=col["name"],
                    dtype=dxt_type,
                    nullable=col["nullable"],
                )

                if dxt_type == DXTType.DECIMAL and hasattr(col["type"], "precision"):
                    field.precision = col["type"].precision
                    field.scale = (
                        col["type"].scale if hasattr(col["type"], "scale") else None
                    )

                fields.append(field)

            return fields

        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Failed to get schema for {ref}: {e}") from e

    def execute(self, query: str, params: Optional[dict] = None) -> Iterator[dict]:
        """Execute a SQL query and yield results.

        Args:
            query: SQL query string
            params: Optional query parameters

        Yields:
            Records as dictionaries (column_name -> value)

        Raises:
            ConnectorError: If not connected or query execution fails
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            with self.engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(
                    text(query), params or {}
                )
                for row in result:
                    yield dict(row._mapping)
        except Exception as e:
            raise ConnectorError(f"Failed to execute query: {e}") from e

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return all results.

        Args:
            query: SQL query string

        Returns:
            List of records as dictionaries

        Raises:
            ConnectorError: If not connected or query execution fails
        """
        return list(self.execute(query))

    def execute_ddl(self, statement: str) -> None:
        """Execute a DDL statement (CREATE, ALTER, DROP, etc.).

        Args:
            statement: SQL DDL statement

        Raises:
            ConnectorError: If not connected or statement execution fails
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            with self.engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
        except Exception as e:
            raise ConnectorError(f"Failed to execute DDL: {e}") from e

    # Alias for backwards compatibility
    execute_statement = execute_ddl

    def table_exists(self, ref: str) -> bool:
        """Check if a table exists in the database.

        Args:
            ref: Table reference

        Returns:
            True if table exists, False otherwise
        """
        if not self.is_connected:
            return False

        try:
            schema_name, table_name = self._parse_table_ref(ref)
            inspector = inspect(self.engine)
            return inspector.has_table(table_name, schema=schema_name)
        except Exception:
            return False

    def list_tables(self, schema: Optional[str] = None) -> Iterator[str]:
        """List tables in the database.

        Args:
            schema: Optional schema name to filter by

        Yields:
            Table names (with schema prefix if applicable)

        Raises:
            ConnectorError: If not connected
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=schema)
            for table in tables:
                if schema:
                    yield f"{schema}.{table}"
                else:
                    yield table
        except Exception as e:
            raise ConnectorError(f"Failed to list tables: {e}") from e


# =============================================================================
# Relational Extractor
# =============================================================================


class RelationalExtractor(Extractor):
    """Base class for relational database extractors.

    Provides SQL-based extraction with support for:
    - Full extraction
    - Incremental extraction with watermark
    - Table, view, and query sources
    - Batch processing with server-side cursors
    - Field selection and WHERE criteria

    Subclasses can override:
    - _format_watermark_filter(): Database-specific watermark syntax
    - _extract_batches(): Database-specific batch extraction
    """

    # Category identifier
    category: str = "relational"

    def __init__(
        self, connector: RelationalConnector, config: Optional[dict[str, Any]] = None
    ):
        """Initialize relational extractor.

        Args:
            connector: RelationalConnector instance
            config: Optional extractor-specific configuration

        Raises:
            ExtractorError: If connector is not a RelationalConnector
        """
        super().__init__(connector, config)
        if not isinstance(connector, RelationalConnector):
            raise ExtractorError("RelationalExtractor requires a RelationalConnector")

    @property
    def relational_connector(self) -> RelationalConnector:
        """Get connector as RelationalConnector type."""
        return self.connector  # type: ignore

    def extract(self, stream: Stream, buffer: Buffer) -> ExtractResult:
        """Extract records from SQL source to buffer.

        Args:
            stream: Stream configuration
            buffer: Buffer to write records to

        Returns:
            ExtractResult with metrics
        """
        started_at = datetime.now()
        records_extracted = 0
        watermark_value = None

        try:
            self.validate_stream(stream)

            if stream.is_incremental:
                watermark_value = self._get_watermark_value(stream)

            query = self.build_query(stream, watermark_value)

            if query is None:
                raise ExtractorError(f"Could not build query for stream {stream.id}")

            batch_size = stream.extract.batch_size
            records_extracted = self._extract_batches(query, buffer, batch_size)

            buffer.finalize()
            buffer.commit()

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return ExtractResult(
                stream_id=stream.id,
                success=True,
                records_extracted=records_extracted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                watermark_value=watermark_value,
            )

        except Exception as e:
            try:
                buffer.rollback()
            except Exception:
                pass

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return ExtractResult(
                stream_id=stream.id,
                success=False,
                records_extracted=records_extracted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                watermark_value=watermark_value,
                error_message=str(e),
            )

    def build_query(
        self, stream: Stream, watermark_value: Optional[Any] = None
    ) -> Optional[str]:
        """Build extraction query from stream configuration.

        Supports source types:
        - relation: Table/view reference
        - sql: Raw SQL query
        - function: Table-valued function

        Args:
            stream: Stream configuration
            watermark_value: Max watermark from target (for incremental)

        Returns:
            SQL query string

        Raises:
            ExtractorError: If query building fails
        """
        try:
            source = stream.source

            if source.type == "sql":
                return str(source.value)

            elif source.type == "relation":
                if stream.fields:
                    field_exprs = [f.source_expression for f in stream.fields]
                    select_clause = ", ".join(field_exprs)
                else:
                    select_clause = "*"

                query = f"SELECT {select_clause} FROM {source.value}"

                where_clauses = []

                if stream.criteria and "where" in stream.criteria:
                    where_clauses.append(f"({stream.criteria['where']})")

                if stream.is_incremental and watermark_value is not None:
                    watermark_field = stream.extract.watermark_field
                    watermark_filter = self._format_watermark_filter(
                        watermark_field, watermark_value, stream.extract.watermark_type
                    )
                    where_clauses.append(watermark_filter)

                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

                return query

            elif source.type == "function":
                if isinstance(source.value, str):
                    query = f"SELECT * FROM {source.value}()"
                elif isinstance(source.value, dict):
                    func_name = source.value.get("function")
                    params = source.value.get("params", {})
                    param_values = [
                        f"'{v}'" if isinstance(v, str) else str(v)
                        for v in params.values()
                    ]
                    params_str = ", ".join(param_values)
                    query = f"SELECT * FROM {func_name}({params_str})"
                else:
                    raise ExtractorError(f"Invalid function source value: {source.value}")
                return query

            else:
                raise ExtractorError(
                    f"Relational extractor does not support source type: {source.type}. "
                    f"Supported types: relation, sql, function"
                )

        except ExtractorError:
            raise
        except Exception as e:
            raise ExtractorError(f"Failed to build query: {e}") from e

    def _format_watermark_filter(
        self, field_name: str, value: Any, watermark_type: str
    ) -> str:
        """Format watermark filter clause.

        Base implementation uses standard SQL syntax.
        Subclasses can override for database-specific optimizations.

        Args:
            field_name: Watermark field name
            value: Watermark value
            watermark_type: Type of watermark (timestamp, numeric, etc.)

        Returns:
            SQL WHERE clause fragment
        """
        if watermark_type == "timestamp":
            return f"{field_name} > '{value}'"
        else:
            return f"{field_name} > {value}"

    def _extract_batches(self, query: str, buffer: Buffer, batch_size: int) -> int:
        """Execute query and write results to buffer in batches.

        Args:
            query: SQL query to execute
            buffer: Buffer to write to
            batch_size: Records per batch

        Returns:
            Total number of records extracted

        Raises:
            ExtractorError: If extraction fails
        """
        total_records = 0

        try:
            with self.relational_connector.engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(text(query))

                batch = []
                for row in result:
                    record = dict(row._mapping)
                    batch.append(record)

                    if len(batch) >= batch_size:
                        buffer.write_batch(batch)
                        total_records += len(batch)
                        batch = []

                if batch:
                    buffer.write_batch(batch)
                    total_records += len(batch)

            return total_records

        except Exception as e:
            raise ExtractorError(f"Failed to extract records: {e}") from e

    def _get_watermark_value(self, stream: Stream) -> Optional[Any]:
        """Get max watermark value from target.

        Args:
            stream: Stream configuration

        Returns:
            Maximum watermark value or None
        """
        # TODO: Integrate with loader to get watermark from target
        return None

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for relational extraction.

        Args:
            stream: Stream configuration

        Raises:
            ValidationError: If stream configuration is invalid
        """
        if not stream.source:
            raise ValidationError(f"Stream '{stream.id}': source is required")

        if stream.is_incremental:
            if not stream.extract.watermark_field:
                raise ValidationError(
                    f"Stream '{stream.id}': incremental mode requires watermark_field"
                )
            if not stream.extract.watermark_type:
                raise ValidationError(
                    f"Stream '{stream.id}': incremental mode requires watermark_type"
                )


# =============================================================================
# Relational Loader
# =============================================================================


class RelationalLoader(Loader):
    """Base class for relational database loaders.

    Provides SQL-based loading with support for:
    - Multiple load modes: append, replace, upsert
    - Automatic table creation from field definitions
    - Batch processing for large datasets

    Subclasses can override:
    - _build_upsert_sql(): Database-specific upsert syntax
    - _load_with_copy(): Database-specific COPY command
    """

    # Category identifier
    category: str = "relational"

    def __init__(
        self, connector: RelationalConnector, config: Optional[dict[str, Any]] = None
    ):
        """Initialize relational loader.

        Args:
            connector: RelationalConnector instance
            config: Optional loader-specific configuration

        Raises:
            LoaderError: If connector is not a RelationalConnector
        """
        super().__init__(connector, config)
        if not isinstance(connector, RelationalConnector):
            raise LoaderError("RelationalLoader requires a RelationalConnector")

    @property
    def relational_connector(self) -> RelationalConnector:
        """Get connector as RelationalConnector type."""
        return self.connector  # type: ignore

    def load(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records from buffer to SQL target.

        Args:
            stream: Stream configuration
            buffer: Buffer to read records from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()

        try:
            self.validate_stream(stream)

            if not self.relational_connector.table_exists(stream.target.value):
                if not stream.fields:
                    raise LoaderError(
                        f"Stream '{stream.id}': fields required to create target table"
                    )
                self.create_target_stream(stream)

            # Dispatch to appropriate load mode
            if stream.load.mode == "append":
                return self._load_append(stream, buffer)
            elif stream.load.mode == "replace":
                return self._load_replace(stream, buffer)
            elif stream.load.mode == "upsert":
                return self._load_upsert(stream, buffer)
            else:
                raise LoaderError(f"Unsupported load mode: {stream.load.mode}")

        except Exception as e:
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=False,
                records_loaded=0,
                records_inserted=0,
                records_updated=0,
                records_deleted=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(e),
            )

    def create_target_stream(self, stream: Stream) -> None:
        """Create target table from field definitions.

        Args:
            stream: Stream configuration with fields

        Raises:
            LoaderError: If table creation fails
        """
        if not stream.fields:
            raise LoaderError(f"Stream '{stream.id}': fields required to create table")

        try:
            table_ref = self._format_table_name(stream.target.value)

            column_defs = []
            for field in stream.fields:
                col_name = field.target_name
                col_type = self.relational_connector.type_mapper.get_ddl_type(
                    field.dtype, field.precision, field.scale, field.target_dtype
                )
                nullable = "" if field.target_nullable else "NOT NULL"
                col_def = f"{col_name} {col_type} {nullable}".strip()
                column_defs.append(col_def)

            columns_sql = ",\n    ".join(column_defs)
            ddl = f"CREATE TABLE {table_ref} (\n    {columns_sql}\n)"

            self.relational_connector.execute_ddl(ddl)

        except Exception as e:
            raise LoaderError(f"Failed to create target table: {e}") from e

    def _format_table_name(self, table_ref: str) -> str:
        """Format table name with schema if applicable.

        Args:
            table_ref: Table reference from stream config

        Returns:
            Formatted table name for SQL statements
        """
        schema_name, table_name = self.relational_connector._parse_table_ref(table_ref)
        if schema_name:
            return f"{schema_name}.{table_name}"
        else:
            return table_name

    def _load_append(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in append mode (INSERT).

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()
        records_loaded = 0

        try:
            fields = stream.fields if stream.fields else buffer.fields

            table_name = self._format_table_name(stream.target.value)
            field_names = [f.target_name for f in fields]
            columns = ", ".join(field_names)
            placeholders = ", ".join([f":{f.id}" for f in fields])

            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            with self.relational_connector.engine.begin() as conn:
                for batch in buffer.read_batches(stream.load.batch_size):
                    if batch:
                        conn.execute(text(insert_sql), batch)
                        records_loaded += len(batch)

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=True,
                records_loaded=records_loaded,
                records_inserted=records_loaded,
                records_updated=0,
                records_deleted=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            raise LoaderError(f"Failed to load in append mode: {e}") from e

    def _load_replace(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in replace mode (TRUNCATE/DELETE + INSERT).

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()
        records_loaded = 0
        records_deleted = 0

        try:
            table_name = self._format_table_name(stream.target.value)

            with self.relational_connector.engine.begin() as conn:
                count_result = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM {table_name}")
                )
                records_deleted = count_result.fetchone()[0]

                try:
                    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                except Exception:
                    conn.execute(text(f"DELETE FROM {table_name}"))

                fields = stream.fields if stream.fields else buffer.fields
                field_names = [f.target_name for f in fields]
                columns = ", ".join(field_names)
                placeholders = ", ".join([f":{f.id}" for f in fields])

                insert_sql = (
                    f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                )

                for batch in buffer.read_batches(stream.load.batch_size):
                    if batch:
                        conn.execute(text(insert_sql), batch)
                        records_loaded += len(batch)

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=True,
                records_loaded=records_loaded,
                records_inserted=records_loaded,
                records_updated=0,
                records_deleted=records_deleted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            raise LoaderError(f"Failed to load in replace mode: {e}") from e

    def _load_upsert(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in upsert mode.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()
        records_loaded = 0

        try:
            if not stream.load.upsert_keys:
                raise LoaderError(
                    f"Stream '{stream.id}': upsert_keys required for upsert mode"
                )

            table_name = self._format_table_name(stream.target.value)
            fields = stream.fields if stream.fields else buffer.fields

            upsert_sql = self._build_upsert_sql(
                table_name, fields, stream.load.upsert_keys
            )

            with self.relational_connector.engine.begin() as conn:
                for batch in buffer.read_batches(stream.load.batch_size):
                    if batch:
                        conn.execute(text(upsert_sql), batch)
                        records_loaded += len(batch)

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=True,
                records_loaded=records_loaded,
                records_inserted=0,  # Can't distinguish in most SQL upserts
                records_updated=0,
                records_deleted=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            raise LoaderError(f"Failed to load in upsert mode: {e}") from e

    def _build_upsert_sql(
        self, table_name: str, fields: list, upsert_keys: list[str]
    ) -> str:
        """Build database-specific upsert SQL.

        Base implementation uses PostgreSQL ON CONFLICT syntax.
        Subclasses should override for database-specific syntax.

        Args:
            table_name: Target table name
            fields: Field definitions
            upsert_keys: Keys for conflict detection

        Returns:
            SQL statement for upsert
        """
        field_names = [f.target_name for f in fields]
        columns = ", ".join(field_names)
        placeholders = ", ".join([f":{f.id}" for f in fields])

        conflict_cols = ", ".join(upsert_keys)

        update_fields = [f for f in fields if f.target_name not in upsert_keys]
        update_set = ", ".join(
            [f"{f.target_name} = EXCLUDED.{f.target_name}" for f in update_fields]
        )

        upsert_sql = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_set}
        """

        return upsert_sql

    def get_max_watermark(
        self, stream_ref: str, watermark_field: str
    ) -> Optional[Any]:
        """Get maximum watermark value from target table.

        Args:
            stream_ref: Table reference
            watermark_field: Watermark column name

        Returns:
            Maximum watermark value or None
        """
        try:
            table_name = self._format_table_name(stream_ref)
            query = f"SELECT MAX({watermark_field}) as max_wm FROM {table_name}"
            result = self.relational_connector.execute_query(query)
            if result and result[0].get("max_wm") is not None:
                return result[0]["max_wm"]
            return None
        except Exception:
            return None

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for relational loading.

        Args:
            stream: Stream configuration

        Raises:
            ValidationError: If stream configuration is invalid
        """
        if not stream.target:
            raise ValidationError(f"Stream '{stream.id}': target is required")

        if stream.load.mode == "upsert" and not stream.load.upsert_keys:
            raise ValidationError(
                f"Stream '{stream.id}': upsert_keys required for upsert mode"
            )
