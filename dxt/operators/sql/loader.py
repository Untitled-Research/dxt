"""Generic SQL loader using SQLAlchemy.

This module provides a concrete base class for loading data into
any SQL database supported by SQLAlchemy.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text

from dxt.core.buffer import Buffer
from dxt.core.loader import Loader
from dxt.exceptions import LoaderError
from dxt.models.results import LoadResult
from dxt.models.stream import Stream
from dxt.operators.sql.connector import SQLConnector


class SQLLoader(Loader):
    """Concrete SQL loader for SQLAlchemy-based databases.

    Works with any SQL database supported by SQLAlchemy.
    Can be used directly or subclassed for database-specific optimizations.

    Supports multiple load methods:
    - insert: Generic INSERT statements (default, works everywhere)
    - bulk: SQLAlchemy bulk operations (future enhancement)
    - copy: Database-specific COPY command (requires subclass implementation)

    Supports multiple load modes:
    - append: INSERT new records
    - replace: TRUNCATE/DELETE + INSERT
    - upsert: INSERT with conflict handling

    Examples:
        >>> # Use directly with any SQL database
        >>> connector = SQLConnector(config)
        >>> loader = SQLLoader(connector)
        >>> with connector:
        ...     result = loader.load(stream, buffer)
        ...     print(f"Loaded {result.records_loaded} records")

        >>> # Or subclass for database-specific optimizations
        >>> class PostgresLoader(SQLLoader):
        ...     def _load_with_copy(self, stream, buffer):
        ...         # PostgreSQL COPY implementation
        ...         pass
    """

    def __init__(self, connector: SQLConnector, config: Optional[dict[str, Any]] = None):
        """Initialize SQL loader.

        Args:
            connector: SQLConnector instance
            config: Optional loader-specific configuration

        Raises:
            LoaderError: If connector is not a SQLConnector
        """
        super().__init__(connector, config)
        if not isinstance(connector, SQLConnector):
            raise LoaderError("SQLLoader requires a SQLConnector")

    def load(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> LoadResult:
        """Load records from buffer to SQL target.

        Dispatches to appropriate load method based on stream config.

        Args:
            stream: Stream configuration
            buffer: Buffer to read records from

        Returns:
            LoadResult with metrics

        Raises:
            LoaderError: If load fails
        """
        started_at = datetime.now()
        records_loaded = 0
        records_inserted = 0
        records_updated = 0
        records_deleted = 0

        try:
            # Validate stream configuration
            self.validate_stream(stream)

            # Ensure target table exists
            if not self.connector.table_exists(stream.target.value):
                if not stream.fields:
                    raise LoaderError(
                        f"Stream '{stream.id}': fields required to create target table"
                    )
                self.create_target_stream(stream)

            # Check for load method (insert, bulk, copy)
            # For now, always use insert method (method selection is future enhancement)
            load_method = "insert"

            if load_method == "copy":
                return self._load_with_copy(stream, buffer)
            elif load_method == "bulk":
                return self._load_with_bulk(stream, buffer)
            elif load_method == "insert":
                return self._load_with_insert(stream, buffer)
            else:
                raise LoaderError(f"Unsupported load method: {load_method}")

        except Exception as e:
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=False,
                records_loaded=records_loaded,
                records_inserted=records_inserted,
                records_updated=records_updated,
                records_deleted=records_deleted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(e),
            )

    def _load_with_insert(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load using INSERT statements (works everywhere).

        Supports append, replace, and upsert modes.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        # Execute load based on mode
        if stream.load.mode == "append":
            return self._load_append(stream, buffer)
        elif stream.load.mode == "replace":
            return self._load_replace(stream, buffer)
        elif stream.load.mode == "upsert":
            return self._load_upsert(stream, buffer)
        else:
            raise LoaderError(f"Unsupported load mode: {stream.load.mode}")

    def _load_with_bulk(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load using SQLAlchemy bulk operations.

        Base implementation not yet implemented.
        Subclasses can override for specific implementations.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics

        Raises:
            LoaderError: Not yet implemented
        """
        raise LoaderError(
            f"{self.__class__.__name__} does not support 'bulk' method yet. "
            "Use 'insert' method or implement _load_with_bulk()"
        )

    def _load_with_copy(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load using database-specific COPY command.

        Base implementation raises NotImplementedError.
        Subclasses override for database-specific COPY implementations.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics

        Raises:
            LoaderError: COPY not supported in base class
        """
        db_name = self.connector._get_database_name()
        raise LoaderError(
            f"{db_name} loader does not support 'copy' method. "
            "Use 'insert' method or implement _load_with_copy() in subclass"
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
            # Build CREATE TABLE statement
            table_ref = self._format_table_name(stream.target.value)

            column_defs = []
            for field in stream.fields:
                col_name = field.target_name
                col_type = self.connector.type_mapper.get_ddl_type(
                    field.dtype, field.precision, field.scale, field.target_dtype
                )
                nullable = "" if field.target_nullable else "NOT NULL"
                col_def = f"{col_name} {col_type} {nullable}".strip()
                column_defs.append(col_def)

            columns_sql = ",\n    ".join(column_defs)
            ddl = f"""
            CREATE TABLE {table_ref} (
                {columns_sql}
            )
            """

            self.connector.execute_statement(ddl)

        except Exception as e:
            raise LoaderError(f"Failed to create target table: {e}") from e

    def _format_table_name(self, table_ref: str) -> str:
        """Format table name with schema if applicable.

        Base implementation uses connector's _parse_table_ref to handle
        database-specific table references (e.g., schema.table vs table).

        Args:
            table_ref: Table reference from stream config

        Returns:
            Formatted table name for SQL statements
        """
        schema_name, table_name = self.connector._parse_table_ref(table_ref)
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
            # Get target fields
            fields = stream.fields if stream.fields else buffer.fields

            # Build INSERT statement
            table_name = self._format_table_name(stream.target.value)
            field_names = [f.target_name for f in fields]
            columns = ", ".join(field_names)
            placeholders = ", ".join([f":{f.id}" for f in fields])

            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # Insert records in batches
            with self.connector.engine.begin() as conn:
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
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()
            raise LoaderError(f"Failed to load in append mode: {e}") from e

    def _load_replace(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in replace mode (TRUNCATE/DELETE + INSERT).

        Tries TRUNCATE first, falls back to DELETE if not supported.

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

            with self.connector.engine.begin() as conn:
                # Get count before truncate/delete
                count_result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {table_name}"))
                records_deleted = count_result.fetchone()[0]

                # Try TRUNCATE, fall back to DELETE if not supported
                try:
                    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                except Exception:
                    # TRUNCATE not supported (e.g., SQLite), use DELETE
                    conn.execute(text(f"DELETE FROM {table_name}"))

                # Insert new records
                fields = stream.fields if stream.fields else buffer.fields
                field_names = [f.target_name for f in fields]
                columns = ", ".join(field_names)
                placeholders = ", ".join([f":{f.id}" for f in fields])

                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

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

        Tries to build database-specific upsert SQL.
        Falls back to error if not supported.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()
        records_loaded = 0
        records_inserted = 0
        records_updated = 0

        try:
            if not stream.load.upsert_keys:
                raise LoaderError(f"Stream '{stream.id}': upsert_keys required for upsert mode")

            table_name = self._format_table_name(stream.target.value)
            fields = stream.fields if stream.fields else buffer.fields

            # Build database-specific upsert SQL
            upsert_sql = self._build_upsert_sql(table_name, fields, stream.load.upsert_keys)

            # Execute upsert in batches
            with self.connector.engine.begin() as conn:
                for batch in buffer.read_batches(stream.load.batch_size):
                    if batch:
                        conn.execute(text(upsert_sql), batch)
                        records_loaded += len(batch)
                        # Note: Most databases don't provide insert/update counts
                        # separately for upsert operations

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

    def _build_upsert_sql(self, table_name: str, fields: list, upsert_keys: list[str]) -> str:
        """Build database-specific upsert SQL.

        Base implementation tries PostgreSQL syntax (INSERT ... ON CONFLICT).
        Subclasses should override for database-specific syntax.

        Args:
            table_name: Target table name
            fields: Field definitions
            upsert_keys: Keys to use for conflict detection

        Returns:
            SQL statement for upsert

        Raises:
            LoaderError: If upsert not supported
        """
        # Try PostgreSQL ON CONFLICT syntax as default
        # Subclasses can override for database-specific syntax
        field_names = [f.target_name for f in fields]
        columns = ", ".join(field_names)
        placeholders = ", ".join([f":{f.id}" for f in fields])

        # Conflict columns (upsert keys)
        conflict_cols = ", ".join(upsert_keys)

        # Update set clause (all fields except upsert keys)
        update_fields = [f for f in fields if f.target_name not in upsert_keys]
        update_set = ", ".join(
            [f"{f.target_name} = EXCLUDED.{f.target_name}" for f in update_fields]
        )

        # PostgreSQL syntax
        upsert_sql = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_set}
        """

        return upsert_sql
