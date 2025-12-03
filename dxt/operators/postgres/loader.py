"""PostgreSQL loader implementation.

This module provides data loading into PostgreSQL databases.
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
from dxt.operators.postgres.connector import PostgresConnector


class PostgresLoader(Loader):
    """PostgreSQL loader.

    Loads records from a buffer into PostgreSQL tables using different
    load strategies (append, replace, upsert).

    Supports:
    - Append mode (INSERT)
    - Replace mode (TRUNCATE + INSERT)
    - Upsert mode (INSERT ... ON CONFLICT)
    - Automatic table creation
    - Bulk loading with COPY (future enhancement)

    Examples:
        >>> connector = PostgresConnector(config)
        >>> loader = PostgresLoader(connector)
        >>> with connector:
        ...     result = loader.load(stream, buffer)
        ...     print(f"Loaded {result.records_loaded} records")
    """

    def __init__(self, connector: PostgresConnector, config: Optional[dict[str, Any]] = None):
        """Initialize PostgreSQL loader.

        Args:
            connector: PostgresConnector instance
            config: Optional loader-specific configuration
        """
        super().__init__(connector, config)
        if not isinstance(connector, PostgresConnector):
            raise LoaderError("PostgresLoader requires a PostgresConnector")

    def load(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> LoadResult:
        """Load records from buffer to PostgreSQL.

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

            # Execute load based on mode
            if stream.load.mode == "append":
                result = self._load_append(stream, buffer)
            elif stream.load.mode == "replace":
                result = self._load_replace(stream, buffer)
            elif stream.load.mode == "upsert":
                result = self._load_upsert(stream, buffer)
            else:
                raise LoaderError(f"Unsupported load mode: {stream.load.mode}")

            return result

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
            schema_name, table_name = self.connector._parse_table_ref(stream.target.value)

            column_defs = []
            for field in stream.fields:
                col_name = field.target_name
                col_type = self.connector.type_mapper.get_ddl_type(
                    field.dtype, field.precision, field.scale, field.target_dtype
                )
                nullable = "NULL" if field.target_nullable else "NOT NULL"
                column_defs.append(f"{col_name} {col_type} {nullable}")

            columns_sql = ",\n    ".join(column_defs)
            ddl = f"""
            CREATE TABLE {schema_name}.{table_name} (
                {columns_sql}
            )
            """

            self.connector.execute_statement(ddl)

        except Exception as e:
            raise LoaderError(f"Failed to create target table: {e}") from e

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
            schema_name, table_name = self.connector._parse_table_ref(stream.target.value)
            field_names = [f.target_name for f in fields]
            columns = ", ".join(field_names)
            placeholders = ", ".join([f":{f.id}" for f in fields])

            insert_sql = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"

            # Insert records in batches
            with self.connector.engine.begin() as conn:
                for batch in buffer.read_batches(stream.load.batch_size):
                    if batch:
                        # Remap record keys to match field IDs for SQLAlchemy
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
        """Load records in replace mode (TRUNCATE + INSERT).

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
            schema_name, table_name = self.connector._parse_table_ref(stream.target.value)

            with self.connector.engine.begin() as conn:
                # Get count before truncate
                count_result = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM {schema_name}.{table_name}")
                )
                records_deleted = count_result.fetchone()[0]

                # Truncate table
                conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name}"))

                # Insert new records
                fields = stream.fields if stream.fields else buffer.fields
                field_names = [f.target_name for f in fields]
                columns = ", ".join(field_names)
                placeholders = ", ".join([f":{f.id}" for f in fields])

                insert_sql = (
                    f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
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
        """Load records in upsert mode (INSERT ... ON CONFLICT).

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

            schema_name, table_name = self.connector._parse_table_ref(stream.target.value)
            fields = stream.fields if stream.fields else buffer.fields

            # Build INSERT ... ON CONFLICT statement
            field_names = [f.target_name for f in fields]
            columns = ", ".join(field_names)
            placeholders = ", ".join([f":{f.id}" for f in fields])

            # Conflict columns (upsert keys)
            conflict_cols = ", ".join(stream.load.upsert_keys)

            # Update set clause (all fields except upsert keys)
            update_fields = [f for f in fields if f.target_name not in stream.load.upsert_keys]
            update_set = ", ".join([f"{f.target_name} = EXCLUDED.{f.target_name}" for f in update_fields])

            upsert_sql = f"""
            INSERT INTO {schema_name}.{table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_set}
            """

            # Execute upsert in batches
            with self.connector.engine.begin() as conn:
                for batch in buffer.read_batches(stream.load.batch_size):
                    if batch:
                        conn.execute(text(upsert_sql), batch)
                        records_loaded += len(batch)
                        # Note: PostgreSQL doesn't provide insert/update counts separately
                        # for ON CONFLICT, so we just track total

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=True,
                records_loaded=records_loaded,
                records_inserted=0,  # Can't distinguish in PostgreSQL ON CONFLICT
                records_updated=0,
                records_deleted=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            raise LoaderError(f"Failed to load in upsert mode: {e}") from e
