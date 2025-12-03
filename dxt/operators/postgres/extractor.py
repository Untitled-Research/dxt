"""PostgreSQL extractor implementation.

This module provides data extraction from PostgreSQL databases.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text

from dxt.core.buffer import Buffer
from dxt.core.extractor import Extractor
from dxt.exceptions import ExtractorError
from dxt.models.results import ExtractResult
from dxt.models.stream import Stream
from dxt.operators.postgres.connector import PostgresConnector


class PostgresExtractor(Extractor):
    """PostgreSQL extractor.

    Extracts records from PostgreSQL tables, views, or custom queries
    and writes them to a buffer.

    Supports:
    - Full extraction
    - Incremental extraction with watermark
    - Table, view, and query sources
    - Batch processing

    Examples:
        >>> connector = PostgresConnector(config)
        >>> extractor = PostgresExtractor(connector)
        >>> with connector:
        ...     result = extractor.extract(stream, buffer)
        ...     print(f"Extracted {result.records_extracted} records")
    """

    def __init__(self, connector: PostgresConnector, config: Optional[dict[str, Any]] = None):
        """Initialize PostgreSQL extractor.

        Args:
            connector: PostgresConnector instance
            config: Optional extractor-specific configuration
        """
        super().__init__(connector, config)
        if not isinstance(connector, PostgresConnector):
            raise ExtractorError("PostgresExtractor requires a PostgresConnector")

    def extract(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> ExtractResult:
        """Extract records from PostgreSQL to buffer.

        Args:
            stream: Stream configuration
            buffer: Buffer to write records to

        Returns:
            ExtractResult with metrics

        Raises:
            ExtractorError: If extraction fails
        """
        started_at = datetime.now()
        records_extracted = 0
        watermark_value = None

        try:
            # Validate stream configuration
            self.validate_stream(stream)

            # For incremental extraction, get watermark from target
            if stream.is_incremental:
                watermark_value = self._get_watermark_value(stream)

            # Build extraction query
            query = self.build_query(stream, watermark_value)

            if query is None:
                raise ExtractorError(f"Could not build query for stream {stream.id}")

            # Execute query and stream results to buffer
            batch_size = stream.extract.batch_size
            records_extracted = self._extract_batches(query, buffer, batch_size)

            # Finalize and commit buffer
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
            # Rollback buffer on error
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
        """Build extraction query.

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

            # Handle different source types
            if source.type == "sql":
                # Direct SQL query - use as-is
                return str(source.value)

            elif source.type == "relation":
                # Table/view - build SELECT query
                # Start with SELECT
                if stream.fields:
                    # Use specified fields with source expressions
                    field_exprs = [f.source_expression for f in stream.fields]
                    select_clause = ", ".join(field_exprs)
                else:
                    # Select all columns
                    select_clause = "*"

                query = f"SELECT {select_clause} FROM {source.value}"

                # Add WHERE clauses
                where_clauses = []

                # Add criteria from stream config
                if stream.criteria and "where" in stream.criteria:
                    where_clauses.append(f"({stream.criteria['where']})")

                # Add watermark filter for incremental extraction
                if stream.is_incremental and watermark_value is not None:
                    watermark_field = stream.extract.watermark_field
                    if stream.extract.watermark_type == "timestamp":
                        where_clauses.append(f"{watermark_field} > '{watermark_value}'")
                    else:
                        where_clauses.append(f"{watermark_field} > {watermark_value}")

                # Combine WHERE clauses
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

                return query

            elif source.type == "function":
                # Table-valued function
                if isinstance(source.value, str):
                    # Simple function name without params
                    query = f"SELECT * FROM {source.value}()"
                elif isinstance(source.value, dict):
                    # Function with params
                    func_name = source.value.get("function")
                    params = source.value.get("params", {})
                    # Build params string
                    param_values = [f"'{v}'" if isinstance(v, str) else str(v) for v in params.values()]
                    params_str = ", ".join(param_values)
                    query = f"SELECT * FROM {func_name}({params_str})"
                else:
                    raise ExtractorError(f"Invalid function source value: {source.value}")

                return query

            else:
                raise ExtractorError(
                    f"PostgreSQL extractor does not support source type: {source.type}. "
                    f"Supported types: relation, sql, function"
                )

        except Exception as e:
            raise ExtractorError(f"Failed to build query: {e}") from e

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
            # Execute query with server-side cursor for large result sets
            with self.connector.engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(text(query))

                batch = []
                for row in result:
                    # Convert row to dict
                    record = dict(row._mapping)
                    batch.append(record)

                    # Write batch when full
                    if len(batch) >= batch_size:
                        buffer.write_batch(batch)
                        total_records += len(batch)
                        batch = []

                # Write remaining records
                if batch:
                    buffer.write_batch(batch)
                    total_records += len(batch)

            return total_records

        except Exception as e:
            raise ExtractorError(f"Failed to extract records: {e}") from e

    def _get_watermark_value(self, stream: Stream) -> Optional[Any]:
        """Get max watermark value from target (if exists).

        This would typically call the loader to query the target,
        but for now we return None (will be handled by loader).

        Args:
            stream: Stream configuration

        Returns:
            Maximum watermark value or None
        """
        # TODO: Integrate with loader to get watermark from target
        # For now, return None and let the loader handle it
        return None

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for PostgreSQL extraction.

        Args:
            stream: Stream configuration

        Raises:
            ValidationError: If stream configuration is invalid
        """
        from dxt.exceptions import ValidationError

        # Check that source is specified
        if not stream.source:
            raise ValidationError(f"Stream '{stream.id}': source is required")

        # For incremental extraction, watermark field is required
        if stream.is_incremental:
            if not stream.extract.watermark_field:
                raise ValidationError(
                    f"Stream '{stream.id}': incremental mode requires watermark_field"
                )
            if not stream.extract.watermark_type:
                raise ValidationError(
                    f"Stream '{stream.id}': incremental mode requires watermark_type"
                )

