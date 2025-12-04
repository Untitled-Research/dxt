"""Generic SQL extractor using SQLAlchemy.

This module provides a concrete base class for extracting data from
any SQL database supported by SQLAlchemy.
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
from dxt.operators.sql.connector import SQLConnector


class SQLExtractor(Extractor):
    """Concrete SQL extractor for SQLAlchemy-based databases.

    Works with any SQL database supported by SQLAlchemy.
    Can be used directly or subclassed for database-specific optimizations.

    Supports:
    - Full extraction
    - Incremental extraction with watermark
    - Table, view, and query sources
    - Batch processing with server-side cursors
    - Field selection and WHERE criteria

    Examples:
        >>> # Use directly with any SQL database
        >>> connector = SQLConnector(config)
        >>> extractor = SQLExtractor(connector)
        >>> with connector:
        ...     result = extractor.extract(stream, buffer)
        ...     print(f"Extracted {result.records_extracted} records")

        >>> # Or subclass for database-specific optimizations
        >>> class PostgresExtractor(SQLExtractor):
        ...     def _format_watermark_filter(self, field, value, field_obj):
        ...         return f"{field} > '{value}'::timestamp"
    """

    def __init__(self, connector: SQLConnector, config: Optional[dict[str, Any]] = None):
        """Initialize SQL extractor.

        Args:
            connector: SQLConnector instance
            config: Optional extractor-specific configuration

        Raises:
            ExtractorError: If connector is not a SQLConnector
        """
        super().__init__(connector, config)
        if not isinstance(connector, SQLConnector):
            raise ExtractorError("SQLExtractor requires a SQLConnector")

    def extract(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> ExtractResult:
        """Extract records from SQL source to buffer.

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

    def build_query(self, stream: Stream, watermark_value: Optional[Any] = None) -> Optional[str]:
        """Build extraction query from stream configuration.

        Supports:
        - source_type: relation (table/view), sql (query), function (stored proc)
        - field selection via stream.fields
        - where criteria via stream.criteria
        - watermark filters for incremental extraction

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
                    watermark_filter = self._format_watermark_filter(
                        watermark_field, watermark_value, stream.extract.watermark_type
                    )
                    where_clauses.append(watermark_filter)

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
                    param_values = [
                        f"'{v}'" if isinstance(v, str) else str(v) for v in params.values()
                    ]
                    params_str = ", ".join(param_values)
                    query = f"SELECT * FROM {func_name}({params_str})"
                else:
                    raise ExtractorError(f"Invalid function source value: {source.value}")

                return query

            else:
                raise ExtractorError(
                    f"SQL extractor does not support source type: {source.type}. "
                    f"Supported types: relation, sql, function"
                )

        except Exception as e:
            raise ExtractorError(f"Failed to build query: {e}") from e

    def _format_watermark_filter(self, field_name: str, value: Any, watermark_type: str) -> str:
        """Format watermark filter for generic SQL.

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
            # Use quoted string for timestamps (ISO8601 format)
            return f"{field_name} > '{value}'"
        else:
            # Numeric or other types - no quotes
            return f"{field_name} > {value}"

    def _extract_batches(self, query: str, buffer: Buffer, batch_size: int) -> int:
        """Execute query and write results to buffer in batches.

        Uses SQLAlchemy's stream_results option for memory-efficient
        processing of large result sets.

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
        """Validate stream configuration for SQL extraction.

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
