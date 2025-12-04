"""PostgreSQL COPY loader for high-performance bulk loading."""

from __future__ import annotations

import io
from datetime import datetime
from typing import Any, Optional

from dxt.core.buffer import Buffer
from dxt.exceptions import LoaderError
from dxt.models.results import LoadResult
from dxt.models.stream import Stream
from dxt.operators.postgres.loader import PostgresLoader


class PostgresCopyLoader(PostgresLoader):
    """PostgreSQL loader using COPY FROM STDIN for high performance.

    Much faster than INSERT for bulk loading large datasets.
    Uses psycopg's COPY protocol for streaming data.

    Configuration options (via loader_config):
    - copy_format: "csv" (default) or "binary"
    - delimiter: CSV delimiter (default: ",")
    - null_string: String to represent NULL (default: "")
    - quote_char: Quote character for CSV (default: '"')
    - escape_char: Escape character for CSV (default: '"')

    Examples:
        YAML specification with full module path:
        ```yaml
        target:
          connection: postgres_target
          object: staging.large_table
          loader: dxt.operators.postgres.copy_loader.PostgresCopyLoader
          loader_config:
            copy_format: csv
            delimiter: ","
            batch_size: 100000
        ```
    """

    def __init__(self, connector, config: Optional[dict[str, Any]] = None):
        """Initialize PostgreSQL COPY loader.

        Args:
            connector: PostgresConnector instance
            config: Optional loader configuration
                - copy_format: "csv" or "binary"
                - delimiter: CSV delimiter
                - null_string: NULL representation
                - quote_char: Quote character
                - escape_char: Escape character
        """
        super().__init__(connector, config)

        # COPY-specific configuration
        self.copy_format = self.config.get("copy_format", "csv")
        self.delimiter = self.config.get("delimiter", ",")
        self.null_string = self.config.get("null_string", "")
        self.quote_char = self.config.get("quote_char", '"')
        self.escape_char = self.config.get("escape_char", '"')

    def _load_append(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load using COPY FROM STDIN instead of INSERT.

        Overrides parent's _load_append to use COPY protocol.
        """
        started_at = datetime.now()
        records_loaded = 0

        try:
            table_name = self._format_table_name(stream.target.value)
            fields = stream.fields if stream.fields else buffer.fields
            field_names = [f.target_name for f in fields]

            if self.copy_format == "csv":
                records_loaded = self._copy_from_csv(
                    table_name, field_names, fields, buffer, stream.load.batch_size
                )
            elif self.copy_format == "binary":
                records_loaded = self._copy_from_binary(
                    table_name, field_names, fields, buffer, stream.load.batch_size
                )
            else:
                raise LoaderError(f"Unsupported COPY format: {self.copy_format}")

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
            raise LoaderError(f"Failed to load with COPY: {e}") from e

    def _copy_from_csv(
        self,
        table_name: str,
        field_names: list[str],
        fields: list,
        buffer: Buffer,
        batch_size: int,
    ) -> int:
        """Execute COPY FROM STDIN with CSV format.

        Args:
            table_name: Target table name
            field_names: List of column names
            fields: Field definitions
            buffer: Buffer to read from
            batch_size: Records per batch

        Returns:
            Number of records loaded
        """
        # Create CSV buffer
        csv_buffer = io.StringIO()
        records_count = 0

        for batch in buffer.read_batches(batch_size):
            for record in batch:
                # Write CSV row
                values = []
                for field in fields:
                    value = record.get(field.id)
                    csv_value = self._format_csv_value(value)
                    values.append(csv_value)

                csv_buffer.write(self.delimiter.join(values) + "\n")
                records_count += 1

        # Reset buffer position
        csv_buffer.seek(0)

        # Execute COPY using psycopg raw connection
        raw_conn = self.connector.engine.raw_connection()
        try:
            cursor = raw_conn.cursor()

            # Build COPY command
            copy_sql = f"""
            COPY {table_name} ({','.join(field_names)})
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER '{self.delimiter}', NULL '{self.null_string}')
            """

            # Execute COPY
            cursor.copy_expert(copy_sql, csv_buffer)
            raw_conn.commit()

        finally:
            raw_conn.close()

        return records_count

    def _copy_from_binary(
        self,
        table_name: str,
        field_names: list[str],
        fields: list,
        buffer: Buffer,
        batch_size: int,
    ) -> int:
        """Execute COPY FROM STDIN with binary format.

        Binary format is faster than CSV but more complex.
        Requires careful encoding of PostgreSQL binary format.

        For now, raises NotImplementedError - future enhancement.
        """
        raise LoaderError("Binary COPY format not yet implemented. Use copy_format: csv")

    def _format_csv_value(self, value: Any) -> str:
        """Format value for CSV output.

        Handles:
        - NULL values
        - Strings with special characters (quotes, delimiters, newlines)
        - Type conversions

        Args:
            value: Value to format

        Returns:
            Formatted CSV string
        """
        if value is None:
            return self.null_string

        if isinstance(value, str):
            # Escape quotes
            value = value.replace(self.quote_char, self.escape_char + self.quote_char)

            # Quote if contains delimiter, newline, or quote char
            if (
                self.delimiter in value
                or "\n" in value
                or self.quote_char in value
                or "\r" in value
            ):
                return f"{self.quote_char}{value}{self.quote_char}"

            return value

        elif isinstance(value, bool):
            # PostgreSQL COPY expects 't' or 'f' for boolean
            return "t" if value else "f"

        elif isinstance(value, (int, float)):
            return str(value)

        elif isinstance(value, datetime):
            # ISO format for timestamps
            return value.isoformat()

        else:
            # Default: convert to string
            return str(value)
