"""Parquet buffer implementation.

This module provides a Parquet-based buffer for efficient, type-preserving
data storage between extract and load operations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Optional

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from dxt.core.buffer import Buffer
from dxt.exceptions import BufferError
from dxt.models.field import DXTType, Field


class ParquetBuffer(Buffer):
    """Parquet buffer - columnar, compressed, typed via PyArrow.

    Provides efficient disk-based storage with:
    - Type preservation (int stays int, datetime stays datetime)
    - Excellent compression (5-10x smaller than CSV)
    - Columnar format (fast for analytics)
    - Industry standard (works with DuckDB, Polars, Pandas, Spark)

    Suitable for:
    - Production pipelines
    - Large datasets (GBs of data)
    - Separate extract/load phases
    - Type-preserving workflows

    Examples:
        >>> fields = [
        ...     Field(id="id", dtype=DXTType.INT64),
        ...     Field(id="amount", dtype=DXTType.DECIMAL, precision=10, scale=2)
        ... ]
        >>> buffer = ParquetBuffer(fields, location=Path("data.parquet"))
        >>> buffer.write_batch([{"id": 1, "amount": Decimal("99.95")}])
        >>> buffer.finalize()
        >>> buffer.commit()
        >>> for batch in buffer.read_batches(10000):
        ...     print(batch)
        >>> buffer.cleanup()
    """

    def __init__(
        self,
        fields: list[Field],
        location: Path,
        compression: str = "snappy",
        **kwargs: Any,
    ):
        """Initialize Parquet buffer.

        Args:
            fields: List of Field definitions
            location: Path for Parquet file
            compression: Compression algorithm ('snappy', 'gzip', 'zstd', 'none')
            **kwargs: Additional PyArrow options

        Raises:
            BufferError: If PyArrow is not installed
        """
        if not PYARROW_AVAILABLE:
            raise BufferError(
                "ParquetBuffer requires PyArrow. Install with: pip install pyarrow"
            )

        super().__init__(fields, location, **kwargs)
        self.compression = compression if compression != "none" else None

        # Build Arrow schema from DXT fields
        self.schema = self._build_arrow_schema(fields)

        # File paths: temp and final
        self.temp_location = location.with_suffix(".tmp.parquet")
        self.final_location = location

        # Writer state
        self._writer: Optional[pq.ParquetWriter] = None
        self._record_count = 0

    def _build_arrow_schema(self, fields: list[Field]) -> pa.Schema:
        """Build PyArrow schema from DXT fields.

        Args:
            fields: List of DXT Field objects

        Returns:
            PyArrow Schema

        Raises:
            BufferError: If field type cannot be mapped
        """
        arrow_fields = []
        for field in fields:
            # Convert DXT type to Arrow type
            arrow_type = self._dxt_to_arrow_type(field)
            arrow_fields.append(pa.field(field.id, arrow_type, nullable=field.nullable))

        return pa.schema(arrow_fields)

    def _dxt_to_arrow_type(self, field: Field) -> pa.DataType:
        """Convert DXT type to PyArrow type.

        Args:
            field: DXT Field with dtype

        Returns:
            PyArrow DataType

        Raises:
            BufferError: If type cannot be mapped
        """
        dtype = field.dtype if isinstance(field.dtype, DXTType) else DXTType(field.dtype)

        type_map: dict[DXTType, pa.DataType] = {
            DXTType.INT8: pa.int8(),
            DXTType.INT16: pa.int16(),
            DXTType.INT32: pa.int32(),
            DXTType.INT64: pa.int64(),
            DXTType.UINT8: pa.uint8(),
            DXTType.UINT16: pa.uint16(),
            DXTType.UINT32: pa.uint32(),
            DXTType.UINT64: pa.uint64(),
            DXTType.FLOAT32: pa.float32(),
            DXTType.FLOAT64: pa.float64(),
            DXTType.STRING: pa.string(),
            DXTType.BOOL: pa.bool_(),
            DXTType.DATE: pa.date32(),
            DXTType.TIME: pa.time64("ns"),
            DXTType.TIMESTAMP: pa.timestamp("ns"),
            DXTType.TIMESTAMP_TZ: pa.timestamp("ns", tz="UTC"),
            DXTType.BINARY: pa.binary(),
            DXTType.JSON: pa.string(),  # Store JSON as string
        }

        # Handle DECIMAL with precision/scale
        if dtype == DXTType.DECIMAL:
            precision = field.precision or 38
            scale = field.scale or 0
            return pa.decimal128(precision, scale)

        arrow_type = type_map.get(dtype)
        if arrow_type is None:
            raise BufferError(f"Unsupported DXT type for Parquet: {dtype}")

        return arrow_type

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of records to temporary Parquet file.

        Args:
            records: List of dicts with field.id as keys, Python native values

        Raises:
            BufferError: If write fails or buffer is finalized
        """
        if self._finalized:
            raise BufferError("Cannot write to finalized buffer")

        if not records:
            return  # Skip empty batches

        try:
            # Convert Python dicts to Arrow Table
            table = pa.Table.from_pylist(records, schema=self.schema)

            # Create writer on first batch
            if self._writer is None:
                self.temp_location.parent.mkdir(parents=True, exist_ok=True)
                self._writer = pq.ParquetWriter(
                    self.temp_location, self.schema, compression=self.compression
                )

            # Write table to file
            self._writer.write_table(table)
            self._record_count += len(records)

        except Exception as e:
            raise BufferError(f"Failed to write batch to Parquet: {e}") from e

    def finalize(self) -> None:
        """Close Parquet writer and flush buffers.

        Raises:
            BufferError: If finalization fails
        """
        if self._writer is not None:
            try:
                self._writer.close()
                self._writer = None
            except Exception as e:
                raise BufferError(f"Failed to finalize Parquet buffer: {e}") from e

        self._finalized = True

    def commit(self) -> None:
        """Atomically rename temp file to final location.

        Raises:
            BufferError: If commit fails or not finalized
        """
        if not self._finalized:
            raise BufferError("Cannot commit before finalize")

        if not self.temp_location.exists():
            # No data was written (empty buffer)
            self._committed = True
            return

        try:
            # Atomic rename: temp → final
            self.temp_location.rename(self.final_location)
            self._committed = True
        except Exception as e:
            raise BufferError(f"Failed to commit Parquet buffer: {e}") from e

    def rollback(self) -> None:
        """Delete temporary file and reset state.

        Raises:
            BufferError: If rollback fails
        """
        try:
            # Close writer if still open
            if self._writer is not None:
                self._writer.close()
                self._writer = None

            # Delete temp file
            if self.temp_location.exists():
                self.temp_location.unlink()

            self._finalized = False
            self._record_count = 0

        except Exception as e:
            raise BufferError(f"Failed to rollback Parquet buffer: {e}") from e

    def read_batches(self, batch_size: int = 10000) -> Iterator[list[dict[str, Any]]]:
        """Read records from Parquet file in batches.

        Args:
            batch_size: Number of records per batch

        Yields:
            Lists of dicts with field.id as keys, Python native values

        Raises:
            BufferError: If read fails or buffer not committed
        """
        if not self._committed:
            raise BufferError("Cannot read from buffer before commit")

        if not self.final_location.exists():
            # Empty buffer
            return

        try:
            # Open Parquet file and iterate over batches
            parquet_file = pq.ParquetFile(self.final_location)

            for batch in parquet_file.iter_batches(batch_size=batch_size):
                # Convert Arrow batch to Python dicts
                yield batch.to_pylist()

        except Exception as e:
            raise BufferError(f"Failed to read from Parquet buffer: {e}") from e

    def exists(self) -> bool:
        """Check if committed Parquet file exists.

        Returns:
            True if final file exists and is committed, False otherwise
        """
        return self._committed and self.final_location.exists()

    def cleanup(self) -> None:
        """Remove Parquet files and reset state.

        Idempotent - can be called multiple times safely.
        """
        try:
            # Close writer if still open
            if self._writer is not None:
                self._writer.close()
                self._writer = None

            # Remove temp file
            if self.temp_location.exists():
                self.temp_location.unlink()

            # Remove final file
            if self.final_location.exists():
                self.final_location.unlink()

            self._finalized = False
            self._committed = False
            self._record_count = 0

        except Exception as e:
            raise BufferError(f"Failed to cleanup Parquet buffer: {e}") from e

    @property
    def record_count(self) -> int:
        """Number of records written to buffer.

        Returns:
            Record count
        """
        if self._committed and self.final_location.exists():
            # For committed buffer, read from metadata
            try:
                parquet_file = pq.ParquetFile(self.final_location)
                return parquet_file.metadata.num_rows
            except Exception:
                return self._record_count
        return self._record_count
