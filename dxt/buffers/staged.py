"""Staged buffer implementation.

This module provides the main Buffer class that stages data to disk
between extract and load operations.
"""

from __future__ import annotations

import csv
import os
import shutil
import uuid
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Optional

from dxt.exceptions import BufferError
from dxt.models.field import DXTType, Field

# Import Stream for type hints only to avoid circular imports
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dxt.models.stream import Stream

# Optional dependencies
try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pa = None
    pq = None


class BufferFormat(str, Enum):
    """Supported buffer formats."""

    PARQUET = "parquet"
    CSV = "csv"
    JSONL = "jsonl"
    PASSTHROUGH = "passthrough"


@dataclass
class CSVOptions:
    """CSV format options."""

    delimiter: str = ","
    quotechar: str = '"'
    escapechar: Optional[str] = None
    doublequote: bool = True
    lineterminator: str = "\n"
    header: bool = True
    encoding: str = "utf-8"
    null_value: str = ""


@dataclass
class StagedFile:
    """Metadata about a staged file."""

    path: str
    format: str
    compression: Optional[str] = None
    size_bytes: int = 0
    row_count: Optional[int] = None
    schema: Optional[list[Field]] = None
    original_name: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class Buffer:
    """Staging buffer for pipeline data.

    The buffer is the intermediary between extractor and loader.
    It handles all persistence and format conversion.

    All data is persisted to disk - no streaming/memory-only mode.
    This enables debugging, retry without re-extraction, and decoupling
    of extract and load phases.

    Example:
        >>> buffer = Buffer(location="/tmp/staging", format="parquet")
        >>>
        >>> # Extractor writes records
        >>> schema = [Field(id="id", dtype=DXTType.INT64)]
        >>> buffer.write_records(records_iterator, schema)
        >>>
        >>> # Loader reads records
        >>> for record in buffer.read_records():
        ...     loader.insert(record)
        >>>
        >>> # Or loader uses files directly
        >>> for staged_file in buffer.get_staged_files():
        ...     loader.copy_from(staged_file.path)
        >>>
        >>> buffer.cleanup()
    """

    def __init__(
        self,
        location: str = ".dxt/staging",
        format: str = "parquet",
        compression: Optional[str] = None,
        batch_size: int = 10000,
        csv_options: Optional[CSVOptions] = None,
        run_id: Optional[str] = None,
        stream: Optional["Stream"] = None,
    ):
        """Initialize buffer.

        Args:
            location: Directory for staged files
            format: File format (parquet, csv, jsonl, passthrough)
            compression: Compression algorithm (snappy, gzip, zstd)
            batch_size: Records per file when writing
            csv_options: CSV-specific options
            run_id: Unique identifier for this run (auto-generated if not provided)
            stream: Optional Stream for field mapping and type coercion
        """
        self.location = Path(location)
        self._format = BufferFormat(format)
        self.compression = compression
        self.batch_size = batch_size
        self.csv_options = csv_options or CSVOptions()
        self.run_id = run_id or str(uuid.uuid4())[:8]
        self._stream = stream

        # State
        self._schema: Optional[list[Field]] = None
        self._staged_files: list[StagedFile] = []
        self._total_rows: int = 0
        self._finalized: bool = False

        # Validate format requirements
        if self._format == BufferFormat.PARQUET and not PYARROW_AVAILABLE:
            raise BufferError(
                "Parquet format requires PyArrow. Install with: pip install pyarrow"
            )

    @property
    def format(self) -> str:
        """Staged format."""
        return self._format.value

    @property
    def schema(self) -> Optional[list[Field]]:
        """Schema of staged data (None for passthrough)."""
        return self._schema

    @property
    def row_count(self) -> Optional[int]:
        """Total rows across all staged files (None for passthrough)."""
        if self._format == BufferFormat.PASSTHROUGH:
            return None
        return self._total_rows

    @property
    def file_count(self) -> int:
        """Number of staged files."""
        return len(self._staged_files)

    @property
    def fields(self) -> Optional[list[Field]]:
        """Get fields from stream or schema.

        Priority: stream.fields > buffer schema
        Returns fields with target_name for loader use.
        """
        if self._stream and self._stream.fields:
            return self._stream.fields
        return self._schema

    @property
    def target_field_names(self) -> list[str]:
        """Get target field names for loading.

        Uses field.target_name (falls back to field.id).
        """
        fields = self.fields
        if not fields:
            return []
        return [f.target_name for f in fields]

    @property
    def source_field_names(self) -> list[str]:
        """Get source field names for extraction.

        Uses field.id (source column name).
        """
        fields = self.fields
        if not fields:
            return []
        return [f.id for f in fields]

    def get_field_by_id(self, field_id: str) -> Optional[Field]:
        """Get a field by its source ID."""
        fields = self.fields
        if not fields:
            return None
        for f in fields:
            if f.id == field_id:
                return f
        return None

    # === Extractor writes to buffer ===

    def write_records(
        self,
        records: Iterator[dict[str, Any]],
        schema: list[Field],
    ) -> None:
        """Serialize records to staged file(s).

        Buffer handles:
        - Batching records into appropriately sized files
        - Serialization to configured format (parquet, csv)
        - Compression if configured

        Args:
            records: Iterator of record dicts
            schema: Field definitions

        Raises:
            BufferError: If format is passthrough or write fails
        """
        if self._format == BufferFormat.PASSTHROUGH:
            raise BufferError("Cannot write records in passthrough mode")

        if self._finalized:
            raise BufferError("Buffer already finalized")

        self._schema = schema
        self._ensure_location()

        if self._format == BufferFormat.PARQUET:
            self._write_records_parquet(records, schema)
        elif self._format == BufferFormat.CSV:
            self._write_records_csv(records, schema)
        elif self._format == BufferFormat.JSONL:
            self._write_records_jsonl(records, schema)

        self._finalized = True

    def write_file(
        self,
        stream: BinaryIO,
        original_name: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Stage a file as-is (passthrough mode).

        Buffer handles:
        - Streaming to disk without loading into memory
        - Preserving original filename and metadata

        Args:
            stream: Binary file stream
            original_name: Original filename
            metadata: Optional metadata dict

        Raises:
            BufferError: If write fails
        """
        self._ensure_location()

        # Generate staged filename
        staged_name = f"{self.run_id}_{original_name}"
        staged_path = self.location / staged_name

        try:
            with open(staged_path, "wb") as f:
                shutil.copyfileobj(stream, f)

            size_bytes = staged_path.stat().st_size

            staged_file = StagedFile(
                path=str(staged_path),
                format="passthrough",
                size_bytes=size_bytes,
                original_name=original_name,
                metadata=metadata or {},
            )
            self._staged_files.append(staged_file)

        except Exception as e:
            raise BufferError(f"Failed to stage file: {e}") from e

    # === Loader reads from buffer ===

    def read_records(
        self,
        apply_field_mapping: bool = True,
        coerce_types: bool = True,
    ) -> Iterator[dict[str, Any]]:
        """Parse staged files and yield records.

        Buffer handles:
        - Reading staged files
        - Deserializing from format (parquet, csv, jsonl)
        - Decompression if needed
        - Type coercion (CSV strings → proper types)
        - Field name mapping (source_name → target_name)

        Args:
            apply_field_mapping: If True, remap field.id → field.target_name
            coerce_types: If True, convert string values to proper types

        Yields:
            Record dicts with target field names and typed values

        Raises:
            BufferError: If format is passthrough or read fails
        """
        if self._format == BufferFormat.PASSTHROUGH:
            raise BufferError("Cannot read records in passthrough mode")

        if not self._finalized:
            raise BufferError("Buffer not finalized")

        # Get raw records from format-specific reader
        if self._format == BufferFormat.PARQUET:
            raw_records = self._read_records_parquet()
        elif self._format == BufferFormat.CSV:
            raw_records = self._read_records_csv()
        elif self._format == BufferFormat.JSONL:
            raw_records = self._read_records_jsonl()
        else:
            raise BufferError(f"Unsupported format: {self._format}")

        # Apply transformations
        for record in raw_records:
            if coerce_types and self._format in (BufferFormat.CSV, BufferFormat.JSONL):
                record = self._coerce_record_types(record)
            if apply_field_mapping:
                record = self._apply_field_mapping(record)
            yield record

    def _coerce_record_types(self, record: dict[str, Any]) -> dict[str, Any]:
        """Coerce string values to proper types based on schema.

        CSV files return all values as strings. This converts them
        to the types defined in the schema.
        """
        fields = self.fields
        if not fields:
            return record  # No schema, return as-is

        result = {}
        for key, value in record.items():
            field = self.get_field_by_id(key)
            if field and value is not None:
                result[key] = self._coerce_value(value, field)
            else:
                result[key] = value
        return result

    def _coerce_value(self, value: Any, field: Field) -> Any:
        """Coerce a single value to its target type."""
        if value is None:
            return None

        # Already the right type (e.g., from parquet)
        if not isinstance(value, str):
            return value

        # Empty string = None (for non-string types)
        dtype = field.dtype if isinstance(field.dtype, DXTType) else DXTType(field.dtype)
        if value == "" and dtype != DXTType.STRING:
            return None

        try:
            if dtype in (DXTType.INT8, DXTType.INT16, DXTType.INT32, DXTType.INT64,
                        DXTType.UINT8, DXTType.UINT16, DXTType.UINT32, DXTType.UINT64):
                return int(value)
            elif dtype in (DXTType.FLOAT32, DXTType.FLOAT64):
                return float(value)
            elif dtype == DXTType.DECIMAL:
                from decimal import Decimal
                return Decimal(value)
            elif dtype == DXTType.BOOL:
                return value.lower() in ("true", "1", "t", "yes", "y")
            elif dtype == DXTType.DATE:
                from datetime import date
                return date.fromisoformat(value)
            elif dtype == DXTType.TIME:
                from datetime import time
                return time.fromisoformat(value)
            elif dtype in (DXTType.TIMESTAMP, DXTType.TIMESTAMP_TZ):
                from datetime import datetime
                return datetime.fromisoformat(value)
            elif dtype == DXTType.JSON:
                import json
                return json.loads(value)
            elif dtype == DXTType.BINARY:
                import base64
                return base64.b64decode(value)
            else:
                return value
        except (ValueError, TypeError):
            return value  # Return as-is if conversion fails

    def _apply_field_mapping(self, record: dict[str, Any]) -> dict[str, Any]:
        """Remap field.id → field.target_name in record keys."""
        fields = self.fields
        if not fields:
            return record  # No schema, return as-is

        # Build mapping: source_id → target_name
        mapping = {f.id: f.target_name for f in fields}

        result = {}
        for key, value in record.items():
            target_key = mapping.get(key, key)
            result[target_key] = value
        return result

    def get_staged_files(self) -> list[StagedFile]:
        """Get list of staged files for direct access.

        Used by loaders that work with files directly:
        - Snowflake COPY (PUT file, then COPY INTO)
        - S3 upload
        - PostgreSQL COPY FROM

        Returns:
            List of StagedFile objects
        """
        return self._staged_files.copy()

    # === Lifecycle ===

    def cleanup(self) -> None:
        """Remove all staged files."""
        for staged_file in self._staged_files:
            try:
                path = Path(staged_file.path)
                if path.exists():
                    path.unlink()
            except Exception:
                pass  # Best effort cleanup

        self._staged_files = []
        self._total_rows = 0
        self._finalized = False
        self._schema = None

    # === Internal methods ===

    def _ensure_location(self) -> None:
        """Create staging directory if needed."""
        self.location.mkdir(parents=True, exist_ok=True)

    def _get_staged_path(self, index: int) -> Path:
        """Generate path for a staged file."""
        ext = self._get_extension()
        return self.location / f"{self.run_id}_{index:04d}{ext}"

    def _get_extension(self) -> str:
        """Get file extension for format."""
        extensions = {
            BufferFormat.PARQUET: ".parquet",
            BufferFormat.CSV: ".csv",
            BufferFormat.JSONL: ".jsonl",
            BufferFormat.PASSTHROUGH: "",
        }
        ext = extensions[self._format]
        if self.compression == "gzip" and self._format in (BufferFormat.CSV, BufferFormat.JSONL):
            ext += ".gz"
        return ext

    # === Parquet implementation ===

    def _write_records_parquet(
        self,
        records: Iterator[dict[str, Any]],
        schema: list[Field],
    ) -> None:
        """Write records to parquet file(s)."""
        arrow_schema = self._build_arrow_schema(schema)
        file_index = 0
        batch: list[dict[str, Any]] = []

        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._write_parquet_batch(batch, arrow_schema, file_index)
                file_index += 1
                batch = []

        # Write remaining records
        if batch:
            self._write_parquet_batch(batch, arrow_schema, file_index)

    def _write_parquet_batch(
        self,
        batch: list[dict[str, Any]],
        arrow_schema: "pa.Schema",
        file_index: int,
    ) -> None:
        """Write a batch of records to a parquet file."""
        path = self._get_staged_path(file_index)

        try:
            table = pa.Table.from_pylist(batch, schema=arrow_schema)
            compression = self.compression if self.compression != "none" else None
            pq.write_table(table, path, compression=compression)

            staged_file = StagedFile(
                path=str(path),
                format="parquet",
                compression=self.compression,
                size_bytes=path.stat().st_size,
                row_count=len(batch),
                schema=self._schema,
            )
            self._staged_files.append(staged_file)
            self._total_rows += len(batch)

        except Exception as e:
            raise BufferError(f"Failed to write parquet batch: {e}") from e

    def _build_arrow_schema(self, fields: list[Field]) -> "pa.Schema":
        """Build PyArrow schema from DXT fields."""
        arrow_fields = []
        for field in fields:
            arrow_type = self._dxt_to_arrow_type(field)
            arrow_fields.append(pa.field(field.id, arrow_type, nullable=field.nullable))
        return pa.schema(arrow_fields)

    def _dxt_to_arrow_type(self, field: Field) -> "pa.DataType":
        """Convert DXT type to PyArrow type."""
        dtype = field.dtype if isinstance(field.dtype, DXTType) else DXTType(field.dtype)

        type_map = {
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
            DXTType.JSON: pa.string(),
            DXTType.ARRAY: pa.string(),
            DXTType.OBJECT: pa.string(),
        }

        if dtype == DXTType.DECIMAL:
            precision = field.precision or 38
            scale = field.scale or 0
            return pa.decimal128(precision, scale)

        arrow_type = type_map.get(dtype)
        if arrow_type is None:
            raise BufferError(f"Unsupported DXT type for Parquet: {dtype}")

        return arrow_type

    def _read_records_parquet(self) -> Iterator[dict[str, Any]]:
        """Read records from parquet file(s)."""
        for staged_file in self._staged_files:
            try:
                parquet_file = pq.ParquetFile(staged_file.path)
                for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                    yield from batch.to_pylist()
            except Exception as e:
                raise BufferError(f"Failed to read parquet file: {e}") from e

    # === CSV implementation ===

    def _write_records_csv(
        self,
        records: Iterator[dict[str, Any]],
        schema: list[Field],
    ) -> None:
        """Write records to CSV file(s)."""
        field_names = [f.id for f in schema]
        file_index = 0
        batch: list[dict[str, Any]] = []

        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._write_csv_batch(batch, field_names, file_index)
                file_index += 1
                batch = []

        # Write remaining records
        if batch:
            self._write_csv_batch(batch, field_names, file_index)

    def _write_csv_batch(
        self,
        batch: list[dict[str, Any]],
        field_names: list[str],
        file_index: int,
    ) -> None:
        """Write a batch of records to a CSV file."""
        path = self._get_staged_path(file_index)
        opts = self.csv_options

        try:
            # Handle gzip compression
            if self.compression == "gzip":
                import gzip

                open_func = lambda p: gzip.open(p, "wt", encoding=opts.encoding)
            else:
                open_func = lambda p: open(p, "w", encoding=opts.encoding, newline="")

            with open_func(path) as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=field_names,
                    delimiter=opts.delimiter,
                    quotechar=opts.quotechar,
                    escapechar=opts.escapechar,
                    doublequote=opts.doublequote,
                    lineterminator=opts.lineterminator,
                    extrasaction="ignore",
                )

                if opts.header:
                    writer.writeheader()

                for record in batch:
                    # Convert None to null_value
                    row = {
                        k: (opts.null_value if v is None else v)
                        for k, v in record.items()
                    }
                    writer.writerow(row)

            staged_file = StagedFile(
                path=str(path),
                format="csv",
                compression=self.compression,
                size_bytes=path.stat().st_size,
                row_count=len(batch),
                schema=self._schema,
            )
            self._staged_files.append(staged_file)
            self._total_rows += len(batch)

        except Exception as e:
            raise BufferError(f"Failed to write CSV batch: {e}") from e

    def _read_records_csv(self) -> Iterator[dict[str, Any]]:
        """Read records from CSV file(s)."""
        opts = self.csv_options

        for staged_file in self._staged_files:
            try:
                # Handle gzip compression
                if staged_file.compression == "gzip":
                    import gzip

                    open_func = lambda p: gzip.open(p, "rt", encoding=opts.encoding)
                else:
                    open_func = lambda p: open(p, "r", encoding=opts.encoding, newline="")

                with open_func(staged_file.path) as f:
                    reader = csv.DictReader(
                        f,
                        delimiter=opts.delimiter,
                        quotechar=opts.quotechar,
                        escapechar=opts.escapechar,
                        doublequote=opts.doublequote,
                    )

                    for row in reader:
                        # Convert null_value back to None
                        record = {
                            k: (None if v == opts.null_value else v)
                            for k, v in row.items()
                        }
                        yield record

            except Exception as e:
                raise BufferError(f"Failed to read CSV file: {e}") from e

    # === JSONL implementation ===

    def _write_records_jsonl(
        self,
        records: Iterator[dict[str, Any]],
        schema: list[Field],
    ) -> None:
        """Write records to JSONL file(s)."""
        import json

        file_index = 0
        batch: list[dict[str, Any]] = []

        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._write_jsonl_batch(batch, file_index)
                file_index += 1
                batch = []

        # Write remaining records
        if batch:
            self._write_jsonl_batch(batch, file_index)

    def _write_jsonl_batch(
        self,
        batch: list[dict[str, Any]],
        file_index: int,
    ) -> None:
        """Write a batch of records to a JSONL file."""
        import json

        path = self._get_staged_path(file_index)

        try:
            # Handle gzip compression
            if self.compression == "gzip":
                import gzip

                open_func = lambda p: gzip.open(p, "wt", encoding="utf-8")
            else:
                open_func = lambda p: open(p, "w", encoding="utf-8")

            with open_func(path) as f:
                for record in batch:
                    # Handle non-JSON-serializable types
                    serializable = self._make_json_serializable(record)
                    f.write(json.dumps(serializable) + "\n")

            staged_file = StagedFile(
                path=str(path),
                format="jsonl",
                compression=self.compression,
                size_bytes=path.stat().st_size,
                row_count=len(batch),
                schema=self._schema,
            )
            self._staged_files.append(staged_file)
            self._total_rows += len(batch)

        except Exception as e:
            raise BufferError(f"Failed to write JSONL batch: {e}") from e

    def _make_json_serializable(self, record: dict[str, Any]) -> dict[str, Any]:
        """Convert record values to JSON-serializable types."""
        from datetime import date, datetime
        from decimal import Decimal

        result = {}
        for k, v in record.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, date):
                result[k] = v.isoformat()
            elif isinstance(v, Decimal):
                result[k] = str(v)
            elif isinstance(v, bytes):
                import base64

                result[k] = base64.b64encode(v).decode("ascii")
            else:
                result[k] = v
        return result

    def _read_records_jsonl(self) -> Iterator[dict[str, Any]]:
        """Read records from JSONL file(s)."""
        import json

        for staged_file in self._staged_files:
            try:
                # Handle gzip compression
                if staged_file.compression == "gzip":
                    import gzip

                    open_func = lambda p: gzip.open(p, "rt", encoding="utf-8")
                else:
                    open_func = lambda p: open(p, "r", encoding="utf-8")

                with open_func(staged_file.path) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            yield json.loads(line)

            except Exception as e:
                raise BufferError(f"Failed to read JSONL file: {e}") from e
