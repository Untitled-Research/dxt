"""Filestore provider base classes.

This module provides base classes for file/object storage providers
(local filesystem, S3, GCS, Azure Blob, SFTP, etc.).

The filestore category covers systems that:
- Store data as files or objects
- Use path-based access
- Support listing, reading, and writing files

Classes:
    FilestoreConnector: Base connector for file storage systems
    FilestoreExtractor: Base extractor for file storage systems
    FilestoreLoader: Base loader for file storage systems
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO, Iterator, Optional

from dxt.core.buffer import Buffer
from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.exceptions import (
    ConnectionError,
    ConnectorError,
    ExtractorError,
    LoaderError,
    ValidationError,
)
from dxt.models.field import Field
from dxt.models.results import ExtractResult, LoadResult
from dxt.models.stream import Stream


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FileInfo:
    """Metadata about a file in storage."""

    path: str
    size: int  # Bytes
    modified: datetime
    format: Optional[str] = None  # Inferred from extension

    @property
    def filename(self) -> str:
        """Get just the filename from the path."""
        return self.path.rsplit("/", 1)[-1] if "/" in self.path else self.path

    @property
    def extension(self) -> str:
        """Get file extension (without dot)."""
        if "." in self.filename:
            return self.filename.rsplit(".", 1)[-1].lower()
        return ""


@dataclass
class StagedFile:
    """A file staged in the buffer."""

    filename: str
    path: str  # Local path to staged file
    size: int
    metadata: dict


# =============================================================================
# Filestore Connector
# =============================================================================


class FilestoreConnector(Connector):
    """Base class for file/object storage connectors.

    Provides common interface for file storage systems including:
    - File listing with glob patterns
    - File reading and writing
    - File metadata retrieval
    - Directory/prefix creation

    Subclasses must implement:
    - connect(): Establish connection/session
    - disconnect(): Close connection/session
    - list_files(): List files matching pattern
    - open_read(): Open file for reading
    - open_write(): Open file for writing
    - exists(): Check if file exists
    - delete(): Delete a file
    - get_info(): Get file metadata

    Example:
        >>> class S3Connector(FilestoreConnector):
        ...     def connect(self):
        ...         self.client = boto3.client('s3', **self.config)
        ...
        ...     def list_files(self, pattern="*", recursive=True):
        ...         # S3 listing implementation
        ...         ...
    """

    # Category identifier
    category: str = "filestore"

    def __init__(self, config: dict[str, Any]):
        """Initialize filestore connector.

        Args:
            config: Connection configuration. Common keys:
                - path: Base path or bucket (e.g., "s3://bucket/prefix" or "/local/path")
                - protocol: Storage protocol (file, s3, gs, az, sftp)
                - region: Cloud region (for S3, Azure)
                - credentials: Auth credentials
        """
        super().__init__(config)
        self._base_path = config.get("path", "")

    @property
    def base_path(self) -> str:
        """Get the base path for this connector."""
        return self._base_path

    @abstractmethod
    def list_files(
        self, pattern: str = "*", recursive: bool = True
    ) -> Iterator[FileInfo]:
        """List files matching a pattern.

        Args:
            pattern: Glob pattern (e.g., "*.csv", "data/**/*.parquet")
            recursive: Whether to search subdirectories

        Yields:
            FileInfo for each matching file

        Raises:
            ConnectorError: If listing fails
        """
        pass

    @abstractmethod
    def open_read(self, path: str) -> BinaryIO:
        """Open a file for reading.

        Args:
            path: Path to file (relative to base_path or absolute)

        Returns:
            Binary file-like object for reading

        Raises:
            ConnectorError: If file doesn't exist or can't be opened
        """
        pass

    @abstractmethod
    def open_write(self, path: str) -> BinaryIO:
        """Open a file for writing.

        Args:
            path: Path to file (relative to base_path or absolute)

        Returns:
            Binary file-like object for writing

        Raises:
            ConnectorError: If file can't be created
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file exists.

        Args:
            path: Path to file

        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete a file.

        Args:
            path: Path to file

        Raises:
            ConnectorError: If file doesn't exist or can't be deleted
        """
        pass

    @abstractmethod
    def get_info(self, path: str) -> FileInfo:
        """Get metadata for a file.

        Args:
            path: Path to file

        Returns:
            FileInfo with file metadata

        Raises:
            ConnectorError: If file doesn't exist
        """
        pass

    def makedirs(self, path: str) -> None:
        """Create directory/prefix.

        Default implementation does nothing (for object stores that don't need it).
        Override for filesystems that require directory creation.

        Args:
            path: Directory path to create
        """
        pass

    def resolve_path(self, path: str) -> str:
        """Resolve a relative path against base_path.

        Args:
            path: Relative or absolute path

        Returns:
            Resolved absolute path
        """
        if path.startswith("/") or "://" in path:
            return path
        if self._base_path:
            return f"{self._base_path.rstrip('/')}/{path}"
        return path

    def infer_format(self, path: str) -> str:
        """Infer file format from extension.

        Args:
            path: File path

        Returns:
            Format string (parquet, csv, jsonl, json)
        """
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        return {
            "parquet": "parquet",
            "pq": "parquet",
            "csv": "csv",
            "tsv": "csv",
            "json": "json",
            "jsonl": "jsonl",
            "ndjson": "jsonl",
        }.get(ext, "binary")

    def test_connection(self) -> bool:
        """Test connectivity to the storage system.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.is_connected:
                self.connect()
            # Try to list files (limited) to verify access
            next(iter(self.list_files("*", recursive=False)), None)
            return True
        except Exception:
            return False

    def get_schema(self, ref: str) -> list[Field]:
        """Get schema for a file (inferred from content).

        For filestore, schema must be inferred from file content.
        This base implementation returns empty list.
        Subclasses or extractors should handle schema inference.

        Args:
            ref: File path

        Returns:
            Empty list (schema inference handled elsewhere)
        """
        return []

    def execute_query(self, query: str) -> list[dict]:
        """Not supported for filestore.

        Raises:
            ConnectorError: Always, as filestore doesn't support queries
        """
        raise ConnectorError("Filestore does not support query execution")


# =============================================================================
# Filestore Extractor
# =============================================================================


class FilestoreExtractor(Extractor):
    """Base class for filestore extractors.

    Supports two extraction modes:
    1. Binary mode: Stage files as-is without parsing (for filestore→filestore)
    2. Record mode: Parse files and extract records (for filestore→relational)

    The mode is determined by the buffer format:
    - buffer.format == "binary": Binary mode
    - buffer.format in ("parquet", "csv", "jsonl"): Record mode
    """

    # Category identifier
    category: str = "filestore"

    def __init__(
        self, connector: FilestoreConnector, config: Optional[dict[str, Any]] = None
    ):
        """Initialize filestore extractor.

        Args:
            connector: FilestoreConnector instance
            config: Optional extractor-specific configuration
        """
        super().__init__(connector, config)
        if not isinstance(connector, FilestoreConnector):
            raise ExtractorError("FilestoreExtractor requires a FilestoreConnector")

    @property
    def filestore_connector(self) -> FilestoreConnector:
        """Get connector as FilestoreConnector type."""
        return self.connector  # type: ignore

    def extract(self, stream: Stream, buffer: Buffer) -> ExtractResult:
        """Extract files from source to buffer.

        Args:
            stream: Stream configuration
            buffer: Buffer to write to

        Returns:
            ExtractResult with metrics
        """
        started_at = datetime.now()
        records_extracted = 0

        try:
            self.validate_stream(stream)

            source = stream.source
            source_files = self._get_source_files(source)

            # Check buffer format to determine extraction mode
            buffer_format = getattr(buffer, "format", "parquet")

            if buffer_format == "binary":
                # Binary mode: stage files as-is
                records_extracted = self._extract_binary(source_files, buffer)
            else:
                # Record mode: parse files and write records
                batch_size = stream.extract.batch_size
                format_options = source.options.get("format_options", {})
                records_extracted = self._extract_records(
                    source_files, buffer, batch_size, format_options
                )

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
                error_message=str(e),
            )

    def _get_source_files(self, source) -> list[FileInfo]:
        """Get list of files to extract based on source config.

        Args:
            source: SourceConfig from stream

        Returns:
            List of FileInfo objects
        """
        if source.type == "file":
            path = self.filestore_connector.resolve_path(source.value)
            return [self.filestore_connector.get_info(path)]

        elif source.type == "glob":
            pattern = source.value
            return list(self.filestore_connector.list_files(pattern, recursive=True))

        elif source.type == "directory":
            pattern = f"{source.value.rstrip('/')}/**/*"
            return list(self.filestore_connector.list_files(pattern, recursive=True))

        else:
            raise ExtractorError(
                f"Filestore extractor does not support source type: {source.type}. "
                f"Supported types: file, glob, directory"
            )

    def _extract_binary(self, files: list[FileInfo], buffer: Buffer) -> int:
        """Extract files in binary mode (no parsing).

        Args:
            files: List of files to extract
            buffer: Buffer to stage files to

        Returns:
            Number of files extracted
        """
        count = 0
        for file_info in files:
            stream = self.filestore_connector.open_read(file_info.path)
            try:
                # Use stage_file if available (FileBuffer)
                if hasattr(buffer, "stage_file"):
                    buffer.stage_file(file_info.filename, stream)
                else:
                    # Fallback: read and write as single record
                    content = stream.read()
                    buffer.write_batch([{"_filename": file_info.filename, "_content": content}])
                count += 1
            finally:
                stream.close()
        return count

    def _extract_records(
        self,
        files: list[FileInfo],
        buffer: Buffer,
        batch_size: int,
        format_options: dict,
    ) -> int:
        """Extract files in record mode (parse and write records).

        Args:
            files: List of files to extract
            buffer: Buffer to write records to
            batch_size: Records per batch
            format_options: Format-specific options

        Returns:
            Number of records extracted
        """
        total_records = 0
        batch = []

        for file_info in files:
            file_format = format_options.get(
                "format", self.filestore_connector.infer_format(file_info.path)
            )

            for record in self._read_file_records(file_info, file_format, format_options):
                batch.append(record)

                if len(batch) >= batch_size:
                    buffer.write_batch(batch)
                    total_records += len(batch)
                    batch = []

        if batch:
            buffer.write_batch(batch)
            total_records += len(batch)

        return total_records

    def _read_file_records(
        self, file_info: FileInfo, file_format: str, options: dict
    ) -> Iterator[dict]:
        """Read records from a file.

        Args:
            file_info: File metadata
            file_format: File format (parquet, csv, jsonl, json)
            options: Format-specific options

        Yields:
            Records as dictionaries
        """
        stream = self.filestore_connector.open_read(file_info.path)

        try:
            if file_format == "parquet":
                yield from self._read_parquet(stream, options)
            elif file_format == "csv":
                yield from self._read_csv(stream, options)
            elif file_format == "jsonl":
                yield from self._read_jsonl(stream, options)
            elif file_format == "json":
                yield from self._read_json(stream, options)
            else:
                raise ExtractorError(f"Unsupported file format: {file_format}")
        finally:
            stream.close()

    def _read_parquet(self, stream: BinaryIO, options: dict) -> Iterator[dict]:
        """Read records from a Parquet file."""
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(stream)
            for batch in table.to_batches():
                for row in batch.to_pydict().values():
                    # Convert columnar to row format
                    pass
                # Simpler approach
                df = table.to_pandas()
                for record in df.to_dict("records"):
                    yield record
        except ImportError:
            raise ExtractorError("PyArrow required for Parquet files: pip install pyarrow")

    def _read_csv(self, stream: BinaryIO, options: dict) -> Iterator[dict]:
        """Read records from a CSV file."""
        import csv
        import io

        encoding = options.get("encoding", "utf-8")
        delimiter = options.get("delimiter", ",")
        has_header = options.get("header", True)

        text_stream = io.TextIOWrapper(stream, encoding=encoding)
        reader = csv.reader(text_stream, delimiter=delimiter)

        if has_header:
            headers = next(reader)
        else:
            # Generate column names
            first_row = next(reader)
            headers = [f"col_{i}" for i in range(len(first_row))]
            yield dict(zip(headers, first_row))

        for row in reader:
            yield dict(zip(headers, row))

    def _read_jsonl(self, stream: BinaryIO, options: dict) -> Iterator[dict]:
        """Read records from a JSONL file."""
        import json

        encoding = options.get("encoding", "utf-8")

        for line in stream:
            line = line.decode(encoding).strip()
            if line:
                yield json.loads(line)

    def _read_json(self, stream: BinaryIO, options: dict) -> Iterator[dict]:
        """Read records from a JSON file (expects array of objects)."""
        import json

        encoding = options.get("encoding", "utf-8")
        content = stream.read().decode(encoding)
        data = json.loads(content)

        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            yield data
        else:
            raise ExtractorError("JSON file must contain array or object")

    def build_query(self, stream: Stream, watermark_value=None) -> Optional[str]:
        """Not applicable for filestore."""
        return None

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for filestore extraction."""
        if not stream.source:
            raise ValidationError(f"Stream '{stream.id}': source is required")

        if stream.source.type not in ("file", "glob", "directory"):
            raise ValidationError(
                f"Stream '{stream.id}': invalid source type '{stream.source.type}'. "
                f"Must be one of: file, glob, directory"
            )


# =============================================================================
# Filestore Loader
# =============================================================================


class FilestoreLoader(Loader):
    """Base class for filestore loaders.

    Supports two loading modes:
    1. Binary mode: Copy staged files directly (from FileBuffer with binary format)
    2. Record mode: Write records to files (from RecordBuffer)
    """

    # Category identifier
    category: str = "filestore"

    def __init__(
        self, connector: FilestoreConnector, config: Optional[dict[str, Any]] = None
    ):
        """Initialize filestore loader.

        Args:
            connector: FilestoreConnector instance
            config: Optional loader-specific configuration
        """
        super().__init__(connector, config)
        if not isinstance(connector, FilestoreConnector):
            raise LoaderError("FilestoreLoader requires a FilestoreConnector")

    @property
    def filestore_connector(self) -> FilestoreConnector:
        """Get connector as FilestoreConnector type."""
        return self.connector  # type: ignore

    def load(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load data from buffer to target files.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()
        records_loaded = 0

        try:
            self.validate_stream(stream)

            target = stream.target
            output_path = self._resolve_output_path(target)
            output_format = target.options.get(
                "format", self.filestore_connector.infer_format(output_path)
            )

            # Handle replace mode
            if stream.load.mode == "replace":
                if self.filestore_connector.exists(output_path):
                    self.filestore_connector.delete(output_path)

            # Check buffer type to determine loading mode
            if hasattr(buffer, "read_files"):
                # Binary mode: copy staged files
                records_loaded = self._load_binary(buffer, output_path)
            else:
                # Record mode: write records to file
                format_options = target.options.get("format_options", {})
                records_loaded = self._load_records(
                    buffer, output_path, output_format, format_options, stream.load.batch_size
                )

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

    def _resolve_output_path(self, target) -> str:
        """Resolve output file path with template substitution.

        Args:
            target: TargetConfig from stream

        Returns:
            Resolved output path
        """
        path = target.value

        # Support timestamp templates
        now = datetime.now()
        path = path.replace("{date}", now.strftime("%Y-%m-%d"))
        path = path.replace("{datetime}", now.strftime("%Y%m%d_%H%M%S"))
        path = path.replace("{timestamp}", str(int(now.timestamp())))
        path = path.replace("{year}", now.strftime("%Y"))
        path = path.replace("{month}", now.strftime("%m"))
        path = path.replace("{day}", now.strftime("%d"))

        return self.filestore_connector.resolve_path(path)

    def _load_binary(self, buffer: Buffer, output_path: str) -> int:
        """Load files in binary mode (direct copy).

        Args:
            buffer: Buffer with staged files
            output_path: Base output path

        Returns:
            Number of files loaded
        """
        count = 0
        for staged_file in buffer.read_files():
            # Determine target path
            if output_path.endswith("/"):
                target_path = f"{output_path}{staged_file.filename}"
            else:
                target_path = output_path

            # Copy file
            with open(staged_file.path, "rb") as src:
                with self.filestore_connector.open_write(target_path) as dst:
                    # Copy in chunks
                    while chunk := src.read(8192):
                        dst.write(chunk)
            count += 1
        return count

    def _load_records(
        self,
        buffer: Buffer,
        output_path: str,
        output_format: str,
        format_options: dict,
        batch_size: int,
    ) -> int:
        """Load records to file.

        Args:
            buffer: Buffer with records
            output_path: Output file path
            output_format: Output format (parquet, csv, jsonl)
            format_options: Format-specific options
            batch_size: Records per batch

        Returns:
            Number of records loaded
        """
        if output_format == "parquet":
            return self._write_parquet(buffer, output_path, format_options, batch_size)
        elif output_format == "csv":
            return self._write_csv(buffer, output_path, format_options, batch_size)
        elif output_format == "jsonl":
            return self._write_jsonl(buffer, output_path, format_options, batch_size)
        else:
            raise LoaderError(f"Unsupported output format: {output_format}")

    def _write_parquet(
        self, buffer: Buffer, output_path: str, options: dict, batch_size: int
    ) -> int:
        """Write records to Parquet file."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            # Collect all records
            all_records = []
            for batch in buffer.read_batches(batch_size):
                all_records.extend(batch)

            if not all_records:
                return 0

            # Convert to PyArrow table
            table = pa.Table.from_pylist(all_records)

            # Write to file
            compression = options.get("compression", "snappy")
            with self.filestore_connector.open_write(output_path) as f:
                pq.write_table(table, f, compression=compression)

            return len(all_records)

        except ImportError:
            raise LoaderError("PyArrow required for Parquet files: pip install pyarrow")

    def _write_csv(
        self, buffer: Buffer, output_path: str, options: dict, batch_size: int
    ) -> int:
        """Write records to CSV file."""
        import csv
        import io

        encoding = options.get("encoding", "utf-8")
        delimiter = options.get("delimiter", ",")

        count = 0
        headers_written = False

        with self.filestore_connector.open_write(output_path) as f:
            text_stream = io.TextIOWrapper(f, encoding=encoding)

            for batch in buffer.read_batches(batch_size):
                if not batch:
                    continue

                if not headers_written:
                    headers = list(batch[0].keys())
                    writer = csv.DictWriter(
                        text_stream, fieldnames=headers, delimiter=delimiter
                    )
                    writer.writeheader()
                    headers_written = True

                writer.writerows(batch)
                count += len(batch)

            text_stream.flush()

        return count

    def _write_jsonl(
        self, buffer: Buffer, output_path: str, options: dict, batch_size: int
    ) -> int:
        """Write records to JSONL file."""
        import json

        encoding = options.get("encoding", "utf-8")
        count = 0

        with self.filestore_connector.open_write(output_path) as f:
            for batch in buffer.read_batches(batch_size):
                for record in batch:
                    line = json.dumps(record) + "\n"
                    f.write(line.encode(encoding))
                    count += 1

        return count

    def create_target_stream(self, stream: Stream) -> None:
        """Create target directory if needed.

        Args:
            stream: Stream configuration
        """
        target_path = self._resolve_output_path(stream.target)
        # Create parent directory
        if "/" in target_path:
            parent = target_path.rsplit("/", 1)[0]
            self.filestore_connector.makedirs(parent)

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for filestore loading."""
        if not stream.target:
            raise ValidationError(f"Stream '{stream.id}': target is required")
