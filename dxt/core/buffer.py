"""Base Buffer abstract class.

This module defines the Buffer interface for temporary storage
between extract and load operations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, Optional

from dxt.models.field import Field


class Buffer(ABC):
    """Abstract base class for all buffer implementations.

    Buffers provide temporary storage for data between extraction and loading
    with transactional semantics (write → finalize → commit → cleanup).

    The buffer lifecycle:
    1. write_batch() - Write records to temporary location
    2. finalize() - Close file handles, flush buffers
    3. commit() - Atomically make writes visible (rename temp → final)
    4. read_batches() - Read records in batches
    5. cleanup() - Remove buffer and release resources

    Examples:
        Writing to a buffer:
        >>> buffer = ParquetBuffer(fields, location=Path("data.parquet"))
        >>> try:
        ...     for batch in extractor.extract_batches():
        ...         buffer.write_batch(batch)
        ...     buffer.finalize()
        ...     buffer.commit()
        ... except Exception:
        ...     buffer.rollback()
        ...     raise

        Reading from a buffer:
        >>> for batch in buffer.read_batches(batch_size=10000):
        ...     loader.load_batch(batch)
        >>> buffer.cleanup()
    """

    def __init__(
        self,
        fields: list[Field],
        location: Optional[Path] = None,
        **kwargs: Any,
    ):
        """Initialize buffer with schema.

        Args:
            fields: List of Field definitions (schema is required)
            location: Path for persistent buffers, None for memory
            **kwargs: Buffer-specific options
        """
        self.fields = fields
        self.location = location
        self._finalized = False
        self._committed = False

    @abstractmethod
    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of records to temporary location.

        Args:
            records: List of dicts with field.id as keys, Python native values
                    (int, float, Decimal, str, bool, datetime, date, bytes)

        Raises:
            BufferError: If write fails
        """
        pass

    @abstractmethod
    def finalize(self) -> None:
        """Finalize writing - close file handles, flush buffers.

        Must be called after all writes before reading.

        Raises:
            BufferError: If finalization fails
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """Atomically make writes visible.

        For persistent buffers: atomic filesystem rename (temp → final).
        For memory buffers: no-op.

        Raises:
            BufferError: If commit fails
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Discard all writes (delete temp files).

        Called on extraction failure to prevent partial data.

        Raises:
            BufferError: If rollback fails
        """
        pass

    @abstractmethod
    def read_batches(self, batch_size: int = 10000) -> Iterator[list[dict[str, Any]]]:
        """Read records in batches as an iterator.

        Args:
            batch_size: Number of records per batch

        Yields:
            Lists of dicts with field.id as keys, Python native values.
            Types are preserved from write (int stays int, datetime stays datetime).

        Raises:
            BufferError: If read fails or not finalized/committed
        """
        pass

    @abstractmethod
    def exists(self) -> bool:
        """Check if buffered data exists (committed).

        Returns:
            True if committed data exists, False otherwise
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Remove buffered data and release resources.

        Should be idempotent - calling multiple times should not error.
        """
        pass

    @property
    @abstractmethod
    def record_count(self) -> int:
        """Number of records in buffer.

        Returns:
            Record count, or 0 if not yet written
        """
        pass

    @property
    def is_persistent(self) -> bool:
        """Whether buffer persists to disk.

        Returns:
            True if buffer writes to disk, False for memory-only
        """
        return self.location is not None

    @property
    def is_finalized(self) -> bool:
        """Whether buffer has been finalized.

        Returns:
            True if finalize() has been called, False otherwise
        """
        return self._finalized

    @property
    def is_committed(self) -> bool:
        """Whether buffer has been committed.

        Returns:
            True if commit() has been called, False otherwise
        """
        return self._committed
