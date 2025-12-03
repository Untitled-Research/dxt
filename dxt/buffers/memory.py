"""In-memory buffer implementation.

This module provides a simple in-memory buffer for testing and
small datasets that don't require persistence.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Optional

from dxt.core.buffer import Buffer
from dxt.exceptions import BufferError
from dxt.models.field import Field


class MemoryBuffer(Buffer):
    """In-memory buffer - no persistence, Python objects as-is.

    Stores records in memory without serialization. Suitable for:
    - Small datasets (< 100K records)
    - Testing and development
    - Single run (no separate extract/load phases)

    Not suitable for:
    - Large datasets (memory constraints)
    - Separate extract/load commands (data not persisted)
    - Production pipelines with failure recovery

    Examples:
        >>> fields = [Field(id="id", dtype="int64"), Field(id="name", dtype="string")]
        >>> buffer = MemoryBuffer(fields)
        >>> buffer.write_batch([{"id": 1, "name": "Alice"}])
        >>> buffer.finalize()
        >>> buffer.commit()
        >>> for batch in buffer.read_batches(1000):
        ...     print(batch)
        >>> buffer.cleanup()
    """

    def __init__(
        self,
        fields: list[Field],
        location: Optional[Path] = None,
        **kwargs: Any,
    ):
        """Initialize memory buffer.

        Args:
            fields: List of Field definitions
            location: Ignored for memory buffer (always None)
            **kwargs: Additional options (ignored)
        """
        super().__init__(fields, location=None, **kwargs)
        self._records: list[dict[str, Any]] = []
        self._temp_records: list[dict[str, Any]] = []

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of records to temporary in-memory storage.

        Args:
            records: List of dicts with field.id as keys, Python native values

        Raises:
            BufferError: If buffer is already finalized
        """
        if self._finalized:
            raise BufferError("Cannot write to finalized buffer")

        # Validate records have expected fields (optional, can be strict or lenient)
        field_ids = {f.id for f in self.fields}
        for record in records:
            # Check for unexpected fields
            record_keys = set(record.keys())
            unexpected = record_keys - field_ids
            if unexpected:
                raise BufferError(
                    f"Record contains unexpected fields: {unexpected}. "
                    f"Expected fields: {field_ids}"
                )

        # Store in temp location (simulate write before commit)
        self._temp_records.extend(records)

    def finalize(self) -> None:
        """Mark writing as complete.

        For memory buffer, this is a no-op but sets the finalized flag.
        """
        self._finalized = True

    def commit(self) -> None:
        """Atomically make writes visible.

        For memory buffer, this moves temp records to committed storage.
        """
        if not self._finalized:
            raise BufferError("Cannot commit before finalize")

        self._records = self._temp_records[:]
        self._committed = True

    def rollback(self) -> None:
        """Discard all uncommitted writes.

        Clears temporary records but keeps committed records intact.
        """
        self._temp_records.clear()
        self._finalized = False

    def read_batches(self, batch_size: int = 10000) -> Iterator[list[dict[str, Any]]]:
        """Read records in batches as an iterator.

        Args:
            batch_size: Number of records per batch

        Yields:
            Lists of dicts with field.id as keys, Python native values

        Raises:
            BufferError: If buffer not committed
        """
        if not self._committed:
            raise BufferError("Cannot read from buffer before commit")

        # Yield slices of records (no deserialization needed)
        for i in range(0, len(self._records), batch_size):
            yield self._records[i : i + batch_size]

    def exists(self) -> bool:
        """Check if buffered data exists.

        Returns:
            True if committed records exist, False otherwise
        """
        return self._committed and len(self._records) > 0

    def cleanup(self) -> None:
        """Clear all records and reset buffer state.

        Idempotent - can be called multiple times safely.
        """
        self._records.clear()
        self._temp_records.clear()
        self._finalized = False
        self._committed = False

    @property
    def record_count(self) -> int:
        """Number of records in buffer.

        Returns:
            Count of committed records
        """
        return len(self._records) if self._committed else 0
