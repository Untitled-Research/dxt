"""Base Loader abstract class.

This module defines the Loader interface for loading data
into target systems.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from dxt.core.buffer import Buffer
from dxt.core.connector import Connector
from dxt.models.field import Field
from dxt.models.results import LoadResult
from dxt.models.stream import Stream


class Loader(ABC):
    """Base class for loading records into a target system.

    Loaders read records from a Buffer and write to a target system.
    They compose a Connector for connection management and use the
    Stream configuration to determine how to load.

    Examples:
        Using a loader:
        >>> connector = PostgresConnector(config)
        >>> loader = PostgresLoader(connector)
        >>> with connector:
        ...     result = loader.load(stream, buffer)
        ...     print(f"Loaded {result.records_loaded} records")
    """

    def __init__(self, connector: Connector, config: Optional[dict[str, Any]] = None):
        """Initialize loader with connector and configuration.

        Args:
            connector: Connector instance for data system access
            config: Optional loader-specific configuration
        """
        self.connector = connector
        self.config = config or {}

    @abstractmethod
    def load(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> LoadResult:
        """Load records from buffer to target.

        The Stream.load config contains the load mode and parameters.
        Responsibilities:
        1. Optionally create/alter target stream
        2. Execute appropriate load mode (append, replace, upsert)
        3. Use bulk load methods where available
        4. Return detailed results

        Args:
            stream: Stream configuration with target reference and load config
            buffer: Buffer to read records from

        Returns:
            LoadResult with metrics and metadata

        Raises:
            LoaderError: If load fails
        """
        pass

    def get_stream_fields(self, stream_ref: str) -> Optional[list[Field]]:
        """Get field definitions from target system.

        Args:
            stream_ref: Reference to the target (table name, file path, etc.)

        Returns:
            List of Field objects representing the schema, or None if doesn't exist

        Raises:
            SchemaError: If schema retrieval fails (not including "doesn't exist")
        """
        try:
            return self.connector.get_schema(stream_ref)
        except Exception:
            # Target may not exist yet
            return None

    @abstractmethod
    def create_target_stream(self, stream: Stream) -> None:
        """Create target stream/table/collection from field definitions.

        Uses stream.fields to create the target with appropriate types.
        Implementation uses connector's SQL builder or native API.

        Args:
            stream: Stream configuration with fields and target reference

        Raises:
            LoaderError: If target creation fails
        """
        pass

    def get_max_watermark(self, stream_ref: str, watermark_field: str) -> Optional[Any]:
        """Query target for max watermark value (for incremental loads).

        Args:
            stream_ref: Target reference (table name, etc.)
            watermark_field: Field name containing watermark values

        Returns:
            Maximum watermark value, or None if target is empty or doesn't exist

        Raises:
            LoaderError: If query fails
        """
        # Default implementation for SQL-based systems
        # Can be overridden for non-SQL systems
        try:
            query = f"SELECT MAX({watermark_field}) FROM {stream_ref}"
            result = self.connector.execute_query(query)
            if result and len(result) > 0 and len(result[0]) > 0:
                return result[0][list(result[0].keys())[0]]
            return None
        except Exception:
            # Target may not exist yet
            return None

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for this loader.

        Can be overridden by subclasses to add loader-specific validation.

        Args:
            stream: Stream configuration to validate

        Raises:
            ValidationError: If stream configuration is invalid for this loader
        """
        # Check upsert_keys for upsert mode
        if stream.load.mode == "upsert" and not stream.load.upsert_keys:
            from dxt.exceptions import ValidationError

            raise ValidationError(
                f"Stream '{stream.id}': upsert mode requires upsert_keys to be specified"
            )

    @abstractmethod
    def _load_append(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in append mode (INSERT).

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        pass

    @abstractmethod
    def _load_replace(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in replace mode (TRUNCATE + INSERT).

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        pass

    @abstractmethod
    def _load_upsert(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load records in upsert mode (INSERT ... ON CONFLICT or MERGE).

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        pass
