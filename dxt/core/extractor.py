"""Base Extractor abstract class.

This module defines the Extractor interface for extracting data
from source systems.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from dxt.core.buffer import Buffer
from dxt.core.connector import Connector
from dxt.models.field import Field
from dxt.models.results import ExtractResult
from dxt.models.stream import Stream


class Extractor(ABC):
    """Base class for extracting records from a source system.

    Extractors read data from a source and write records to a Buffer.
    They compose a Connector for connection management and use the
    Stream configuration to determine what and how to extract.

    Examples:
        Using an extractor:
        >>> connector = PostgresConnector(config)
        >>> extractor = PostgresExtractor(connector)
        >>> with connector:
        ...     result = extractor.extract(stream, buffer)
        ...     print(f"Extracted {result.records_extracted} records")
    """

    def __init__(self, connector: Connector, config: Optional[dict[str, Any]] = None):
        """Initialize extractor with connector and configuration.

        Args:
            connector: Connector instance for data system access
            config: Optional extractor-specific configuration
        """
        self.connector = connector
        self.config = config or {}

    @abstractmethod
    def extract(
        self,
        stream: Stream,
        buffer: Buffer,
    ) -> ExtractResult:
        """Extract records from source to buffer.

        The Stream.extract config contains the extract mode and parameters.
        For incremental extraction, the extractor should:
        1. Query target (via separate loader) for max watermark if needed
        2. Build appropriate query with WHERE clause
        3. Stream records to buffer in batches

        Handles different stream kinds:
        - kind="table": Build SELECT query from table reference, apply criteria
        - kind="query": Use source as literal SQL query
        - kind="view": Same as table
        - kind="file": Read from file path
        - kind="api": Call API endpoint

        Args:
            stream: Stream configuration with source reference and extract config
            buffer: Buffer to write extracted records to

        Returns:
            ExtractResult with metrics and metadata

        Raises:
            ExtractorError: If extraction fails
        """
        pass

    def get_stream_fields(self, stream_ref: str) -> list[Field]:
        """Get field definitions from source system.

        Args:
            stream_ref: Reference to the source (table name, file path, etc.)

        Returns:
            List of Field objects representing the schema

        Raises:
            SchemaError: If schema cannot be retrieved
        """
        return self.connector.get_schema(stream_ref)

    @abstractmethod
    def build_query(
        self, stream: Stream, watermark_value: Optional[Any] = None
    ) -> Optional[str]:
        """Build extraction query based on stream kind and configuration.

        For kind="table":
          - Start with SELECT * FROM {source}
          - Apply criteria WHERE clauses if present
          - Add watermark filter if incremental
          - Apply system-specific optimizations

        For kind="query":
          - Return source as-is (literal SQL)
          - Watermark filtering not supported (use WHERE in query)

        For kind="view":
          - Same as table

        For kind="file" or kind="api":
          - Return None (not applicable)

        Args:
            stream: Stream configuration
            watermark_value: Maximum watermark value from target (for incremental)

        Returns:
            Query string or None if not applicable

        Raises:
            ExtractorError: If query building fails
        """
        pass

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for this extractor.

        Can be overridden by subclasses to add extractor-specific validation.

        Args:
            stream: Stream configuration to validate

        Raises:
            ValidationError: If stream configuration is invalid for this extractor
        """
        pass
