"""PostgreSQL extractor implementation.

This module provides data extraction from PostgreSQL databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.operators.postgres.connector import PostgresConnector
from dxt.operators.sql.extractor import SQLExtractor


class PostgresExtractor(SQLExtractor):
    """PostgreSQL extractor.

    Inherits all functionality from SQLExtractor.
    Can override methods for PostgreSQL-specific optimizations.

    Currently, no PostgreSQL-specific overrides are needed.
    The base SQLExtractor implementation works perfectly for PostgreSQL.

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
        # Initialize with SQLConnector validation (not PostgresConnector)
        # This allows more flexibility while maintaining type safety
        super().__init__(connector, config)

    # Future optimization example:
    # def _format_watermark_filter(self, field_name: str, value: Any, watermark_type: str) -> str:
    #     """Use PostgreSQL-specific timestamp casting."""
    #     if watermark_type == "timestamp":
    #         return f"{field_name} > '{value}'::timestamp"
    #     return super()._format_watermark_filter(field_name, value, watermark_type)
