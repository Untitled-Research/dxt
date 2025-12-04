"""PostgreSQL extractor implementation.

This module provides data extraction from PostgreSQL databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.providers.base.relational import RelationalConnector, RelationalExtractor


class PostgresExtractor(RelationalExtractor):
    """PostgreSQL-specific extractor.

    Extends RelationalExtractor with PostgreSQL-specific optimizations:
    - Server-side cursors for large result sets
    - PostgreSQL-specific watermark filtering

    For most use cases, the base RelationalExtractor is sufficient.
    This class can be extended for PostgreSQL-specific features like
    COPY TO for bulk extraction.

    Example:
        >>> from dxt.providers.postgres import PostgresConnector, PostgresExtractor
        >>>
        >>> with PostgresConnector(config) as conn:
        ...     extractor = PostgresExtractor(conn)
        ...     result = extractor.extract(stream, buffer)
    """

    def _format_watermark_filter(
        self, field_name: str, value: Any, watermark_type: str
    ) -> str:
        """Format watermark filter with PostgreSQL-specific casting.

        Args:
            field_name: Watermark field name
            value: Watermark value
            watermark_type: Type of watermark

        Returns:
            PostgreSQL WHERE clause fragment
        """
        if watermark_type == "timestamp":
            # Use PostgreSQL timestamp casting for proper comparison
            return f"{field_name} > '{value}'::timestamp"
        elif watermark_type == "date":
            return f"{field_name} > '{value}'::date"
        else:
            return f"{field_name} > {value}"
