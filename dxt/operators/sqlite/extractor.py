"""SQLite extractor implementation.

This module provides data extraction from SQLite databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.operators.sqlite.connector import SQLiteConnector
from dxt.operators.sql.extractor import SQLExtractor


class SQLiteExtractor(SQLExtractor):
    """SQLite extractor.

    Inherits all functionality from SQLExtractor.
    No SQLite-specific overrides needed - base implementation is optimal.

    Examples:
        >>> connector = SQLiteConnector(config)
        >>> extractor = SQLiteExtractor(connector)
        >>> with connector:
        ...     result = extractor.extract(stream, buffer)
        ...     print(f"Extracted {result.records_extracted} records")
    """

    def __init__(self, connector: SQLiteConnector, config: Optional[dict[str, Any]] = None):
        """Initialize SQLite extractor.

        Args:
            connector: SQLiteConnector instance
            config: Optional extractor-specific configuration
        """
        # Initialize with SQLConnector validation (not SQLiteConnector)
        # This allows more flexibility while maintaining type safety
        super().__init__(connector, config)
