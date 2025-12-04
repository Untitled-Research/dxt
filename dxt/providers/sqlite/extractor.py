"""SQLite extractor implementation.

This module provides data extraction from SQLite databases.
"""

from __future__ import annotations

from dxt.providers.base.relational import RelationalExtractor


class SQLiteExtractor(RelationalExtractor):
    """SQLite-specific extractor.

    Inherits all functionality from RelationalExtractor.
    SQLite doesn't require special extraction optimizations.

    Example:
        >>> from dxt.providers.sqlite import SQLiteConnector, SQLiteExtractor
        >>>
        >>> with SQLiteConnector(config) as conn:
        ...     extractor = SQLiteExtractor(conn)
        ...     result = extractor.extract(stream, buffer)
    """

    pass
