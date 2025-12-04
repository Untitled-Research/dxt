"""SQLite provider for DXT.

This package provides SQLite-specific implementations:
- SQLiteConnector: Connection management
- SQLiteExtractor: Data extraction
- SQLiteLoader: Data loading
- SQLiteTypeMapper: Type conversion

Example:
    >>> from dxt.providers.sqlite import SQLiteConnector, SQLiteExtractor
    >>>
    >>> config = {"database": "/path/to/database.db"}
    >>> with SQLiteConnector(config) as conn:
    ...     extractor = SQLiteExtractor(conn)
    ...     # ... extraction logic
"""

from dxt.providers.sqlite.connector import SQLiteConnector
from dxt.providers.sqlite.extractor import SQLiteExtractor
from dxt.providers.sqlite.loader import SQLiteLoader
from dxt.providers.sqlite.type_mapper import SQLiteTypeMapper

__all__ = [
    "SQLiteConnector",
    "SQLiteExtractor",
    "SQLiteLoader",
    "SQLiteTypeMapper",
]
