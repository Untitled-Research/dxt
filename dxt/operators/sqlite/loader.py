"""SQLite loader implementation.

This module provides data loading into SQLite databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.operators.sqlite.connector import SQLiteConnector
from dxt.operators.sql.loader import SQLLoader


class SQLiteLoader(SQLLoader):
    """SQLite loader.

    Inherits all functionality from SQLLoader.
    Overrides upsert mode to use SQLite's INSERT OR REPLACE syntax.

    SQLite-specific behavior:
    - Uses DELETE instead of TRUNCATE (handled by base class)
    - Uses INSERT OR REPLACE for upsert mode

    Examples:
        >>> connector = SQLiteConnector(config)
        >>> loader = SQLiteLoader(connector)
        >>> with connector:
        ...     result = loader.load(stream, buffer)
        ...     print(f"Loaded {result.records_loaded} records")
    """

    def __init__(self, connector: SQLiteConnector, config: Optional[dict[str, Any]] = None):
        """Initialize SQLite loader.

        Args:
            connector: SQLiteConnector instance
            config: Optional loader-specific configuration
        """
        # Initialize with SQLConnector validation (not SQLiteConnector)
        # This allows more flexibility while maintaining type safety
        super().__init__(connector, config)

    def _build_upsert_sql(self, table_name: str, fields: list, upsert_keys: list[str]) -> str:
        """Build SQLite-specific upsert SQL using INSERT OR REPLACE.

        SQLite uses INSERT OR REPLACE syntax, which requires a PRIMARY KEY
        or UNIQUE constraint on the upsert keys.

        Args:
            table_name: Target table name
            fields: Field definitions
            upsert_keys: Keys to use for conflict detection

        Returns:
            SQL statement for upsert
        """
        field_names = [f.target_name for f in fields]
        columns = ", ".join(field_names)
        placeholders = ", ".join([f":{f.id}" for f in fields])

        # SQLite syntax
        upsert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

        return upsert_sql
