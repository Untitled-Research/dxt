"""SQLite loader implementation.

This module provides data loading into SQLite databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.providers.base.relational import RelationalLoader


class SQLiteLoader(RelationalLoader):
    """SQLite-specific loader.

    Extends RelationalLoader with SQLite-specific behavior:
    - Uses DELETE instead of TRUNCATE (handled by base class fallback)
    - Uses INSERT OR REPLACE for upsert mode

    Example:
        >>> from dxt.providers.sqlite import SQLiteConnector, SQLiteLoader
        >>>
        >>> with SQLiteConnector(config) as conn:
        ...     loader = SQLiteLoader(conn)
        ...     result = loader.load(stream, buffer)
    """

    def _build_upsert_sql(
        self, table_name: str, fields: list, upsert_keys: list[str]
    ) -> str:
        """Build SQLite-specific upsert SQL using INSERT OR REPLACE.

        SQLite uses INSERT OR REPLACE syntax, which requires a PRIMARY KEY
        or UNIQUE constraint on the upsert keys.

        Args:
            table_name: Target table name
            fields: Field definitions
            upsert_keys: Keys for conflict detection

        Returns:
            SQLite INSERT OR REPLACE statement
        """
        field_names = [f.target_name for f in fields]
        columns = ", ".join(field_names)
        placeholders = ", ".join([f":{f.id}" for f in fields])

        return f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
