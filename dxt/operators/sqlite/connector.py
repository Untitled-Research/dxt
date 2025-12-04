"""SQLite connector implementation using SQLAlchemy.

This module provides connection management for SQLite databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.operators.sql.connector import SQLConnector
from dxt.core.type_mapper import TypeMapper
from dxt.exceptions import ConnectorError
from dxt.operators.sqlite.type_mapper import SQLiteTypeMapper


class SQLiteConnector(SQLConnector):
    """SQLite connector using SQLAlchemy.

    Manages connections to SQLite databases and provides
    schema introspection and query execution.

    Configuration keys:
        - database: Database file path (required, or ":memory:" for in-memory)
        - connection_string: Full connection string (alternative)
        - echo: Enable SQL logging (default: False)

    Examples:
        >>> config = {"database": "/path/to/database.db"}
        >>> with SQLiteConnector(config) as conn:
        ...     schema = conn.get_schema("orders")
        ...     results = conn.execute_query("SELECT * FROM orders LIMIT 10")

        >>> # In-memory database
        >>> config = {"database": ":memory:"}
        >>> with SQLiteConnector(config) as conn:
        ...     conn.execute_statement("CREATE TABLE test (id INTEGER PRIMARY KEY)")
    """

    def _build_connection_string(self) -> str:
        """Build SQLite connection string from config.

        Returns:
            SQLAlchemy connection string

        Raises:
            ConnectorError: If required config is missing
        """
        # If connection_string provided, use it directly
        if "connection_string" in self.config:
            return self.config["connection_string"]

        # Build from database path
        if "database" not in self.config:
            raise ConnectorError("Missing required config key: database")

        database = self.config["database"]

        # SQLite connection string format
        return f"sqlite:///{database}"

    def _parse_table_ref(self, ref: str) -> tuple[Optional[str], str]:
        """Parse table reference into schema and table name.

        SQLite doesn't have schemas in the PostgreSQL sense, so schema
        is always None and ref is used as the table name directly.

        Args:
            ref: Table name

        Returns:
            Tuple of (None, table_name)
        """
        return None, ref

    def _get_type_mapper(self) -> TypeMapper:
        """Get SQLite type mapper.

        Returns:
            SQLiteTypeMapper instance
        """
        return SQLiteTypeMapper()

    def _get_database_name(self) -> str:
        """Get database name for error messages.

        Returns:
            "SQLite"
        """
        return "SQLite"
