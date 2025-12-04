"""PostgreSQL connector implementation.

This module provides connection management for PostgreSQL databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.core.type_mapper import TypeMapper
from dxt.exceptions import ConnectorError
from dxt.providers.base.relational import RelationalConnector
from dxt.providers.postgres.type_mapper import PostgresTypeMapper


class PostgresConnector(RelationalConnector):
    """PostgreSQL connector using SQLAlchemy.

    Manages connections to PostgreSQL databases and provides
    schema introspection and query execution.

    Configuration:
        - host: Database host (default: localhost)
        - port: Database port (default: 5432)
        - database: Database name (required)
        - user: Username (required)
        - password: Password (required)
        - schema: Default schema (default: public)
        - connection_string: Full connection string (alternative to params)
        - echo: Enable SQL logging (default: False)

    Example:
        >>> config = {
        ...     "host": "localhost",
        ...     "port": 5432,
        ...     "database": "mydb",
        ...     "user": "postgres",
        ...     "password": "secret"
        ... }
        >>> with PostgresConnector(config) as conn:
        ...     schema = conn.get_schema("public.orders")
        ...     for row in conn.execute("SELECT * FROM orders LIMIT 10"):
        ...         print(row)
    """

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from config.

        Returns:
            SQLAlchemy connection string

        Raises:
            ConnectorError: If required config is missing
        """
        if "connection_string" in self.config:
            return self.config["connection_string"]

        required_keys = ["database", "user", "password"]
        for key in required_keys:
            if key not in self.config:
                raise ConnectorError(f"Missing required config key: {key}")

        host = self.config.get("host", "localhost")
        port = self.config.get("port", 5432)
        database = self.config["database"]
        user = self.config["user"]
        password = self.config["password"]

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def _parse_table_ref(self, ref: str) -> tuple[Optional[str], str]:
        """Parse table reference into schema and table name.

        PostgreSQL supports schemas. References can be:
        - "schema.table" -> (schema, table)
        - "table" -> (default_schema, table)

        Args:
            ref: Table reference (e.g., "public.orders" or "orders")

        Returns:
            Tuple of (schema_name, table_name)

        Raises:
            ConnectorError: If reference format is invalid
        """
        parts = ref.split(".")
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            default_schema = self.config.get("schema", "public")
            return default_schema, parts[0]
        else:
            raise ConnectorError(f"Invalid table reference: {ref}")

    def _get_type_mapper(self) -> TypeMapper:
        """Get PostgreSQL type mapper.

        Returns:
            PostgresTypeMapper instance
        """
        return PostgresTypeMapper()

    def _get_database_name(self) -> str:
        """Get database name for error messages.

        Returns:
            "PostgreSQL"
        """
        return "PostgreSQL"
