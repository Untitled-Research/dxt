"""PostgreSQL connector implementation using SQLAlchemy.

This module provides connection management for PostgreSQL databases.
"""

from __future__ import annotations

from typing import Any, Optional

try:
    from sqlalchemy import create_engine, text, inspect, MetaData, Table
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from dxt.core.connector import Connector
from dxt.exceptions import ConnectionError, ConnectorError, SchemaError
from dxt.models.field import DXTType, Field
from dxt.operators.postgres.type_mapper import PostgresTypeMapper


class PostgresConnector(Connector):
    """PostgreSQL connector using SQLAlchemy.

    Manages connections to PostgreSQL databases and provides
    schema introspection and query execution.

    Configuration keys:
        - host: Database host (default: localhost)
        - port: Database port (default: 5432)
        - database: Database name (required)
        - user: Username (required)
        - password: Password (required)
        - schema: Default schema (default: public)
        - connection_string: Full connection string (alternative to individual params)

    Examples:
        >>> config = {
        ...     "host": "localhost",
        ...     "port": 5432,
        ...     "database": "mydb",
        ...     "user": "postgres",
        ...     "password": "secret"
        ... }
        >>> with PostgresConnector(config) as conn:
        ...     schema = conn.get_schema("public.orders")
        ...     results = conn.execute_query("SELECT * FROM orders LIMIT 10")
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize PostgreSQL connector.

        Args:
            config: Connection configuration dictionary

        Raises:
            ConnectorError: If SQLAlchemy is not installed
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ConnectorError(
                "PostgresConnector requires SQLAlchemy. Install with: pip install sqlalchemy psycopg[binary]"
            )

        super().__init__(config)
        self.engine: Optional[Engine] = None
        self.type_mapper = PostgresTypeMapper()

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from config.

        Returns:
            SQLAlchemy connection string

        Raises:
            ConnectorError: If required config is missing
        """
        # If connection_string provided, use it directly
        if "connection_string" in self.config:
            return self.config["connection_string"]

        # Build from individual parameters
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

    def connect(self) -> None:
        """Establish connection to PostgreSQL.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection_string = self._build_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,  # Verify connections before using
                echo=self.config.get("echo", False),  # SQL logging
            )
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.connection = self.engine
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e

    def disconnect(self) -> None:
        """Close connection to PostgreSQL."""
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.connection = None

    def test_connection(self) -> bool:
        """Test connectivity to PostgreSQL.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.is_connected:
                self.connect()
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def get_schema(self, ref: str) -> list[Field]:
        """Get schema (field definitions) for a table.

        Args:
            ref: Table reference in format "schema.table" or "table"

        Returns:
            List of Field objects representing the schema

        Raises:
            SchemaError: If schema cannot be retrieved
        """
        if not self.is_connected:
            raise SchemaError("Not connected to database")

        try:
            # Parse schema and table name
            schema_name, table_name = self._parse_table_ref(ref)

            # Use SQLAlchemy inspector to get table metadata
            inspector = inspect(self.engine)

            # Check if table exists
            if not inspector.has_table(table_name, schema=schema_name):
                raise SchemaError(f"Table does not exist: {ref}")

            # Get columns
            columns = inspector.get_columns(table_name, schema=schema_name)

            # Convert to Field objects
            fields = []
            for col in columns:
                # Map PostgreSQL type to DXT type
                pg_type = str(col["type"])
                dxt_type = self.type_mapper.from_source(pg_type)

                # Create field
                field = Field(
                    id=col["name"],
                    dtype=dxt_type,
                    nullable=col["nullable"],
                )

                # Add precision/scale for DECIMAL types
                if dxt_type == DXTType.DECIMAL and hasattr(col["type"], "precision"):
                    field.precision = col["type"].precision
                    field.scale = col["type"].scale if hasattr(col["type"], "scale") else None

                fields.append(field)

            return fields

        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Failed to get schema for {ref}: {e}") from e

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return results.

        Args:
            query: SQL query string

        Returns:
            List of records as dictionaries

        Raises:
            ConnectorError: If query execution fails
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                # Convert rows to dictionaries
                return [dict(row._mapping) for row in result]
        except Exception as e:
            raise ConnectorError(f"Failed to execute query: {e}") from e

    def execute_statement(self, statement: str) -> None:
        """Execute a SQL statement (DDL, DML) without returning results.

        Args:
            statement: SQL statement string

        Raises:
            ConnectorError: If statement execution fails
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            with self.engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
        except Exception as e:
            raise ConnectorError(f"Failed to execute statement: {e}") from e

    def _parse_table_ref(self, ref: str) -> tuple[str, str]:
        """Parse table reference into schema and table name.

        Args:
            ref: Table reference (e.g., "public.orders" or "orders")

        Returns:
            Tuple of (schema_name, table_name)
        """
        parts = ref.split(".")
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            # Use default schema from config or 'public'
            default_schema = self.config.get("schema", "public")
            return default_schema, parts[0]
        else:
            raise ConnectorError(f"Invalid table reference: {ref}")

    def table_exists(self, ref: str) -> bool:
        """Check if a table exists.

        Args:
            ref: Table reference

        Returns:
            True if table exists, False otherwise
        """
        if not self.is_connected:
            return False

        try:
            schema_name, table_name = self._parse_table_ref(ref)
            inspector = inspect(self.engine)
            return inspector.has_table(table_name, schema=schema_name)
        except Exception:
            return False
