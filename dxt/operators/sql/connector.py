"""SQL-based connector base class using SQLAlchemy.

This module provides a base class for SQL database connectors
that use SQLAlchemy for connection management.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Optional

try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from dxt.core.connector import Connector
from dxt.core.type_mapper import TypeMapper
from dxt.exceptions import ConnectionError, ConnectorError, SchemaError
from dxt.models.field import DXTType, Field


class SQLConnector(Connector):
    """Base class for SQL database connectors using SQLAlchemy.

    Provides common functionality for SQL-based data systems including:
    - SQLAlchemy engine management
    - Connection lifecycle (connect, disconnect, test)
    - Schema introspection via SQLAlchemy inspector
    - Query execution with result mapping
    - Statement execution with transaction handling
    - Table existence checking

    Subclasses must implement:
    - _build_connection_string(): Database-specific connection string
    - _parse_table_ref(): Database-specific table reference parsing
    - _get_type_mapper(): Return database-specific type mapper
    - _get_database_name(): Return database name for error messages

    This class is abstract and cannot be instantiated directly.

    Examples:
        Subclass implementation:
        >>> class MyDBConnector(SQLConnector):
        ...     def _build_connection_string(self) -> str:
        ...         return f"mydb://{self.config['host']}/{self.config['database']}"
        ...
        ...     def _parse_table_ref(self, ref: str) -> tuple[Optional[str], str]:
        ...         # Returns (schema, table) or (None, table)
        ...         parts = ref.split(".")
        ...         return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
        ...
        ...     def _get_type_mapper(self) -> TypeMapper:
        ...         return MyDBTypeMapper()
        ...
        ...     def _get_database_name(self) -> str:
        ...         return "MyDB"
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize SQL connector.

        Args:
            config: Connection configuration dictionary

        Raises:
            ConnectorError: If SQLAlchemy is not installed
        """
        if not SQLALCHEMY_AVAILABLE:
            db_name = self._get_database_name()
            raise ConnectorError(
                f"{db_name}Connector requires SQLAlchemy. Install with: pip install sqlalchemy"
            )

        super().__init__(config)
        self.engine: Optional[Engine] = None
        self.type_mapper: TypeMapper = self._get_type_mapper()

    @abstractmethod
    def _build_connection_string(self) -> str:
        """Build database-specific connection string from config.

        This method must be implemented by subclasses to construct
        the appropriate SQLAlchemy connection string for the target database.

        Returns:
            SQLAlchemy connection string (e.g., "postgresql://...", "sqlite:///...")

        Raises:
            ConnectorError: If required config is missing or invalid

        Examples:
            PostgreSQL:
            >>> return f"postgresql://{user}:{password}@{host}:{port}/{database}"

            SQLite:
            >>> return f"sqlite:///{database_path}"
        """
        pass

    @abstractmethod
    def _parse_table_ref(self, ref: str) -> tuple[Optional[str], str]:
        """Parse table reference into schema and table name.

        This method must be implemented by subclasses to handle
        database-specific table reference formats.

        Args:
            ref: Table reference string (e.g., "public.orders", "orders")

        Returns:
            Tuple of (schema_name, table_name).
            schema_name may be None for databases without schema support.

        Raises:
            ConnectorError: If table reference format is invalid

        Examples:
            PostgreSQL (supports schemas):
            >>> self._parse_table_ref("public.orders")
            ('public', 'orders')
            >>> self._parse_table_ref("orders")
            ('public', 'orders')  # Uses default schema

            SQLite (no schema support):
            >>> self._parse_table_ref("orders")
            (None, 'orders')
        """
        pass

    @abstractmethod
    def _get_type_mapper(self) -> TypeMapper:
        """Get database-specific type mapper.

        Returns:
            TypeMapper instance for this database

        Examples:
            >>> return PostgresTypeMapper()
            >>> return SQLiteTypeMapper()
        """
        pass

    @abstractmethod
    def _get_database_name(self) -> str:
        """Get database name for error messages.

        Returns:
            Human-readable database name (e.g., "PostgreSQL", "SQLite")

        Examples:
            >>> return "PostgreSQL"
            >>> return "SQLite"
        """
        pass

    def connect(self) -> None:
        """Establish connection to the SQL database.

        Creates a SQLAlchemy engine with the connection string from
        _build_connection_string() and tests the connection.

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
            db_name = self._get_database_name()
            raise ConnectionError(f"Failed to connect to {db_name}: {e}") from e

    def disconnect(self) -> None:
        """Close connection to the SQL database.

        Disposes the SQLAlchemy engine and clears connection references.
        Safe to call even if already disconnected.
        """
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.connection = None

    def test_connection(self) -> bool:
        """Test connectivity to the SQL database.

        Attempts to connect (if not already connected) and execute
        a simple query to verify the connection works.

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

        Uses SQLAlchemy inspector to introspect table metadata and
        converts to DXT Field objects using the database-specific type mapper.

        Args:
            ref: Table reference (format depends on database)

        Returns:
            List of Field objects representing the schema

        Raises:
            SchemaError: If not connected, table doesn't exist, or introspection fails
        """
        if not self.is_connected:
            raise SchemaError("Not connected to database")

        try:
            # Parse schema and table name (database-specific)
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
                # Map database type to DXT type using database-specific mapper
                db_type = str(col["type"])
                dxt_type = self.type_mapper.from_source(db_type)

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
        """Execute a SQL query and return results.

        Executes the query using SQLAlchemy and converts result rows
        to dictionaries for easy consumption.

        Args:
            query: SQL query string

        Returns:
            List of records as dictionaries (column_name -> value)

        Raises:
            ConnectorError: If not connected or query execution fails
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                # Convert rows to dictionaries using _mapping
                return [dict(row._mapping) for row in result]
        except Exception as e:
            raise ConnectorError(f"Failed to execute query: {e}") from e

    def execute_statement(self, statement: str) -> None:
        """Execute a SQL statement (DDL, DML) without returning results.

        Executes the statement and commits the transaction.
        Use for CREATE, INSERT, UPDATE, DELETE, etc.

        Args:
            statement: SQL statement string

        Raises:
            ConnectorError: If not connected or statement execution fails
        """
        if not self.is_connected:
            raise ConnectorError("Not connected to database")

        try:
            with self.engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
        except Exception as e:
            raise ConnectorError(f"Failed to execute statement: {e}") from e

    def table_exists(self, ref: str) -> bool:
        """Check if a table exists in the database.

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
