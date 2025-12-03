"""Base Connector abstract class.

This module defines the Connector interface for managing connections
to data systems.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from dxt.models.field import Field


class Connector(ABC):
    """Base class for managing connections to data systems.

    Connectors handle connection lifecycle, schema introspection,
    and basic query execution. They are composed by Extractors and Loaders
    rather than inherited.

    Examples:
        Using a connector as a context manager:
        >>> with PostgresConnector(config) as conn:
        ...     schema = conn.get_schema("public.orders")
        ...     results = conn.execute_query("SELECT * FROM orders LIMIT 10")
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize connector with configuration.

        Args:
            config: Connection configuration dictionary
        """
        self.config = config
        self.connection: Optional[Any] = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data system.

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data system.

        Should handle cases where connection is already closed gracefully.
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test connectivity to the data system.

        Returns:
            True if connection is successful, False otherwise
        """
        pass

    @abstractmethod
    def get_schema(self, ref: str) -> list[Field]:
        """Get schema (field definitions) for a table/view/resource.

        Args:
            ref: Reference to the data source (e.g., "public.orders", "orders.json")

        Returns:
            List of Field objects representing the schema

        Raises:
            SchemaError: If schema cannot be retrieved
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return results.

        Args:
            query: Query string (SQL, etc.)

        Returns:
            List of records as dictionaries

        Raises:
            ConnectorError: If query execution fails
        """
        pass

    def __enter__(self) -> Connector:
        """Context manager entry: establish connection.

        Returns:
            Self
        """
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit: close connection.

        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Exception traceback (if any)
        """
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        """Check if connection is established.

        Returns:
            True if connected, False otherwise
        """
        return self.connection is not None
