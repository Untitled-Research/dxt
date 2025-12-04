"""PostgreSQL provider for DXT.

This package provides PostgreSQL-specific implementations:
- PostgresConnector: Connection management
- PostgresExtractor: Data extraction
- PostgresLoader: Data loading
- PostgresCopyLoader: Optimized bulk loading via COPY
- PostgresTypeMapper: Type conversion

Example:
    >>> from dxt.providers.postgres import PostgresConnector, PostgresExtractor
    >>>
    >>> config = {
    ...     "host": "localhost",
    ...     "database": "mydb",
    ...     "user": "postgres",
    ...     "password": "secret"
    ... }
    >>> with PostgresConnector(config) as conn:
    ...     extractor = PostgresExtractor(conn)
    ...     # ... extraction logic
"""

from dxt.providers.postgres.connector import PostgresConnector
from dxt.providers.postgres.extractor import PostgresExtractor
from dxt.providers.postgres.loader import PostgresLoader
from dxt.providers.postgres.copy_loader import PostgresCopyLoader
from dxt.providers.postgres.type_mapper import PostgresTypeMapper

__all__ = [
    "PostgresConnector",
    "PostgresExtractor",
    "PostgresLoader",
    "PostgresCopyLoader",
    "PostgresTypeMapper",
]
