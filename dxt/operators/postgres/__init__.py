"""PostgreSQL operator for DXT.

This package provides complete PostgreSQL support for DXT, including
connection management, data extraction, and loading.
"""

from dxt.operators.postgres.connector import PostgresConnector
from dxt.operators.postgres.copy_loader import PostgresCopyLoader
from dxt.operators.postgres.extractor import PostgresExtractor
from dxt.operators.postgres.loader import PostgresLoader
from dxt.operators.postgres.type_mapper import PostgresTypeMapper

__all__ = [
    "PostgresConnector",
    "PostgresCopyLoader",
    "PostgresExtractor",
    "PostgresLoader",
    "PostgresTypeMapper",
]
