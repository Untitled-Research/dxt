"""Generic SQL operators for SQLAlchemy-based databases.

This package provides concrete base classes for SQL database operations:
- SQLConnector: Connection management using SQLAlchemy
- SQLExtractor: Data extraction with query building
- SQLLoader: Data loading with multiple methods (INSERT, COPY, bulk)

These classes are fully functional and can be used directly for any
SQL database supported by SQLAlchemy. Database-specific subclasses
(PostgresConnector, MySQLConnector, etc.) can override methods for
optimizations but inherit all core functionality.
"""

from dxt.operators.sql.connector import SQLConnector
from dxt.operators.sql.extractor import SQLExtractor
from dxt.operators.sql.loader import SQLLoader

__all__ = ["SQLConnector", "SQLExtractor", "SQLLoader"]
