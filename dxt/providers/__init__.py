"""DXT Providers.

This package contains provider implementations for various data systems.
Providers are organized by category (relational, document, filestore, etc.)
with base classes in the `base` subpackage.

Categories:
    - relational: SQL databases (PostgreSQL, MySQL, SQLite, Snowflake, etc.)
    - document: Document stores (MongoDB, Firestore, DynamoDB, etc.)
    - filestore: File/object storage (Local, S3, GCS, Azure, SFTP, etc.)
    - webapi: HTTP APIs (REST, GraphQL, etc.)
    - keyvalue: Key-value stores (Redis, Memcached, etc.)
    - graph: Graph databases (Neo4j, Neptune, etc.) [future]

Usage:
    Pipeline YAML specifies concrete provider classes directly:

        extractor:
          class: dxt.providers.postgres.PostgresExtractor
          config:
            host: localhost
            database: mydb

    The system imports and instantiates the specified class.
    Validation ensures the class inherits from the appropriate base.

Example:
    >>> from dxt.providers.postgres import PostgresConnector, PostgresExtractor
    >>> from dxt.providers.base import RelationalConnector
    >>>
    >>> # Direct instantiation
    >>> connector = PostgresConnector({"host": "localhost", "database": "mydb"})
    >>>
    >>> # Validation via inheritance
    >>> assert isinstance(connector, RelationalConnector)
"""
