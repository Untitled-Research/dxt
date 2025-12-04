"""Key-Value store provider base classes.

This module provides base classes for key-value store providers
like Redis, Memcached, etcd, and similar systems.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Iterator, Optional, Union

from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.models.field import Field, DXTType


class KeyValueType(str, Enum):
    """Types of values in key-value stores."""

    STRING = "string"
    HASH = "hash"
    LIST = "list"
    SET = "set"
    SORTED_SET = "sorted_set"
    STREAM = "stream"
    JSON = "json"


@dataclass
class KeyValueEntry:
    """A key-value entry.

    Attributes:
        key: The key
        value: The value (type depends on store and data type)
        value_type: Type of the value
        ttl: Time-to-live in seconds (None if no expiry)
        metadata: Additional metadata (varies by store)
    """

    key: str
    value: Any
    value_type: KeyValueType = KeyValueType.STRING
    ttl: Optional[int] = None
    metadata: Optional[dict[str, Any]] = None


@dataclass
class ScanResult:
    """Result from a key scan operation.

    Attributes:
        keys: List of keys matching the pattern
        cursor: Cursor for next iteration (None if complete)
        count: Number of keys returned
    """

    keys: list[str]
    cursor: Optional[str] = None
    count: int = 0


class KeyValueConnector(Connector):
    """Base connector for Key-Value stores.

    Manages connections to key-value stores like Redis, Memcached,
    etcd, and similar systems.

    Subclasses must implement:
        - connect(): Establish connection
        - disconnect(): Clean up connection
        - get(): Get value by key
        - set(): Set value for key
        - delete(): Delete key
        - exists(): Check if key exists
        - scan(): Scan keys matching pattern

    Example:
        >>> class RedisConnector(KeyValueConnector):
        ...     def connect(self):
        ...         self._client = redis.Redis(
        ...             host=self.host, port=self.port, db=self.database
        ...         )
        ...
        ...     def get(self, key):
        ...         value = self._client.get(key)
        ...         return KeyValueEntry(key=key, value=value) if value else None
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        database: Union[int, str] = 0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl: bool = False,
        ssl_cert_path: Optional[str] = None,
        connection_timeout: float = 5.0,
        socket_timeout: float = 5.0,
    ):
        """Initialize Key-Value connector.

        Args:
            host: Server hostname
            port: Server port
            database: Database number or name
            username: Authentication username
            password: Authentication password
            ssl: Whether to use SSL/TLS
            ssl_cert_path: Path to SSL certificate
            connection_timeout: Connection timeout in seconds
            socket_timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.ssl = ssl
        self.ssl_cert_path = ssl_cert_path
        self.connection_timeout = connection_timeout
        self.socket_timeout = socket_timeout
        self._connected = False

    @abstractmethod
    def get(self, key: str) -> Optional[KeyValueEntry]:
        """Get value by key.

        Args:
            key: The key to retrieve

        Returns:
            KeyValueEntry if found, None otherwise
        """
        pass

    @abstractmethod
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, timedelta]] = None,
        value_type: KeyValueType = KeyValueType.STRING,
    ) -> bool:
        """Set value for key.

        Args:
            key: The key to set
            value: The value to store
            ttl: Time-to-live (seconds or timedelta)
            value_type: Type of value being stored

        Returns:
            True if successful
        """
        pass

    @abstractmethod
    def delete(self, *keys: str) -> int:
        """Delete one or more keys.

        Args:
            keys: Keys to delete

        Returns:
            Number of keys deleted
        """
        pass

    @abstractmethod
    def exists(self, *keys: str) -> int:
        """Check if keys exist.

        Args:
            keys: Keys to check

        Returns:
            Number of keys that exist
        """
        pass

    @abstractmethod
    def scan(
        self,
        pattern: str = "*",
        count: int = 100,
        cursor: Optional[str] = None,
        value_type: Optional[KeyValueType] = None,
    ) -> ScanResult:
        """Scan keys matching pattern.

        Args:
            pattern: Key pattern (glob-style)
            count: Hint for number of keys per iteration
            cursor: Cursor from previous scan
            value_type: Filter by value type (if supported)

        Returns:
            Scan result with keys and cursor
        """
        pass

    def mget(self, *keys: str) -> list[Optional[KeyValueEntry]]:
        """Get multiple values by keys.

        Default implementation calls get() for each key.
        Override for batch-optimized implementations.

        Args:
            keys: Keys to retrieve

        Returns:
            List of entries (None for missing keys)
        """
        return [self.get(key) for key in keys]

    def mset(self, entries: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Set multiple key-value pairs.

        Default implementation calls set() for each entry.
        Override for batch-optimized implementations.

        Args:
            entries: Dictionary of key-value pairs
            ttl: Time-to-live for all entries

        Returns:
            True if all successful
        """
        for key, value in entries.items():
            if not self.set(key, value, ttl=ttl):
                return False
        return True

    def keys(self, pattern: str = "*") -> list[str]:
        """Get all keys matching pattern.

        Warning: May be slow for large key spaces.
        Prefer scan() for large datasets.

        Args:
            pattern: Key pattern (glob-style)

        Returns:
            List of matching keys
        """
        all_keys: list[str] = []
        cursor: Optional[str] = "0"
        while cursor:
            result = self.scan(pattern=pattern, cursor=cursor)
            all_keys.extend(result.keys)
            cursor = result.cursor
        return all_keys

    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected


class KeyValueExtractor(Extractor):
    """Base extractor for Key-Value stores.

    Extracts data from key-value stores by scanning keys
    and retrieving their values.

    Subclasses must implement:
        - extract(): Extract key-value entries

    Example:
        >>> class RedisExtractor(KeyValueExtractor):
        ...     def extract(self):
        ...         cursor = "0"
        ...         while cursor:
        ...             result = self.connector.scan(
        ...                 pattern=self.key_pattern, cursor=cursor
        ...             )
        ...             entries = self.connector.mget(*result.keys)
        ...             for entry in entries:
        ...                 if entry:
        ...                     yield self._entry_to_record(entry)
        ...             cursor = result.cursor
    """

    def __init__(
        self,
        connector: KeyValueConnector,
        key_pattern: str = "*",
        value_type: Optional[KeyValueType] = None,
        include_metadata: bool = False,
        batch_size: int = 100,
    ):
        """Initialize Key-Value extractor.

        Args:
            connector: Key-Value connector instance
            key_pattern: Pattern to match keys (glob-style)
            value_type: Filter by value type
            include_metadata: Whether to include TTL and metadata
            batch_size: Number of keys per scan iteration
        """
        self.connector = connector
        self.key_pattern = key_pattern
        self.value_type = value_type
        self.include_metadata = include_metadata
        self.batch_size = batch_size

    @abstractmethod
    def extract(self) -> Iterator[dict[str, Any]]:
        """Extract key-value entries as records.

        Yields:
            Records with key, value, and optional metadata
        """
        pass

    def get_schema(self) -> list[Field]:
        """Get schema for key-value records.

        Returns a basic schema with key and value fields.
        Override to provide more specific schemas based on
        value structure.

        Returns:
            List of field schemas
        """
        fields = [
            Field(id="key", dtype=DXTType.STRING, nullable=False),
            Field(id="value", dtype=DXTType.STRING, nullable=True),
        ]

        if self.include_metadata:
            fields.extend([
                Field(id="value_type", dtype=DXTType.STRING, nullable=True),
                Field(id="ttl", dtype=DXTType.INT64, nullable=True),
            ])

        return fields

    def _entry_to_record(self, entry: KeyValueEntry) -> dict[str, Any]:
        """Convert KeyValueEntry to record dictionary.

        Args:
            entry: Key-value entry

        Returns:
            Record dictionary
        """
        record: dict[str, Any] = {
            "key": entry.key,
            "value": entry.value,
        }

        if self.include_metadata:
            record["value_type"] = entry.value_type.value
            record["ttl"] = entry.ttl
            if entry.metadata:
                record.update(entry.metadata)

        return record

    def get_record_count(self) -> Optional[int]:
        """Get estimated record count.

        Returns:
            Estimated count or None if unknown
        """
        return None


class KeyValueLoader(Loader):
    """Base loader for Key-Value stores.

    Loads records to key-value stores.

    Subclasses must implement:
        - load(): Load records to store

    Example:
        >>> class RedisLoader(KeyValueLoader):
        ...     def load(self, records):
        ...         batch = {}
        ...         for record in records:
        ...             batch[record["key"]] = record["value"]
        ...             if len(batch) >= self.batch_size:
        ...                 self.connector.mset(batch, ttl=self.default_ttl)
        ...                 self.rows_loaded += len(batch)
        ...                 batch = {}
        ...         if batch:
        ...             self.connector.mset(batch, ttl=self.default_ttl)
        ...             self.rows_loaded += len(batch)
        ...         return self.rows_loaded
    """

    def __init__(
        self,
        connector: KeyValueConnector,
        key_field: str = "key",
        value_field: str = "value",
        ttl_field: Optional[str] = None,
        default_ttl: Optional[int] = None,
        key_prefix: str = "",
        key_separator: str = ":",
        batch_size: int = 100,
        overwrite: bool = True,
    ):
        """Initialize Key-Value loader.

        Args:
            connector: Key-Value connector instance
            key_field: Field name containing the key
            value_field: Field name containing the value
            ttl_field: Field name containing TTL (optional)
            default_ttl: Default TTL for all entries
            key_prefix: Prefix to add to all keys
            key_separator: Separator between prefix and key
            batch_size: Number of entries per batch operation
            overwrite: Whether to overwrite existing keys
        """
        self.connector = connector
        self.key_field = key_field
        self.value_field = value_field
        self.ttl_field = ttl_field
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self.key_separator = key_separator
        self.batch_size = batch_size
        self.overwrite = overwrite
        self.rows_loaded = 0

    @abstractmethod
    def load(self, records: Iterator[dict[str, Any]]) -> int:
        """Load records to key-value store.

        Args:
            records: Iterator of records to load

        Returns:
            Number of records loaded
        """
        pass

    def _build_key(self, record: dict[str, Any]) -> str:
        """Build the full key from a record.

        Args:
            record: Record containing key field

        Returns:
            Full key with prefix
        """
        key = str(record[self.key_field])
        if self.key_prefix:
            return f"{self.key_prefix}{self.key_separator}{key}"
        return key

    def _get_ttl(self, record: dict[str, Any]) -> Optional[int]:
        """Get TTL for a record.

        Args:
            record: Record potentially containing TTL field

        Returns:
            TTL in seconds or None
        """
        if self.ttl_field and self.ttl_field in record:
            return record[self.ttl_field]
        return self.default_ttl

    def _record_to_entry(self, record: dict[str, Any]) -> KeyValueEntry:
        """Convert record to KeyValueEntry.

        Args:
            record: Record dictionary

        Returns:
            KeyValueEntry
        """
        return KeyValueEntry(
            key=self._build_key(record),
            value=record.get(self.value_field),
            ttl=self._get_ttl(record),
        )


@dataclass
class HashField:
    """A field within a hash structure.

    Attributes:
        field: Field name
        value: Field value
    """

    field: str
    value: Any


class HashOperations:
    """Mixin for hash operations in key-value stores.

    Provides methods for working with hash data structures
    (like Redis hashes).
    """

    @abstractmethod
    def hget(self, key: str, field: str) -> Optional[Any]:
        """Get a field from a hash.

        Args:
            key: Hash key
            field: Field name

        Returns:
            Field value or None
        """
        pass

    @abstractmethod
    def hset(self, key: str, field: str, value: Any) -> bool:
        """Set a field in a hash.

        Args:
            key: Hash key
            field: Field name
            value: Field value

        Returns:
            True if field was added, False if updated
        """
        pass

    @abstractmethod
    def hgetall(self, key: str) -> dict[str, Any]:
        """Get all fields from a hash.

        Args:
            key: Hash key

        Returns:
            Dictionary of field-value pairs
        """
        pass

    @abstractmethod
    def hmset(self, key: str, mapping: dict[str, Any]) -> bool:
        """Set multiple fields in a hash.

        Args:
            key: Hash key
            mapping: Dictionary of field-value pairs

        Returns:
            True if successful
        """
        pass

    @abstractmethod
    def hdel(self, key: str, *fields: str) -> int:
        """Delete fields from a hash.

        Args:
            key: Hash key
            fields: Field names to delete

        Returns:
            Number of fields deleted
        """
        pass
