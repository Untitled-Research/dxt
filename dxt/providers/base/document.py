"""Document store provider base classes.

This module provides base classes for document database providers
(MongoDB, Firestore, DynamoDB, CouchDB, etc.).

The document category covers systems that:
- Store data as documents (typically JSON-like)
- Organize documents into collections
- Support flexible, schema-less data structures

Classes:
    DocumentConnector: Base connector for document databases
    DocumentExtractor: Base extractor for document databases
    DocumentLoader: Base loader for document databases
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import Any, Iterator, Optional

from dxt.core.buffer import Buffer
from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.exceptions import (
    ConnectionError,
    ConnectorError,
    ExtractorError,
    LoaderError,
    ValidationError,
)
from dxt.models.field import DXTType, Field
from dxt.models.results import ExtractResult, LoadResult
from dxt.models.stream import Stream


# =============================================================================
# Document Connector
# =============================================================================


class DocumentConnector(Connector):
    """Base class for document database connectors.

    Provides common interface for document stores including:
    - Collection listing and management
    - Document querying with filters and projections
    - Aggregation pipelines
    - Document CRUD operations

    Subclasses must implement:
    - connect(): Establish connection
    - disconnect(): Close connection
    - find(): Query documents
    - insert_many(): Insert documents
    - list_collections(): List collections
    - collection_exists(): Check collection existence

    Example:
        >>> class MongoConnector(DocumentConnector):
        ...     def connect(self):
        ...         self.client = MongoClient(**self.config)
        ...         self.db = self.client[self.config["database"]]
        ...
        ...     def find(self, collection, filter=None, projection=None):
        ...         return self.db[collection].find(filter, projection)
    """

    # Category identifier
    category: str = "document"

    def __init__(self, config: dict[str, Any]):
        """Initialize document connector.

        Args:
            config: Connection configuration. Common keys:
                - hosts: List of host:port strings
                - database: Database name
                - auth: Authentication config
                - replica_set: Replica set name
                - tls: Enable TLS
        """
        super().__init__(config)
        self._database = config.get("database", "")

    @property
    def database(self) -> str:
        """Get the database name."""
        return self._database

    @abstractmethod
    def find(
        self,
        collection: str,
        filter: Optional[dict] = None,
        projection: Optional[dict] = None,
        sort: Optional[list[tuple[str, int]]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> Iterator[dict]:
        """Query documents from a collection.

        Args:
            collection: Collection name
            filter: Query filter document (e.g., {"status": "active"})
            projection: Fields to include/exclude (e.g., {"password": 0})
            sort: Sort specification (e.g., [("created_at", -1)])
            limit: Maximum documents to return
            skip: Number of documents to skip

        Yields:
            Documents as dictionaries

        Raises:
            ConnectorError: If query fails
        """
        pass

    def aggregate(
        self, collection: str, pipeline: list[dict]
    ) -> Iterator[dict]:
        """Run an aggregation pipeline.

        Default implementation raises NotImplementedError.
        Override in subclasses that support aggregation.

        Args:
            collection: Collection name
            pipeline: Aggregation pipeline stages

        Yields:
            Aggregation results as dictionaries

        Raises:
            ConnectorError: If aggregation not supported or fails
        """
        raise ConnectorError(
            f"{self.__class__.__name__} does not support aggregation pipelines"
        )

    @abstractmethod
    def insert_many(self, collection: str, documents: list[dict]) -> int:
        """Insert multiple documents.

        Args:
            collection: Collection name
            documents: Documents to insert

        Returns:
            Number of documents inserted

        Raises:
            ConnectorError: If insert fails
        """
        pass

    def update_many(
        self, collection: str, filter: dict, update: dict
    ) -> int:
        """Update documents matching filter.

        Default implementation raises NotImplementedError.
        Override in subclasses that support updates.

        Args:
            collection: Collection name
            filter: Query filter
            update: Update operations

        Returns:
            Number of documents updated

        Raises:
            ConnectorError: If update not supported or fails
        """
        raise ConnectorError(
            f"{self.__class__.__name__} does not support update_many"
        )

    def delete_many(self, collection: str, filter: dict) -> int:
        """Delete documents matching filter.

        Default implementation raises NotImplementedError.
        Override in subclasses that support deletes.

        Args:
            collection: Collection name
            filter: Query filter

        Returns:
            Number of documents deleted

        Raises:
            ConnectorError: If delete not supported or fails
        """
        raise ConnectorError(
            f"{self.__class__.__name__} does not support delete_many"
        )

    @abstractmethod
    def list_collections(self) -> Iterator[str]:
        """List collections in the database.

        Yields:
            Collection names

        Raises:
            ConnectorError: If listing fails
        """
        pass

    @abstractmethod
    def collection_exists(self, collection: str) -> bool:
        """Check if a collection exists.

        Args:
            collection: Collection name

        Returns:
            True if collection exists, False otherwise
        """
        pass

    def create_collection(self, collection: str) -> None:
        """Create a collection.

        Default implementation does nothing (most document DBs auto-create).
        Override if explicit creation is needed.

        Args:
            collection: Collection name
        """
        pass

    def drop_collection(self, collection: str) -> None:
        """Drop a collection.

        Default implementation raises NotImplementedError.
        Override in subclasses.

        Args:
            collection: Collection name

        Raises:
            ConnectorError: If drop not supported or fails
        """
        raise ConnectorError(
            f"{self.__class__.__name__} does not support drop_collection"
        )

    def count(self, collection: str, filter: Optional[dict] = None) -> int:
        """Count documents in a collection.

        Default implementation iterates through find().
        Override for more efficient implementations.

        Args:
            collection: Collection name
            filter: Optional query filter

        Returns:
            Document count
        """
        return sum(1 for _ in self.find(collection, filter))

    def infer_schema(
        self, collection: str, sample_size: int = 100
    ) -> list[Field]:
        """Infer schema from document sample.

        Args:
            collection: Collection name
            sample_size: Number of documents to sample

        Returns:
            List of Field objects representing inferred schema
        """
        sample = list(self.find(collection, limit=sample_size))
        return self._infer_schema_from_documents(sample)

    def _infer_schema_from_documents(self, documents: list[dict]) -> list[Field]:
        """Infer schema from a list of documents.

        Args:
            documents: Sample documents

        Returns:
            List of Field objects
        """
        if not documents:
            return []

        # Collect all keys and their types
        field_types: dict[str, set] = {}

        for doc in documents:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(type(value).__name__)

        # Convert to Fields
        fields = []
        for key, types in field_types.items():
            dxt_type = self._infer_dxt_type(types)
            fields.append(
                Field(
                    id=key,
                    dtype=dxt_type,
                    nullable=True,  # Document DBs are typically nullable
                )
            )

        return fields

    def _infer_dxt_type(self, python_types: set[str]) -> DXTType:
        """Infer DXT type from Python types.

        Args:
            python_types: Set of Python type names

        Returns:
            Corresponding DXTType
        """
        # Remove None type
        types = python_types - {"NoneType"}

        if not types:
            return DXTType.STRING

        if len(types) > 1:
            # Mixed types - use JSON
            return DXTType.JSON

        type_name = next(iter(types))

        return {
            "str": DXTType.STRING,
            "int": DXTType.INT64,
            "float": DXTType.FLOAT64,
            "bool": DXTType.BOOL,
            "list": DXTType.ARRAY,
            "dict": DXTType.OBJECT,
            "datetime": DXTType.TIMESTAMP,
            "date": DXTType.DATE,
            "bytes": DXTType.BINARY,
        }.get(type_name, DXTType.JSON)

    def test_connection(self) -> bool:
        """Test connectivity to the document database.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.is_connected:
                self.connect()
            # Try to list collections to verify access
            next(iter(self.list_collections()), None)
            return True
        except Exception:
            return False

    def get_schema(self, ref: str) -> list[Field]:
        """Get schema for a collection (inferred from documents).

        Args:
            ref: Collection name

        Returns:
            Inferred schema as list of Fields
        """
        return self.infer_schema(ref)

    def execute_query(self, query: str) -> list[dict]:
        """Not directly supported for document stores.

        Use find() or aggregate() instead.

        Raises:
            ConnectorError: Always
        """
        raise ConnectorError(
            "Document stores don't support SQL queries. Use find() or aggregate()."
        )


# =============================================================================
# Document Extractor
# =============================================================================


class DocumentExtractor(Extractor):
    """Base class for document database extractors.

    Supports extraction from:
    - Collections (full or filtered)
    - Aggregation pipelines
    """

    # Category identifier
    category: str = "document"

    def __init__(
        self, connector: DocumentConnector, config: Optional[dict[str, Any]] = None
    ):
        """Initialize document extractor.

        Args:
            connector: DocumentConnector instance
            config: Optional extractor-specific configuration
        """
        super().__init__(connector, config)
        if not isinstance(connector, DocumentConnector):
            raise ExtractorError("DocumentExtractor requires a DocumentConnector")

    @property
    def document_connector(self) -> DocumentConnector:
        """Get connector as DocumentConnector type."""
        return self.connector  # type: ignore

    def extract(self, stream: Stream, buffer: Buffer) -> ExtractResult:
        """Extract documents from source to buffer.

        Args:
            stream: Stream configuration
            buffer: Buffer to write to

        Returns:
            ExtractResult with metrics
        """
        started_at = datetime.now()
        records_extracted = 0

        try:
            self.validate_stream(stream)

            source = stream.source
            batch_size = stream.extract.batch_size

            # Get documents based on source type
            if source.type == "collection":
                documents = self.document_connector.find(source.value)
            elif source.type == "query":
                filter_doc = source.options.get("filter", {})
                projection = source.options.get("projection")
                documents = self.document_connector.find(
                    source.value, filter=filter_doc, projection=projection
                )
            elif source.type == "aggregate":
                pipeline = source.options.get("pipeline", [])
                documents = self.document_connector.aggregate(source.value, pipeline)
            else:
                raise ExtractorError(
                    f"Document extractor does not support source type: {source.type}. "
                    f"Supported types: collection, query, aggregate"
                )

            # Write to buffer in batches
            batch = []
            for doc in documents:
                # Clean up document (e.g., convert ObjectId to string)
                cleaned = self._clean_document(doc)
                batch.append(cleaned)

                if len(batch) >= batch_size:
                    buffer.write_batch(batch)
                    records_extracted += len(batch)
                    batch = []

            if batch:
                buffer.write_batch(batch)
                records_extracted += len(batch)

            buffer.finalize()
            buffer.commit()

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return ExtractResult(
                stream_id=stream.id,
                success=True,
                records_extracted=records_extracted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            try:
                buffer.rollback()
            except Exception:
                pass

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return ExtractResult(
                stream_id=stream.id,
                success=False,
                records_extracted=records_extracted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(e),
            )

    def _clean_document(self, doc: dict) -> dict:
        """Clean a document for serialization.

        Converts MongoDB-specific types to JSON-serializable types.
        Override in subclasses for provider-specific cleaning.

        Args:
            doc: Raw document

        Returns:
            Cleaned document
        """
        cleaned = {}
        for key, value in doc.items():
            # Skip MongoDB _id by default (can be configured)
            if key == "_id" and not self.config.get("include_id", False):
                continue

            # Convert non-serializable types
            if hasattr(value, "__str__") and not isinstance(
                value, (str, int, float, bool, list, dict, type(None))
            ):
                cleaned[key] = str(value)
            elif isinstance(value, dict):
                cleaned[key] = self._clean_document(value)
            elif isinstance(value, list):
                cleaned[key] = [
                    self._clean_document(v) if isinstance(v, dict) else v
                    for v in value
                ]
            else:
                cleaned[key] = value

        return cleaned

    def build_query(self, stream: Stream, watermark_value=None) -> Optional[str]:
        """Not applicable for document stores."""
        return None

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for document extraction."""
        if not stream.source:
            raise ValidationError(f"Stream '{stream.id}': source is required")

        if stream.source.type not in ("collection", "query", "aggregate"):
            raise ValidationError(
                f"Stream '{stream.id}': invalid source type '{stream.source.type}'. "
                f"Must be one of: collection, query, aggregate"
            )


# =============================================================================
# Document Loader
# =============================================================================


class DocumentLoader(Loader):
    """Base class for document database loaders.

    Supports load modes:
    - append: Insert documents
    - replace: Drop collection and insert
    - upsert: Update or insert based on key
    """

    # Category identifier
    category: str = "document"

    def __init__(
        self, connector: DocumentConnector, config: Optional[dict[str, Any]] = None
    ):
        """Initialize document loader.

        Args:
            connector: DocumentConnector instance
            config: Optional loader-specific configuration
        """
        super().__init__(connector, config)
        if not isinstance(connector, DocumentConnector):
            raise LoaderError("DocumentLoader requires a DocumentConnector")

    @property
    def document_connector(self) -> DocumentConnector:
        """Get connector as DocumentConnector type."""
        return self.connector  # type: ignore

    def load(self, stream: Stream, buffer: Buffer) -> LoadResult:
        """Load documents from buffer to target collection.

        Args:
            stream: Stream configuration
            buffer: Buffer to read from

        Returns:
            LoadResult with metrics
        """
        started_at = datetime.now()
        records_loaded = 0
        records_deleted = 0

        try:
            self.validate_stream(stream)

            collection = stream.target.value
            batch_size = stream.load.batch_size

            # Handle replace mode
            if stream.load.mode == "replace":
                if self.document_connector.collection_exists(collection):
                    records_deleted = self.document_connector.count(collection)
                    self.document_connector.drop_collection(collection)

            # Load documents
            if stream.load.mode == "upsert":
                records_loaded = self._load_upsert(
                    stream, buffer, collection, batch_size
                )
            else:
                records_loaded = self._load_append(buffer, collection, batch_size)

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=True,
                records_loaded=records_loaded,
                records_inserted=records_loaded,
                records_updated=0,
                records_deleted=records_deleted,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return LoadResult(
                stream_id=stream.id,
                success=False,
                records_loaded=0,
                records_inserted=0,
                records_updated=0,
                records_deleted=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(e),
            )

    def _load_append(
        self, buffer: Buffer, collection: str, batch_size: int
    ) -> int:
        """Load documents in append mode (insert).

        Args:
            buffer: Buffer to read from
            collection: Target collection
            batch_size: Documents per batch

        Returns:
            Number of documents loaded
        """
        total = 0
        for batch in buffer.read_batches(batch_size):
            if batch:
                self.document_connector.insert_many(collection, batch)
                total += len(batch)
        return total

    def _load_upsert(
        self, stream: Stream, buffer: Buffer, collection: str, batch_size: int
    ) -> int:
        """Load documents in upsert mode.

        Default implementation raises NotImplementedError.
        Override in subclasses that support upsert.

        Args:
            stream: Stream configuration (for upsert keys)
            buffer: Buffer to read from
            collection: Target collection
            batch_size: Documents per batch

        Returns:
            Number of documents loaded

        Raises:
            LoaderError: If upsert not supported
        """
        raise LoaderError(
            f"{self.__class__.__name__} does not support upsert mode. "
            "Override _load_upsert() in subclass."
        )

    def create_target_stream(self, stream: Stream) -> None:
        """Create target collection if needed.

        Args:
            stream: Stream configuration
        """
        collection = stream.target.value
        if not self.document_connector.collection_exists(collection):
            self.document_connector.create_collection(collection)

    def validate_stream(self, stream: Stream) -> None:
        """Validate stream configuration for document loading."""
        if not stream.target:
            raise ValidationError(f"Stream '{stream.id}': target is required")

        if stream.load.mode == "upsert" and not stream.load.upsert_keys:
            raise ValidationError(
                f"Stream '{stream.id}': upsert_keys required for upsert mode"
            )
