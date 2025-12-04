"""Base classes for DXT provider categories.

This package contains abstract base classes for each provider category.
Concrete providers inherit from these base classes.

Categories:
    - RelationalConnector, RelationalExtractor, RelationalLoader
    - DocumentConnector, DocumentExtractor, DocumentLoader
    - FilestoreConnector, FilestoreExtractor, FilestoreLoader
    - WebAPIConnector, WebAPIExtractor, WebAPILoader
    - KeyValueConnector, KeyValueExtractor, KeyValueLoader
    - GraphConnector, GraphExtractor, GraphLoader [future]
"""

from dxt.providers.base.relational import (
    RelationalConnector,
    RelationalExtractor,
    RelationalLoader,
)
from dxt.providers.base.document import (
    DocumentConnector,
    DocumentExtractor,
    DocumentLoader,
)
from dxt.providers.base.filestore import (
    FilestoreConnector,
    FilestoreExtractor,
    FilestoreLoader,
    FileInfo,
    StagedFile,
)
from dxt.providers.base.webapi import (
    WebAPIConnector,
    WebAPIExtractor,
    WebAPILoader,
    HttpMethod,
    AuthType,
    PaginationConfig,
    RateLimitConfig,
    ApiResponse,
    GraphQLConfig,
    WebhookConfig,
)
from dxt.providers.base.keyvalue import (
    KeyValueConnector,
    KeyValueExtractor,
    KeyValueLoader,
    KeyValueType,
    KeyValueEntry,
    ScanResult,
    HashOperations,
)

__all__ = [
    # Relational
    "RelationalConnector",
    "RelationalExtractor",
    "RelationalLoader",
    # Document
    "DocumentConnector",
    "DocumentExtractor",
    "DocumentLoader",
    # Filestore
    "FilestoreConnector",
    "FilestoreExtractor",
    "FilestoreLoader",
    "FileInfo",
    "StagedFile",
    # Web API
    "WebAPIConnector",
    "WebAPIExtractor",
    "WebAPILoader",
    "HttpMethod",
    "AuthType",
    "PaginationConfig",
    "RateLimitConfig",
    "ApiResponse",
    "GraphQLConfig",
    "WebhookConfig",
    # Key-Value
    "KeyValueConnector",
    "KeyValueExtractor",
    "KeyValueLoader",
    "KeyValueType",
    "KeyValueEntry",
    "ScanResult",
    "HashOperations",
]
