"""Web API provider base classes.

This module provides base classes for REST and GraphQL API providers.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator, Optional

from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.models.field import Field


class HttpMethod(str, Enum):
    """HTTP methods supported for API requests."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class AuthType(str, Enum):
    """Authentication types for API connections."""

    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"


@dataclass
class PaginationConfig:
    """Configuration for API pagination.

    Attributes:
        style: Pagination style (offset, cursor, page, link)
        page_size: Number of records per page
        page_param: Query parameter name for page number
        offset_param: Query parameter name for offset
        limit_param: Query parameter name for limit
        cursor_param: Query parameter name for cursor
        cursor_path: JSON path to cursor in response
        next_link_path: JSON path to next page URL in response
        total_path: JSON path to total count in response
        data_path: JSON path to records array in response
    """

    style: str = "offset"
    page_size: int = 100
    page_param: str = "page"
    offset_param: str = "offset"
    limit_param: str = "limit"
    cursor_param: str = "cursor"
    cursor_path: Optional[str] = None
    next_link_path: Optional[str] = None
    total_path: Optional[str] = None
    data_path: str = "data"


@dataclass
class RateLimitConfig:
    """Configuration for API rate limiting.

    Attributes:
        requests_per_second: Maximum requests per second
        requests_per_minute: Maximum requests per minute
        retry_on_429: Whether to retry on rate limit responses
        max_retries: Maximum number of retries
        backoff_factor: Exponential backoff multiplier
    """

    requests_per_second: Optional[float] = None
    requests_per_minute: Optional[float] = None
    retry_on_429: bool = True
    max_retries: int = 3
    backoff_factor: float = 2.0


@dataclass
class ApiResponse:
    """Response from an API request.

    Attributes:
        status_code: HTTP status code
        headers: Response headers
        data: Parsed response data (JSON)
        raw_body: Raw response body bytes
        next_cursor: Cursor for next page (if paginated)
        next_url: URL for next page (if paginated)
        total_count: Total record count (if available)
    """

    status_code: int
    headers: dict[str, str]
    data: Any
    raw_body: Optional[bytes] = None
    next_cursor: Optional[str] = None
    next_url: Optional[str] = None
    total_count: Optional[int] = None


class WebAPIConnector(Connector):
    """Base connector for Web APIs.

    Manages HTTP connections, authentication, and rate limiting
    for REST and GraphQL APIs.

    Subclasses must implement:
        - connect(): Establish connection and authenticate
        - disconnect(): Clean up connection resources
        - request(): Make HTTP request

    Example:
        >>> class MyAPIConnector(WebAPIConnector):
        ...     def connect(self):
        ...         self._session = requests.Session()
        ...         self._session.headers["Authorization"] = f"Bearer {self.api_key}"
        ...
        ...     def request(self, method, endpoint, **kwargs):
        ...         url = f"{self.base_url}/{endpoint}"
        ...         response = self._session.request(method.value, url, **kwargs)
        ...         return ApiResponse(
        ...             status_code=response.status_code,
        ...             headers=dict(response.headers),
        ...             data=response.json(),
        ...         )
    """

    def __init__(
        self,
        base_url: str,
        auth_type: AuthType = AuthType.NONE,
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        bearer_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        oauth_config: Optional[dict[str, Any]] = None,
        default_headers: Optional[dict[str, str]] = None,
        timeout: float = 30.0,
        rate_limit: Optional[RateLimitConfig] = None,
        verify_ssl: bool = True,
    ):
        """Initialize Web API connector.

        Args:
            base_url: Base URL for the API
            auth_type: Authentication type
            api_key: API key for api_key auth
            api_key_header: Header name for API key
            bearer_token: Token for bearer auth
            username: Username for basic auth
            password: Password for basic auth
            oauth_config: OAuth2 configuration
            default_headers: Default headers for all requests
            timeout: Request timeout in seconds
            rate_limit: Rate limiting configuration
            verify_ssl: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.bearer_token = bearer_token
        self.username = username
        self.password = password
        self.oauth_config = oauth_config or {}
        self.default_headers = default_headers or {}
        self.timeout = timeout
        self.rate_limit = rate_limit or RateLimitConfig()
        self.verify_ssl = verify_ssl
        self._connected = False

    @abstractmethod
    def request(
        self,
        method: HttpMethod,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> ApiResponse:
        """Make an HTTP request to the API.

        Args:
            method: HTTP method
            endpoint: API endpoint (appended to base_url)
            params: Query parameters
            json_body: JSON request body
            headers: Additional headers

        Returns:
            API response object
        """
        pass

    def get(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> ApiResponse:
        """Make a GET request.

        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: Additional headers

        Returns:
            API response object
        """
        return self.request(HttpMethod.GET, endpoint, params=params, headers=headers)

    def post(
        self,
        endpoint: str,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> ApiResponse:
        """Make a POST request.

        Args:
            endpoint: API endpoint
            json_body: JSON request body
            params: Query parameters
            headers: Additional headers

        Returns:
            API response object
        """
        return self.request(
            HttpMethod.POST, endpoint, params=params, json_body=json_body, headers=headers
        )

    @abstractmethod
    def paginate(
        self,
        method: HttpMethod,
        endpoint: str,
        pagination: PaginationConfig,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Iterator[ApiResponse]:
        """Iterate through paginated API responses.

        Args:
            method: HTTP method
            endpoint: API endpoint
            pagination: Pagination configuration
            params: Query parameters
            json_body: JSON request body (for POST pagination)

        Yields:
            API response objects for each page
        """
        pass

    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected


class WebAPIExtractor(Extractor):
    """Base extractor for Web APIs.

    Extracts data from REST or GraphQL APIs with support for
    pagination, filtering, and incremental extraction.

    Subclasses must implement:
        - extract(): Extract records from API
        - get_schema(): Get schema for endpoint/query

    Example:
        >>> class MyAPIExtractor(WebAPIExtractor):
        ...     def extract(self):
        ...         pagination = PaginationConfig(style="cursor", data_path="results")
        ...         for response in self.connector.paginate(
        ...             HttpMethod.GET, self.endpoint, pagination
        ...         ):
        ...             for record in response.data["results"]:
        ...                 yield record
    """

    def __init__(
        self,
        connector: WebAPIConnector,
        endpoint: Optional[str] = None,
        method: HttpMethod = HttpMethod.GET,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        pagination: Optional[PaginationConfig] = None,
        headers: Optional[dict[str, str]] = None,
    ):
        """Initialize Web API extractor.

        Args:
            connector: Web API connector instance
            endpoint: API endpoint to extract from
            method: HTTP method for extraction
            params: Query parameters
            json_body: JSON request body
            pagination: Pagination configuration
            headers: Additional headers for extraction
        """
        self.connector = connector
        self.endpoint = endpoint
        self.method = method
        self.params = params or {}
        self.json_body = json_body
        self.pagination = pagination
        self.headers = headers or {}

    @abstractmethod
    def extract(self) -> Iterator[dict[str, Any]]:
        """Extract records from API.

        Yields:
            Records as dictionaries
        """
        pass

    @abstractmethod
    def get_schema(self) -> list[Field]:
        """Get schema for the API response.

        For APIs without explicit schemas, this may involve
        sampling responses and inferring field types.

        Returns:
            List of field schemas
        """
        pass

    def get_record_count(self) -> Optional[int]:
        """Get total record count if available.

        Returns:
            Total count or None if unknown
        """
        return None


class GraphQLConfig:
    """Configuration for GraphQL queries.

    Attributes:
        query: GraphQL query string
        variables: Query variables
        operation_name: Operation name for multi-operation documents
        data_path: Path to records in response data
    """

    def __init__(
        self,
        query: str,
        variables: Optional[dict[str, Any]] = None,
        operation_name: Optional[str] = None,
        data_path: str = "data",
    ):
        """Initialize GraphQL configuration.

        Args:
            query: GraphQL query string
            variables: Query variables
            operation_name: Operation name
            data_path: Path to records in response
        """
        self.query = query
        self.variables = variables or {}
        self.operation_name = operation_name
        self.data_path = data_path


class WebAPILoader(Loader):
    """Base loader for Web APIs.

    Loads data to APIs via POST/PUT/PATCH requests with support
    for batching and rate limiting.

    Subclasses must implement:
        - load(): Load records to API
        - validate_record(): Validate record before loading

    Example:
        >>> class MyAPILoader(WebAPILoader):
        ...     def load(self, records):
        ...         for batch in self._batch_records(records):
        ...             self.connector.post(self.endpoint, json_body={"items": batch})
        ...             self.rows_loaded += len(batch)
    """

    def __init__(
        self,
        connector: WebAPIConnector,
        endpoint: str,
        method: HttpMethod = HttpMethod.POST,
        batch_size: int = 100,
        headers: Optional[dict[str, str]] = None,
    ):
        """Initialize Web API loader.

        Args:
            connector: Web API connector instance
            endpoint: API endpoint to load to
            method: HTTP method for loading
            batch_size: Number of records per batch
            headers: Additional headers for loading
        """
        self.connector = connector
        self.endpoint = endpoint
        self.method = method
        self.batch_size = batch_size
        self.headers = headers or {}
        self.rows_loaded = 0

    @abstractmethod
    def load(self, records: Iterator[dict[str, Any]]) -> int:
        """Load records to API.

        Args:
            records: Iterator of records to load

        Returns:
            Number of records loaded
        """
        pass

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Validate a record before loading.

        Override this method to add custom validation.

        Args:
            record: Record to validate

        Returns:
            True if valid, False otherwise
        """
        return True

    def _batch_records(
        self, records: Iterator[dict[str, Any]]
    ) -> Iterator[list[dict[str, Any]]]:
        """Group records into batches.

        Args:
            records: Iterator of records

        Yields:
            Batches of records
        """
        batch: list[dict[str, Any]] = []
        for record in records:
            if self.validate_record(record):
                batch.append(record)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch


@dataclass
class WebhookConfig:
    """Configuration for webhook endpoints.

    Attributes:
        path: Webhook endpoint path
        secret: Webhook signing secret
        verify_signature: Whether to verify webhook signatures
        signature_header: Header containing signature
        signature_algorithm: Algorithm for signature verification
    """

    path: str
    secret: Optional[str] = None
    verify_signature: bool = True
    signature_header: str = "X-Webhook-Signature"
    signature_algorithm: str = "sha256"
