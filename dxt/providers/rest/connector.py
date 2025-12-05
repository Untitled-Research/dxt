"""REST API connector implementation."""

from __future__ import annotations

import time
from typing import Any, Iterator, Optional

import httpx

from dxt.providers.base.webapi import (
    ApiResponse,
    AuthType,
    HttpMethod,
    PaginationConfig,
    RateLimitConfig,
    WebAPIConnector,
)


class RestConnector(WebAPIConnector):
    """Concrete REST API connector using httpx.

    Handles HTTP connections, authentication, rate limiting,
    and pagination for REST APIs.

    Can be initialized either with explicit parameters or via a config dict
    (for compatibility with the pipeline executor).

    Example (explicit):
        >>> connector = RestConnector(
        ...     base_url="https://api.example.com/v1",
        ...     auth_type=AuthType.BEARER,
        ...     bearer_token="secret-token"
        ... )
        >>> connector.connect()
        >>> response = connector.get("/users")
        >>> print(response.data)

    Example (config dict - used by pipeline executor):
        >>> connector = RestConnector({
        ...     "connection_string": "https://api.example.com/v1",
        ...     "auth_type": "bearer",
        ...     "bearer_token": "secret-token"
        ... })
    """

    def __init__(
        self,
        config_or_base_url: str | dict[str, Any],
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
        """Initialize REST connector.

        Args:
            config_or_base_url: Either a config dict or the base URL string.
                If dict, expects keys like:
                    - connection_string: Base URL (required)
                    - auth_type: "none", "api_key", "bearer", "basic"
                    - api_key, bearer_token, username, password: Auth credentials
                    - timeout, verify_ssl: Connection options
            auth_type: Authentication type (when using explicit params)
            api_key: API key (when using explicit params)
            ... other explicit params ...
        """
        # Handle config dict initialization (from pipeline executor)
        if isinstance(config_or_base_url, dict):
            config = config_or_base_url
            base_url = config.get("connection_string", config.get("base_url", ""))

            # Parse auth_type from string if provided
            auth_type_str = config.get("auth_type", "none")
            if isinstance(auth_type_str, str):
                auth_type = AuthType(auth_type_str.lower())
            else:
                auth_type = auth_type_str

            api_key = config.get("api_key")
            api_key_header = config.get("api_key_header", "X-API-Key")
            bearer_token = config.get("bearer_token")
            username = config.get("username")
            password = config.get("password")
            oauth_config = config.get("oauth_config")
            default_headers = config.get("default_headers")
            timeout = config.get("timeout", 30.0)
            verify_ssl = config.get("verify_ssl", True)

            # Build rate limit config if provided
            rate_limit_config = config.get("rate_limit")
            if rate_limit_config and isinstance(rate_limit_config, dict):
                rate_limit = RateLimitConfig(**rate_limit_config)
            else:
                rate_limit = None

            # Store config for compatibility with base Connector
            self.config = config
        else:
            base_url = config_or_base_url
            self.config = {"connection_string": base_url}

        super().__init__(
            base_url=base_url,
            auth_type=auth_type,
            api_key=api_key,
            api_key_header=api_key_header,
            bearer_token=bearer_token,
            username=username,
            password=password,
            oauth_config=oauth_config,
            default_headers=default_headers,
            timeout=timeout,
            rate_limit=rate_limit,
            verify_ssl=verify_ssl,
        )
        self._client: Optional[httpx.Client] = None
        self._last_request_time: float = 0

    def connect(self) -> None:
        """Establish HTTP client connection."""
        headers = dict(self.default_headers)

        # Set up authentication headers
        if self.auth_type == AuthType.API_KEY and self.api_key:
            headers[self.api_key_header] = self.api_key
        elif self.auth_type == AuthType.BEARER and self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        # Create client with basic auth if needed
        auth = None
        if self.auth_type == AuthType.BASIC and self.username and self.password:
            auth = httpx.BasicAuth(self.username, self.password)

        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers,
            auth=auth,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        self._connected = True

    def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None
        self._connected = False

    def test_connection(self) -> bool:
        """Test connection by making a simple request."""
        if not self._client:
            return False
        try:
            # Try a HEAD request to base URL
            response = self._client.head("/")
            return response.status_code < 500
        except httpx.HTTPError:
            return False

    def get_schema(self, stream_ref: str) -> list:
        """Get schema - not directly supported for REST APIs."""
        return []

    def execute_query(self, query: str) -> list:
        """Execute query - not directly applicable for REST APIs."""
        raise NotImplementedError("REST APIs don't support SQL queries")

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting if configured."""
        if self.rate_limit.requests_per_second:
            min_interval = 1.0 / self.rate_limit.requests_per_second
            elapsed = time.time() - self._last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    def _make_request_with_retry(
        self,
        method: HttpMethod,
        url: str,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Make request with retry logic for rate limiting."""
        if not self._client:
            raise RuntimeError("Connector not connected. Call connect() first.")

        retries = 0
        while True:
            self._apply_rate_limit()

            response = self._client.request(
                method=method.value,
                url=url,
                params=params,
                json=json_body,
                headers=headers,
            )

            # Handle rate limiting
            if response.status_code == 429 and self.rate_limit.retry_on_429:
                if retries >= self.rate_limit.max_retries:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After", "1")
                wait_time = float(retry_after) if retry_after.isdigit() else 1.0
                wait_time *= self.rate_limit.backoff_factor**retries
                time.sleep(wait_time)
                retries += 1
                continue

            return response

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
        response = self._make_request_with_retry(
            method=method,
            url=endpoint,
            params=params,
            json_body=json_body,
            headers=headers,
        )

        # Parse JSON response
        try:
            data = response.json()
        except Exception:
            data = None

        return ApiResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            data=data,
            raw_body=response.content,
        )

    def paginate(
        self,
        method: HttpMethod,
        endpoint: str,
        pagination: PaginationConfig,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Iterator[ApiResponse]:
        """Iterate through paginated API responses.

        Supports offset, cursor, page, and link-based pagination.

        Args:
            method: HTTP method
            endpoint: API endpoint
            pagination: Pagination configuration
            params: Query parameters
            json_body: JSON request body (for POST pagination)

        Yields:
            API response objects for each page
        """
        params = dict(params or {})
        current_offset = 0
        current_page = 1
        current_cursor: Optional[str] = None
        current_url = endpoint

        while True:
            # Apply pagination parameters based on style
            request_params = dict(params)

            if pagination.style == "offset":
                request_params[pagination.offset_param] = current_offset
                request_params[pagination.limit_param] = pagination.page_size
            elif pagination.style == "page":
                request_params[pagination.page_param] = current_page
                request_params[pagination.limit_param] = pagination.page_size
            elif pagination.style == "cursor" and current_cursor:
                request_params[pagination.cursor_param] = current_cursor
                request_params[pagination.limit_param] = pagination.page_size
            elif pagination.style == "cursor":
                # First request without cursor
                request_params[pagination.limit_param] = pagination.page_size

            response = self.request(
                method=method,
                endpoint=current_url,
                params=request_params,
                json_body=json_body,
            )

            # Extract pagination metadata from response
            if pagination.style == "cursor" and pagination.cursor_path:
                current_cursor = self._extract_path(response.data, pagination.cursor_path)
                response.next_cursor = current_cursor
            elif pagination.style == "link" and pagination.next_link_path:
                next_url = self._extract_path(response.data, pagination.next_link_path)
                response.next_url = next_url

            if pagination.total_path:
                response.total_count = self._extract_path(response.data, pagination.total_path)

            yield response

            # Determine if there are more pages
            records = self._extract_path(response.data, pagination.data_path) or []
            if not records:
                break

            if pagination.style == "offset":
                current_offset += pagination.page_size
                if len(records) < pagination.page_size:
                    break
            elif pagination.style == "page":
                current_page += 1
                if len(records) < pagination.page_size:
                    break
            elif pagination.style == "cursor":
                if not current_cursor:
                    break
            elif pagination.style == "link":
                if not response.next_url:
                    break
                current_url = response.next_url

    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract value from nested data using dot notation path.

        Args:
            data: Nested dict/list structure
            path: Dot-separated path (e.g., "meta.next_cursor" or "results.0.id")

        Returns:
            Value at path or None if not found
        """
        if data is None or not path:
            return None

        parts = path.split(".")
        current = data

        for part in parts:
            if current is None:
                return None
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    return None
            else:
                return None

        return current
