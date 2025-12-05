"""REST API extractor implementation."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator, Optional

from dxt.core.buffer import Buffer
from dxt.models.field import DXTType, Field
from dxt.models.results import ExtractResult
from dxt.models.stream import Stream
from dxt.providers.base.webapi import (
    HttpMethod,
    PaginationConfig,
    WebAPIExtractor,
)
from dxt.providers.rest.connector import RestConnector


class RestExtractor(WebAPIExtractor):
    """REST API extractor.

    Extracts data from REST APIs with support for pagination,
    JSON path-based record extraction, and schema inference.

    Example:
        >>> connector = RestConnector(base_url="https://api.example.com")
        >>> extractor = RestExtractor(connector)
        >>> # Stream config specifies endpoint, pagination, etc.
        >>> result = extractor.extract(stream, buffer)
    """

    def __init__(
        self,
        connector: RestConnector,
        config: Optional[dict[str, Any]] = None,
    ):
        """Initialize REST extractor.

        Args:
            connector: REST connector instance
            config: Optional extractor configuration (currently unused,
                    but required for pipeline executor compatibility)
        """
        super().__init__(connector)
        self.connector: RestConnector = connector
        self.extractor_config = config or {}

    def extract(self, stream: Stream, buffer: Buffer) -> ExtractResult:
        """Extract records from REST API endpoint.

        The stream.source should contain:
            - value: The endpoint path (e.g., "/users")
            - options: Optional dict with:
                - method: HTTP method (default: GET)
                - params: Query parameters
                - data_path: JSON path to records array (default: root or "data")
                - pagination: Pagination config dict

        Args:
            stream: Stream configuration
            buffer: Buffer to write records to

        Returns:
            ExtractResult with extraction statistics
        """
        started_at = datetime.now()
        records_extracted = 0
        error_message: Optional[str] = None

        try:
            # Get endpoint and options from stream source
            endpoint = stream.source.value
            options = stream.source.options or {}

            method = HttpMethod(options.get("method", "GET").upper())
            params = options.get("params", {})
            data_path = options.get("data_path", "")
            json_body = options.get("body")

            # Build pagination config if specified
            pagination_opts = options.get("pagination")
            pagination = None
            if pagination_opts:
                pagination = PaginationConfig(
                    style=pagination_opts.get("style", "offset"),
                    page_size=pagination_opts.get("page_size", 100),
                    page_param=pagination_opts.get("page_param", "page"),
                    offset_param=pagination_opts.get("offset_param", "offset"),
                    limit_param=pagination_opts.get("limit_param", "limit"),
                    cursor_param=pagination_opts.get("cursor_param", "cursor"),
                    cursor_path=pagination_opts.get("cursor_path"),
                    next_link_path=pagination_opts.get("next_link_path"),
                    total_path=pagination_opts.get("total_path"),
                    data_path=pagination_opts.get("data_path", data_path or "data"),
                )
                # Use pagination data_path if set
                if not data_path:
                    data_path = pagination.data_path

            batch_size = stream.extract.batch_size if stream.extract else 1000
            batch: list[dict[str, Any]] = []

            if pagination:
                # Paginated extraction
                for response in self.connector.paginate(
                    method=method,
                    endpoint=endpoint,
                    pagination=pagination,
                    params=params,
                    json_body=json_body,
                ):
                    if response.status_code >= 400:
                        raise RuntimeError(
                            f"API request failed with status {response.status_code}"
                        )

                    records = self._extract_records(response.data, data_path)
                    for record in records:
                        # Apply field mapping if fields are defined
                        mapped_record = self._map_record(record, stream.fields)
                        batch.append(mapped_record)
                        records_extracted += 1

                        if len(batch) >= batch_size:
                            buffer.write_batch(batch)
                            batch = []
            else:
                # Single request extraction
                response = self.connector.request(
                    method=method,
                    endpoint=endpoint,
                    params=params,
                    json_body=json_body,
                )

                if response.status_code >= 400:
                    raise RuntimeError(
                        f"API request failed with status {response.status_code}"
                    )

                records = self._extract_records(response.data, data_path)
                for record in records:
                    mapped_record = self._map_record(record, stream.fields)
                    batch.append(mapped_record)
                    records_extracted += 1

            # Write remaining batch
            if batch:
                buffer.write_batch(batch)

            buffer.finalize()
            buffer.commit()
            success = True

        except Exception as e:
            error_message = str(e)
            success = False
            buffer.rollback()

        return ExtractResult(
            stream_id=stream.id,
            success=success,
            records_extracted=records_extracted,
            duration_seconds=(datetime.now() - started_at).total_seconds(),
            started_at=started_at,
            completed_at=datetime.now() if success else None,
            error_message=error_message,
        )

    def _extract_records(
        self, data: Any, data_path: str
    ) -> list[dict[str, Any]]:
        """Extract records array from response data using JSON path.

        Args:
            data: Response data (dict or list)
            data_path: Dot-notation path to records array

        Returns:
            List of record dicts
        """
        if data is None:
            return []

        # If no path specified, assume data is the records array or single record
        if not data_path:
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []

        # Navigate to data path
        current = data
        for part in data_path.split("."):
            if current is None:
                return []
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    return []
            else:
                return []

        if isinstance(current, list):
            return current
        elif isinstance(current, dict):
            return [current]
        return []

    def _map_record(
        self, record: dict[str, Any], fields: Optional[list[Field]]
    ) -> dict[str, Any]:
        """Map source record to output record based on field definitions.

        Args:
            record: Source record from API
            fields: Optional field definitions with source mappings

        Returns:
            Mapped record dict
        """
        if not fields:
            return record

        result = {}
        for field in fields:
            # Use source expression to get value, or field id if no source
            source_key = field.source_expression
            value = self._extract_value(record, source_key)
            result[field.id] = value

        return result

    def _extract_value(self, record: dict[str, Any], path: str) -> Any:
        """Extract value from record using dot notation path.

        Supports nested paths like "user.address.city" and array access
        like "items.0.name".

        Args:
            record: Source record
            path: Dot-notation path to value

        Returns:
            Value at path or None
        """
        if not path:
            return None

        parts = path.split(".")
        current = record

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

    def build_query(
        self, stream: Stream, watermark_value: Optional[Any] = None
    ) -> Optional[str]:
        """Build query - not applicable for REST APIs.

        REST APIs use endpoints and params, not SQL queries.

        Returns:
            None (not applicable)
        """
        return None

    def get_schema(self) -> list[Field]:
        """Get schema by sampling the API endpoint.

        For REST APIs, schema is typically inferred from response data.

        Returns:
            List of inferred fields
        """
        # Schema inference would require making a request and analyzing response
        # This is a basic implementation that returns empty
        return []

    def infer_schema_from_records(
        self, records: list[dict[str, Any]]
    ) -> list[Field]:
        """Infer schema from a sample of records.

        Args:
            records: Sample records to analyze

        Returns:
            List of inferred Field objects
        """
        if not records:
            return []

        # Collect all keys and their types
        field_types: dict[str, set[type]] = {}
        for record in records:
            self._collect_types(record, "", field_types)

        # Convert to Field objects
        fields = []
        for field_id, types in sorted(field_types.items()):
            dtype = self._infer_dxt_type(types)
            nullable = type(None) in types
            fields.append(Field(id=field_id, dtype=dtype, nullable=nullable))

        return fields

    def _collect_types(
        self,
        data: Any,
        prefix: str,
        field_types: dict[str, set[type]],
    ) -> None:
        """Recursively collect field types from nested data.

        Args:
            data: Data to analyze
            prefix: Current path prefix
            field_types: Dict to accumulate field types
        """
        if isinstance(data, dict):
            for key, value in data.items():
                path = f"{prefix}.{key}" if prefix else key
                if isinstance(value, dict):
                    # For nested objects, record as JSON type
                    field_types.setdefault(path, set()).add(dict)
                elif isinstance(value, list):
                    # For arrays, record as ARRAY type
                    field_types.setdefault(path, set()).add(list)
                else:
                    field_types.setdefault(path, set()).add(type(value))

    def _infer_dxt_type(self, types: set[type]) -> DXTType:
        """Infer DXTType from observed Python types.

        Args:
            types: Set of observed Python types

        Returns:
            Inferred DXTType
        """
        # Remove None from consideration for type inference
        non_null_types = types - {type(None)}

        if not non_null_types:
            return DXTType.STRING

        if non_null_types == {int}:
            return DXTType.INT64
        elif non_null_types == {float} or non_null_types == {int, float}:
            return DXTType.FLOAT64
        elif non_null_types == {bool}:
            return DXTType.BOOL
        elif non_null_types == {str}:
            return DXTType.STRING
        elif dict in non_null_types:
            return DXTType.JSON
        elif list in non_null_types:
            return DXTType.ARRAY
        else:
            return DXTType.STRING
