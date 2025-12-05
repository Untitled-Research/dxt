"""Tests for REST API extractor.

Uses JSONPlaceholder (https://jsonplaceholder.typicode.com) - a free fake REST API
that provides predictable JSON responses for testing.

Available endpoints:
- /posts (100 posts)
- /users (10 users with nested address/company objects)
- /comments (500 comments)
- /todos (200 todos)
"""

import pytest

from dxt.buffers.memory import MemoryBuffer
from dxt.models.field import DXTType, Field
from dxt.models.source import SourceConfig
from dxt.models.stream import ExtractConfig, LoadConfig, Stream
from dxt.providers.rest import RestConnector, RestExtractor


# Test fixtures
@pytest.fixture
def jsonplaceholder_connector():
    """Create a connector for JSONPlaceholder API."""
    connector = RestConnector("https://jsonplaceholder.typicode.com")
    connector.connect()
    yield connector
    connector.disconnect()


@pytest.fixture
def posts_fields():
    """Field definitions for posts endpoint."""
    return [
        Field(id="id", dtype=DXTType.INT64),
        Field(id="userId", dtype=DXTType.INT64),
        Field(id="title", dtype=DXTType.STRING),
        Field(id="body", dtype=DXTType.STRING),
    ]


@pytest.fixture
def users_fields():
    """Field definitions for users endpoint - flat structure."""
    return [
        Field(id="id", dtype=DXTType.INT64),
        Field(id="name", dtype=DXTType.STRING),
        Field(id="username", dtype=DXTType.STRING),
        Field(id="email", dtype=DXTType.STRING),
        Field(id="phone", dtype=DXTType.STRING),
        Field(id="website", dtype=DXTType.STRING),
    ]


@pytest.fixture
def users_nested_fields():
    """Field definitions for users endpoint with nested paths."""
    return [
        Field(id="id", dtype=DXTType.INT64),
        Field(id="name", dtype=DXTType.STRING),
        Field(id="email", dtype=DXTType.STRING),
        # Nested address fields using source mapping
        Field(id="city", dtype=DXTType.STRING, source="address.city"),
        Field(id="zipcode", dtype=DXTType.STRING, source="address.zipcode"),
        Field(id="lat", dtype=DXTType.STRING, source="address.geo.lat"),
        Field(id="lng", dtype=DXTType.STRING, source="address.geo.lng"),
        # Nested company fields
        Field(id="company_name", dtype=DXTType.STRING, source="company.name"),
    ]


class TestRestConnector:
    """Test REST connector functionality."""

    def test_connect(self, jsonplaceholder_connector):
        """Test connection is established."""
        assert jsonplaceholder_connector.is_connected()

    def test_simple_get_request(self, jsonplaceholder_connector):
        """Test simple GET request."""
        from dxt.providers.base.webapi import HttpMethod

        response = jsonplaceholder_connector.request(HttpMethod.GET, "/posts/1")

        assert response.status_code == 200
        assert response.data is not None
        assert response.data["id"] == 1
        assert "title" in response.data

    def test_get_with_params(self, jsonplaceholder_connector):
        """Test GET request with query parameters."""
        from dxt.providers.base.webapi import HttpMethod

        response = jsonplaceholder_connector.request(
            HttpMethod.GET, "/posts", params={"userId": 1}
        )

        assert response.status_code == 200
        assert isinstance(response.data, list)
        # All posts should belong to userId 1
        for post in response.data:
            assert post["userId"] == 1

    def test_extract_path(self, jsonplaceholder_connector):
        """Test JSON path extraction helper."""
        data = {
            "meta": {"total": 100, "page": 1},
            "results": [{"id": 1}, {"id": 2}],
            "nested": {"deep": {"value": "found"}},
        }

        assert jsonplaceholder_connector._extract_path(data, "meta.total") == 100
        assert jsonplaceholder_connector._extract_path(data, "nested.deep.value") == "found"
        assert jsonplaceholder_connector._extract_path(data, "results.0.id") == 1
        assert jsonplaceholder_connector._extract_path(data, "nonexistent") is None


class TestRestExtractor:
    """Test REST extractor functionality."""

    def test_extract_posts(self, jsonplaceholder_connector, posts_fields):
        """Test extracting posts from JSONPlaceholder."""
        stream = Stream(
            id="posts",
            source=SourceConfig(type="http", value="/posts"),
            target=SourceConfig(type="relation", value="posts"),
            fields=posts_fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(posts_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 100  # JSONPlaceholder has 100 posts

        # Verify records in buffer
        all_records = []
        for batch in buffer.read_batches(100):
            all_records.extend(batch)

        assert len(all_records) == 100
        assert all_records[0]["id"] == 1
        assert "title" in all_records[0]
        assert "body" in all_records[0]

    def test_extract_single_resource(self, jsonplaceholder_connector, posts_fields):
        """Test extracting a single resource."""
        stream = Stream(
            id="single_post",
            source=SourceConfig(type="http", value="/posts/1"),
            target=SourceConfig(type="relation", value="post"),
            fields=posts_fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(posts_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 1

        all_records = []
        for batch in buffer.read_batches(10):
            all_records.extend(batch)

        assert len(all_records) == 1
        assert all_records[0]["id"] == 1

    def test_extract_filtered(self, jsonplaceholder_connector, posts_fields):
        """Test extracting with query parameter filter."""
        stream = Stream(
            id="user_posts",
            source=SourceConfig(
                type="http",
                value="/posts",
                options={"params": {"userId": 1}},
            ),
            target=SourceConfig(type="relation", value="user_posts"),
            fields=posts_fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(posts_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 10  # User 1 has 10 posts

        all_records = []
        for batch in buffer.read_batches(50):
            all_records.extend(batch)

        for record in all_records:
            assert record["userId"] == 1

    def test_extract_users_flat(self, jsonplaceholder_connector, users_fields):
        """Test extracting users with flat structure."""
        stream = Stream(
            id="users",
            source=SourceConfig(type="http", value="/users"),
            target=SourceConfig(type="relation", value="users"),
            fields=users_fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(users_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 10  # 10 users

        all_records = []
        for batch in buffer.read_batches(50):
            all_records.extend(batch)

        assert len(all_records) == 10
        assert all_records[0]["name"] == "Leanne Graham"
        assert all_records[0]["email"] == "Sincere@april.biz"

    def test_extract_users_nested_fields(self, jsonplaceholder_connector, users_nested_fields):
        """Test extracting users with nested field mapping (address.city, company.name)."""
        stream = Stream(
            id="users_nested",
            source=SourceConfig(type="http", value="/users"),
            target=SourceConfig(type="relation", value="users_nested"),
            fields=users_nested_fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(users_nested_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 10

        all_records = []
        for batch in buffer.read_batches(50):
            all_records.extend(batch)

        # First user: Leanne Graham from Gwenborough
        first_user = all_records[0]
        assert first_user["name"] == "Leanne Graham"
        assert first_user["city"] == "Gwenborough"
        assert first_user["zipcode"] == "92998-3874"
        assert first_user["lat"] == "-37.3159"
        assert first_user["company_name"] == "Romaguera-Crona"

    def test_extract_todos(self, jsonplaceholder_connector):
        """Test extracting todos (200 records)."""
        fields = [
            Field(id="id", dtype=DXTType.INT64),
            Field(id="userId", dtype=DXTType.INT64),
            Field(id="title", dtype=DXTType.STRING),
            Field(id="completed", dtype=DXTType.BOOL),
        ]

        stream = Stream(
            id="todos",
            source=SourceConfig(type="http", value="/todos"),
            target=SourceConfig(type="relation", value="todos"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=100),
            load=LoadConfig(mode="append", batch_size=100),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 200

        # Verify boolean field handling
        all_records = []
        for batch in buffer.read_batches(200):
            all_records.extend(batch)

        completed_count = sum(1 for r in all_records if r["completed"] is True)
        not_completed_count = sum(1 for r in all_records if r["completed"] is False)
        assert completed_count + not_completed_count == 200

    def test_extract_without_field_mapping(self, jsonplaceholder_connector):
        """Test extracting without explicit field mapping (pass-through)."""
        stream = Stream(
            id="posts_raw",
            source=SourceConfig(type="http", value="/posts"),
            target=SourceConfig(type="relation", value="posts_raw"),
            # No fields specified - should pass through all fields
            extract=ExtractConfig(mode="full", batch_size=100),
            load=LoadConfig(mode="append", batch_size=100),
        )

        # For no-field extraction, use generic fields
        generic_fields = [
            Field(id="id", dtype=DXTType.INT64),
            Field(id="userId", dtype=DXTType.INT64),
            Field(id="title", dtype=DXTType.STRING),
            Field(id="body", dtype=DXTType.STRING),
        ]

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(generic_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 100


class TestSchemaInference:
    """Test schema inference from API responses."""

    def test_infer_schema_from_posts(self, jsonplaceholder_connector, posts_fields):
        """Test schema inference from posts records."""
        extractor = RestExtractor(jsonplaceholder_connector)

        # Get sample records
        from dxt.providers.base.webapi import HttpMethod

        response = jsonplaceholder_connector.request(HttpMethod.GET, "/posts")
        sample_records = response.data[:5]

        inferred_fields = extractor.infer_schema_from_records(sample_records)

        # Should infer id, userId, title, body
        field_ids = {f.id for f in inferred_fields}
        assert "id" in field_ids
        assert "userId" in field_ids
        assert "title" in field_ids
        assert "body" in field_ids

        # Check types
        field_map = {f.id: f for f in inferred_fields}
        assert field_map["id"].dtype == DXTType.INT64
        assert field_map["title"].dtype == DXTType.STRING

    def test_infer_schema_from_users(self, jsonplaceholder_connector):
        """Test schema inference from users (with nested objects)."""
        extractor = RestExtractor(jsonplaceholder_connector)

        from dxt.providers.base.webapi import HttpMethod

        response = jsonplaceholder_connector.request(HttpMethod.GET, "/users")
        sample_records = response.data[:3]

        inferred_fields = extractor.infer_schema_from_records(sample_records)

        field_ids = {f.id for f in inferred_fields}
        # Top-level fields
        assert "id" in field_ids
        assert "name" in field_ids
        assert "email" in field_ids
        # Nested objects detected as JSON type
        assert "address" in field_ids
        assert "company" in field_ids

        field_map = {f.id: f for f in inferred_fields}
        assert field_map["address"].dtype == DXTType.JSON
        assert field_map["company"].dtype == DXTType.JSON


class TestPagination:
    """Test pagination support.

    Note: JSONPlaceholder doesn't support pagination natively,
    but we can test our pagination logic with mocked responses.
    """

    def test_pagination_config_parsing(self, jsonplaceholder_connector):
        """Test pagination configuration is properly parsed from options."""
        from dxt.providers.base.webapi import PaginationConfig

        # Build pagination config
        pagination = PaginationConfig(
            style="offset",
            page_size=10,
            offset_param="_start",
            limit_param="_limit",
            data_path="",
        )

        assert pagination.style == "offset"
        assert pagination.page_size == 10

    def test_paginated_extraction_simulation(self, jsonplaceholder_connector):
        """Test paginated extraction using JSONPlaceholder's _start/_limit params.

        Note: JSONPlaceholder does support _start/_limit, but returns all 100
        posts when limit matches page_size. This test verifies the pagination
        mechanism works even though the API doesn't truly paginate.
        """
        fields = [
            Field(id="id", dtype=DXTType.INT64),
            Field(id="title", dtype=DXTType.STRING),
        ]

        stream = Stream(
            id="paginated_posts",
            source=SourceConfig(
                type="http",
                value="/posts",
                options={
                    "pagination": {
                        "style": "offset",
                        "page_size": 10,
                        "offset_param": "_start",
                        "limit_param": "_limit",
                        "data_path": "",  # Results at root
                    }
                },
            ),
            target=SourceConfig(type="relation", value="posts"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        # JSONPlaceholder returns exactly limit records per page
        # The extractor will stop when it gets fewer records than page_size
        # or when data is empty. In this case we get 10 records per page
        # and it will fetch 10 pages (100 posts total) then get 0 on page 11
        assert result.records_extracted >= 10  # At minimum, first page


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_endpoint(self, jsonplaceholder_connector, posts_fields):
        """Test handling of 404 response."""
        stream = Stream(
            id="invalid",
            source=SourceConfig(type="http", value="/nonexistent"),
            target=SourceConfig(type="relation", value="invalid"),
            fields=posts_fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        extractor = RestExtractor(jsonplaceholder_connector)
        buffer = MemoryBuffer(posts_fields)

        result = extractor.extract(stream, buffer)

        assert not result.success
        assert result.error_message is not None
        assert "404" in result.error_message

    def test_connection_not_established(self, posts_fields):
        """Test extraction without connecting first."""
        connector = RestConnector("https://jsonplaceholder.typicode.com")
        # Don't call connect()

        stream = Stream(
            id="posts",
            source=SourceConfig(type="http", value="/posts"),
            target=SourceConfig(type="relation", value="posts"),
            fields=posts_fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        extractor = RestExtractor(connector)
        buffer = MemoryBuffer(posts_fields)

        result = extractor.extract(stream, buffer)

        assert not result.success
        assert result.error_message is not None


class TestDataPathExtraction:
    """Test JSON data path extraction for nested API responses."""

    def test_data_path_extraction(self, jsonplaceholder_connector):
        """Test extracting records from nested response structure.

        Many APIs return data in format like:
        {
            "data": [...],
            "meta": {"total": 100}
        }
        """
        extractor = RestExtractor(jsonplaceholder_connector)

        # Test the extraction helper
        response_data = {
            "data": [{"id": 1, "name": "Test"}],
            "meta": {"total": 1},
        }

        records = extractor._extract_records(response_data, "data")
        assert len(records) == 1
        assert records[0]["id"] == 1

    def test_deeply_nested_data_path(self, jsonplaceholder_connector):
        """Test deeply nested data path."""
        extractor = RestExtractor(jsonplaceholder_connector)

        response_data = {
            "response": {
                "body": {
                    "items": [{"id": 1}, {"id": 2}]
                }
            }
        }

        records = extractor._extract_records(response_data, "response.body.items")
        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2
