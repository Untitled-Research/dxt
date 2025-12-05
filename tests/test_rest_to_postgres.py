"""Integration tests: REST API to PostgreSQL.

Tests the full pipeline: REST API extraction → Buffer → PostgreSQL loading.
Uses JSONPlaceholder API and local PostgreSQL instance.

Requirements:
- PostgreSQL running (configured via environment variables)
- Internet access to jsonplaceholder.typicode.com

Environment variables:
- PGHOST: PostgreSQL host (default: postgres)
- PGPORT: PostgreSQL port (default: 5432)
- PGDATABASE: Database name (default: postgres)
- PGUSER: Username (default: postgres)
- PGPASSWORD: Password (required)
"""

import os

import pytest

from dxt.buffers.memory import MemoryBuffer
from dxt.models.field import DXTType, Field
from dxt.models.source import SourceConfig
from dxt.models.stream import ExtractConfig, LoadConfig, Stream
from dxt.providers.rest import RestConnector, RestExtractor
from dxt.providers.postgres import PostgresConnector, PostgresLoader


def get_pg_config() -> dict:
    """Get PostgreSQL config from environment variables.

    Note: Uses 'dxt' as the test database to avoid conflicts with
    the PGDATABASE env var which may be set to a different value.
    """
    return {
        "host": os.environ.get("PGHOST", "postgres"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "database": "dxt",  # Use dedicated test database
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
    }


@pytest.fixture
def pg_connector():
    """Create PostgreSQL connector."""
    connector = PostgresConnector(get_pg_config())
    connector.connect()
    yield connector
    connector.disconnect()


@pytest.fixture
def rest_connector():
    """Create REST connector for JSONPlaceholder."""
    connector = RestConnector("https://jsonplaceholder.typicode.com")
    connector.connect()
    yield connector
    connector.disconnect()


@pytest.fixture
def clean_test_tables(pg_connector):
    """Clean up test tables before and after tests."""
    tables = [
        "api_posts",
        "api_users",
        "api_users_nested",
        "api_todos",
    ]
    for table in tables:
        pg_connector.execute_statement(f"DROP TABLE IF EXISTS {table} CASCADE")

    yield

    # Cleanup after test
    for table in tables:
        pg_connector.execute_statement(f"DROP TABLE IF EXISTS {table} CASCADE")


class TestRestToPostgresSimple:
    """Simple REST to PostgreSQL integration tests."""

    def test_posts_to_postgres(self, rest_connector, pg_connector, clean_test_tables):
        """Test extracting posts from API and loading to PostgreSQL."""
        # Define fields
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="user_id", dtype=DXTType.INT64, source="userId"),
            Field(id="title", dtype=DXTType.STRING),
            Field(id="body", dtype=DXTType.STRING),
        ]

        # Create stream for extraction
        extract_stream = Stream(
            id="posts",
            source=SourceConfig(type="http", value="/posts"),
            target=SourceConfig(type="relation", value="api_posts"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        # Extract from API
        extractor = RestExtractor(rest_connector)
        buffer = MemoryBuffer(fields)
        extract_result = extractor.extract(extract_stream, buffer)

        assert extract_result.success
        assert extract_result.records_extracted == 100

        # Load to PostgreSQL
        load_stream = Stream(
            id="posts",
            source=SourceConfig(type="relation", value="source"),
            target=SourceConfig(type="relation", value="api_posts"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        loader = PostgresLoader(pg_connector)
        load_result = loader.load(load_stream, buffer)

        assert load_result.success
        assert load_result.records_loaded == 100
        assert load_result.records_inserted == 100

        # Verify data in PostgreSQL
        rows = pg_connector.execute_query("SELECT COUNT(*) as cnt FROM api_posts")
        assert rows[0]["cnt"] == 100

        # Check data integrity
        row = pg_connector.execute_query("SELECT * FROM api_posts WHERE id = 1")[0]
        assert row["user_id"] == 1
        assert "title" in row and row["title"] is not None

    def test_todos_to_postgres(self, rest_connector, pg_connector, clean_test_tables):
        """Test extracting todos (with boolean field) to PostgreSQL."""
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="user_id", dtype=DXTType.INT64, source="userId"),
            Field(id="title", dtype=DXTType.STRING),
            Field(id="completed", dtype=DXTType.BOOL),
        ]

        stream = Stream(
            id="todos",
            source=SourceConfig(type="http", value="/todos"),
            target=SourceConfig(type="relation", value="api_todos"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=100),
            load=LoadConfig(mode="append", batch_size=100),
        )

        # Extract
        extractor = RestExtractor(rest_connector)
        buffer = MemoryBuffer(fields)
        extract_result = extractor.extract(stream, buffer)

        assert extract_result.success
        assert extract_result.records_extracted == 200

        # Load
        loader = PostgresLoader(pg_connector)
        load_result = loader.load(stream, buffer)

        assert load_result.success
        assert load_result.records_loaded == 200

        # Verify boolean values
        completed_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM api_todos WHERE completed = true"
        )[0]["cnt"]
        not_completed_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM api_todos WHERE completed = false"
        )[0]["cnt"]

        assert completed_count + not_completed_count == 200
        assert completed_count > 0  # Should have some completed todos
        assert not_completed_count > 0  # Should have some incomplete todos


class TestRestToPostgresNested:
    """Tests for nested JSON data to PostgreSQL."""

    def test_users_nested_to_flat_columns(
        self, rest_connector, pg_connector, clean_test_tables
    ):
        """Test extracting nested user data and flattening to columns."""
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="name", dtype=DXTType.STRING),
            Field(id="username", dtype=DXTType.STRING),
            Field(id="email", dtype=DXTType.STRING),
            Field(id="phone", dtype=DXTType.STRING),
            # Nested address fields
            Field(id="street", dtype=DXTType.STRING, source="address.street"),
            Field(id="city", dtype=DXTType.STRING, source="address.city"),
            Field(id="zipcode", dtype=DXTType.STRING, source="address.zipcode"),
            # Deeply nested geo fields
            Field(id="lat", dtype=DXTType.STRING, source="address.geo.lat"),
            Field(id="lng", dtype=DXTType.STRING, source="address.geo.lng"),
            # Company fields
            Field(id="company_name", dtype=DXTType.STRING, source="company.name"),
            Field(id="company_catchphrase", dtype=DXTType.STRING, source="company.catchPhrase"),
        ]

        stream = Stream(
            id="users_nested",
            source=SourceConfig(type="http", value="/users"),
            target=SourceConfig(type="relation", value="api_users_nested"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        # Extract
        extractor = RestExtractor(rest_connector)
        buffer = MemoryBuffer(fields)
        extract_result = extractor.extract(stream, buffer)

        assert extract_result.success
        assert extract_result.records_extracted == 10

        # Load
        loader = PostgresLoader(pg_connector)
        load_result = loader.load(stream, buffer)

        assert load_result.success
        assert load_result.records_loaded == 10

        # Verify nested data was properly flattened
        row = pg_connector.execute_query(
            "SELECT * FROM api_users_nested WHERE id = 1"
        )[0]

        assert row["name"] == "Leanne Graham"
        assert row["city"] == "Gwenborough"
        assert row["zipcode"] == "92998-3874"
        assert row["lat"] == "-37.3159"
        assert row["lng"] == "81.1496"
        assert row["company_name"] == "Romaguera-Crona"

    @pytest.mark.skip(
        reason="JSON column support requires dict->JSON serialization in loader. "
               "See docs/design/json-handling.md for planned implementation."
    )
    def test_users_with_json_column(
        self, rest_connector, pg_connector, clean_test_tables
    ):
        """Test storing nested objects as JSONB columns.

        TODO: This test is skipped until JSON serialization is implemented
        in the PostgresLoader. Currently psycopg2 doesn't auto-serialize
        Python dicts to JSONB. Need to add json.dumps() for JSON-typed fields.
        """
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="name", dtype=DXTType.STRING),
            Field(id="email", dtype=DXTType.STRING),
            # Store entire nested objects as JSON
            Field(id="address", dtype=DXTType.JSON, source="address"),
            Field(id="company", dtype=DXTType.JSON, source="company"),
        ]

        stream = Stream(
            id="users_json",
            source=SourceConfig(type="http", value="/users"),
            target=SourceConfig(type="relation", value="api_users"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        # Extract
        extractor = RestExtractor(rest_connector)
        buffer = MemoryBuffer(fields)
        extract_result = extractor.extract(stream, buffer)

        assert extract_result.success

        # Load
        loader = PostgresLoader(pg_connector)
        load_result = loader.load(stream, buffer)

        assert load_result.success
        assert load_result.records_loaded == 10

        # Verify JSON columns contain nested data
        # PostgreSQL should store these as JSONB
        row = pg_connector.execute_query(
            "SELECT id, name, address, company FROM api_users WHERE id = 1"
        )[0]

        assert row["name"] == "Leanne Graham"

        # The address should be a dict (JSONB parsed by psycopg)
        address = row["address"]
        if isinstance(address, dict):
            assert address.get("city") == "Gwenborough"
            assert address.get("geo", {}).get("lat") == "-37.3159"


class TestReplaceMode:
    """Test replace (truncate + insert) mode."""

    def test_replace_existing_data(
        self, rest_connector, pg_connector, clean_test_tables
    ):
        """Test that replace mode truncates existing data."""
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="user_id", dtype=DXTType.INT64, source="userId"),
            Field(id="title", dtype=DXTType.STRING),
        ]

        stream = Stream(
            id="posts",
            source=SourceConfig(type="http", value="/posts"),
            target=SourceConfig(type="relation", value="api_posts"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="replace", batch_size=50),
        )

        # First load
        extractor = RestExtractor(rest_connector)
        buffer1 = MemoryBuffer(fields)
        extractor.extract(stream, buffer1)

        loader = PostgresLoader(pg_connector)
        result1 = loader.load(stream, buffer1)
        assert result1.success
        assert result1.records_loaded == 100

        # Second load (should replace, not append)
        buffer2 = MemoryBuffer(fields)
        extractor.extract(stream, buffer2)

        result2 = loader.load(stream, buffer2)
        assert result2.success
        assert result2.records_loaded == 100
        assert result2.records_deleted == 100  # Should have deleted previous 100

        # Verify count is still 100, not 200
        count = pg_connector.execute_query("SELECT COUNT(*) as cnt FROM api_posts")[0][
            "cnt"
        ]
        assert count == 100


class TestFilteredExtraction:
    """Test filtered extraction from API."""

    def test_filtered_posts_to_postgres(
        self, rest_connector, pg_connector, clean_test_tables
    ):
        """Test extracting filtered posts (by userId) to PostgreSQL."""
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="user_id", dtype=DXTType.INT64, source="userId"),
            Field(id="title", dtype=DXTType.STRING),
        ]

        # Filter to only get posts from user 1
        stream = Stream(
            id="user1_posts",
            source=SourceConfig(
                type="http",
                value="/posts",
                options={"params": {"userId": 1}},
            ),
            target=SourceConfig(type="relation", value="api_posts"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=50),
            load=LoadConfig(mode="append", batch_size=50),
        )

        # Extract
        extractor = RestExtractor(rest_connector)
        buffer = MemoryBuffer(fields)
        extract_result = extractor.extract(stream, buffer)

        assert extract_result.success
        assert extract_result.records_extracted == 10  # User 1 has 10 posts

        # Load
        loader = PostgresLoader(pg_connector)
        load_result = loader.load(stream, buffer)

        assert load_result.success
        assert load_result.records_loaded == 10

        # Verify all posts belong to user 1
        rows = pg_connector.execute_query("SELECT DISTINCT user_id FROM api_posts")
        assert len(rows) == 1
        assert rows[0]["user_id"] == 1
