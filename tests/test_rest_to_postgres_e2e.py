"""End-to-end tests: REST API to PostgreSQL using YAML pipeline.

Tests the full pipeline run flow:
1. Load pipeline from YAML file
2. Run pipeline using PipelineRunner
3. Verify data in PostgreSQL

Uses JSONPlaceholder API and local PostgreSQL instance.

Environment variables:
- PGHOST: PostgreSQL host (default: postgres)
- PGPORT: PostgreSQL port (default: 5432)
- PGDATABASE: Database name (default: postgres)
- PGUSER: Username (default: postgres)
- PGPASSWORD: Password (required)
"""

import os
from pathlib import Path

import pytest

from dxt.core.pipeline_runner import PipelineRunner
from dxt.models.pipeline import Pipeline
from dxt.providers.postgres import PostgresConnector


# Test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


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
    """Create PostgreSQL connector for verification."""
    connector = PostgresConnector(get_pg_config())
    connector.connect()
    yield connector
    connector.disconnect()


@pytest.fixture
def clean_test_tables(pg_connector):
    """Clean up test tables before and after tests."""
    tables = [
        "test_api_posts",
        "test_api_users",
        "test_api_todos",
    ]
    for table in tables:
        pg_connector.execute_statement(f"DROP TABLE IF EXISTS {table} CASCADE")

    yield

    # Cleanup after test
    for table in tables:
        pg_connector.execute_statement(f"DROP TABLE IF EXISTS {table} CASCADE")


class TestYamlPipelineExecution:
    """Test full pipeline execution from YAML configuration."""

    def test_execute_rest_to_postgres_pipeline(self, pg_connector, clean_test_tables):
        """Test executing the full REST to PostgreSQL pipeline."""
        # Load pipeline from YAML
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        assert pipeline.name == "rest_to_postgres"
        assert len(pipeline.streams) == 3

        # Run pipeline
        runner = PipelineRunner()
        result = runner.run(pipeline)

        # Check overall result
        assert result.success, f"Pipeline failed: {result.error_message}"
        assert result.streams_processed == 3
        assert result.streams_succeeded == 3
        assert result.streams_failed == 0

        # Verify posts table
        posts_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_posts"
        )[0]["cnt"]
        assert posts_count == 100  # JSONPlaceholder has 100 posts

        # Verify posts data
        post = pg_connector.execute_query(
            "SELECT * FROM test_api_posts WHERE id = 1"
        )[0]
        assert post["user_id"] == 1
        assert post["title"] is not None

        # Verify users table
        users_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_users"
        )[0]["cnt"]
        assert users_count == 10  # JSONPlaceholder has 10 users

        # Verify nested field extraction worked
        user = pg_connector.execute_query(
            "SELECT * FROM test_api_users WHERE id = 1"
        )[0]
        assert user["name"] == "Leanne Graham"
        assert user["city"] == "Gwenborough"
        assert user["zipcode"] == "92998-3874"
        assert user["lat"] == "-37.3159"
        assert user["company_name"] == "Romaguera-Crona"

        # Verify todos table (filtered to user 1)
        todos_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_todos"
        )[0]["cnt"]
        assert todos_count == 20  # User 1 has 20 todos

        # Verify all todos belong to user 1
        user_ids = pg_connector.execute_query(
            "SELECT DISTINCT user_id FROM test_api_todos"
        )
        assert len(user_ids) == 1
        assert user_ids[0]["user_id"] == 1

        # Verify boolean field
        completed_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_todos WHERE completed = true"
        )[0]["cnt"]
        assert completed_count > 0  # Some todos should be completed

    def test_execute_single_stream(self, pg_connector, clean_test_tables):
        """Test executing a single stream from the pipeline."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        # Run only the users stream
        runner = PipelineRunner()
        result = runner.run(pipeline, select="api_users")

        assert result.success
        assert result.streams_processed == 1
        assert result.streams_succeeded == 1

        # Verify users table exists and has data
        users_count = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_users"
        )[0]["cnt"]
        assert users_count == 10

        # Verify other tables don't exist (weren't created)
        with pytest.raises(Exception):  # Table doesn't exist
            pg_connector.execute_query("SELECT COUNT(*) FROM test_api_posts")

    def test_dry_run(self, pg_connector, clean_test_tables):
        """Test dry run mode (validate without executing)."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        # Run in dry run mode
        runner = PipelineRunner()
        result = runner.run(pipeline, dry_run=True)

        assert result.success
        assert result.metadata.get("dry_run") is True
        assert result.total_records_transferred == 0

        # Verify no tables were created
        with pytest.raises(Exception):
            pg_connector.execute_query("SELECT COUNT(*) FROM test_api_posts")

    def test_pipeline_idempotency(self, pg_connector, clean_test_tables):
        """Test that running pipeline twice produces same results (replace mode)."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        runner = PipelineRunner()

        # First run
        result1 = runner.run(pipeline)
        assert result1.success

        count1 = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_posts"
        )[0]["cnt"]

        # Second run (should replace, not append)
        result2 = runner.run(pipeline)
        assert result2.success

        count2 = pg_connector.execute_query(
            "SELECT COUNT(*) as cnt FROM test_api_posts"
        )[0]["cnt"]

        # Count should be same (replaced, not appended)
        assert count1 == count2 == 100


class TestStreamResults:
    """Test stream-level results from pipeline execution."""

    def test_stream_results_available(self, pg_connector, clean_test_tables):
        """Test that stream-level results are available."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        runner = PipelineRunner()
        result = runner.run(pipeline)

        assert result.success
        assert len(result.stream_results) == 3

        # Check each stream result
        stream_ids = {sr.stream_id for sr in result.stream_results}
        assert stream_ids == {"api_posts", "api_users", "api_todos"}

        # Verify extract and load results for posts
        posts_result = next(
            sr for sr in result.stream_results if sr.stream_id == "api_posts"
        )
        assert posts_result.success
        assert posts_result.extract_result.records_extracted == 100
        assert posts_result.load_result.records_loaded == 100

    def test_total_records_transferred(self, pg_connector, clean_test_tables):
        """Test that total records transferred is calculated correctly."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        runner = PipelineRunner()
        result = runner.run(pipeline)

        assert result.success
        # 100 posts + 10 users + 20 todos (filtered) = 130 records
        assert result.total_records_transferred == 130


class TestYamlConfiguration:
    """Test YAML configuration parsing and validation."""

    def test_pipeline_structure(self):
        """Test that pipeline YAML is correctly parsed."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        # Check source/target connections
        assert pipeline.source.connection == "https://jsonplaceholder.typicode.com"
        assert "postgresql://" in pipeline.target.connection

        # Check buffer config
        assert pipeline.buffer.format == "memory"
        assert pipeline.buffer.cleanup_on_success is True

        # Check streams
        posts_stream = pipeline.get_stream_by_id("api_posts")
        assert posts_stream is not None
        assert posts_stream.source.type == "http"
        assert posts_stream.source.value == "/posts"
        assert len(posts_stream.fields) == 4

        # Check nested field mapping
        users_stream = pipeline.get_stream_by_id("api_users")
        city_field = next(f for f in users_stream.fields if f.id == "city")
        assert city_field.source_expression == "address.city"

    def test_stream_options(self):
        """Test that stream options (like params) are parsed."""
        pipeline_path = FIXTURES_DIR / "rest_to_postgres.yaml"
        pipeline = Pipeline.load_from_yaml(pipeline_path)

        todos_stream = pipeline.get_stream_by_id("api_todos")
        assert todos_stream.source.options is not None
        assert todos_stream.source.options.get("params", {}).get("userId") == 1
