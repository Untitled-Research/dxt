"""Tests for SQLite operators."""

import pytest
from pathlib import Path
import tempfile

from dxt.buffers.memory import MemoryBuffer
from dxt.models.field import DXTType, Field
from dxt.models.source import SourceConfig
from dxt.models.stream import ExtractConfig, LoadConfig, Stream
from dxt.operators.sqlite import SQLiteConnector, SQLiteExtractor, SQLiteLoader


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def sqlite_connector(temp_db):
    """Create a SQLite connector for testing."""
    config = {"database": temp_db}
    connector = SQLiteConnector(config)
    connector.connect()
    yield connector
    connector.disconnect()


@pytest.fixture
def sample_table(sqlite_connector):
    """Create a sample table with test data."""
    create_sql = """
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer_name TEXT NOT NULL,
        order_date TEXT,
        amount REAL,
        is_paid INTEGER
    )
    """
    sqlite_connector.execute_statement(create_sql)

    # Insert sample data
    insert_sql = """
    INSERT INTO orders (order_id, customer_name, order_date, amount, is_paid)
    VALUES
        (1, 'Alice', '2024-01-15', 100.50, 1),
        (2, 'Bob', '2024-01-16', 250.75, 1),
        (3, 'Charlie', '2024-01-17', 75.25, 0)
    """
    sqlite_connector.execute_statement(insert_sql)

    return "orders"


class TestSQLiteConnector:
    """Test SQLite connector."""

    def test_connection(self, sqlite_connector):
        """Test basic connection."""
        assert sqlite_connector.is_connected
        assert sqlite_connector.test_connection()

    def test_table_exists(self, sqlite_connector, sample_table):
        """Test table existence check."""
        assert sqlite_connector.table_exists(sample_table)
        assert not sqlite_connector.table_exists("nonexistent_table")

    def test_get_schema(self, sqlite_connector, sample_table):
        """Test schema introspection."""
        fields = sqlite_connector.get_schema(sample_table)
        assert len(fields) == 5

        field_names = [f.id for f in fields]
        assert "order_id" in field_names
        assert "customer_name" in field_names
        assert "amount" in field_names

    def test_execute_query(self, sqlite_connector, sample_table):
        """Test query execution."""
        results = sqlite_connector.execute_query("SELECT * FROM orders ORDER BY order_id")
        assert len(results) == 3
        assert results[0]["customer_name"] == "Alice"
        assert results[1]["amount"] == 250.75


class TestSQLiteExtractor:
    """Test SQLite extractor."""

    def test_extract_full(self, sqlite_connector, sample_table):
        """Test full extraction."""
        # Create stream config
        stream = Stream(
            id="test_stream",
            source=SourceConfig(type="relation", value="orders"),
            target=SourceConfig(type="relation", value="orders_target"),
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        # Create extractor and buffer
        extractor = SQLiteExtractor(sqlite_connector)
        # Get fields from source for buffer
        source_fields = sqlite_connector.get_schema("orders")
        buffer = MemoryBuffer(source_fields)

        # Extract
        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 3
        # Read records back from buffer
        all_records = []
        for batch in buffer.read_batches(100):
            all_records.extend(batch)
        assert len(all_records) == 3

    def test_extract_with_fields(self, sqlite_connector, sample_table):
        """Test extraction with specific fields."""
        # Define fields
        fields = [
            Field(id="order_id", dtype=DXTType.INT64),
            Field(id="customer_name", dtype=DXTType.STRING),
        ]

        stream = Stream(
            id="test_stream",
            source=SourceConfig(type="relation", value="orders"),
            target=SourceConfig(type="relation", value="orders_target"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        extractor = SQLiteExtractor(sqlite_connector)
        buffer = MemoryBuffer(fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 3
        # Check only specified fields are present
        all_records = []
        for batch in buffer.read_batches(100):
            all_records.extend(batch)
        first_record = all_records[0]
        assert "order_id" in first_record
        assert "customer_name" in first_record

    def test_extract_with_criteria(self, sqlite_connector, sample_table):
        """Test extraction with WHERE criteria."""
        stream = Stream(
            id="test_stream",
            source=SourceConfig(type="relation", value="orders"),
            target=SourceConfig(type="relation", value="orders_target"),
            criteria={"where": "amount > 100"},
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        extractor = SQLiteExtractor(sqlite_connector)
        source_fields = sqlite_connector.get_schema("orders")
        buffer = MemoryBuffer(source_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        # Alice (100.50 > 100) and Bob (250.75 > 100) match the criteria
        assert result.records_extracted == 2
        all_records = []
        for batch in buffer.read_batches(100):
            all_records.extend(batch)
        assert len(all_records) == 2
        names = [r["customer_name"] for r in all_records]
        assert "Alice" in names
        assert "Bob" in names

    def test_extract_sql_query(self, sqlite_connector, sample_table):
        """Test extraction with direct SQL query."""
        stream = Stream(
            id="test_stream",
            source=SourceConfig(
                type="sql",
                value="SELECT customer_name, amount FROM orders WHERE is_paid = 1",
            ),
            target=SourceConfig(type="relation", value="orders_target"),
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        extractor = SQLiteExtractor(sqlite_connector)
        # For SQL queries, we need to define fields manually or use generic ones
        query_fields = [
            Field(id="customer_name", dtype=DXTType.STRING),
            Field(id="amount", dtype=DXTType.FLOAT64),
        ]
        buffer = MemoryBuffer(query_fields)

        result = extractor.extract(stream, buffer)

        assert result.success
        assert result.records_extracted == 2  # Alice and Bob paid


class TestSQLiteLoader:
    """Test SQLite loader."""

    def test_load_append_create_table(self, sqlite_connector):
        """Test loading with automatic table creation."""
        # Define fields for table creation
        fields = [
            Field(id="id", dtype=DXTType.INT64, nullable=False),
            Field(id="name", dtype=DXTType.STRING),
            Field(id="score", dtype=DXTType.FLOAT64),
        ]

        # Create buffer with data
        buffer = MemoryBuffer(fields)
        buffer.write_batch(
            [
                {"id": 1, "name": "Alice", "score": 95.5},
                {"id": 2, "name": "Bob", "score": 87.0},
            ]
        )
        buffer.finalize()
        buffer.commit()

        stream = Stream(
            id="test_stream",
            source=SourceConfig(type="relation", value="source"),
            target=SourceConfig(type="relation", value="students"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="append", batch_size=10),
        )

        # Load
        loader = SQLiteLoader(sqlite_connector)
        result = loader.load(stream, buffer)

        assert result.success
        assert result.records_loaded == 2
        assert result.records_inserted == 2

        # Verify data
        rows = sqlite_connector.execute_query("SELECT * FROM students ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"

    def test_load_replace(self, sqlite_connector, sample_table):
        """Test replace mode."""
        fields = [
            Field(id="order_id", dtype=DXTType.INT64),
            Field(id="customer_name", dtype=DXTType.STRING),
            Field(id="order_date", dtype=DXTType.STRING),
            Field(id="amount", dtype=DXTType.FLOAT64),
            Field(id="is_paid", dtype=DXTType.INT64),
        ]

        # Create buffer with new data
        buffer = MemoryBuffer(fields)
        buffer.write_batch(
            [
                {"order_id": 10, "customer_name": "Dave", "order_date": "2024-02-01", "amount": 500.0, "is_paid": 1},
            ]
        )
        buffer.finalize()
        buffer.commit()

        stream = Stream(
            id="test_stream",
            source=SourceConfig(type="relation", value="source"),
            target=SourceConfig(type="relation", value="orders"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="replace", batch_size=10),
        )

        loader = SQLiteLoader(sqlite_connector)
        result = loader.load(stream, buffer)

        assert result.success
        assert result.records_loaded == 1
        assert result.records_deleted == 3  # Original records deleted

        # Verify only new data exists
        rows = sqlite_connector.execute_query("SELECT * FROM orders")
        assert len(rows) == 1
        assert rows[0]["customer_name"] == "Dave"

    def test_load_upsert(self, sqlite_connector):
        """Test upsert mode."""
        # Create table with primary key
        create_sql = """
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL
        )
        """
        sqlite_connector.execute_statement(create_sql)

        # Insert initial data
        sqlite_connector.execute_statement(
            "INSERT INTO products VALUES (1, 'Widget', 10.0), (2, 'Gadget', 20.0)"
        )

        fields = [
            Field(id="product_id", dtype=DXTType.INT64),
            Field(id="name", dtype=DXTType.STRING),
            Field(id="price", dtype=DXTType.FLOAT64),
        ]

        # Create buffer with updates and new records
        buffer = MemoryBuffer(fields)
        buffer.write_batch(
            [
                {"product_id": 1, "name": "Widget", "price": 12.0},  # Update
                {"product_id": 3, "name": "Doohickey", "price": 30.0},  # Insert
            ]
        )
        buffer.finalize()
        buffer.commit()

        stream = Stream(
            id="test_stream",
            source=SourceConfig(type="relation", value="source"),
            target=SourceConfig(type="relation", value="products"),
            fields=fields,
            extract=ExtractConfig(mode="full", batch_size=10),
            load=LoadConfig(mode="upsert", batch_size=10, upsert_keys=["product_id"]),
        )

        loader = SQLiteLoader(sqlite_connector)
        result = loader.load(stream, buffer)

        assert result.success
        assert result.records_loaded == 2

        # Verify data
        rows = sqlite_connector.execute_query("SELECT * FROM products ORDER BY product_id")
        assert len(rows) == 3
        assert rows[0]["price"] == 12.0  # Updated
        assert rows[2]["name"] == "Doohickey"  # Inserted
