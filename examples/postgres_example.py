"""End-to-end PostgreSQL example.

This example demonstrates a complete data movement pipeline from
PostgreSQL to PostgreSQL using DXT's core framework.

Prerequisites:
- PostgreSQL running (via devcontainer)
- Database connection available
"""

from decimal import Decimal

from dxt import DXTType, ExtractConfig, Field, LoadConfig, Pipeline, Stream
from dxt.core import PipelineExecutor
from dxt.models import BufferConfig, ConnectionConfig
from dxt.operators.postgres import PostgresConnector


def setup_test_data():
    """Create test database and tables with sample data."""
    print("Setting up test data...")

    config = {
        "connection_string": "postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
    }

    with PostgresConnector(config) as conn:
        # Create source table
        conn.execute_statement("""
            DROP TABLE IF EXISTS source_orders;
            CREATE TABLE source_orders (
                order_id BIGINT PRIMARY KEY,
                customer_name VARCHAR(100),
                amount NUMERIC(10,2),
                status VARCHAR(20),
                created_at TIMESTAMP
            );
        """)

        # Insert test data
        conn.execute_statement("""
            INSERT INTO source_orders (order_id, customer_name, amount, status, created_at)
            VALUES
                (1, 'Alice', 99.95, 'completed', '2025-01-01 12:00:00'),
                (2, 'Bob', 149.50, 'completed', '2025-01-02 14:30:00'),
                (3, 'Charlie', 75.00, 'pending', '2025-01-03 10:15:00'),
                (4, 'Diana', 299.99, 'completed', '2025-01-04 16:45:00'),
                (5, 'Eve', 50.25, 'cancelled', '2025-01-05 09:00:00');
        """)

        # Drop target table if exists
        conn.execute_statement("DROP TABLE IF EXISTS target_orders;")

    print("  ✓ Test data created")


def example_full_load():
    """Example 1: Full load (replace mode)."""
    print("\n" + "=" * 60)
    print("Example 1: Full Load (Replace Mode)")
    print("=" * 60)

    # Define pipeline
    pipeline = Pipeline(
        name="postgres_full_load",
        version=1,
        source=ConnectionConfig(
            connection="postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
        ),
        target=ConnectionConfig(
            connection="postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
        ),
        buffer=BufferConfig(format="memory"),
        streams=[
            Stream(
                id="orders",
                source="source_orders",
                target="target_orders",
                kind="table",
                fields=[
                    Field(id="order_id", dtype=DXTType.INT64, nullable=False),
                    Field(id="customer_name", dtype=DXTType.STRING),
                    Field(id="amount", dtype=DXTType.DECIMAL, precision=10, scale=2),
                    Field(id="status", dtype=DXTType.STRING),
                    Field(id="created_at", dtype=DXTType.TIMESTAMP),
                ],
                load=LoadConfig(mode="replace"),
            )
        ],
    )

    # Execute pipeline
    executor = PipelineExecutor()
    result = executor.execute(pipeline)

    # Display results
    print(f"\nPipeline: {result.pipeline_name}")
    print(f"Success: {result.success}")
    print(f"Streams processed: {result.streams_processed}")
    print(f"Records transferred: {result.total_records_transferred}")
    print(f"Duration: {result.duration_seconds:.2f}s")

    if result.stream_results:
        for stream_result in result.stream_results:
            print(f"\nStream: {stream_result.stream_id}")
            if stream_result.extract_result:
                print(f"  Extracted: {stream_result.extract_result.records_extracted} records")
            if stream_result.load_result:
                print(f"  Loaded: {stream_result.load_result.records_loaded} records")

    return result


def example_filtered_load():
    """Example 2: Filtered load with criteria."""
    print("\n" + "=" * 60)
    print("Example 2: Filtered Load with Criteria")
    print("=" * 60)

    # Define pipeline with criteria
    pipeline = Pipeline(
        name="postgres_filtered_load",
        version=1,
        source=ConnectionConfig(
            connection="postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
        ),
        target=ConnectionConfig(
            connection="postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
        ),
        buffer=BufferConfig(format="parquet"),
        streams=[
            Stream(
                id="completed_orders",
                source="source_orders",
                target="target_completed_orders",
                kind="table",
                criteria={"where": "status = 'completed'"},
                fields=[
                    Field(id="order_id", dtype=DXTType.INT64, nullable=False),
                    Field(id="customer_name", dtype=DXTType.STRING),
                    Field(id="amount", dtype=DXTType.DECIMAL, precision=10, scale=2),
                    Field(id="created_at", dtype=DXTType.TIMESTAMP),
                ],
                load=LoadConfig(mode="replace"),
            )
        ],
    )

    # Execute pipeline
    executor = PipelineExecutor()
    result = executor.execute(pipeline)

    # Display results
    print(f"\nPipeline: {result.pipeline_name}")
    print(f"Success: {result.success}")
    print(f"Records transferred: {result.total_records_transferred}")
    print("  (Only 'completed' status orders)")

    return result


def example_upsert_load():
    """Example 3: Upsert load mode."""
    print("\n" + "=" * 60)
    print("Example 3: Upsert Load Mode")
    print("=" * 60)

    # First, do an initial load
    pipeline = Pipeline(
        name="postgres_upsert_load",
        version=1,
        source=ConnectionConfig(
            connection="postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
        ),
        target=ConnectionConfig(
            connection="postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
        ),
        buffer=BufferConfig(format="memory"),
        streams=[
            Stream(
                id="orders_upsert",
                source="source_orders",
                target="target_orders_upsert",
                kind="table",
                fields=[
                    Field(id="order_id", dtype=DXTType.INT64, nullable=False),
                    Field(id="customer_name", dtype=DXTType.STRING),
                    Field(id="amount", dtype=DXTType.DECIMAL, precision=10, scale=2),
                    Field(id="status", dtype=DXTType.STRING),
                ],
                load=LoadConfig(mode="upsert", upsert_keys=["order_id"]),
            )
        ],
    )

    executor = PipelineExecutor()
    result = executor.execute(pipeline)

    print(f"\nInitial upsert: {result.total_records_transferred} records")

    # Now update some data and upsert again
    config = {
        "connection_string": "postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
    }

    with PostgresConnector(config) as conn:
        conn.execute_statement("""
            UPDATE source_orders
            SET status = 'shipped', amount = amount + 10
            WHERE order_id IN (1, 2);
        """)

    result2 = executor.execute(pipeline)
    print(f"Second upsert: {result2.total_records_transferred} records (updates existing)")

    return result2


def verify_results():
    """Verify the loaded data."""
    print("\n" + "=" * 60)
    print("Verifying Results")
    print("=" * 60)

    config = {
        "connection_string": "postgresql+psycopg://postgres:postgres@postgres:5432/postgres"
    }

    with PostgresConnector(config) as conn:
        # Check target_orders
        result = conn.execute_query("SELECT COUNT(*) as cnt FROM target_orders")
        print(f"\ntarget_orders count: {result[0]['cnt']}")

        # Check target_completed_orders
        try:
            result = conn.execute_query("SELECT COUNT(*) as cnt FROM target_completed_orders")
            print(f"target_completed_orders count: {result[0]['cnt']}")
        except Exception:
            pass

        # Check target_orders_upsert
        try:
            result = conn.execute_query("SELECT COUNT(*) as cnt FROM target_orders_upsert")
            print(f"target_orders_upsert count: {result[0]['cnt']}")

            # Show some sample data
            result = conn.execute_query(
                "SELECT * FROM target_orders_upsert ORDER BY order_id LIMIT 3"
            )
            print("\nSample data from target_orders_upsert:")
            for row in result:
                print(f"  Order {row['order_id']}: {row['customer_name']} - ${row['amount']} - {row['status']}")
        except Exception:
            pass


def main():
    """Run all examples."""
    print("=" * 60)
    print("DXT PostgreSQL End-to-End Examples")
    print("=" * 60)

    try:
        # Setup
        setup_test_data()

        # Run examples
        example_full_load()
        example_filtered_load()
        example_upsert_load()

        # Verify
        verify_results()

        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
