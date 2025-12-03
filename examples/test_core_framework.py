"""Test script to demonstrate the core framework implementation.

This script validates that all core components can be instantiated
and demonstrates the basic API.
"""

from datetime import datetime
from decimal import Decimal

from dxt import (
    DXTType,
    Field,
    MemoryBuffer,
    Pipeline,
    Stream,
)


def test_field_models():
    """Test Field model creation and properties."""
    print("Testing Field models...")

    # Simple field
    field1 = Field(id="customer_id", dtype=DXTType.INT64)
    assert field1.id == "customer_id"
    assert field1.dtype == DXTType.INT64
    assert field1.source_expression == "customer_id"
    assert field1.target_name == "customer_id"

    # Field with source rename
    field2 = Field(id="order_date", dtype=DXTType.TIMESTAMP, source="created_at")
    assert field2.source_expression == "created_at"
    assert field2.target_name == "order_date"

    # Decimal field
    field3 = Field(id="amount", dtype=DXTType.DECIMAL, precision=10, scale=2)
    assert field3.precision == 10
    assert field3.scale == 2

    print("  ✓ Field models working correctly")


def test_memory_buffer():
    """Test MemoryBuffer write/read cycle."""
    print("\nTesting MemoryBuffer...")

    # Define schema
    fields = [
        Field(id="id", dtype=DXTType.INT64, nullable=False),
        Field(id="name", dtype=DXTType.STRING),
        Field(id="amount", dtype=DXTType.DECIMAL, precision=10, scale=2),
        Field(id="created_at", dtype=DXTType.TIMESTAMP),
    ]

    # Create buffer
    buffer = MemoryBuffer(fields)

    # Write data
    records = [
        {
            "id": 1,
            "name": "Alice",
            "amount": Decimal("99.95"),
            "created_at": datetime(2025, 1, 1, 12, 0),
        },
        {
            "id": 2,
            "name": "Bob",
            "amount": Decimal("149.50"),
            "created_at": datetime(2025, 1, 2, 14, 30),
        },
    ]

    buffer.write_batch(records)
    buffer.finalize()
    buffer.commit()

    # Read data
    read_count = 0
    for batch in buffer.read_batches(batch_size=10):
        read_count += len(batch)
        for record in batch:
            assert "id" in record
            assert "name" in record
            assert isinstance(record["amount"], Decimal)
            assert isinstance(record["created_at"], datetime)

    assert read_count == 2
    assert buffer.record_count == 2

    # Cleanup
    buffer.cleanup()

    print(f"  ✓ MemoryBuffer: wrote and read {read_count} records")


def test_stream_configuration():
    """Test Stream model and configuration."""
    print("\nTesting Stream configuration...")

    # Simple stream
    stream = Stream(
        id="orders",
        source="public.orders",
        target="analytics.orders",
        kind="table",
        tags=["critical", "daily"],
    )

    assert stream.id == "orders"
    assert stream.source == "public.orders"
    assert stream.target == "analytics.orders"
    assert "critical" in stream.tags

    # Incremental stream
    from dxt import ExtractConfig, LoadConfig

    stream2 = Stream(
        id="customers",
        source="customers",
        target="customers_target",
        extract=ExtractConfig(
            mode="incremental", watermark_field="updated_at", watermark_type="timestamp"
        ),
        load=LoadConfig(mode="upsert", upsert_keys=["customer_id"]),
    )

    assert stream2.is_incremental
    assert stream2.extract.watermark_field == "updated_at"
    assert stream2.requires_upsert_keys
    assert stream2.load.upsert_keys == ["customer_id"]

    print("  ✓ Stream configuration working correctly")


def test_pipeline_configuration():
    """Test Pipeline model."""
    print("\nTesting Pipeline configuration...")

    from dxt import ConnectionConfig

    pipeline = Pipeline(
        name="test_pipeline",
        version=1,
        source=ConnectionConfig(connection="source_db"),
        target=ConnectionConfig(connection="target_db"),
        streams=[
            Stream(id="orders", source="orders", target="orders"),
            Stream(id="customers", source="customers", target="customers"),
        ],
    )

    assert pipeline.name == "test_pipeline"
    assert pipeline.stream_count == 2
    assert "orders" in pipeline.stream_ids
    assert "customers" in pipeline.stream_ids

    # Test stream lookup
    orders_stream = pipeline.get_stream_by_id("orders")
    assert orders_stream is not None
    assert orders_stream.id == "orders"

    print("  ✓ Pipeline configuration working correctly")


def main():
    """Run all tests."""
    print("=" * 60)
    print("DXT Core Framework Test Suite")
    print("=" * 60)

    test_field_models()
    test_memory_buffer()
    test_stream_configuration()
    test_pipeline_configuration()

    print("\n" + "=" * 60)
    print("✅ All tests passed! Core framework is operational.")
    print("=" * 60)


if __name__ == "__main__":
    main()
