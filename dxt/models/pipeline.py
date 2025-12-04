"""Pipeline and connection configuration models.

This module defines the Pipeline model that represents the complete
pipeline configuration, along with connection and buffer settings.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field as PydanticField, field_validator

from dxt.models.stream import Stream


class ExtractDefaults(BaseModel):
    """Pipeline-level extract defaults.

    Specifies default extractor operator and configuration that applies
    to all streams unless overridden at stream level.

    Examples:
        >>> ExtractDefaults(
        ...     extractor="dxt.operators.sql.SQLExtractor",
        ...     batch_size=10000
        ... )
    """

    extractor: Optional[str] = PydanticField(
        None,
        description="Default extractor operator (full module path)",
    )

    batch_size: Optional[int] = PydanticField(
        None,
        description="Default extract batch size",
        gt=0,
    )

    # Allow additional config options
    model_config = {"extra": "allow"}


class LoadDefaults(BaseModel):
    """Pipeline-level load defaults.

    Specifies default loader operator and configuration that applies
    to all streams unless overridden at stream level.

    Examples:
        >>> LoadDefaults(
        ...     loader="dxt.operators.sql.SQLLoader",
        ...     batch_size=5000
        ... )
    """

    loader: Optional[str] = PydanticField(
        None,
        description="Default loader operator (full module path)",
    )

    batch_size: Optional[int] = PydanticField(
        None,
        description="Default load batch size",
        gt=0,
    )

    # Allow additional config options
    model_config = {"extra": "allow"}


class ConnectionConfig(BaseModel):
    """Connection configuration for a data system.

    Can represent either a connection reference (string) or
    an inline connection configuration (dict).
    """

    connection: str = PydanticField(
        ...,
        description="Connection reference (from dxt_project.yaml) or connection string",
    )

    model_config = {"extra": "allow"}  # Allow system-specific options


class BufferConfig(BaseModel):
    """Buffer configuration for data staging.

    Controls buffer format, compression, persistence, and cleanup behavior.
    """

    format: str = PydanticField(
        "parquet",
        description="Buffer format: 'parquet', 'csv', 'jsonl', 'memory'",
    )

    compression: Optional[str] = PydanticField(
        "snappy",
        description="Compression algorithm (for parquet): 'snappy', 'gzip', 'zstd', 'none'",
    )

    persist: bool = PydanticField(
        True,
        description="Whether to persist buffer to disk (enables separate extract/load)",
    )

    location: Optional[str] = PydanticField(
        None,
        description="Directory for buffer files (defaults to ./.dxt/buffer)",
    )

    cleanup_on_success: bool = PydanticField(
        True,
        description="Remove buffer after successful load",
    )

    retain_on_failure: bool = PydanticField(
        True,
        description="Keep buffer for debugging if load fails",
    )

    options: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional buffer options",
    )

    model_config = {"extra": "forbid"}

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        """Validate buffer format."""
        valid_formats = {"parquet", "csv", "jsonl", "memory"}
        if v not in valid_formats:
            raise ValueError(f"Invalid buffer format: {v}. Must be one of: {valid_formats}")
        return v

    @field_validator("compression")
    @classmethod
    def validate_compression(cls, v: Optional[str]) -> Optional[str]:
        """Validate compression algorithm."""
        if v is None or v == "none":
            return None
        valid_compressions = {"snappy", "gzip", "zstd", "brotli", "lz4"}
        if v not in valid_compressions:
            raise ValueError(
                f"Invalid compression: {v}. Must be one of: {valid_compressions} or 'none'"
            )
        return v


class Pipeline(BaseModel):
    """Pipeline configuration.

    Represents a complete data movement pipeline with source, target,
    streams, and buffer configuration.

    Examples:
        Basic pipeline:
        >>> Pipeline(
        ...     version=1,
        ...     name="postgres_to_postgres",
        ...     source=ConnectionConfig(connection="postgres_prod"),
        ...     target=ConnectionConfig(connection="postgres_dev"),
        ...     streams=[
        ...         Stream(id="orders", source="orders", target="orders")
        ...     ]
        ... )
    """

    version: int = PydanticField(
        1,
        description="Pipeline schema version",
    )

    name: str = PydanticField(
        ...,
        description="Pipeline name/identifier",
    )

    description: Optional[str] = PydanticField(
        None,
        description="Human-readable pipeline description",
    )

    source: ConnectionConfig = PydanticField(
        ...,
        description="Source connection configuration",
    )

    target: ConnectionConfig = PydanticField(
        ...,
        description="Target connection configuration",
    )

    extract: Optional[ExtractDefaults] = PydanticField(
        None,
        description="Pipeline-level extract defaults (operator and config)",
    )

    load: Optional[LoadDefaults] = PydanticField(
        None,
        description="Pipeline-level load defaults (operator and config)",
    )

    buffer: BufferConfig = PydanticField(
        default_factory=BufferConfig,
        description="Buffer configuration",
    )

    streams: list[Stream] = PydanticField(
        ...,
        description="List of streams to process",
        min_length=1,
    )

    metadata: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional pipeline metadata",
    )

    model_config = {"extra": "forbid"}

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: int) -> int:
        """Validate pipeline version."""
        if v != 1:
            raise ValueError(f"Unsupported pipeline version: {v}. Only version 1 is supported.")
        return v

    def get_stream_by_id(self, stream_id: str) -> Optional[Stream]:
        """Get a stream by its ID.

        Args:
            stream_id: Stream identifier

        Returns:
            Stream object if found, None otherwise
        """
        for stream in self.streams:
            if stream.id == stream_id:
                return stream
        return None

    def get_streams_by_selector(self, selector: str) -> list[Stream]:
        """Get streams matching a selector.

        Args:
            selector: Selector pattern (e.g., "orders", "tag:critical", "*")

        Returns:
            List of matching streams
        """
        if selector == "all" or selector == "*":
            return self.streams

        matching = []
        for stream in self.streams:
            if stream.matches_selector(selector):
                matching.append(stream)
        return matching

    @property
    def stream_count(self) -> int:
        """Total number of streams in pipeline.

        Returns:
            Number of streams
        """
        return len(self.streams)

    @property
    def stream_ids(self) -> list[str]:
        """Get all stream IDs.

        Returns:
            List of stream identifiers
        """
        return [stream.id for stream in self.streams]
