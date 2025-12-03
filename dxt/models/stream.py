"""Stream model and configuration classes.

This module defines the core Stream model that represents a dataset to transfer,
along with its extract and load configuration.
"""

from __future__ import annotations

from typing import Any, Optional, Union

from pydantic import BaseModel, Field as PydanticField, field_validator, model_validator

from dxt.models.field import Field
from dxt.models.source import SourceConfig


class ExtractConfig(BaseModel):
    """Extract phase configuration.

    Controls how data is extracted from the source system, including
    extraction mode (full or incremental) and batching behavior.
    """

    mode: str = PydanticField(
        "full",
        description="Extract mode: 'full' or 'incremental'",
    )

    batch_size: int = PydanticField(
        10000,
        description="Number of records to fetch per batch",
        gt=0,
    )

    # Incremental extraction
    watermark_field: Optional[str] = PydanticField(
        None,
        description="Field name for incremental extraction (e.g., 'updated_at')",
    )

    watermark_type: Optional[str] = PydanticField(
        None,
        description="Type of watermark: 'timestamp', 'integer', 'date'",
    )

    # System-specific options (API keys, pagination, file encoding, etc.)
    options: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="System-specific extraction options",
    )

    model_config = {"extra": "forbid"}

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        """Validate extract mode."""
        valid_modes = {"full", "incremental"}
        if v not in valid_modes:
            raise ValueError(f"Invalid extract mode: {v}. Must be one of: {valid_modes}")
        return v

    @field_validator("watermark_type")
    @classmethod
    def validate_watermark_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate watermark type."""
        if v is None:
            return v
        valid_types = {"timestamp", "integer", "date"}
        if v not in valid_types:
            raise ValueError(f"Invalid watermark type: {v}. Must be one of: {valid_types}")
        return v


class LoadConfig(BaseModel):
    """Load phase configuration.

    Controls how data is loaded into the target system, including
    load mode (append, replace, upsert) and batching behavior.
    """

    mode: str = PydanticField(
        "append",
        description="Load mode: 'append', 'replace', or 'upsert'",
    )

    batch_size: int = PydanticField(
        10000,
        description="Number of records to load per batch",
        gt=0,
    )

    # Keys for upsert mode
    upsert_keys: list[str] = PydanticField(
        default_factory=list,
        description="Field names to use as upsert keys (required for upsert mode)",
    )

    # System-specific options (Snowflake stage, BigQuery dataset, etc.)
    options: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="System-specific load options",
    )

    model_config = {"extra": "forbid"}

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        """Validate load mode."""
        valid_modes = {"append", "replace", "upsert"}
        if v not in valid_modes:
            raise ValueError(f"Invalid load mode: {v}. Must be one of: {valid_modes}")
        return v


class Stream(BaseModel):
    """A stream of records to extract and load.

    Represents a dataset to transfer with its source, target, and
    configuration for both extract and load phases.

    The Stream model maps directly to the YAML pipeline structure.
    Source and target can be simple strings (with smart type inference)
    or explicit configuration objects.

    Examples:
        Simple table stream with shorthand:
        >>> Stream(
        ...     id="orders",
        ...     source="public.orders",  # Inferred as relation
        ...     target="analytics.orders"
        ... )

        Explicit SQL query:
        >>> Stream(
        ...     id="active_orders",
        ...     source={"type": "sql", "sql": "SELECT * FROM orders WHERE status='active'"},
        ...     target="analytics.active_orders"
        ... )

        Incremental stream with watermark:
        >>> Stream(
        ...     id="orders",
        ...     source="public.orders",
        ...     target="analytics.orders",
        ...     extract=ExtractConfig(
        ...         mode="incremental",
        ...         watermark_field="updated_at",
        ...         watermark_type="timestamp"
        ...     ),
        ...     load=LoadConfig(mode="upsert", upsert_keys=["order_id"])
        ... )
    """

    # Identity
    id: str = PydanticField(
        ...,
        description="Unique identifier within pipeline",
    )

    source: SourceConfig = PydanticField(
        ...,
        description="Source reference: string or explicit configuration",
    )

    target: SourceConfig = PydanticField(
        ...,
        description="Target reference: string or explicit configuration",
    )

    tags: list[str] = PydanticField(
        default_factory=list,
        description="Tags for selector filtering",
    )

    # Query building (for relation sources with criteria)
    criteria: Optional[dict[str, Any]] = PydanticField(
        None,
        description="Query criteria (e.g., {'where': \"status = 'active'\"})",
    )

    # Schema (optional, can auto-detect)
    fields: Optional[list[Field]] = PydanticField(
        None,
        description="Field definitions (schema). If None, will be auto-detected.",
    )

    # Extract configuration
    extract: ExtractConfig = PydanticField(
        default_factory=ExtractConfig,
        description="Extract phase configuration",
    )

    # Load configuration
    load: LoadConfig = PydanticField(
        default_factory=LoadConfig,
        description="Load phase configuration",
    )

    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    @classmethod
    def normalize_source_target(cls, data: Any) -> Any:
        """Normalize source and target to SourceConfig objects.

        Handles both string shortcuts and explicit configuration dicts.
        """
        if not isinstance(data, dict):
            return data

        # Normalize source
        if "source" in data:
            source = data["source"]
            if isinstance(source, str):
                # String shorthand - infer type
                data["source"] = SourceConfig.from_string(source)
            elif isinstance(source, dict):
                # Explicit config
                data["source"] = SourceConfig.from_dict(source)
            # else: already a SourceConfig, leave as-is

        # Normalize target
        if "target" in data:
            target = data["target"]
            if isinstance(target, str):
                # String shorthand - infer type
                data["target"] = SourceConfig.from_string(target)
            elif isinstance(target, dict):
                # Explicit config
                data["target"] = SourceConfig.from_dict(target)
            # else: already a SourceConfig, leave as-is

        return data

    @property
    def is_incremental(self) -> bool:
        """Check if stream uses incremental extraction.

        Returns:
            True if extract mode is 'incremental', False otherwise.
        """
        return self.extract.mode == "incremental"

    @property
    def requires_upsert_keys(self) -> bool:
        """Check if stream requires upsert keys.

        Returns:
            True if load mode is 'upsert', False otherwise.
        """
        return self.load.mode == "upsert"

    def matches_selector(self, selector: str) -> bool:
        """Check if stream matches a selector pattern.

        Args:
            selector: Selector string (e.g., "orders", "tag:critical", "sales_*")

        Returns:
            True if stream matches selector, False otherwise.

        Note:
            Full selector matching logic will be implemented in core.selector module.
            This is a placeholder for basic ID matching.
        """
        # Basic implementation - will be replaced by full selector logic
        if selector == "all":
            return True
        if selector == self.id:
            return True
        if selector.startswith("tag:"):
            tag = selector[4:]
            return tag in self.tags
        return False
