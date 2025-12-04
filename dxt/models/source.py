"""Source and target reference models.

This module defines the SourceConfig model that represents how to reference
a data source or target in a flexible, operator-agnostic way.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field as PydanticField, field_validator


class SourceConfig(BaseModel):
    """Internal representation of a source or target reference.

    The YAML can use ergonomic field names (sql, relation, file, etc.),
    but internally we normalize to type + value for consistent handling.

    Examples:
        From YAML:
        >>> # {type: "sql", sql: "SELECT * FROM orders"}
        >>> SourceConfig.from_dict({"type": "sql", "sql": "SELECT * FROM orders"})
        SourceConfig(type='sql', value='SELECT * FROM orders', options={})

        >>> # {type: "function", function: "get_sales", params: {...}}
        >>> SourceConfig.from_dict({
        ...     "type": "function",
        ...     "function": "get_sales",
        ...     "params": {"start_date": "2024-01-01"}
        ... })
        SourceConfig(type='function', value={'function': 'get_sales', 'params': {...}}, options={})
    """

    type: str = PydanticField(
        ...,
        description="Source type (e.g., 'relation', 'sql', 'function', 'file', 'http')",
    )

    value: Any = PydanticField(
        ...,
        description="The actual reference - could be string, dict, or other structure",
    )

    options: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional options for this source",
    )

    # Operator specification (full module paths)
    connector: Optional[str] = PydanticField(
        None,
        description="Connector operator override (full module path, e.g., 'dxt.operators.postgres.connector.PostgresConnector')",
    )

    extractor: Optional[str] = PydanticField(
        None,
        description="Extractor operator override (full module path, e.g., 'dxt.operators.sql.extractor.SQLExtractor')",
    )

    loader: Optional[str] = PydanticField(
        None,
        description="Loader operator override (full module path, e.g., 'dxt.operators.postgres.copy_loader.PostgresCopyLoader')",
    )

    # Operator configuration
    connector_config: Optional[dict[str, Any]] = PydanticField(
        None,
        description="Configuration passed to connector __init__",
    )

    extractor_config: Optional[dict[str, Any]] = PydanticField(
        None,
        description="Configuration passed to extractor __init__",
    )

    loader_config: Optional[dict[str, Any]] = PydanticField(
        None,
        description="Configuration passed to loader __init__",
    )

    model_config = {"extra": "forbid"}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceConfig:
        """Parse YAML dict into normalized internal format.

        Handles ergonomic YAML format where field name matches type:
        - {type: "sql", sql: "SELECT..."}  -> {type: "sql", value: "SELECT..."}
        - {type: "relation", relation: "public.orders"} -> {type: "relation", value: "public.orders"}
        - {type: "function", function: "get_data", params: {...}} -> {type: "function", value: {...}}

        Args:
            data: Dictionary from YAML

        Returns:
            Normalized SourceConfig

        Raises:
            ValueError: If type field is missing or structure is invalid
        """
        if "type" not in data:
            raise ValueError("Source config must have 'type' field")

        source_type = data["type"]
        options = data.get("options", {})

        # Extract the value based on type
        # Look for field matching type name first (most common pattern)
        if source_type in data:
            # Simple case: type field matches value field
            # {type: "sql", sql: "SELECT..."}
            value = data[source_type]

        else:
            # Complex case: collect all non-type, non-options fields as value
            # {type: "function", function: "...", params: {...}}
            value_fields = {
                k: v for k, v in data.items() if k not in ["type", "options"]
            }

            # If only one field, extract its value directly
            if len(value_fields) == 1:
                value = list(value_fields.values())[0]
            else:
                # Multiple fields, keep as dict
                value = value_fields

        return cls(type=source_type, value=value, options=options)

    @classmethod
    def from_string(cls, ref: str, inferred_type: Optional[str] = None) -> SourceConfig:
        """Create SourceConfig from a string reference with type inference.

        Args:
            ref: String reference (e.g., "public.orders", "SELECT * FROM ...", "/path/to/file.csv")
            inferred_type: Optional explicit type, otherwise will be inferred

        Returns:
            SourceConfig with inferred or specified type
        """
        if inferred_type:
            # Explicit type provided
            return cls(type=inferred_type, value=ref, options={})

        # Infer type from string content
        ref_upper = ref.strip().upper()

        # SQL query detection
        if ref_upper.startswith(("SELECT ", "WITH ", "(")):
            return cls(type="sql", value=ref, options={})

        # URL detection
        if ref.startswith(("http://", "https://")):
            return cls(type="http", value=ref, options={})

        # File path detection (has / or file extension)
        if "/" in ref or ref.startswith(("s3://", "gs://", "azure://")) or any(
            ref.endswith(ext) for ext in [".csv", ".parquet", ".json", ".jsonl", ".txt"]
        ):
            return cls(type="file", value=ref, options={})

        # Default to relation (database table/view)
        return cls(type="relation", value=ref, options={})

    def to_dict(self) -> dict[str, Any]:
        """Convert back to ergonomic YAML format.

        Returns:
            Dictionary suitable for YAML output
        """
        result: dict[str, Any] = {"type": self.type}

        # Put value under type-specific field name
        if isinstance(self.value, (str, int, float, bool)):
            result[self.type] = self.value
        elif isinstance(self.value, dict):
            # Expand dict fields at top level
            result.update(self.value)
        else:
            result[self.type] = self.value

        if self.options:
            result["options"] = self.options

        return result

    def __str__(self) -> str:
        """String representation for display."""
        if isinstance(self.value, str):
            return f"{self.type}:{self.value}"
        return f"{self.type}:{self.value}"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"SourceConfig(type={self.type!r}, value={self.value!r}, options={self.options!r})"
