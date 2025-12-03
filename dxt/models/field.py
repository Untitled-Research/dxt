"""Field and type system models for DXT.

This module defines the Field model and DXT's internal type system,
which provides a consistent representation of data types across different
source and target systems.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel, Field as PydanticField, field_validator


class DXTType(str, Enum):
    """DXT internal data types.

    These types form the common type system used throughout DXT for data
    representation in buffers and type mapping between systems.

    Inspired by Apache Arrow/Parquet type system.
    """

    # Numeric types
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    DECIMAL = "decimal"  # Arbitrary precision

    # String
    STRING = "string"

    # Boolean
    BOOL = "bool"

    # Temporal
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"

    # Binary
    BINARY = "binary"

    # Complex
    JSON = "json"

    # Collections (for NoSQL support in future)
    ARRAY = "array"
    OBJECT = "object"


class SourceField(BaseModel):
    """Source-side field configuration.

    Used when a field requires special handling during extraction,
    such as SQL expressions or source-specific type information.
    """

    expression: str = PydanticField(
        ...,
        description="Column name or SQL expression (e.g., 'YEAR(order_date)')",
    )
    dtype: Optional[str] = PydanticField(
        None,
        description="Source system's native type (e.g., 'INTEGER', 'VARCHAR(255)')",
    )

    model_config = {"extra": "forbid"}


class TargetField(BaseModel):
    """Target-side field configuration.

    Used to specify target column names and types that differ from
    the internal field representation.
    """

    name: str = PydanticField(
        ...,
        description="Target column/field name",
    )
    dtype: Optional[str] = PydanticField(
        None,
        description="Target system's native type (e.g., 'BIGINT', 'timestamp_ntz(0)')",
    )
    nullable: Optional[bool] = PydanticField(
        None,
        description="Override field-level nullable if specified",
    )

    model_config = {"extra": "forbid"}


class Field(BaseModel):
    """Field metadata for a stream.

    Represents a single field (column) in a data stream with its type,
    nullability, and optional source/target mappings.

    The field model supports progressive complexity:
    - Simple: Just id and dtype
    - Intermediate: Add source/target as strings for renaming
    - Advanced: Full SourceField/TargetField objects with expressions

    Examples:
        Simple field with just ID and type:
        >>> Field(id="customer_id", dtype=DXTType.INT64)

        Field with source rename:
        >>> Field(id="order_date", dtype=DXTType.TIMESTAMP, source="created_at")

        Field with full configuration:
        >>> Field(
        ...     id="order_year",
        ...     dtype=DXTType.INT64,
        ...     source=SourceField(expression="YEAR(order_date)", dtype="INTEGER"),
        ...     target=TargetField(name="year", dtype="INT")
        ... )
    """

    id: str = PydanticField(
        ...,
        description="Internal field identifier (immutable, used in buffer and logs)",
    )

    dtype: Union[DXTType, str] = PydanticField(
        ...,
        description="DXT internal type (e.g., 'int64', 'timestamp', 'string')",
    )

    source: Optional[Union[str, SourceField]] = PydanticField(
        None,
        description="Source mapping: string (column name) or SourceField object. "
        "Defaults to 'id' if not specified.",
    )

    target: Optional[Union[str, TargetField]] = PydanticField(
        None,
        description="Target mapping: string (column name) or TargetField object. "
        "Defaults to 'id' if not specified.",
    )

    nullable: bool = PydanticField(
        True,
        description="Whether the field can contain null values",
    )

    precision: Optional[int] = PydanticField(
        None,
        description="Precision for DECIMAL types",
    )

    scale: Optional[int] = PydanticField(
        None,
        description="Scale for DECIMAL types",
    )

    metadata: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional metadata (e.g., generated_by, description)",
    )

    model_config = {"extra": "forbid"}

    @field_validator("dtype")
    @classmethod
    def validate_dtype(cls, v: Union[DXTType, str]) -> Union[DXTType, str]:
        """Validate dtype is a valid DXTType."""
        if isinstance(v, str):
            try:
                return DXTType(v)
            except ValueError:
                raise ValueError(
                    f"Invalid dtype: {v}. Must be one of: {', '.join(t.value for t in DXTType)}"
                )
        return v

    @property
    def source_expression(self) -> str:
        """Get the source expression or column name.

        Returns:
            The source SQL expression/column name, or self.id if not specified.
        """
        if self.source is None:
            return self.id
        if isinstance(self.source, str):
            return self.source
        return self.source.expression

    @property
    def target_name(self) -> str:
        """Get the target column name.

        Returns:
            The target column name, or self.id if not specified.
        """
        if self.target is None:
            return self.id
        if isinstance(self.target, str):
            return self.target
        return self.target.name

    @property
    def target_dtype(self) -> Optional[str]:
        """Get the target system's native type hint, if specified.

        Returns:
            Target system type or None if not specified.
        """
        if self.target is None or isinstance(self.target, str):
            return None
        return self.target.dtype

    @property
    def target_nullable(self) -> bool:
        """Get the target nullable setting.

        Returns:
            Target-specific nullable setting, or field-level nullable if not overridden.
        """
        if self.target is not None and isinstance(self.target, TargetField):
            if self.target.nullable is not None:
                return self.target.nullable
        return self.nullable
