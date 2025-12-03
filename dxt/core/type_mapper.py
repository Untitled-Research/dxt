"""Base TypeMapper abstract class.

This module defines the TypeMapper interface for converting between
system-specific types and DXT's internal type system.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from dxt.models.field import DXTType


class TypeMapper(ABC):
    """Base class for converting between system types and DXT internal types.

    TypeMappers handle bidirectional type conversion:
    - from_source: Convert source system types to DXT types
    - to_target: Convert DXT types to target system types

    Each operator provides its own TypeMapper implementation with
    system-specific type mappings.

    Examples:
        Using a type mapper:
        >>> mapper = PostgresTypeMapper()
        >>> dxt_type = mapper.from_source("BIGINT")
        >>> print(dxt_type)  # DXTType.INT64
        >>> target_type = mapper.to_target(DXTType.INT64)
        >>> print(target_type)  # "BIGINT"
    """

    @abstractmethod
    def from_source(self, source_type: str) -> DXTType:
        """Convert source system type to DXT internal type.

        Args:
            source_type: Source system's native type (e.g., "INTEGER", "VARCHAR(255)")

        Returns:
            Corresponding DXT internal type

        Raises:
            TypeMappingError: If type cannot be mapped

        Examples:
            PostgreSQL:
                "INTEGER" -> DXTType.INT32
                "BIGINT" -> DXTType.INT64
                "TIMESTAMP" -> DXTType.TIMESTAMP

            Snowflake:
                "NUMBER(38,0)" -> DXTType.INT64
                "VARCHAR" -> DXTType.STRING
                "TIMESTAMP_NTZ" -> DXTType.TIMESTAMP
        """
        pass

    @abstractmethod
    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        """Convert DXT internal type to target system type.

        Args:
            dxt_type: DXT internal type
            target_hint: User-specified target type (from Field.target.dtype)
                        If provided, use this exact type instead of default mapping

        Returns:
            Target system type string

        Raises:
            TypeMappingError: If type cannot be mapped

        Examples:
            With default mapping:
                DXTType.INT64, hint=None -> "BIGINT" (PostgreSQL)
                DXTType.STRING, hint=None -> "TEXT" (PostgreSQL)

            With target hint:
                DXTType.INT64, hint="INTEGER" -> "INTEGER"
                DXTType.TIMESTAMP, hint="timestamp_ntz(0)" -> "timestamp_ntz(0)"
        """
        pass

    def normalize_source_type(self, source_type: str) -> str:
        """Normalize source type string for consistent mapping.

        Removes parameters, converts to lowercase, handles aliases.

        Args:
            source_type: Raw source type string

        Returns:
            Normalized type string

        Examples:
            "VARCHAR(255)" -> "varchar"
            "NUMERIC(10,2)" -> "numeric"
            "INT" -> "integer" (normalize alias)
        """
        # Default implementation: strip parameters and lowercase
        normalized = source_type.lower().strip()

        # Remove parameters like (255) or (10,2)
        if "(" in normalized:
            normalized = normalized.split("(")[0].strip()

        return normalized

    def get_python_type(self, dxt_type: DXTType) -> type:
        """Get Python native type for a DXT type.

        Used by buffers and type conversion logic.

        Args:
            dxt_type: DXT internal type

        Returns:
            Python type (int, str, float, bool, datetime, etc.)

        Examples:
            DXTType.INT64 -> int
            DXTType.STRING -> str
            DXTType.TIMESTAMP -> datetime
            DXTType.DECIMAL -> Decimal
        """
        from datetime import date, datetime, time
        from decimal import Decimal

        type_map: dict[DXTType, type] = {
            DXTType.INT8: int,
            DXTType.INT16: int,
            DXTType.INT32: int,
            DXTType.INT64: int,
            DXTType.UINT8: int,
            DXTType.UINT16: int,
            DXTType.UINT32: int,
            DXTType.UINT64: int,
            DXTType.FLOAT32: float,
            DXTType.FLOAT64: float,
            DXTType.DECIMAL: Decimal,
            DXTType.STRING: str,
            DXTType.BOOL: bool,
            DXTType.DATE: date,
            DXTType.TIME: time,
            DXTType.TIMESTAMP: datetime,
            DXTType.TIMESTAMP_TZ: datetime,
            DXTType.BINARY: bytes,
            DXTType.JSON: dict,  # or str, depending on implementation
            DXTType.ARRAY: list,
            DXTType.OBJECT: dict,
        }

        return type_map.get(dxt_type, str)  # Default to str for unknown types
