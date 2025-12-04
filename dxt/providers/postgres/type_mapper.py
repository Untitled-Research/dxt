"""PostgreSQL type mapper implementation.

This module provides type conversion between PostgreSQL types
and DXT's internal type system.
"""

from __future__ import annotations

from typing import Optional

from dxt.core.type_mapper import TypeMapper
from dxt.models.field import DXTType


class PostgresTypeMapper(TypeMapper):
    """Type mapper for PostgreSQL.

    Handles conversion between PostgreSQL types and DXT internal types.

    Example:
        >>> mapper = PostgresTypeMapper()
        >>> mapper.from_source("BIGINT")
        <DXTType.INT64>
        >>> mapper.to_target(DXTType.INT64)
        'BIGINT'
    """

    SOURCE_TO_DXT = {
        # Integer types
        "smallint": DXTType.INT16,
        "int2": DXTType.INT16,
        "integer": DXTType.INT32,
        "int": DXTType.INT32,
        "int4": DXTType.INT32,
        "bigint": DXTType.INT64,
        "int8": DXTType.INT64,
        "serial": DXTType.INT32,
        "bigserial": DXTType.INT64,
        # Floating point types
        "real": DXTType.FLOAT32,
        "float4": DXTType.FLOAT32,
        "double precision": DXTType.FLOAT64,
        "float8": DXTType.FLOAT64,
        # Decimal/Numeric
        "numeric": DXTType.DECIMAL,
        "decimal": DXTType.DECIMAL,
        "money": DXTType.DECIMAL,
        # String types
        "character varying": DXTType.STRING,
        "varchar": DXTType.STRING,
        "character": DXTType.STRING,
        "char": DXTType.STRING,
        "text": DXTType.STRING,
        "name": DXTType.STRING,
        "citext": DXTType.STRING,
        # Boolean
        "boolean": DXTType.BOOL,
        "bool": DXTType.BOOL,
        # Date/Time types
        "date": DXTType.DATE,
        "time": DXTType.TIME,
        "time without time zone": DXTType.TIME,
        "timetz": DXTType.TIME,
        "time with time zone": DXTType.TIME,
        "timestamp": DXTType.TIMESTAMP,
        "timestamp without time zone": DXTType.TIMESTAMP,
        "timestamptz": DXTType.TIMESTAMP_TZ,
        "timestamp with time zone": DXTType.TIMESTAMP_TZ,
        "interval": DXTType.STRING,
        # Binary types
        "bytea": DXTType.BINARY,
        # JSON types
        "json": DXTType.JSON,
        "jsonb": DXTType.JSON,
        # UUID
        "uuid": DXTType.STRING,
        # Network types
        "inet": DXTType.STRING,
        "cidr": DXTType.STRING,
        "macaddr": DXTType.STRING,
        # Geometric types (as string)
        "point": DXTType.STRING,
        "line": DXTType.STRING,
        "box": DXTType.STRING,
        "path": DXTType.STRING,
        "polygon": DXTType.STRING,
        "circle": DXTType.STRING,
        # Array (generic)
        "array": DXTType.ARRAY,
        # Range types (as string)
        "int4range": DXTType.STRING,
        "int8range": DXTType.STRING,
        "numrange": DXTType.STRING,
        "tsrange": DXTType.STRING,
        "tstzrange": DXTType.STRING,
        "daterange": DXTType.STRING,
    }

    DXT_TO_TARGET = {
        DXTType.INT8: "SMALLINT",
        DXTType.INT16: "SMALLINT",
        DXTType.INT32: "INTEGER",
        DXTType.INT64: "BIGINT",
        DXTType.UINT8: "SMALLINT",
        DXTType.UINT16: "INTEGER",
        DXTType.UINT32: "BIGINT",
        DXTType.UINT64: "NUMERIC(20,0)",
        DXTType.FLOAT32: "REAL",
        DXTType.FLOAT64: "DOUBLE PRECISION",
        DXTType.DECIMAL: "NUMERIC",
        DXTType.STRING: "TEXT",
        DXTType.BOOL: "BOOLEAN",
        DXTType.DATE: "DATE",
        DXTType.TIME: "TIME",
        DXTType.TIMESTAMP: "TIMESTAMP",
        DXTType.TIMESTAMP_TZ: "TIMESTAMPTZ",
        DXTType.BINARY: "BYTEA",
        DXTType.JSON: "JSONB",
        DXTType.ARRAY: "JSONB",
        DXTType.OBJECT: "JSONB",
    }

    def from_source(self, source_type: str) -> DXTType:
        """Convert PostgreSQL type to DXT internal type.

        Args:
            source_type: PostgreSQL type string

        Returns:
            Corresponding DXT internal type
        """
        normalized = self.normalize_source_type(source_type)

        if normalized in self.SOURCE_TO_DXT:
            return self.SOURCE_TO_DXT[normalized]

        # Handle array types
        if normalized.endswith("[]"):
            return DXTType.ARRAY

        # Default to STRING for unknown types
        return DXTType.STRING

    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        """Convert DXT internal type to PostgreSQL type.

        Args:
            dxt_type: DXT internal type
            target_hint: User-specified PostgreSQL type

        Returns:
            PostgreSQL type string
        """
        if target_hint:
            return target_hint

        if dxt_type in self.DXT_TO_TARGET:
            return self.DXT_TO_TARGET[dxt_type]

        return "TEXT"

    def normalize_source_type(self, source_type: str) -> str:
        """Normalize PostgreSQL type string.

        Args:
            source_type: Raw PostgreSQL type string

        Returns:
            Normalized type string
        """
        normalized = source_type.lower().strip()

        if normalized.endswith("[]"):
            return normalized

        if "(" in normalized:
            base_type = normalized.split("(")[0].strip()

            if base_type in ["time", "timestamp"]:
                if "without time zone" in normalized:
                    return f"{base_type} without time zone"
                elif "with time zone" in normalized:
                    return f"{base_type} with time zone"
                else:
                    return base_type
            else:
                return base_type
        else:
            if "without time zone" in normalized:
                base = normalized.split("without")[0].strip()
                return f"{base} without time zone"
            elif "with time zone" in normalized:
                base = normalized.split("with")[0].strip()
                return f"{base} with time zone"
            else:
                return normalized

    def get_ddl_type(
        self,
        dxt_type: DXTType,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        target_hint: Optional[str] = None,
    ) -> str:
        """Get PostgreSQL DDL type string with precision/scale.

        Args:
            dxt_type: DXT internal type
            precision: Optional precision for DECIMAL types
            scale: Optional scale for DECIMAL types
            target_hint: User-specified type hint

        Returns:
            PostgreSQL DDL type string
        """
        if target_hint:
            return target_hint

        base_type = self.to_target(dxt_type)

        if dxt_type == DXTType.DECIMAL and precision is not None:
            if scale is not None:
                return f"NUMERIC({precision},{scale})"
            else:
                return f"NUMERIC({precision})"

        return base_type
