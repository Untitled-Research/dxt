"""PostgreSQL type mapper implementation.

This module provides type conversion between PostgreSQL types
and DXT's internal type system.
"""

from __future__ import annotations

from typing import Optional

from dxt.core.type_mapper import TypeMapper
from dxt.exceptions import TypeMappingError
from dxt.models.field import DXTType


class PostgresTypeMapper(TypeMapper):
    """Type mapper for PostgreSQL.

    Handles conversion between PostgreSQL types and DXT internal types.

    Examples:
        >>> mapper = PostgresTypeMapper()
        >>> dxt_type = mapper.from_source("BIGINT")
        >>> print(dxt_type)  # DXTType.INT64
        >>> pg_type = mapper.to_target(DXTType.INT64)
        >>> print(pg_type)  # "BIGINT"
    """

    # PostgreSQL type -> DXT type mappings
    SOURCE_TO_DXT = {
        # Integer types
        "smallint": DXTType.INT16,
        "int2": DXTType.INT16,
        "integer": DXTType.INT32,
        "int": DXTType.INT32,
        "int4": DXTType.INT32,
        "bigint": DXTType.INT64,
        "int8": DXTType.INT64,
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
        # Boolean
        "boolean": DXTType.BOOL,
        "bool": DXTType.BOOL,
        # Date/Time types
        "date": DXTType.DATE,
        "time": DXTType.TIME,
        "time without time zone": DXTType.TIME,
        "timestamp": DXTType.TIMESTAMP,
        "timestamp without time zone": DXTType.TIMESTAMP,
        "timestamptz": DXTType.TIMESTAMP_TZ,
        "timestamp with time zone": DXTType.TIMESTAMP_TZ,
        # Binary types
        "bytea": DXTType.BINARY,
        # JSON types
        "json": DXTType.JSON,
        "jsonb": DXTType.JSON,
        # UUID
        "uuid": DXTType.STRING,  # Store UUID as string
        # Array (generic - needs special handling)
        "array": DXTType.ARRAY,
    }

    # DXT type -> PostgreSQL type mappings
    DXT_TO_TARGET = {
        DXTType.INT8: "SMALLINT",  # PostgreSQL doesn't have INT8, use SMALLINT
        DXTType.INT16: "SMALLINT",
        DXTType.INT32: "INTEGER",
        DXTType.INT64: "BIGINT",
        DXTType.UINT8: "SMALLINT",  # PostgreSQL doesn't have unsigned types
        DXTType.UINT16: "INTEGER",
        DXTType.UINT32: "BIGINT",
        DXTType.UINT64: "NUMERIC(20,0)",  # BIGINT can't hold full UINT64 range
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
        DXTType.ARRAY: "JSONB",  # Store arrays as JSONB by default
        DXTType.OBJECT: "JSONB",
    }

    def from_source(self, source_type: str) -> DXTType:
        """Convert PostgreSQL type to DXT internal type.

        Args:
            source_type: PostgreSQL type string (e.g., "BIGINT", "VARCHAR(255)")

        Returns:
            Corresponding DXT internal type

        Raises:
            TypeMappingError: If type cannot be mapped

        Examples:
            >>> mapper = PostgresTypeMapper()
            >>> mapper.from_source("BIGINT")
            <DXTType.INT64: 'int64'>
            >>> mapper.from_source("VARCHAR(255)")
            <DXTType.STRING: 'string'>
            >>> mapper.from_source("TIMESTAMP WITHOUT TIME ZONE")
            <DXTType.TIMESTAMP: 'timestamp'>
        """
        # Normalize the type string
        normalized = self.normalize_source_type(source_type)

        # Try exact match first
        if normalized in self.SOURCE_TO_DXT:
            return self.SOURCE_TO_DXT[normalized]

        # Handle special cases
        # Array types: "integer[]" -> "array"
        if normalized.endswith("[]"):
            return DXTType.ARRAY

        # Default to STRING for unknown types
        return DXTType.STRING

    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        """Convert DXT internal type to PostgreSQL type.

        Args:
            dxt_type: DXT internal type
            target_hint: User-specified PostgreSQL type (from Field.target.dtype)

        Returns:
            PostgreSQL type string

        Raises:
            TypeMappingError: If type cannot be mapped

        Examples:
            >>> mapper = PostgresTypeMapper()
            >>> mapper.to_target(DXTType.INT64)
            'BIGINT'
            >>> mapper.to_target(DXTType.STRING, target_hint="VARCHAR(100)")
            'VARCHAR(100)'
            >>> mapper.to_target(DXTType.DECIMAL)
            'NUMERIC'
        """
        # If user provided explicit type hint, use it
        if target_hint:
            return target_hint

        # Use default mapping
        if dxt_type in self.DXT_TO_TARGET:
            return self.DXT_TO_TARGET[dxt_type]

        # Fallback to TEXT for unknown types
        return "TEXT"

    def normalize_source_type(self, source_type: str) -> str:
        """Normalize PostgreSQL type string.

        Args:
            source_type: Raw PostgreSQL type string

        Returns:
            Normalized type string

        Examples:
            >>> mapper = PostgresTypeMapper()
            >>> mapper.normalize_source_type("VARCHAR(255)")
            'varchar'
            >>> mapper.normalize_source_type("NUMERIC(10,2)")
            'numeric'
            >>> mapper.normalize_source_type("TIMESTAMP WITHOUT TIME ZONE")
            'timestamp without time zone'
        """
        # Convert to lowercase and strip whitespace
        normalized = source_type.lower().strip()

        # Remove array brackets for base type lookup
        if normalized.endswith("[]"):
            return normalized  # Keep [] for array detection

        # Remove parameters like (255) or (10,2), but preserve the rest
        # Special handling for types with keywords
        if "(" in normalized:
            # Extract base type before parentheses
            base_type = normalized.split("(")[0].strip()

            # For types like "timestamp without time zone", preserve the full name
            if base_type in ["time", "timestamp"]:
                # Keep "without time zone" or "with time zone" part
                if "without time zone" in normalized:
                    return f"{base_type} without time zone"
                elif "with time zone" in normalized:
                    return f"{base_type} with time zone"
                else:
                    return base_type
            else:
                return base_type
        else:
            # Check if it's a composite type name
            if "without time zone" in normalized:
                base = normalized.split("without")[0].strip()
                return f"{base} without time zone"
            elif "with time zone" in normalized:
                base = normalized.split("with")[0].strip()
                return f"{base} with time zone"
            else:
                return normalized

    def get_ddl_type(self, dxt_type: DXTType, precision: Optional[int] = None, scale: Optional[int] = None, target_hint: Optional[str] = None) -> str:
        """Get PostgreSQL DDL type string with precision/scale.

        Args:
            dxt_type: DXT internal type
            precision: Optional precision for DECIMAL types
            scale: Optional scale for DECIMAL types
            target_hint: User-specified type hint

        Returns:
            PostgreSQL DDL type string

        Examples:
            >>> mapper = PostgresTypeMapper()
            >>> mapper.get_ddl_type(DXTType.DECIMAL, precision=10, scale=2)
            'NUMERIC(10,2)'
            >>> mapper.get_ddl_type(DXTType.INT64)
            'BIGINT'
        """
        # Use target hint if provided
        if target_hint:
            return target_hint

        # Get base type
        base_type = self.to_target(dxt_type)

        # Add precision/scale for DECIMAL/NUMERIC types
        if dxt_type == DXTType.DECIMAL and precision is not None:
            if scale is not None:
                return f"NUMERIC({precision},{scale})"
            else:
                return f"NUMERIC({precision})"

        return base_type
