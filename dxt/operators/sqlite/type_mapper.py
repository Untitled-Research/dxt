"""SQLite type mapper implementation.

This module provides type conversion between SQLite types
and DXT's internal type system.
"""

from __future__ import annotations

from typing import Optional

from dxt.core.type_mapper import TypeMapper
from dxt.exceptions import TypeMappingError
from dxt.models.field import DXTType


class SQLiteTypeMapper(TypeMapper):
    """Type mapper for SQLite.

    Handles conversion between SQLite types and DXT internal types.
    Note: SQLite has dynamic typing with only 5 storage classes:
    INTEGER, REAL, TEXT, BLOB, NULL.

    Examples:
        >>> mapper = SQLiteTypeMapper()
        >>> dxt_type = mapper.from_source("INTEGER")
        >>> print(dxt_type)  # DXTType.INT64
        >>> sqlite_type = mapper.to_target(DXTType.INT64)
        >>> print(sqlite_type)  # "INTEGER"
    """

    # SQLite type -> DXT type mappings
    # SQLite has flexible typing, so we map common type affinities
    SOURCE_TO_DXT = {
        # Integer types (all map to INTEGER storage class)
        "integer": DXTType.INT64,
        "int": DXTType.INT64,
        "tinyint": DXTType.INT8,
        "smallint": DXTType.INT16,
        "mediumint": DXTType.INT32,
        "bigint": DXTType.INT64,
        "unsigned big int": DXTType.UINT64,
        "int2": DXTType.INT16,
        "int8": DXTType.INT64,
        # Real/Float types
        "real": DXTType.FLOAT64,
        "double": DXTType.FLOAT64,
        "double precision": DXTType.FLOAT64,
        "float": DXTType.FLOAT32,
        # Numeric/Decimal (stored as REAL or TEXT)
        "numeric": DXTType.DECIMAL,
        "decimal": DXTType.DECIMAL,
        # String types (all map to TEXT storage class)
        "text": DXTType.STRING,
        "character": DXTType.STRING,
        "varchar": DXTType.STRING,
        "varying character": DXTType.STRING,
        "nchar": DXTType.STRING,
        "native character": DXTType.STRING,
        "nvarchar": DXTType.STRING,
        "clob": DXTType.STRING,
        # Boolean (stored as INTEGER 0/1)
        "boolean": DXTType.BOOL,
        "bool": DXTType.BOOL,
        # Date/Time (stored as TEXT or INTEGER)
        "date": DXTType.DATE,
        "datetime": DXTType.TIMESTAMP,
        "timestamp": DXTType.TIMESTAMP,
        "time": DXTType.TIME,
        # Binary (BLOB storage class)
        "blob": DXTType.BINARY,
        "binary": DXTType.BINARY,
    }

    # DXT type -> SQLite type mappings
    DXT_TO_TARGET = {
        DXTType.INT8: "INTEGER",
        DXTType.INT16: "INTEGER",
        DXTType.INT32: "INTEGER",
        DXTType.INT64: "INTEGER",
        DXTType.UINT8: "INTEGER",
        DXTType.UINT16: "INTEGER",
        DXTType.UINT32: "INTEGER",
        DXTType.UINT64: "INTEGER",
        DXTType.FLOAT32: "REAL",
        DXTType.FLOAT64: "REAL",
        DXTType.DECIMAL: "NUMERIC",  # SQLite NUMERIC affinity
        DXTType.STRING: "TEXT",
        DXTType.BOOL: "INTEGER",  # SQLite stores booleans as 0/1
        DXTType.DATE: "TEXT",  # ISO8601 strings: "YYYY-MM-DD"
        DXTType.TIME: "TEXT",  # ISO8601 strings: "HH:MM:SS.SSS"
        DXTType.TIMESTAMP: "TEXT",  # ISO8601 strings: "YYYY-MM-DD HH:MM:SS.SSS"
        DXTType.TIMESTAMP_TZ: "TEXT",  # ISO8601 strings with timezone
        DXTType.BINARY: "BLOB",
        DXTType.JSON: "TEXT",  # SQLite stores JSON as text
        DXTType.ARRAY: "TEXT",  # Store arrays as JSON text
        DXTType.OBJECT: "TEXT",  # Store objects as JSON text
    }

    def from_source(self, source_type: str) -> DXTType:
        """Convert SQLite type to DXT internal type.

        Args:
            source_type: SQLite type string (e.g., "INTEGER", "TEXT", "REAL")

        Returns:
            Corresponding DXT internal type

        Raises:
            TypeMappingError: If type cannot be mapped

        Examples:
            >>> mapper = SQLiteTypeMapper()
            >>> mapper.from_source("INTEGER")
            <DXTType.INT64: 'int64'>
            >>> mapper.from_source("TEXT")
            <DXTType.STRING: 'string'>
            >>> mapper.from_source("VARCHAR(255)")
            <DXTType.STRING: 'string'>
        """
        # Normalize the type string
        normalized = self.normalize_source_type(source_type)

        # Try exact match first
        if normalized in self.SOURCE_TO_DXT:
            return self.SOURCE_TO_DXT[normalized]

        # SQLite type affinity rules
        # If type contains "INT" -> INTEGER affinity
        if "int" in normalized:
            return DXTType.INT64
        # If type contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity
        if any(keyword in normalized for keyword in ["char", "clob", "text"]):
            return DXTType.STRING
        # If type contains "BLOB" or no type specified -> BLOB affinity
        if "blob" in normalized or normalized == "":
            return DXTType.BINARY
        # If type contains "REAL", "FLOA", or "DOUB" -> REAL affinity
        if any(keyword in normalized for keyword in ["real", "floa", "doub"]):
            return DXTType.FLOAT64

        # Default to STRING for unknown types
        return DXTType.STRING

    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        """Convert DXT internal type to SQLite type.

        Args:
            dxt_type: DXT internal type
            target_hint: User-specified SQLite type (from Field.target.dtype)

        Returns:
            SQLite type string

        Raises:
            TypeMappingError: If type cannot be mapped

        Examples:
            >>> mapper = SQLiteTypeMapper()
            >>> mapper.to_target(DXTType.INT64)
            'INTEGER'
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
        """Normalize SQLite type string.

        Args:
            source_type: Raw SQLite type string

        Returns:
            Normalized type string

        Examples:
            >>> mapper = SQLiteTypeMapper()
            >>> mapper.normalize_source_type("VARCHAR(255)")
            'varchar'
            >>> mapper.normalize_source_type("NUMERIC(10,2)")
            'numeric'
            >>> mapper.normalize_source_type("INTEGER")
            'integer'
        """
        # Convert to lowercase and strip whitespace
        normalized = source_type.lower().strip()

        # Remove parameters like (255) or (10,2)
        if "(" in normalized:
            normalized = normalized.split("(")[0].strip()

        return normalized

    def get_ddl_type(
        self,
        dxt_type: DXTType,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        target_hint: Optional[str] = None,
    ) -> str:
        """Get SQLite DDL type string.

        SQLite doesn't enforce precision/scale, but we preserve them
        for documentation purposes.

        Args:
            dxt_type: DXT internal type
            precision: Optional precision for DECIMAL types (preserved but not enforced)
            scale: Optional scale for DECIMAL types (preserved but not enforced)
            target_hint: User-specified type hint

        Returns:
            SQLite DDL type string

        Examples:
            >>> mapper = SQLiteTypeMapper()
            >>> mapper.get_ddl_type(DXTType.DECIMAL, precision=10, scale=2)
            'NUMERIC(10,2)'
            >>> mapper.get_ddl_type(DXTType.INT64)
            'INTEGER'
        """
        # Use target hint if provided
        if target_hint:
            return target_hint

        # Get base type
        base_type = self.to_target(dxt_type)

        # Add precision/scale for DECIMAL/NUMERIC types (for documentation)
        if dxt_type == DXTType.DECIMAL and precision is not None:
            if scale is not None:
                return f"NUMERIC({precision},{scale})"
            else:
                return f"NUMERIC({precision})"

        return base_type
