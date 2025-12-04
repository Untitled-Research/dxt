"""SQLite type mapper implementation.

This module provides type conversion between SQLite types
and DXT's internal type system.
"""

from __future__ import annotations

from typing import Optional

from dxt.core.type_mapper import TypeMapper
from dxt.models.field import DXTType


class SQLiteTypeMapper(TypeMapper):
    """Type mapper for SQLite.

    Handles conversion between SQLite types and DXT internal types.

    Note: SQLite has dynamic typing with only 5 storage classes:
    INTEGER, REAL, TEXT, BLOB, NULL.

    Example:
        >>> mapper = SQLiteTypeMapper()
        >>> mapper.from_source("INTEGER")
        <DXTType.INT64>
        >>> mapper.to_target(DXTType.INT64)
        'INTEGER'
    """

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
        DXTType.DECIMAL: "NUMERIC",
        DXTType.STRING: "TEXT",
        DXTType.BOOL: "INTEGER",
        DXTType.DATE: "TEXT",
        DXTType.TIME: "TEXT",
        DXTType.TIMESTAMP: "TEXT",
        DXTType.TIMESTAMP_TZ: "TEXT",
        DXTType.BINARY: "BLOB",
        DXTType.JSON: "TEXT",
        DXTType.ARRAY: "TEXT",
        DXTType.OBJECT: "TEXT",
    }

    def from_source(self, source_type: str) -> DXTType:
        """Convert SQLite type to DXT internal type.

        Args:
            source_type: SQLite type string

        Returns:
            Corresponding DXT internal type
        """
        normalized = self.normalize_source_type(source_type)

        if normalized in self.SOURCE_TO_DXT:
            return self.SOURCE_TO_DXT[normalized]

        # SQLite type affinity rules
        if "int" in normalized:
            return DXTType.INT64
        if any(keyword in normalized for keyword in ["char", "clob", "text"]):
            return DXTType.STRING
        if "blob" in normalized or normalized == "":
            return DXTType.BINARY
        if any(keyword in normalized for keyword in ["real", "floa", "doub"]):
            return DXTType.FLOAT64

        return DXTType.STRING

    def to_target(self, dxt_type: DXTType, target_hint: Optional[str] = None) -> str:
        """Convert DXT internal type to SQLite type.

        Args:
            dxt_type: DXT internal type
            target_hint: User-specified SQLite type

        Returns:
            SQLite type string
        """
        if target_hint:
            return target_hint

        if dxt_type in self.DXT_TO_TARGET:
            return self.DXT_TO_TARGET[dxt_type]

        return "TEXT"

    def normalize_source_type(self, source_type: str) -> str:
        """Normalize SQLite type string.

        Args:
            source_type: Raw SQLite type string

        Returns:
            Normalized type string
        """
        normalized = source_type.lower().strip()

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
            precision: Optional precision for DECIMAL types
            scale: Optional scale for DECIMAL types
            target_hint: User-specified type hint

        Returns:
            SQLite DDL type string
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
