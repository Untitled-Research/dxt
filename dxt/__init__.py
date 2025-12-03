"""DXT - The YAML-first Data Move Tool."""

__version__ = "0.0.2"

# Re-export key models for convenience
from dxt.models import (
    BufferConfig,
    ConnectionConfig,
    DXTType,
    ExecutionResult,
    ExtractConfig,
    ExtractResult,
    Field,
    LoadConfig,
    LoadResult,
    Pipeline,
    SourceField,
    Stream,
    StreamResult,
    TargetField,
)

# Re-export core classes for custom operators
from dxt.core import Buffer, Connector, Extractor, Loader, TypeMapper

# Re-export buffer implementations
from dxt.buffers import MemoryBuffer, ParquetBuffer

__all__ = [
    # Version
    "__version__",
    # Models
    "DXTType",
    "Field",
    "SourceField",
    "TargetField",
    "Stream",
    "ExtractConfig",
    "LoadConfig",
    "Pipeline",
    "ConnectionConfig",
    "BufferConfig",
    "ExtractResult",
    "LoadResult",
    "StreamResult",
    "ExecutionResult",
    # Core ABCs
    "Buffer",
    "Connector",
    "Extractor",
    "Loader",
    "TypeMapper",
    # Buffers
    "MemoryBuffer",
    "ParquetBuffer",
]
