"""DXT - The YAML-first Data Move Tool."""

__version__ = "0.0.2"

# Re-export key models for convenience
from dxt.models import (
    BufferConfig,
    ConnectionConfig,
    DXTType,
    ExecutionResult,  # Backwards compatibility
    ExtractConfig,
    ExtractResult,
    Field,
    LoadConfig,
    LoadResult,
    Pipeline,
    RunResult,
    SourceField,
    Stream,
    StreamResult,
    TargetField,
)

# Re-export core classes for custom operators
from dxt.core import Buffer, Connector, Extractor, Loader, PipelineRunner, TypeMapper

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
    "RunResult",
    "ExecutionResult",  # Backwards compatibility
    # Core ABCs
    "Buffer",
    "Connector",
    "Extractor",
    "Loader",
    "PipelineRunner",
    "TypeMapper",
    # Buffers
    "MemoryBuffer",
    "ParquetBuffer",
]
