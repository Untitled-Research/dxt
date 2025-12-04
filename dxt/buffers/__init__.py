"""DXT buffers package.

This package contains buffer implementations for temporary data storage
between extract and load operations.

The main Buffer class (from staged.py) is the recommended interface:
- Always persists to disk (no memory-only mode)
- Supports multiple formats (parquet, csv, jsonl, passthrough)
- Single source of truth for format/location config

Legacy buffer implementations (MemoryBuffer, ParquetBuffer) are kept
for backwards compatibility.
"""

from dxt.buffers.staged import Buffer, BufferFormat, CSVOptions, StagedFile
from dxt.buffers.memory import MemoryBuffer
from dxt.buffers.parquet import ParquetBuffer

__all__ = [
    # New unified buffer
    "Buffer",
    "BufferFormat",
    "CSVOptions",
    "StagedFile",
    # Legacy (for backwards compatibility)
    "MemoryBuffer",
    "ParquetBuffer",
]
