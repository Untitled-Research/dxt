"""Run ID generation utilities for DXT.

This module provides functions for generating and managing run IDs
that uniquely identify pipeline executions.
"""

from __future__ import annotations

import secrets
from datetime import datetime
from pathlib import Path

from dxt.core.config import config


def generate_run_id() -> str:
    """Generate a sortable, unique run ID.

    Format: YYYYMMDD_HHMMSS_<random>
    Example: 20250604_143052_a1b2c3

    The format ensures:
    - Sortable by timestamp (lexicographic ordering = chronological)
    - Human-readable date/time component
    - Uniqueness via random suffix (6 hex chars = 16M combinations)

    Returns:
        Unique run ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    random_suffix = secrets.token_hex(3)  # 6 hex characters
    return f"{timestamp}_{random_suffix}"


def get_buffer_dir() -> Path:
    """Get the base buffer directory from centralized config.

    Uses the buffer_dir setting from DXTConfig, which reads from
    DXT_BUFFER_DIR environment variable with a default of .dxt/buffer.

    Returns:
        Path to buffer directory (absolute)
    """
    return config.get_buffer_dir()


def get_run_dir(pipeline_name: str, run_id: str, buffer_location: str | None = None) -> Path:
    """Get the directory for a specific pipeline run.

    Directory structure:
        {buffer_dir}/{pipeline_name}/{run_id}/

    Args:
        pipeline_name: Name of the pipeline
        run_id: Unique run identifier
        buffer_location: Optional override for buffer base directory

    Returns:
        Path to run directory
    """
    if buffer_location:
        base_dir = Path(buffer_location)
    else:
        base_dir = get_buffer_dir()

    return base_dir / pipeline_name / run_id


def get_stream_buffer_path(
    pipeline_name: str,
    run_id: str,
    stream_id: str,
    buffer_format: str = "parquet",
    buffer_location: str | None = None,
) -> Path:
    """Get the buffer file path for a specific stream.

    Directory structure:
        {buffer_dir}/{pipeline_name}/{run_id}/{stream_id}.{format}

    Args:
        pipeline_name: Name of the pipeline
        run_id: Unique run identifier
        stream_id: Stream identifier
        buffer_format: Buffer format (parquet, csv, jsonl)
        buffer_location: Optional override for buffer base directory

    Returns:
        Path to stream buffer file
    """
    run_dir = get_run_dir(pipeline_name, run_id, buffer_location)

    # Determine file extension
    extensions = {
        "parquet": "parquet",
        "csv": "csv",
        "jsonl": "jsonl",
        "memory": None,  # No file for memory buffer
    }
    ext = extensions.get(buffer_format, buffer_format)

    if ext is None:
        # Memory buffer doesn't have a file path, but we still return
        # a path for metadata purposes
        return run_dir / f"{stream_id}.memory"

    return run_dir / f"{stream_id}.{ext}"


def list_runs(pipeline_name: str, buffer_location: str | None = None) -> list[str]:
    """List all run IDs for a pipeline, sorted newest first.

    Args:
        pipeline_name: Name of the pipeline
        buffer_location: Optional override for buffer base directory

    Returns:
        List of run IDs, sorted newest first (reverse chronological)
    """
    if buffer_location:
        base_dir = Path(buffer_location)
    else:
        base_dir = get_buffer_dir()

    pipeline_dir = base_dir / pipeline_name

    if not pipeline_dir.exists():
        return []

    runs = [
        d.name
        for d in pipeline_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]

    # Sort reverse chronologically (newest first)
    # Our format YYYYMMDD_HHMMSS_xxx sorts lexicographically
    return sorted(runs, reverse=True)


def get_latest_run_id(pipeline_name: str, buffer_location: str | None = None) -> str | None:
    """Get the most recent run ID for a pipeline.

    Args:
        pipeline_name: Name of the pipeline
        buffer_location: Optional override for buffer base directory

    Returns:
        Most recent run ID, or None if no runs exist
    """
    runs = list_runs(pipeline_name, buffer_location)
    return runs[0] if runs else None
