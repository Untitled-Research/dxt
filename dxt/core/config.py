"""DXT configuration management.

This module centralizes all configuration loading from environment variables
and provides sensible defaults. All modules should import configuration
values from here rather than reading environment variables directly.

Environment Variables:
    DXT_BUFFER_DIR: Base directory for buffer/staging files
                    Default: .dxt/buffer (relative to cwd)

    DXT_LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                   Default: INFO

    DXT_LOG_FORMAT: Log output format (text, json)
                    Default: text

    DXT_DEFAULT_BUFFER_FORMAT: Default buffer format when not specified in pipeline
                               Options: parquet, csv, jsonl, memory
                               Default: parquet

    DXT_DEFAULT_COMPRESSION: Default compression for parquet buffers
                             Options: snappy, gzip, zstd, lz4, none
                             Default: snappy

    DXT_PARALLEL_STREAMS: Enable parallel stream execution (experimental)
                          Default: false

    DXT_MAX_WORKERS: Maximum worker threads for parallel execution
                     Default: 4

    DXT_CONNECTION_TIMEOUT: Default connection timeout in seconds
                            Default: 30

    DXT_REQUEST_TIMEOUT: Default request/query timeout in seconds
                         Default: 300

    DXT_RETRY_MAX_ATTEMPTS: Maximum retry attempts for transient failures
                            Default: 3

    DXT_RETRY_BACKOFF_FACTOR: Exponential backoff factor for retries
                              Default: 2.0

    DXT_CLEANUP_ON_SUCCESS: Default cleanup behavior for buffers
                            Default: true

    DXT_RETAIN_ON_FAILURE: Retain buffers on failure for debugging
                           Default: true
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


def _get_bool(key: str, default: bool) -> bool:
    """Get boolean from environment variable."""
    value = os.environ.get(key, "").lower()
    if not value:
        return default
    return value in ("true", "1", "yes", "on")


def _get_int(key: str, default: int) -> int:
    """Get integer from environment variable."""
    value = os.environ.get(key, "")
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(key: str, default: float) -> float:
    """Get float from environment variable."""
    value = os.environ.get(key, "")
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_str(key: str, default: str) -> str:
    """Get string from environment variable."""
    return os.environ.get(key, default)


@dataclass
class DXTConfig:
    """DXT configuration container.

    All configuration values are loaded from environment variables
    with sensible defaults. Configuration is immutable after creation.

    Usage:
        from dxt.core.config import config

        buffer_dir = config.buffer_dir
        timeout = config.connection_timeout
    """

    # Buffer/Staging Configuration
    buffer_dir: Path = field(default_factory=lambda: Path(_get_str("DXT_BUFFER_DIR", ".dxt/buffer")))
    default_buffer_format: str = field(default_factory=lambda: _get_str("DXT_DEFAULT_BUFFER_FORMAT", "parquet"))
    default_compression: str = field(default_factory=lambda: _get_str("DXT_DEFAULT_COMPRESSION", "snappy"))
    cleanup_on_success: bool = field(default_factory=lambda: _get_bool("DXT_CLEANUP_ON_SUCCESS", True))
    retain_on_failure: bool = field(default_factory=lambda: _get_bool("DXT_RETAIN_ON_FAILURE", True))

    # Logging Configuration
    log_level: str = field(default_factory=lambda: _get_str("DXT_LOG_LEVEL", "INFO").upper())
    log_format: str = field(default_factory=lambda: _get_str("DXT_LOG_FORMAT", "text"))

    # Execution Configuration
    parallel_streams: bool = field(default_factory=lambda: _get_bool("DXT_PARALLEL_STREAMS", False))
    max_workers: int = field(default_factory=lambda: _get_int("DXT_MAX_WORKERS", 4))

    # Connection/Timeout Configuration
    connection_timeout: int = field(default_factory=lambda: _get_int("DXT_CONNECTION_TIMEOUT", 30))
    request_timeout: int = field(default_factory=lambda: _get_int("DXT_REQUEST_TIMEOUT", 300))

    # Retry Configuration
    retry_max_attempts: int = field(default_factory=lambda: _get_int("DXT_RETRY_MAX_ATTEMPTS", 3))
    retry_backoff_factor: float = field(default_factory=lambda: _get_float("DXT_RETRY_BACKOFF_FACTOR", 2.0))

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Validate buffer format
        valid_formats = {"parquet", "csv", "jsonl", "memory"}
        if self.default_buffer_format not in valid_formats:
            raise ValueError(
                f"Invalid DXT_DEFAULT_BUFFER_FORMAT: {self.default_buffer_format}. "
                f"Must be one of: {valid_formats}"
            )

        # Validate compression
        valid_compressions = {"snappy", "gzip", "zstd", "lz4", "brotli", "none"}
        if self.default_compression not in valid_compressions:
            raise ValueError(
                f"Invalid DXT_DEFAULT_COMPRESSION: {self.default_compression}. "
                f"Must be one of: {valid_compressions}"
            )

        # Validate log level
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level not in valid_levels:
            raise ValueError(
                f"Invalid DXT_LOG_LEVEL: {self.log_level}. "
                f"Must be one of: {valid_levels}"
            )

        # Validate log format
        valid_formats = {"text", "json"}
        if self.log_format not in valid_formats:
            raise ValueError(
                f"Invalid DXT_LOG_FORMAT: {self.log_format}. "
                f"Must be one of: {valid_formats}"
            )

        # Validate numeric ranges
        if self.max_workers < 1:
            raise ValueError(f"DXT_MAX_WORKERS must be >= 1, got {self.max_workers}")

        if self.connection_timeout < 1:
            raise ValueError(f"DXT_CONNECTION_TIMEOUT must be >= 1, got {self.connection_timeout}")

        if self.request_timeout < 1:
            raise ValueError(f"DXT_REQUEST_TIMEOUT must be >= 1, got {self.request_timeout}")

        if self.retry_max_attempts < 0:
            raise ValueError(f"DXT_RETRY_MAX_ATTEMPTS must be >= 0, got {self.retry_max_attempts}")

        if self.retry_backoff_factor < 1.0:
            raise ValueError(f"DXT_RETRY_BACKOFF_FACTOR must be >= 1.0, got {self.retry_backoff_factor}")

    def get_buffer_dir(self) -> Path:
        """Get buffer directory, creating if necessary.

        Returns:
            Absolute path to buffer directory
        """
        path = self.buffer_dir
        if not path.is_absolute():
            path = Path.cwd() / path
        return path

    def as_dict(self) -> dict:
        """Export configuration as dictionary.

        Returns:
            Dictionary of all configuration values
        """
        return {
            "buffer_dir": str(self.buffer_dir),
            "default_buffer_format": self.default_buffer_format,
            "default_compression": self.default_compression,
            "cleanup_on_success": self.cleanup_on_success,
            "retain_on_failure": self.retain_on_failure,
            "log_level": self.log_level,
            "log_format": self.log_format,
            "parallel_streams": self.parallel_streams,
            "max_workers": self.max_workers,
            "connection_timeout": self.connection_timeout,
            "request_timeout": self.request_timeout,
            "retry_max_attempts": self.retry_max_attempts,
            "retry_backoff_factor": self.retry_backoff_factor,
        }


def load_config() -> DXTConfig:
    """Load configuration from environment.

    This function creates a new DXTConfig instance by reading
    current environment variables. Call this to refresh config
    if environment has changed.

    Returns:
        New DXTConfig instance
    """
    return DXTConfig()


# Global configuration instance - loaded once at import time
# Use load_config() to refresh if needed
config = load_config()
