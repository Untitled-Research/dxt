"""DXT exception hierarchy."""

from __future__ import annotations


class DXTError(Exception):
    """Base exception for all DXT errors."""

    pass


class ConfigurationError(DXTError):
    """Raised when configuration is invalid or missing."""

    pass


class ConnectionError(DXTError):
    """Raised when connection to a data system fails."""

    pass


class ConnectorError(DXTError):
    """Raised when a connector operation fails."""

    pass


class ExtractorError(DXTError):
    """Raised when extraction fails."""

    pass


class LoaderError(DXTError):
    """Raised when loading fails."""

    pass


class BufferError(DXTError):
    """Raised when buffer operations fail."""

    pass


class ValidationError(DXTError):
    """Raised when validation fails."""

    pass


class SchemaError(DXTError):
    """Raised when schema operations fail."""

    pass


class TypeMappingError(DXTError):
    """Raised when type mapping/conversion fails."""

    pass


class PipelineExecutionError(DXTError):
    """Raised when pipeline execution fails."""

    pass


class SelectorError(DXTError):
    """Raised when stream selector parsing or matching fails."""

    pass
