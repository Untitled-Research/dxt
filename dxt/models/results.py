"""Result models for extract, load, and pipeline execution.

This module defines result classes that capture outcomes and metrics
from extract, load, and overall pipeline operations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field as PydanticField


class ExtractResult(BaseModel):
    """Result of an extraction operation.

    Contains metrics and metadata about the extraction process,
    including record counts, duration, and any errors encountered.
    """

    stream_id: str = PydanticField(
        ...,
        description="Stream identifier",
    )

    success: bool = PydanticField(
        ...,
        description="Whether extraction succeeded",
    )

    records_extracted: int = PydanticField(
        0,
        description="Number of records extracted",
        ge=0,
    )

    duration_seconds: float = PydanticField(
        0.0,
        description="Duration of extraction in seconds",
        ge=0.0,
    )

    started_at: datetime = PydanticField(
        ...,
        description="Extraction start time",
    )

    completed_at: Optional[datetime] = PydanticField(
        None,
        description="Extraction completion time",
    )

    watermark_value: Optional[Any] = PydanticField(
        None,
        description="Watermark value used for incremental extraction",
    )

    error_message: Optional[str] = PydanticField(
        None,
        description="Error message if extraction failed",
    )

    metadata: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional metadata",
    )

    model_config = {"extra": "forbid"}


class LoadResult(BaseModel):
    """Result of a load operation.

    Contains metrics and metadata about the load process,
    including record counts, duration, and any errors encountered.
    """

    stream_id: str = PydanticField(
        ...,
        description="Stream identifier",
    )

    success: bool = PydanticField(
        ...,
        description="Whether load succeeded",
    )

    records_loaded: int = PydanticField(
        0,
        description="Number of records loaded",
        ge=0,
    )

    records_inserted: int = PydanticField(
        0,
        description="Number of records inserted (for append/upsert)",
        ge=0,
    )

    records_updated: int = PydanticField(
        0,
        description="Number of records updated (for upsert)",
        ge=0,
    )

    records_deleted: int = PydanticField(
        0,
        description="Number of records deleted (for replace)",
        ge=0,
    )

    duration_seconds: float = PydanticField(
        0.0,
        description="Duration of load in seconds",
        ge=0.0,
    )

    started_at: datetime = PydanticField(
        ...,
        description="Load start time",
    )

    completed_at: Optional[datetime] = PydanticField(
        None,
        description="Load completion time",
    )

    error_message: Optional[str] = PydanticField(
        None,
        description="Error message if load failed",
    )

    metadata: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional metadata",
    )

    model_config = {"extra": "forbid"}


class StreamResult(BaseModel):
    """Combined result for a single stream (extract + load).

    Contains results from both extraction and load phases
    for a single stream.
    """

    stream_id: str = PydanticField(
        ...,
        description="Stream identifier",
    )

    success: bool = PydanticField(
        ...,
        description="Whether both extract and load succeeded",
    )

    extract_result: Optional[ExtractResult] = PydanticField(
        None,
        description="Extract phase result",
    )

    load_result: Optional[LoadResult] = PydanticField(
        None,
        description="Load phase result",
    )

    duration_seconds: float = PydanticField(
        0.0,
        description="Total duration (extract + load) in seconds",
        ge=0.0,
    )

    error_message: Optional[str] = PydanticField(
        None,
        description="Error message if processing failed",
    )

    model_config = {"extra": "forbid"}

    @property
    def records_transferred(self) -> int:
        """Total records successfully transferred.

        Returns:
            Number of records that were both extracted and loaded.
        """
        if self.success and self.load_result:
            return self.load_result.records_loaded
        return 0


class ExecutionResult(BaseModel):
    """Result of a complete pipeline execution.

    Contains results for all streams processed, along with
    summary statistics and overall execution metadata.
    """

    pipeline_name: str = PydanticField(
        ...,
        description="Pipeline name/identifier",
    )

    success: bool = PydanticField(
        ...,
        description="Whether pipeline execution succeeded overall",
    )

    streams_processed: int = PydanticField(
        0,
        description="Total number of streams processed",
        ge=0,
    )

    streams_succeeded: int = PydanticField(
        0,
        description="Number of streams that succeeded",
        ge=0,
    )

    streams_failed: int = PydanticField(
        0,
        description="Number of streams that failed",
        ge=0,
    )

    total_records_transferred: int = PydanticField(
        0,
        description="Total records transferred across all streams",
        ge=0,
    )

    duration_seconds: float = PydanticField(
        0.0,
        description="Total pipeline execution duration in seconds",
        ge=0.0,
    )

    started_at: datetime = PydanticField(
        ...,
        description="Pipeline start time",
    )

    completed_at: Optional[datetime] = PydanticField(
        None,
        description="Pipeline completion time",
    )

    stream_results: list[StreamResult] = PydanticField(
        default_factory=list,
        description="Results for each stream",
    )

    error_message: Optional[str] = PydanticField(
        None,
        description="Error message if pipeline failed",
    )

    metadata: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Additional metadata",
    )

    model_config = {"extra": "forbid"}

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as a percentage.

        Returns:
            Failure rate (0.0 to 100.0), or 0.0 if no streams processed.
        """
        if self.streams_processed == 0:
            return 0.0
        return (self.streams_failed / self.streams_processed) * 100.0
