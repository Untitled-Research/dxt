"""Pipeline execution engine.

This module provides the core pipeline executor that orchestrates
extract and load operations.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from dxt.buffers import MemoryBuffer, ParquetBuffer
from dxt.core.buffer import Buffer
from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.exceptions import PipelineExecutionError
from dxt.models.pipeline import Pipeline
from dxt.models.results import ExecutionResult, StreamResult
from dxt.models.stream import Stream


class PipelineExecutor:
    """Pipeline execution engine.

    Orchestrates the execution of data pipelines, coordinating
    extraction, buffering, and loading operations.

    Examples:
        >>> executor = PipelineExecutor()
        >>> result = executor.execute(pipeline, dry_run=False)
        >>> print(f"Transferred {result.total_records_transferred} records")
    """

    def __init__(self):
        """Initialize pipeline executor."""
        self._operator_registry: dict[str, dict[str, type]] = {}
        self._register_builtin_operators()

    def _register_builtin_operators(self) -> None:
        """Register built-in operators."""
        try:
            from dxt.operators.postgres import (
                PostgresConnector,
                PostgresExtractor,
                PostgresLoader,
            )

            self._operator_registry["postgres"] = {
                "connector": PostgresConnector,
                "extractor": PostgresExtractor,
                "loader": PostgresLoader,
            }
        except ImportError:
            pass  # PostgreSQL operator not available

    def execute(
        self,
        pipeline: Pipeline,
        dry_run: bool = False,
        select: Optional[str] = None,
    ) -> ExecutionResult:
        """Execute a pipeline.

        Args:
            pipeline: Pipeline configuration
            dry_run: If True, validate but don't execute
            select: Stream selector (e.g., "orders", "tag:critical", "*")

        Returns:
            ExecutionResult with metrics

        Raises:
            PipelineExecutionError: If execution fails
        """
        started_at = datetime.now()

        try:
            # Select streams to process
            streams = self._select_streams(pipeline, select)

            if not streams:
                raise PipelineExecutionError("No streams selected for execution")

            # Dry run: just validate
            if dry_run:
                return ExecutionResult(
                    pipeline_name=pipeline.name,
                    success=True,
                    streams_processed=len(streams),
                    streams_succeeded=0,
                    streams_failed=0,
                    total_records_transferred=0,
                    duration_seconds=0,
                    started_at=started_at,
                    completed_at=started_at,
                    metadata={"dry_run": True},
                )

            # Execute each stream
            stream_results = []
            for stream in streams:
                result = self._execute_stream(pipeline, stream)
                stream_results.append(result)

            # Aggregate results
            streams_succeeded = sum(1 for r in stream_results if r.success)
            streams_failed = len(stream_results) - streams_succeeded
            total_records = sum(r.records_transferred for r in stream_results)

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return ExecutionResult(
                pipeline_name=pipeline.name,
                success=streams_failed == 0,
                streams_processed=len(streams),
                streams_succeeded=streams_succeeded,
                streams_failed=streams_failed,
                total_records_transferred=total_records,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                stream_results=stream_results,
            )

        except Exception as e:
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return ExecutionResult(
                pipeline_name=pipeline.name,
                success=False,
                streams_processed=0,
                streams_succeeded=0,
                streams_failed=0,
                total_records_transferred=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(e),
            )

    def _execute_stream(self, pipeline: Pipeline, stream: Stream) -> StreamResult:
        """Execute a single stream.

        Args:
            pipeline: Pipeline configuration
            stream: Stream to execute

        Returns:
            StreamResult with metrics
        """
        try:
            # Create source and target connectors
            source_type = self._get_connection_type(pipeline.source.connection)
            target_type = self._get_connection_type(pipeline.target.connection)

            source_connector = self._create_connector(source_type, pipeline.source.connection)
            target_connector = self._create_connector(target_type, pipeline.target.connection)

            # Create extractor and loader
            extractor = self._create_extractor(source_type, source_connector)
            loader = self._create_loader(target_type, target_connector)

            # Create buffer
            buffer = self._create_buffer(pipeline, stream)

            # Execute extract and load
            with source_connector, target_connector:
                extract_result = extractor.extract(stream, buffer)

                if not extract_result.success:
                    return StreamResult(
                        stream_id=stream.id,
                        success=False,
                        extract_result=extract_result,
                        load_result=None,
                        duration_seconds=extract_result.duration_seconds,
                        error_message=extract_result.error_message,
                    )

                load_result = loader.load(stream, buffer)

                if not load_result.success:
                    return StreamResult(
                        stream_id=stream.id,
                        success=False,
                        extract_result=extract_result,
                        load_result=load_result,
                        duration_seconds=extract_result.duration_seconds
                        + load_result.duration_seconds,
                        error_message=load_result.error_message,
                    )

                # Cleanup buffer on success if configured
                if pipeline.buffer.cleanup_on_success:
                    buffer.cleanup()

                return StreamResult(
                    stream_id=stream.id,
                    success=True,
                    extract_result=extract_result,
                    load_result=load_result,
                    duration_seconds=extract_result.duration_seconds + load_result.duration_seconds,
                )

        except Exception as e:
            return StreamResult(
                stream_id=stream.id,
                success=False,
                extract_result=None,
                load_result=None,
                duration_seconds=0,
                error_message=str(e),
            )

    def _select_streams(self, pipeline: Pipeline, select: Optional[str]) -> list[Stream]:
        """Select streams based on selector.

        Args:
            pipeline: Pipeline configuration
            select: Stream selector (None means all streams)

        Returns:
            List of selected streams
        """
        if select is None or select == "all" or select == "*":
            return pipeline.streams

        return pipeline.get_streams_by_selector(select)

    def _get_connection_type(self, connection: str) -> str:
        """Extract connection type from connection string or reference.

        Args:
            connection: Connection string or reference

        Returns:
            Connection type (e.g., "postgres", "mysql")

        Raises:
            PipelineExecutionError: If type cannot be determined
        """
        # Check if it's a connection string (starts with protocol)
        if "://" in connection:
            protocol = connection.split("://")[0]
            # Map protocols to types
            type_map = {
                "postgresql": "postgres",
                "postgres": "postgres",
                "postgresql+psycopg": "postgres",
                "postgresql+psycopg2": "postgres",
                "mysql": "mysql",
                "sqlite": "sqlite",
            }
            return type_map.get(protocol, protocol)

        # Otherwise assume it's a reference - for now, default to postgres
        # TODO: Load from dxt_project.yaml and get type from there
        return "postgres"

    def _create_connector(self, conn_type: str, connection: str) -> Connector:
        """Create a connector instance.

        Args:
            conn_type: Connection type
            connection: Connection string or reference

        Returns:
            Connector instance

        Raises:
            PipelineExecutionError: If connector cannot be created
        """
        if conn_type not in self._operator_registry:
            raise PipelineExecutionError(f"Unsupported connection type: {conn_type}")

        connector_class = self._operator_registry[conn_type]["connector"]

        # Parse connection string into config
        if "://" in connection:
            config = {"connection_string": connection}
        else:
            # TODO: Load from dxt_project.yaml
            raise PipelineExecutionError(
                f"Connection references not yet implemented. Use connection strings."
            )

        return connector_class(config)

    def _create_extractor(self, conn_type: str, connector: Connector) -> Extractor:
        """Create an extractor instance.

        Args:
            conn_type: Connection type
            connector: Connector instance

        Returns:
            Extractor instance

        Raises:
            PipelineExecutionError: If extractor cannot be created
        """
        if conn_type not in self._operator_registry:
            raise PipelineExecutionError(f"Unsupported connection type: {conn_type}")

        extractor_class = self._operator_registry[conn_type]["extractor"]
        return extractor_class(connector)

    def _create_loader(self, conn_type: str, connector: Connector) -> Loader:
        """Create a loader instance.

        Args:
            conn_type: Connection type
            connector: Connector instance

        Returns:
            Loader instance

        Raises:
            PipelineExecutionError: If loader cannot be created
        """
        if conn_type not in self._operator_registry:
            raise PipelineExecutionError(f"Unsupported connection type: {conn_type}")

        loader_class = self._operator_registry[conn_type]["loader"]
        return loader_class(connector)

    def _create_buffer(self, pipeline: Pipeline, stream: Stream) -> Buffer:
        """Create a buffer instance.

        Args:
            pipeline: Pipeline configuration
            stream: Stream configuration

        Returns:
            Buffer instance

        Raises:
            PipelineExecutionError: If buffer cannot be created
        """
        # Get fields for buffer schema
        fields = stream.fields
        if not fields:
            raise PipelineExecutionError(
                f"Stream '{stream.id}': fields required (schema auto-detection not yet implemented)"
            )

        # Create buffer based on format
        buffer_format = pipeline.buffer.format

        if buffer_format == "memory":
            return MemoryBuffer(fields)

        elif buffer_format == "parquet":
            # Determine buffer location
            if pipeline.buffer.location:
                buffer_dir = Path(pipeline.buffer.location)
            else:
                buffer_dir = Path(".dxt/buffer")

            buffer_dir.mkdir(parents=True, exist_ok=True)
            buffer_path = buffer_dir / f"{stream.id}.parquet"

            return ParquetBuffer(
                fields,
                location=buffer_path,
                compression=pipeline.buffer.compression or "snappy",
            )

        else:
            raise PipelineExecutionError(f"Unsupported buffer format: {buffer_format}")
