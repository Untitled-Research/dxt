"""Pipeline runner engine.

This module provides the core pipeline runner that orchestrates
extract and load operations.
"""

from __future__ import annotations

import importlib
import json
from datetime import datetime
from pathlib import Path
from typing import Callable, Literal, Optional

from dxt.buffers import MemoryBuffer, ParquetBuffer
from dxt.core.buffer import Buffer
from dxt.core.config import config
from dxt.core.connector import Connector
from dxt.core.extractor import Extractor
from dxt.core.loader import Loader
from dxt.exceptions import PipelineExecutionError
from dxt.models.pipeline import Pipeline
from dxt.models.results import RunResult, StreamResult
from dxt.models.stream import Stream
from dxt.utils.run_id import generate_run_id, get_run_dir, get_stream_buffer_path


# Type alias for operation phases
Operation = Literal["run", "extract", "load"]

# Default operators - maps protocol to default operator classes
DEFAULT_OPERATORS = {
    "postgresql": {
        "connector": "dxt.providers.postgres.connector.PostgresConnector",
        "extractor": "dxt.providers.postgres.extractor.PostgresExtractor",
        "loader": "dxt.providers.postgres.loader.PostgresLoader",
    },
    "sqlite": {
        "connector": "dxt.providers.sqlite.connector.SQLiteConnector",
        "extractor": "dxt.providers.sqlite.extractor.SQLiteExtractor",
        "loader": "dxt.providers.sqlite.loader.SQLiteLoader",
    },
    # REST API (http/https protocols)
    "http": {
        "connector": "dxt.providers.rest.connector.RestConnector",
        "extractor": "dxt.providers.rest.extractor.RestExtractor",
        # No default loader for REST (would need specific API loader)
    },
    "https": {
        "connector": "dxt.providers.rest.connector.RestConnector",
        "extractor": "dxt.providers.rest.extractor.RestExtractor",
        # No default loader for REST
    },
    # Future: mysql, duckdb, mongodb, etc.
}


class PipelineRunner:
    """Pipeline runner engine.

    Orchestrates the running of data pipelines, coordinating
    extraction, buffering, and loading operations.

    Examples:
        >>> runner = PipelineRunner()
        >>> result = runner.run(pipeline)
        >>> print(f"Transferred {result.total_records_transferred} records")
    """

    def __init__(self):
        """Initialize pipeline runner."""
        pass  # No initialization needed - using dynamic loading

    def run(
        self,
        pipeline: Pipeline,
        dry_run: bool = False,
        select: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> RunResult:
        """Run a pipeline (extract + load).

        Args:
            pipeline: Pipeline configuration
            dry_run: If True, validate but don't execute
            select: Stream selector (e.g., "orders", "tag:critical", "*")
            run_id: Optional run identifier. Auto-generated if not provided.

        Returns:
            ExecutionResult with metrics

        Raises:
            PipelineExecutionError: If execution fails
        """
        if dry_run:
            run_id = run_id or generate_run_id()
            started_at = datetime.now()
            streams = self._select_streams(pipeline, select)
            if not streams:
                raise PipelineExecutionError("No streams selected for execution")
            return RunResult(
                pipeline_name=pipeline.name,
                run_id=run_id,
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

        return self._run_operation(
            pipeline=pipeline,
            operation="run",
            select=select,
            run_id=run_id,
            stream_processor=self._execute_stream,
            record_counter=lambda r: r.records_transferred,
        )

    def extract(
        self,
        pipeline: Pipeline,
        select: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> RunResult:
        """Extract data from source to buffer (no load).

        Args:
            pipeline: Pipeline configuration
            select: Stream selector (e.g., "orders", "tag:critical", "*")
            run_id: Optional run identifier. Auto-generated if not provided.

        Returns:
            RunResult with metrics
        """
        return self._run_operation(
            pipeline=pipeline,
            operation="extract",
            select=select,
            run_id=run_id,
            stream_processor=self._extract_stream,
            record_counter=lambda r: r.extract_result.records_extracted if r.extract_result else 0,
        )

    def load(
        self,
        pipeline: Pipeline,
        run_id: str,
        select: Optional[str] = None,
    ) -> RunResult:
        """Load data from buffer to target.

        Args:
            pipeline: Pipeline configuration
            run_id: Run identifier (required - must match a previous extract)
            select: Stream selector (e.g., "orders", "tag:critical", "*")

        Returns:
            RunResult with metrics
        """
        return self._run_operation(
            pipeline=pipeline,
            operation="load",
            select=select,
            run_id=run_id,
            stream_processor=self._load_stream,
            record_counter=lambda r: r.load_result.records_loaded if r.load_result else 0,
            require_existing_run=True,
        )

    def _run_operation(
        self,
        pipeline: Pipeline,
        operation: Operation,
        select: Optional[str],
        run_id: Optional[str],
        stream_processor: Callable[[Pipeline, Stream, str], StreamResult],
        record_counter: Callable[[StreamResult], int],
        require_existing_run: bool = False,
    ) -> RunResult:
        """Run a pipeline operation (extract, load, or full run).

        Consolidates common logic for all operation types:
        - Run ID generation/validation
        - Stream selection
        - Duration tracking
        - Result aggregation
        - Metadata management

        Args:
            pipeline: Pipeline configuration
            operation: Operation type ("run", "extract", "load")
            select: Stream selector
            run_id: Run identifier (generated if not provided for extract/run)
            stream_processor: Function to process each stream
            record_counter: Function to count records from StreamResult
            require_existing_run: If True, verify run directory exists (for load)

        Returns:
            RunResult with metrics
        """
        # Generate or validate run_id
        if run_id is None:
            run_id = generate_run_id()

        started_at = datetime.now()
        metadata: dict = {"operation": operation} if operation != "run" else {}

        try:
            # Verify run exists (for load operation)
            if require_existing_run:
                run_dir = get_run_dir(pipeline.name, run_id, pipeline.buffer.location)
                if not run_dir.exists():
                    raise PipelineExecutionError(
                        f"Run '{run_id}' not found for pipeline '{pipeline.name}'. "
                        f"Expected directory: {run_dir}"
                    )

            # Select streams
            streams = self._select_streams(pipeline, select)
            if not streams:
                raise PipelineExecutionError("No streams selected for execution")

            # Initialize run directory (not needed for load - already exists)
            if not require_existing_run:
                self._init_run(pipeline, run_id, streams, started_at, operation)

            # Process streams
            stream_results = [
                stream_processor(pipeline, stream, run_id)
                for stream in streams
            ]

            # Aggregate results
            streams_succeeded = sum(1 for r in stream_results if r.success)
            streams_failed = len(stream_results) - streams_succeeded
            total_records = sum(record_counter(r) for r in stream_results)

            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            # Determine final status
            status = self._get_final_status(operation, streams_failed == 0)
            self._finalize_run(pipeline, run_id, status, completed_at, stream_results)

            return RunResult(
                pipeline_name=pipeline.name,
                run_id=run_id,
                success=streams_failed == 0,
                streams_processed=len(streams),
                streams_succeeded=streams_succeeded,
                streams_failed=streams_failed,
                total_records_transferred=total_records,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                stream_results=stream_results,
                metadata=metadata,
            )

        except Exception as e:
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()

            return RunResult(
                pipeline_name=pipeline.name,
                run_id=run_id,
                success=False,
                streams_processed=0,
                streams_succeeded=0,
                streams_failed=0,
                total_records_transferred=0,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=completed_at,
                error_message=str(e),
                metadata=metadata,
            )

    def _get_final_status(self, operation: Operation, success: bool) -> str:
        """Get the final status string for a completed operation.

        Args:
            operation: The operation type
            success: Whether the operation succeeded

        Returns:
            Status string for metadata
        """
        status_map = {
            ("run", True): "completed",
            ("run", False): "failed",
            ("extract", True): "extracted",
            ("extract", False): "extract_failed",
            ("load", True): "loaded",
            ("load", False): "load_failed",
        }
        return status_map[(operation, success)]

    def _init_run(
        self,
        pipeline: Pipeline,
        run_id: str,
        streams: list[Stream],
        started_at: datetime,
        operation: Operation = "run",
    ) -> None:
        """Initialize a run directory with metadata.

        Args:
            pipeline: Pipeline configuration
            run_id: Run identifier
            streams: Streams to be processed
            started_at: Timestamp when run started
            operation: Operation type (run, extract, load)
        """
        run_dir = get_run_dir(pipeline.name, run_id, pipeline.buffer.location)
        run_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "run_id": run_id,
            "pipeline_name": pipeline.name,
            "operation": operation,
            "started_at": started_at.isoformat(),
            "streams": [s.id for s in streams],
            "status": "running",
        }

        metadata_path = run_dir / "_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def _finalize_run(
        self,
        pipeline: Pipeline,
        run_id: str,
        status: str,
        completed_at: datetime,
        stream_results: list[StreamResult],
    ) -> None:
        """Update run metadata with final status.

        Args:
            pipeline: Pipeline configuration
            run_id: Run identifier
            status: Final status (completed, failed, extracted, loaded, etc.)
            completed_at: Timestamp when run completed
            stream_results: Results from stream execution
        """
        run_dir = get_run_dir(pipeline.name, run_id, pipeline.buffer.location)
        metadata_path = run_dir / "_metadata.json"

        if metadata_path.exists():
            with open(metadata_path) as f:
                metadata = json.load(f)
        else:
            metadata = {
                "run_id": run_id,
                "pipeline_name": pipeline.name,
            }

        metadata["status"] = status
        metadata["completed_at"] = completed_at.isoformat()
        metadata["stream_results"] = {
            sr.stream_id: {
                "success": sr.success,
                "records_transferred": sr.records_transferred,
                "error_message": sr.error_message,
            }
            for sr in stream_results
        }

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def _execute_stream(
        self, pipeline: Pipeline, stream: Stream, run_id: str
    ) -> StreamResult:
        """Execute a single stream (extract + load).

        Args:
            pipeline: Pipeline configuration
            stream: Stream to execute
            run_id: Run identifier

        Returns:
            StreamResult with metrics
        """
        try:
            # Create source components
            source_connector, extractor = self._create_source_components(pipeline, stream)

            # Create target components
            target_connector, loader = self._create_target_components(pipeline, stream)

            # Create buffer
            buffer = self._create_buffer(pipeline, stream, run_id)

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

    def _extract_stream(
        self, pipeline: Pipeline, stream: Stream, run_id: str
    ) -> StreamResult:
        """Extract a single stream to buffer.

        Args:
            pipeline: Pipeline configuration
            stream: Stream to extract
            run_id: Run identifier

        Returns:
            StreamResult with extract metrics
        """
        try:
            source_connector, extractor = self._create_source_components(pipeline, stream)
            buffer = self._create_buffer(pipeline, stream, run_id)

            with source_connector:
                extract_result = extractor.extract(stream, buffer)

                return StreamResult(
                    stream_id=stream.id,
                    success=extract_result.success,
                    extract_result=extract_result,
                    load_result=None,
                    duration_seconds=extract_result.duration_seconds,
                    error_message=extract_result.error_message if not extract_result.success else None,
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

    def _load_stream(
        self, pipeline: Pipeline, stream: Stream, run_id: str
    ) -> StreamResult:
        """Load a single stream from buffer.

        Args:
            pipeline: Pipeline configuration
            stream: Stream to load
            run_id: Run identifier

        Returns:
            StreamResult with load metrics
        """
        try:
            target_connector, loader = self._create_target_components(pipeline, stream)
            buffer = self._load_buffer(pipeline, stream, run_id)

            with target_connector:
                load_result = loader.load(stream, buffer)

                # Cleanup buffer on success if configured
                if load_result.success and pipeline.buffer.cleanup_on_success:
                    buffer.cleanup()

                return StreamResult(
                    stream_id=stream.id,
                    success=load_result.success,
                    extract_result=None,
                    load_result=load_result,
                    duration_seconds=load_result.duration_seconds,
                    error_message=load_result.error_message if not load_result.success else None,
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

    def _create_source_components(
        self, pipeline: Pipeline, stream: Stream
    ) -> tuple[Connector, Extractor]:
        """Create source connector and extractor for a stream.

        Args:
            pipeline: Pipeline configuration
            stream: Stream configuration

        Returns:
            Tuple of (connector, extractor)
        """
        # Resolve extractor spec
        extractor_spec = (
            stream.source.extractor
            or (pipeline.extract.extractor if pipeline.extract else None)
        )

        # Resolve extractor config
        pipeline_extract_config = None
        if pipeline.extract:
            pipeline_extract_config = pipeline.extract.model_dump(
                exclude_none=True, exclude={"extractor"}
            )

        extractor_config = self._merge_configs(
            pipeline_extract_config,
            stream.source.extractor_config,
        )

        # Create connector
        source_connector = self._create_connector(
            pipeline.source.connection,
            stream.source.connector,
            stream.source.connector_config,
        )

        # Create extractor
        extractor = self._create_extractor(
            source_connector,
            extractor_spec,
            extractor_config,
        )

        return source_connector, extractor

    def _create_target_components(
        self, pipeline: Pipeline, stream: Stream
    ) -> tuple[Connector, Loader]:
        """Create target connector and loader for a stream.

        Args:
            pipeline: Pipeline configuration
            stream: Stream configuration

        Returns:
            Tuple of (connector, loader)
        """
        # Resolve loader spec
        loader_spec = (
            stream.target.loader
            or (pipeline.load.loader if pipeline.load else None)
        )

        # Resolve loader config
        pipeline_load_config = None
        if pipeline.load:
            pipeline_load_config = pipeline.load.model_dump(
                exclude_none=True, exclude={"loader"}
            )

        loader_config = self._merge_configs(
            pipeline_load_config,
            stream.target.loader_config,
        )

        # Create connector
        target_connector = self._create_connector(
            pipeline.target.connection,
            stream.target.connector,
            stream.target.connector_config,
        )

        # Create loader
        loader = self._create_loader(
            target_connector,
            loader_spec,
            loader_config,
        )

        return target_connector, loader

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

    def _get_connection_protocol(self, connection: str) -> str:
        """Extract connection protocol from connection string.

        Args:
            connection: Connection string or reference

        Returns:
            Connection protocol (e.g., "postgresql", "sqlite")

        Raises:
            PipelineExecutionError: If protocol cannot be determined
        """
        # Check if it's a connection string (starts with protocol)
        if "://" in connection:
            protocol = connection.split("://")[0]
            # Normalize protocol (strip dialect suffix)
            normalized = protocol.split("+")[0]  # postgresql+psycopg → postgresql
            return normalized

        # Otherwise assume it's a reference - for now, raise error
        # TODO: Load from dxt_project.yaml and get type from there
        raise PipelineExecutionError(
            f"Connection references not yet implemented. Use connection strings. Got: {connection}"
        )

    def _load_operator_class(self, module_path: str, class_name: str) -> type:
        """Dynamically import and return operator class.

        Args:
            module_path: e.g., "dxt.providers.postgres.copy_loader"
            class_name: e.g., "PostgresCopyLoader"

        Returns:
            Operator class (Connector, Extractor, or Loader)

        Raises:
            PipelineExecutionError: If module/class not found
        """
        try:
            module = importlib.import_module(module_path)
            operator_class = getattr(module, class_name)
            return operator_class
        except ImportError as e:
            raise PipelineExecutionError(
                f"Failed to import operator module '{module_path}'.\n"
                f"Error: {e}\n"
                f"Make sure the module exists and is importable."
            ) from e
        except AttributeError as e:
            available_classes = [name for name in dir(module) if not name.startswith("_")]
            raise PipelineExecutionError(
                f"Class '{class_name}' not found in module '{module_path}'.\n"
                f"Error: {e}\n"
                f"Available classes: {available_classes}"
            ) from e

    def _resolve_operator_spec(
        self,
        spec: Optional[str],
        operator_type: str,
        connection_protocol: str,
    ) -> tuple[str, str]:
        """Resolve operator specification to (module_path, class_name).

        Args:
            spec: Full module path from YAML or None to use default
            operator_type: "connector", "extractor", or "loader"
            connection_protocol: e.g., "postgresql", "sqlite" (for defaults)

        Returns:
            Tuple of (module_path, class_name)

        Resolution rules:
        1. If spec is None → use default for connection protocol
        2. If spec is full module path → parse module + class

        Supports both forms:
        - "dxt.providers.postgres.PostgresCopyLoader" (package re-export)
        - "dxt.providers.postgres.copy_loader.PostgresCopyLoader" (full path)
        """
        if spec is None:
            # Use default operator for this protocol
            return self._get_default_operator(connection_protocol, operator_type)

        # Full module path specified
        # Parse: "dxt.providers.postgres.PostgresCopyLoader"
        #    OR: "dxt.providers.postgres.copy_loader.PostgresCopyLoader"
        #    OR: "mycompany.etl.CustomLoader"
        parts = spec.split(".")
        class_name = parts[-1]
        module_path = ".".join(parts[:-1])
        return module_path, class_name

    def _get_default_operator(self, protocol: str, operator_type: str) -> tuple[str, str]:
        """Get default operator for connection protocol.

        Args:
            protocol: e.g., "postgresql", "sqlite", "mysql"
            operator_type: "connector", "extractor", or "loader"

        Returns:
            Tuple of (module_path, class_name)

        Raises:
            PipelineExecutionError: If no default exists for protocol
        """
        if protocol not in DEFAULT_OPERATORS:
            raise PipelineExecutionError(
                f"No default operator registered for protocol '{protocol}'.\n"
                f"Available protocols: {', '.join(DEFAULT_OPERATORS.keys())}\n"
                f"Please specify an operator in your YAML configuration:\n"
                f"  {operator_type}: dxt.providers.your_provider.ClassName"
            )

        if operator_type not in DEFAULT_OPERATORS[protocol]:
            raise PipelineExecutionError(
                f"No default {operator_type} registered for protocol '{protocol}'.\n"
                f"Please specify a {operator_type} in your YAML configuration."
            )

        # Get full module path from registry
        full_path = DEFAULT_OPERATORS[protocol][operator_type]

        # Parse into module + class
        parts = full_path.split(".")
        class_name = parts[-1]
        module_path = ".".join(parts[:-1])

        return module_path, class_name

    def _merge_configs(
        self, base_config: Optional[dict], override_config: Optional[dict]
    ) -> Optional[dict]:
        """Merge two configuration dictionaries.

        Args:
            base_config: Base configuration (e.g., pipeline defaults)
            override_config: Override configuration (e.g., stream-level)

        Returns:
            Merged configuration with overrides taking precedence
        """
        if base_config is None and override_config is None:
            return None

        if base_config is None:
            return override_config

        if override_config is None:
            return base_config

        # Merge: override takes precedence
        merged = {**base_config, **override_config}
        return merged

    def _create_connector(
        self,
        connection_string: str,
        connector_spec: Optional[str] = None,
        connector_config: Optional[dict] = None,
    ) -> Connector:
        """Create connector instance using dynamic loading.

        Args:
            connection_string: Database connection string
            connector_spec: Optional operator spec from YAML
            connector_config: Optional config dict for connector

        Returns:
            Connector instance

        Raises:
            PipelineExecutionError: If connector cannot be created
        """
        # Get connection protocol
        protocol = self._get_connection_protocol(connection_string)

        # Resolve operator
        module_path, class_name = self._resolve_operator_spec(
            connector_spec, "connector", protocol
        )

        # Load operator class
        connector_class = self._load_operator_class(module_path, class_name)

        # Create config
        connector_cfg = {"connection_string": connection_string}
        if connector_config:
            connector_cfg.update(connector_config)

        # Instantiate
        return connector_class(connector_cfg)

    def _create_extractor(
        self,
        connector: Connector,
        extractor_spec: Optional[str] = None,
        extractor_config: Optional[dict] = None,
    ) -> Extractor:
        """Create extractor instance using dynamic loading.

        Args:
            connector: Connector instance
            extractor_spec: Optional operator spec from YAML
            extractor_config: Optional config dict for extractor

        Returns:
            Extractor instance

        Raises:
            PipelineExecutionError: If extractor cannot be created
        """
        # Infer protocol from connector's connection string
        protocol = self._get_connection_protocol(connector.config["connection_string"])

        # Resolve operator
        module_path, class_name = self._resolve_operator_spec(
            extractor_spec, "extractor", protocol
        )

        # Load operator class
        extractor_class = self._load_operator_class(module_path, class_name)

        # Instantiate with connector and config
        return extractor_class(connector, extractor_config or {})

    def _create_loader(
        self,
        connector: Connector,
        loader_spec: Optional[str] = None,
        loader_config: Optional[dict] = None,
    ) -> Loader:
        """Create loader instance using dynamic loading.

        Args:
            connector: Connector instance
            loader_spec: Optional operator spec from YAML
            loader_config: Optional config dict for loader

        Returns:
            Loader instance

        Raises:
            PipelineExecutionError: If loader cannot be created
        """
        # Infer protocol from connector's connection string
        protocol = self._get_connection_protocol(connector.config["connection_string"])

        # Resolve operator
        module_path, class_name = self._resolve_operator_spec(loader_spec, "loader", protocol)

        # Load operator class
        loader_class = self._load_operator_class(module_path, class_name)

        # Instantiate with connector and config
        return loader_class(connector, loader_config or {})

    def _create_buffer(
        self, pipeline: Pipeline, stream: Stream, run_id: str
    ) -> Buffer:
        """Create a buffer instance for writing.

        Args:
            pipeline: Pipeline configuration
            stream: Stream configuration
            run_id: Run identifier

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
            # Get buffer path using run_id structure
            buffer_path = get_stream_buffer_path(
                pipeline.name,
                run_id,
                stream.id,
                buffer_format="parquet",
                buffer_location=pipeline.buffer.location,
            )

            # Ensure parent directory exists
            buffer_path.parent.mkdir(parents=True, exist_ok=True)

            return ParquetBuffer(
                fields,
                location=buffer_path,
                compression=pipeline.buffer.compression or config.default_compression,
            )

        else:
            raise PipelineExecutionError(f"Unsupported buffer format: {buffer_format}")

    def _load_buffer(
        self, pipeline: Pipeline, stream: Stream, run_id: str
    ) -> Buffer:
        """Load an existing buffer for reading.

        Args:
            pipeline: Pipeline configuration
            stream: Stream configuration
            run_id: Run identifier

        Returns:
            Buffer instance with existing data

        Raises:
            PipelineExecutionError: If buffer cannot be loaded
        """
        fields = stream.fields
        if not fields:
            raise PipelineExecutionError(
                f"Stream '{stream.id}': fields required"
            )

        buffer_format = pipeline.buffer.format

        if buffer_format == "memory":
            raise PipelineExecutionError(
                "Cannot load from memory buffer - memory buffers are not persisted. "
                "Use parquet or csv format for separate extract/load operations."
            )

        elif buffer_format == "parquet":
            buffer_path = get_stream_buffer_path(
                pipeline.name,
                run_id,
                stream.id,
                buffer_format="parquet",
                buffer_location=pipeline.buffer.location,
            )

            if not buffer_path.exists():
                raise PipelineExecutionError(
                    f"Buffer file not found: {buffer_path}. "
                    f"Did you run extract first for run_id '{run_id}'?"
                )

            return ParquetBuffer(
                fields,
                location=buffer_path,
                compression=pipeline.buffer.compression or config.default_compression,
            )

        else:
            raise PipelineExecutionError(f"Unsupported buffer format: {buffer_format}")


# Backwards compatibility alias
PipelineExecutor = PipelineRunner
