"""Pipeline execution engine.

This module provides the core pipeline executor that orchestrates
extract and load operations.
"""

from __future__ import annotations

import importlib
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
    # Future: mysql, duckdb, etc.
}


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
        pass  # No initialization needed - using dynamic loading

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
            # Resolve operator specs (stream-level overrides, then pipeline defaults, then infer)
            source_extractor_spec = (
                stream.source.extractor
                or (pipeline.extract.extractor if pipeline.extract else None)
            )
            target_loader_spec = (
                stream.target.loader or (pipeline.load.loader if pipeline.load else None)
            )

            # Resolve configs (merge stream + pipeline defaults)
            # Convert pipeline extract/load defaults to config dicts
            pipeline_extract_config = None
            if pipeline.extract:
                pipeline_extract_config = pipeline.extract.model_dump(exclude_none=True, exclude={"extractor"})

            pipeline_load_config = None
            if pipeline.load:
                pipeline_load_config = pipeline.load.model_dump(exclude_none=True, exclude={"loader"})

            extractor_config = self._merge_configs(
                pipeline_extract_config,
                stream.source.extractor_config,
            )
            loader_config = self._merge_configs(
                pipeline_load_config,
                stream.target.loader_config,
            )

            # Create connectors
            source_connector = self._create_connector(
                pipeline.source.connection,
                stream.source.connector,
                stream.source.connector_config,
            )
            target_connector = self._create_connector(
                pipeline.target.connection,
                stream.target.connector,
                stream.target.connector_config,
            )

            # Create extractor and loader with dynamic loading
            extractor = self._create_extractor(
                source_connector,
                source_extractor_spec,
                extractor_config,
            )
            loader = self._create_loader(
                target_connector,
                target_loader_spec,
                loader_config,
            )

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
        config = {"connection_string": connection_string}
        if connector_config:
            config.update(connector_config)

        # Instantiate
        return connector_class(config)

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
