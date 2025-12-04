"""Tests for dynamic operator loading."""

import pytest

from dxt.core.pipeline_executor import PipelineExecutor
from dxt.exceptions import PipelineExecutionError


class TestOperatorResolution:
    """Test operator resolution logic."""

    def test_resolve_full_module_path_with_filename(self):
        """Test resolving full module paths with filename."""
        executor = PipelineExecutor()

        module_path, class_name = executor._resolve_operator_spec(
            "dxt.operators.postgres.copy_loader.PostgresCopyLoader", "loader", "postgresql"
        )

        assert module_path == "dxt.operators.postgres.copy_loader"
        assert class_name == "PostgresCopyLoader"

    def test_resolve_package_reexport_path(self):
        """Test resolving package re-export paths (shorter form)."""
        executor = PipelineExecutor()

        # This should work because PostgresCopyLoader is re-exported in __init__.py
        module_path, class_name = executor._resolve_operator_spec(
            "dxt.operators.postgres.PostgresCopyLoader", "loader", "postgresql"
        )

        assert module_path == "dxt.operators.postgres"
        assert class_name == "PostgresCopyLoader"

    def test_resolve_default_operator(self):
        """Test default operator resolution."""
        executor = PipelineExecutor()

        # None with postgresql → PostgresLoader (default)
        module_path, class_name = executor._resolve_operator_spec(
            None, "loader", "postgresql"
        )

        assert module_path == "dxt.operators.postgres.loader"
        assert class_name == "PostgresLoader"

    def test_resolve_custom_operator(self):
        """Test loading custom user operator."""
        executor = PipelineExecutor()

        # Custom operator from user's codebase
        module_path, class_name = executor._resolve_operator_spec(
            "mycompany.etl.loaders.CustomLoader", "loader", "postgresql"
        )

        assert module_path == "mycompany.etl.loaders"
        assert class_name == "CustomLoader"

    def test_missing_protocol_error(self):
        """Test error when no default for protocol."""
        executor = PipelineExecutor()

        with pytest.raises(
            PipelineExecutionError, match="No default operators registered for protocol"
        ):
            executor._resolve_operator_spec(None, "loader", "mongodb")

    def test_load_operator_class_module_not_found(self):
        """Test descriptive error when operator module can't be loaded."""
        executor = PipelineExecutor()

        with pytest.raises(PipelineExecutionError, match="Failed to import operator module"):
            executor._load_operator_class("nonexistent.module", "NonexistentClass")

    def test_load_operator_class_class_not_found(self):
        """Test descriptive error when class doesn't exist in module."""
        executor = PipelineExecutor()

        with pytest.raises(PipelineExecutionError, match="Class .* not found in module"):
            # Try to import a class that doesn't exist from a real module
            executor._load_operator_class("dxt.operators.postgres", "NonExistentClass")


class TestOperatorLoading:
    """Test actual operator loading."""

    def test_load_postgres_copy_loader_full_path(self):
        """Test loading PostgresCopyLoader with full module path."""
        executor = PipelineExecutor()

        operator_class = executor._load_operator_class(
            "dxt.operators.postgres.copy_loader", "PostgresCopyLoader"
        )

        assert operator_class.__name__ == "PostgresCopyLoader"

    def test_load_postgres_copy_loader_package_export(self):
        """Test loading PostgresCopyLoader via package re-export."""
        executor = PipelineExecutor()

        # This should work because PostgresCopyLoader is in __all__
        operator_class = executor._load_operator_class(
            "dxt.operators.postgres", "PostgresCopyLoader"
        )

        assert operator_class.__name__ == "PostgresCopyLoader"

    def test_load_sql_loader(self):
        """Test loading generic SQLLoader."""
        executor = PipelineExecutor()

        operator_class = executor._load_operator_class("dxt.operators.sql", "SQLLoader")

        assert operator_class.__name__ == "SQLLoader"


class TestDefaultOperators:
    """Test default operator registry."""

    def test_get_default_postgres_loader(self):
        """Test getting default PostgreSQL loader."""
        executor = PipelineExecutor()

        module_path, class_name = executor._get_default_operator("postgresql", "loader")

        assert module_path == "dxt.operators.postgres.loader"
        assert class_name == "PostgresLoader"

    def test_get_default_sqlite_extractor(self):
        """Test getting default SQLite extractor."""
        executor = PipelineExecutor()

        module_path, class_name = executor._get_default_operator("sqlite", "extractor")

        assert module_path == "dxt.operators.sqlite.extractor"
        assert class_name == "SQLiteExtractor"

    def test_protocol_normalization(self):
        """Test protocol normalization strips dialect suffix."""
        executor = PipelineExecutor()

        # postgresql+psycopg should normalize to postgresql
        module_path, class_name = executor._get_default_operator(
            "postgresql", "connector"
        )

        assert module_path == "dxt.operators.postgres.connector"
        assert class_name == "PostgresConnector"


class TestConfigMerging:
    """Test configuration merging logic."""

    def test_merge_configs_both_none(self):
        """Test merging when both configs are None."""
        executor = PipelineExecutor()

        result = executor._merge_configs(None, None)

        assert result is None

    def test_merge_configs_base_only(self):
        """Test merging when only base config exists."""
        executor = PipelineExecutor()

        base = {"batch_size": 1000}
        result = executor._merge_configs(base, None)

        assert result == {"batch_size": 1000}

    def test_merge_configs_override_only(self):
        """Test merging when only override config exists."""
        executor = PipelineExecutor()

        override = {"batch_size": 2000}
        result = executor._merge_configs(None, override)

        assert result == {"batch_size": 2000}

    def test_merge_configs_override_takes_precedence(self):
        """Test that override config takes precedence."""
        executor = PipelineExecutor()

        base = {"batch_size": 1000, "compression": "snappy"}
        override = {"batch_size": 2000}

        result = executor._merge_configs(base, override)

        assert result == {"batch_size": 2000, "compression": "snappy"}
