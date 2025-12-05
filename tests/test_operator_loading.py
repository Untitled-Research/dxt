"""Tests for dynamic operator loading."""

import pytest

from dxt.core.pipeline_runner import PipelineRunner
from dxt.exceptions import PipelineExecutionError


class TestOperatorResolution:
    """Test operator resolution logic."""

    def test_resolve_full_module_path_with_filename(self):
        """Test resolving full module paths with filename."""
        executor = PipelineRunner()

        module_path, class_name = executor._resolve_operator_spec(
            "dxt.providers.postgres.copy_loader.PostgresCopyLoader", "loader", "postgresql"
        )

        assert module_path == "dxt.providers.postgres.copy_loader"
        assert class_name == "PostgresCopyLoader"

    def test_resolve_package_reexport_path(self):
        """Test resolving package re-export paths (shorter form)."""
        executor = PipelineRunner()

        # This should work because PostgresCopyLoader is re-exported in __init__.py
        module_path, class_name = executor._resolve_operator_spec(
            "dxt.providers.postgres.PostgresCopyLoader", "loader", "postgresql"
        )

        assert module_path == "dxt.providers.postgres"
        assert class_name == "PostgresCopyLoader"

    def test_resolve_default_operator(self):
        """Test default operator resolution."""
        executor = PipelineRunner()

        # None with postgresql → PostgresLoader (default)
        module_path, class_name = executor._resolve_operator_spec(
            None, "loader", "postgresql"
        )

        assert module_path == "dxt.providers.postgres.loader"
        assert class_name == "PostgresLoader"

    def test_resolve_custom_operator(self):
        """Test loading custom user operator."""
        executor = PipelineRunner()

        # Custom operator from user's codebase
        module_path, class_name = executor._resolve_operator_spec(
            "mycompany.etl.loaders.CustomLoader", "loader", "postgresql"
        )

        assert module_path == "mycompany.etl.loaders"
        assert class_name == "CustomLoader"

    def test_missing_protocol_error(self):
        """Test error when no default operator for protocol."""
        executor = PipelineRunner()

        with pytest.raises(
            PipelineExecutionError, match="No default operator registered for protocol"
        ):
            executor._resolve_operator_spec(None, "loader", "mongodb")

    def test_load_operator_class_module_not_found(self):
        """Test descriptive error when operator module can't be loaded."""
        executor = PipelineRunner()

        with pytest.raises(PipelineExecutionError, match="Failed to import operator module"):
            executor._load_operator_class("nonexistent.module", "NonexistentClass")

    def test_load_operator_class_class_not_found(self):
        """Test descriptive error when class doesn't exist in module."""
        executor = PipelineRunner()

        with pytest.raises(PipelineExecutionError, match="Class .* not found in module"):
            # Try to import a class that doesn't exist from a real module
            executor._load_operator_class("dxt.providers.postgres", "NonExistentClass")


class TestOperatorLoading:
    """Test actual operator loading."""

    def test_load_postgres_copy_loader_full_path(self):
        """Test loading PostgresCopyLoader with full module path."""
        executor = PipelineRunner()

        operator_class = executor._load_operator_class(
            "dxt.providers.postgres.copy_loader", "PostgresCopyLoader"
        )

        assert operator_class.__name__ == "PostgresCopyLoader"

    def test_load_postgres_copy_loader_package_export(self):
        """Test loading PostgresCopyLoader via package re-export."""
        executor = PipelineRunner()

        # This should work because PostgresCopyLoader is in __all__
        operator_class = executor._load_operator_class(
            "dxt.providers.postgres", "PostgresCopyLoader"
        )

        assert operator_class.__name__ == "PostgresCopyLoader"

    def test_load_sqlite_loader(self):
        """Test loading SQLiteLoader."""
        executor = PipelineRunner()

        operator_class = executor._load_operator_class("dxt.providers.sqlite", "SQLiteLoader")

        assert operator_class.__name__ == "SQLiteLoader"


class TestDefaultOperators:
    """Test default operator registry."""

    def test_get_default_postgres_loader(self):
        """Test getting default PostgreSQL loader."""
        executor = PipelineRunner()

        module_path, class_name = executor._get_default_operator("postgresql", "loader")

        assert module_path == "dxt.providers.postgres.loader"
        assert class_name == "PostgresLoader"

    def test_get_default_sqlite_extractor(self):
        """Test getting default SQLite extractor."""
        executor = PipelineRunner()

        module_path, class_name = executor._get_default_operator("sqlite", "extractor")

        assert module_path == "dxt.providers.sqlite.extractor"
        assert class_name == "SQLiteExtractor"

    def test_protocol_normalization(self):
        """Test protocol normalization strips dialect suffix."""
        executor = PipelineRunner()

        # postgresql+psycopg should normalize to postgresql
        module_path, class_name = executor._get_default_operator(
            "postgresql", "connector"
        )

        assert module_path == "dxt.providers.postgres.connector"
        assert class_name == "PostgresConnector"


class TestConfigMerging:
    """Test configuration merging logic."""

    def test_merge_configs_both_none(self):
        """Test merging when both configs are None."""
        executor = PipelineRunner()

        result = executor._merge_configs(None, None)

        assert result is None

    def test_merge_configs_base_only(self):
        """Test merging when only base config exists."""
        executor = PipelineRunner()

        base = {"batch_size": 1000}
        result = executor._merge_configs(base, None)

        assert result == {"batch_size": 1000}

    def test_merge_configs_override_only(self):
        """Test merging when only override config exists."""
        executor = PipelineRunner()

        override = {"batch_size": 2000}
        result = executor._merge_configs(None, override)

        assert result == {"batch_size": 2000}

    def test_merge_configs_override_takes_precedence(self):
        """Test that override config takes precedence."""
        executor = PipelineRunner()

        base = {"batch_size": 1000, "compression": "snappy"}
        override = {"batch_size": 2000}

        result = executor._merge_configs(base, override)

        assert result == {"batch_size": 2000, "compression": "snappy"}
