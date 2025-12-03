"""YAML parsing utilities for DXT.

This module provides functions for loading and parsing pipeline
and configuration YAML files with validation.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError as PydanticValidationError

from dxt.exceptions import ValidationError
from dxt.models.pipeline import Pipeline


def substitute_env_vars(data: Any) -> Any:
    """Recursively substitute environment variables in data structure.

    Supports ${VAR_NAME} and ${VAR_NAME:-default} syntax.

    Args:
        data: Data structure (dict, list, str, etc.)

    Returns:
        Data with environment variables substituted

    Examples:
        >>> os.environ['DB_HOST'] = 'localhost'
        >>> substitute_env_vars('postgresql://${DB_HOST}:5432/mydb')
        'postgresql://localhost:5432/mydb'
        >>> substitute_env_vars('${MISSING:-default_value}')
        'default_value'
    """
    if isinstance(data, dict):
        return {k: substitute_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [substitute_env_vars(item) for item in data]
    elif isinstance(data, str):
        # Pattern matches ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r'\$\{([^}:]+)(?::-([^}]*))?\}'

        def replace_var(match: re.Match) -> str:
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ""

            # Get from environment, use default if not found
            value = os.environ.get(var_name, default_value)

            # If no value found and no default, raise error
            if not value and default_value == "":
                raise ValidationError(
                    f"Environment variable '{var_name}' not found and no default provided"
                )

            return value

        return re.sub(pattern, replace_var, data)
    else:
        return data


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file into a dictionary with environment variable substitution.

    Supports ${VAR_NAME} and ${VAR_NAME:-default} syntax for environment variables.

    Args:
        path: Path to YAML file

    Returns:
        Dictionary with YAML contents and environment variables substituted

    Raises:
        ValidationError: If file cannot be read or parsed, or required env vars are missing
    """
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)
            if data is None:
                raise ValidationError(f"Empty YAML file: {path}")
            # Substitute environment variables
            return substitute_env_vars(data)
    except yaml.YAMLError as e:
        raise ValidationError(f"Invalid YAML in {path}: {e}") from e
    except FileNotFoundError:
        raise ValidationError(f"File not found: {path}")
    except Exception as e:
        raise ValidationError(f"Failed to load {path}: {e}") from e


def load_pipeline(path: Path) -> Pipeline:
    """Load and validate a pipeline from a YAML file.

    Args:
        path: Path to pipeline YAML file

    Returns:
        Validated Pipeline object

    Raises:
        ValidationError: If pipeline is invalid
    """
    try:
        data = load_yaml(path)
        return Pipeline(**data)
    except PydanticValidationError as e:
        raise ValidationError(f"Invalid pipeline in {path}: {e}") from e


def save_yaml(data: dict[str, Any], path: Path) -> None:
    """Save dictionary to YAML file.

    Args:
        data: Dictionary to save
        path: Path to save to

    Raises:
        ValidationError: If save fails
    """
    try:
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            yaml.safe_dump(
                data,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
    except Exception as e:
        raise ValidationError(f"Failed to save YAML to {path}: {e}") from e
