# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

XLT (pronounced "ex-ell-tee") is a lightweight Extract-Load tool for building simple, reliable batch data movement pipelines. It aims to be a YAML-first, CLI-driven alternative to writing custom Python scripts or deploying full ELT platforms.

**Current Status**: Active development - basic CLI and package structure implemented.

**Core Principles**:
- Simplicity over magic
- Declarative over imperative
- Small surface area
- No servers required
- Do one thing well: move data

## Development Environment

The project uses a devcontainer setup with:
- **Python**: 3.14
- **PostgreSQL**: 17 (runs on port 5532 externally, 5432 internally)
- **Node.js**: 24
- **Package Manager**: uv (installed via Astral)
- **Additional Tools**: PostgreSQL client, pylint

### Starting Development

1. Open the project in VS Code with the Dev Containers extension
2. The container will automatically build and run the `postCreateCommand`: `bash scripts/dev-init.sh`
3. The workspace is mounted at `/workspace` inside the container
4. XLT is installed in editable mode and available as the `xlt` command

### Database Access

A PostgreSQL instance runs in the container network:
- **Host**: `postgres` (when connecting from within the dev container)
- **Port**: 5432 (internal), 5532 (external from host)
- Database credentials are configured via the `.env` file

Sample data is available at [assets/data/dvdrental.tar](assets/data/dvdrental.tar) for testing.

## Project Architecture (Planned)

Based on the README, XLT will be structured around:
- **Extractors**: Pull data from sources (databases, APIs, files, cloud warehouses)
- **Loaders**: Write data to destinations
- **Pipeline Engine**: Orchestrates extract and load steps defined in YAML
- **Incremental Patterns**: Watermark & key-based incremental loading
- **Load Strategies**: Append, truncate-insert, merge (planned)

The tool is designed to be:
- Portable (runs anywhere Python runs)
- Orchestration-agnostic (integrates with Airflow, Prefect, cron, GitHub Actions)
- Git-native (pipelines defined in version-controlled YAML files)

## Development Commands

### Installation
```bash
uv pip install -e ".[dev]"  # Install in editable mode with dev dependencies (uses uv for speed)
```

### XLT CLI Commands
```bash
xlt --version              # Show version
xlt --help                 # Show help
xlt run <pipeline.yaml>    # Run a pipeline (not yet implemented)
xlt validate <pipeline.yaml>  # Validate a pipeline (not yet implemented)
xlt init [name]            # Create a pipeline template (not yet implemented)
```

### Development Tools
```bash
pytest                     # Run tests
pytest --cov=xlt          # Run tests with coverage
ruff check .              # Lint code
ruff format .             # Format code
mypy xlt                  # Type check
```

### Project Structure
```
xlt/
├── __init__.py          # Package initialization with version
├── cli.py               # Typer-based CLI entry point
├── (to be created)      # Core engine, extractors, loaders, etc.
```
