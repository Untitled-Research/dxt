# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.2] - First working pipeline

### Added
- Environment variable substitution in YAML files with `${VAR_NAME}` and `${VAR_NAME:-default}` syntax
- Flexible source/target reference system with smart type inference
- Support for explicit SQL queries via `type: sql` configuration
- Support for PostgreSQL table-valued functions via `type: function`
- SourceConfig model for normalized internal representation of sources and targets
- Comprehensive README documentation with quick start guide
- Example pipelines demonstrating shorthand and explicit configurations

### Changed
- **BREAKING**: Removed `kind` field from Stream model
- Source and target references now use `SourceConfig` objects internally
- Type inference automatically detects SQL queries, file paths, URLs, and relations
- CLI validation output now shows source/target types instead of kind
- PostgreSQL extractor now handles different source types (relation, sql, function)

### Fixed
- PostgreSQL loader now correctly uses target value from SourceConfig

## [0.0.1] - Initial Release

### Added
- Core pipeline execution engine
- YAML-based pipeline configuration
- PostgreSQL source and target support
- Parquet and memory buffer implementations
- CLI with run, validate, and init commands
- Full and incremental extraction modes
- Append, replace, and upsert load modes
- Batch processing support
- Type system with DXT internal types
- Field mapping and schema management
