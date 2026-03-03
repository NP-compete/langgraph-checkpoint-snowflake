# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Repository best practices: SECURITY.md, CODE_OF_CONDUCT.md, issue templates, PR template
- Comprehensive GitHub labels for issue triage

## [0.1.0] - 2026-01-30

### Added
- Initial release of langgraph-checkpoint-snowflake
- `SnowflakeSaver` - Synchronous checkpoint saver for LangGraph
- `AsyncSnowflakeSaver` - Async-compatible checkpoint saver (uses thread pool)
- Support for password and key pair authentication
- Automatic table creation via `setup()` method
- In-memory LRU cache for read optimization (`CacheConfig`)
- Configurable retry with exponential backoff (`RetryConfig`)
- Operation metrics collection (`Metrics`)
- TTL-based checkpoint cleanup (`delete_before()`)
- Batch operations for thread management
- Built-in benchmarking utility
- Property-based tests using Hypothesis
- Redis/Valkey write-back cache for 100x faster writes (`RedisWriteCacheConfig`)
  - Background sync to Snowflake
  - Distributed locking for multi-pod Kubernetes deployments
  - Graceful shutdown with flush
  - Valkey-compatible commands only

### Security
- Key pair authentication support for production deployments
- No credential logging
- TLS connections by default

## Types of Changes

- `Added` for new features
- `Changed` for changes in existing functionality
- `Deprecated` for soon-to-be removed features
- `Removed` for now removed features
- `Fixed` for any bug fixes
- `Security` for vulnerability fixes

[Unreleased]: https://github.com/NP-compete/langgraph-checkpoint-snowflake/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/NP-compete/langgraph-checkpoint-snowflake/releases/tag/v0.1.0
