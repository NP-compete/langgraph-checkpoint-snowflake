# Contributing to LangGraph Checkpoint Snowflake

Thank you for your interest in contributing to this project. This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Style Guide](#style-guide)
- [Release Process](#release-process)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

### Prerequisites

- Python 3.10 or higher
- Git
- A Snowflake account (for integration testing)

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:

```bash
git clone https://github.com/YOUR_USERNAME/langgraph-checkpoint-snowflake.git
cd langgraph-checkpoint-snowflake
```

3. Add the upstream remote:

```bash
git remote add upstream https://github.com/sodutta/langgraph-checkpoint-snowflake.git
```

## Development Setup

### Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Install Dependencies

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Verify Setup

```bash
# Run linters
make lint

# Run tests
make test
```

## Making Changes

### Branch Naming

Create a branch for your changes:

```bash
git checkout -b <type>/<short-description>
```

Branch types:
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test additions or fixes
- `chore/` - Maintenance tasks

Examples:
- `feature/add-connection-pooling`
- `fix/retry-logic-timeout`
- `docs/update-readme`

### Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

Types:
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation
- `style` - Formatting (no code change)
- `refactor` - Code restructuring
- `test` - Adding tests
- `chore` - Maintenance

Examples:
```
feat(cache): add LRU cache for checkpoint reads

Implements an optional in-memory cache to reduce database queries
for frequently accessed checkpoints.

- Add CacheConfig dataclass
- Add CheckpointCache class with TTL support
- Integrate cache into get_tuple method
```

```
fix(retry): handle warehouse suspension errors

Adds SnowflakeWarehouseError to the list of retryable errors
to handle cases where the warehouse is starting up.

Fixes #42
```

## Testing

### Running Tests

```bash
# Run all tests (excluding integration)
make test

# Run specific test file
pytest tests/test_sync.py -v

# Run with coverage
pytest tests/ --cov=src/langgraph_checkpoint_snowflake --cov-report=html

# Run property-based tests
pytest tests/test_property.py -v
```

### Integration Tests

Integration tests require a live Snowflake connection:

```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"

# Run integration tests
pytest tests/test_integration.py -v
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_<module>.py`
- Name test functions `test_<description>`
- Use pytest fixtures for setup/teardown
- Mock external dependencies (Snowflake connector)

Example test:

```python
import pytest
from unittest.mock import MagicMock

from langgraph_checkpoint_snowflake import SnowflakeSaver


class TestSnowflakeSaver:
    @pytest.fixture
    def mock_conn(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        return conn

    def test_setup_creates_tables(self, mock_conn):
        saver = SnowflakeSaver(mock_conn)
        saver.setup()

        assert saver.is_setup
        assert mock_conn.cursor.called
```

## Submitting Changes

### Before Submitting

1. Ensure all tests pass:
   ```bash
   make test
   ```

2. Run linters and fix any issues:
   ```bash
   make lint
   make format
   ```

3. Update documentation if needed

4. Add or update tests for your changes

### Pull Request Process

1. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature
   ```

2. Open a Pull Request against the `main` branch

3. Fill out the PR template with:
   - Description of changes
   - Related issues
   - Testing performed
   - Checklist completion

4. Wait for CI checks to pass

5. Address review feedback

6. Once approved, a maintainer will merge your PR

### PR Checklist

- [ ] Code follows the project style guide
- [ ] Tests added/updated for changes
- [ ] Documentation updated if needed
- [ ] All CI checks pass
- [ ] Commit messages follow conventions
- [ ] No unrelated changes included

## Style Guide

### Python Style

This project uses:
- [Ruff](https://github.com/astral-sh/ruff) for linting and formatting
- [mypy](https://mypy.readthedocs.io/) for type checking

Key conventions:
- Line length: 88 characters
- Use type hints for all public functions
- Use docstrings (Google style) for public APIs
- Prefer `from __future__ import annotations` for forward references

### Code Organization

```
src/langgraph_checkpoint_snowflake/
    __init__.py          # Main SnowflakeSaver class, public API
    aio.py               # AsyncSnowflakeSaver class
    base.py              # BaseSnowflakeSaver with shared logic
    _internal.py         # Internal utilities (not public API)
    exceptions.py        # Custom exception classes
```

### Docstring Format

Use Google-style docstrings:

```python
def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
    """Get a checkpoint tuple from the database.

    Args:
        config: Configuration specifying which checkpoint to retrieve.
            Must contain `thread_id` in configurable.

    Returns:
        The checkpoint tuple if found, None otherwise.

    Raises:
        SnowflakeConnectionError: If database connection fails.

    Example:
        >>> config = {"configurable": {"thread_id": "my-thread"}}
        >>> checkpoint = saver.get_tuple(config)
    """
```

### Import Order

Imports should be organized as:
1. Standard library
2. Third-party packages
3. Local imports

Ruff will automatically sort imports when you run `make format`.

## Release Process

Releases are automated via GitHub Actions when a tag is pushed.

### Version Numbering

This project follows [Semantic Versioning](https://semver.org/):
- MAJOR: Breaking API changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes (backward compatible)

### Creating a Release

1. Update version in `src/langgraph_checkpoint_snowflake/__init__.py`
2. Update CHANGELOG.md
3. Create and push a tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. GitHub Actions will automatically publish to PyPI

## Questions?

If you have questions about contributing:
- Open a [GitHub Discussion](https://github.com/sodutta/langgraph-checkpoint-snowflake/discussions)
- Check existing [Issues](https://github.com/sodutta/langgraph-checkpoint-snowflake/issues)

Thank you for contributing!
