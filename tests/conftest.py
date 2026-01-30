"""Pytest configuration and fixtures for langgraph-checkpoint-snowflake tests."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from langgraph_checkpoint_snowflake import SnowflakeSaver


# Check if we have real Snowflake credentials
def has_snowflake_credentials() -> bool:
    """Check if Snowflake credentials are available in environment."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    return all(os.environ.get(var) for var in required_vars)


# Skip marker for integration tests
requires_snowflake = pytest.mark.skipif(
    not has_snowflake_credentials(),
    reason="Snowflake credentials not available",
)


class MockCursor:
    """Mock Snowflake cursor for unit tests."""

    def __init__(self) -> None:
        self.executed_queries: list[tuple[str, tuple[Any, ...] | None]] = []
        self.results: list[tuple[Any, ...]] = []
        self._result_index = 0
        self._rowcount: int | None = None

    @property
    def rowcount(self) -> int | None:
        """Return the number of rows affected by the last operation."""
        return self._rowcount

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> MockCursor:
        self.executed_queries.append((query, params))
        return self

    def executemany(self, query: str, params_list: list[tuple[Any, ...]]) -> MockCursor:
        for params in params_list:
            self.executed_queries.append((query, params))
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._result_index < len(self.results):
            result = self.results[self._result_index]
            self._result_index += 1
            return result
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        results = self.results[self._result_index :]
        self._result_index = len(self.results)
        return results

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        while self._result_index < len(self.results):
            yield self.results[self._result_index]
            self._result_index += 1

    def close(self) -> None:
        pass

    def set_results(self, results: list[tuple[Any, ...]]) -> None:
        """Set the results that will be returned by fetch methods."""
        self.results = results
        self._result_index = 0


class MockConnection:
    """Mock Snowflake connection for unit tests."""

    def __init__(self) -> None:
        self._cursor = MockCursor()
        self._closed = False

    def cursor(self) -> MockCursor:
        return self._cursor

    def close(self) -> None:
        self._closed = True

    @property
    def is_closed(self) -> bool:
        return self._closed


@pytest.fixture
def mock_conn() -> MockConnection:
    """Fixture providing a mock Snowflake connection."""
    return MockConnection()


@pytest.fixture
def mock_cursor(mock_conn: MockConnection) -> MockCursor:
    """Fixture providing the mock cursor from mock connection."""
    return mock_conn._cursor


@pytest.fixture
def mock_saver(mock_conn: MockConnection) -> SnowflakeSaver:
    """Fixture providing a SnowflakeSaver with mock connection."""
    from langgraph_checkpoint_snowflake import SnowflakeSaver

    saver = SnowflakeSaver(mock_conn)  # type: ignore
    saver.is_setup = True  # Skip setup
    return saver


@pytest.fixture
def test_config() -> dict[str, Any]:
    """Fixture providing a test configuration."""
    return {
        "configurable": {
            "thread_id": f"test-thread-{uuid4().hex[:8]}",
            "checkpoint_ns": "",
        }
    }


@pytest.fixture
def test_checkpoint() -> dict[str, Any]:
    """Fixture providing a test checkpoint."""
    return {
        "v": 1,
        "id": str(uuid4()),
        "ts": "2024-01-01T00:00:00+00:00",
        "channel_values": {"messages": ["hello", "world"]},
        "channel_versions": {"messages": "1.0"},
        "versions_seen": {},
    }


@pytest.fixture
def test_metadata() -> dict[str, Any]:
    """Fixture providing test metadata."""
    return {
        "source": "input",
        "step": 0,
    }


# Integration test fixtures (only used when Snowflake credentials are available)


@contextmanager
def create_test_schema() -> Iterator[str]:
    """Create a temporary test schema in Snowflake."""
    import snowflake.connector

    schema_name = f"test_langgraph_{uuid4().hex[:8]}"

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )

    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cur.close()
        yield schema_name
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        cur.close()
        conn.close()


@pytest.fixture
def integration_saver() -> Iterator[SnowflakeSaver]:
    """Fixture providing a real SnowflakeSaver for integration tests."""
    if not has_snowflake_credentials():
        pytest.skip("Snowflake credentials not available")

    from langgraph_checkpoint_snowflake import SnowflakeSaver

    with (
        create_test_schema() as schema_name,
        SnowflakeSaver.from_conn_string(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=schema_name,
        ) as saver,
    ):
        saver.setup()
        yield saver


@pytest.fixture
async def async_integration_saver() -> Any:
    """Fixture providing a real AsyncSnowflakeSaver for integration tests."""
    if not has_snowflake_credentials():
        pytest.skip("Snowflake credentials not available")

    from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

    with create_test_schema() as schema_name:
        async with AsyncSnowflakeSaver.from_conn_string(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=schema_name,
        ) as saver:
            await saver.setup()
            yield saver
