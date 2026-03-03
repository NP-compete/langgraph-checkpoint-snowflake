"""Tests for new features: retry, metrics, TTL, connection pooling."""

from __future__ import annotations

import time
from datetime import timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from tests.conftest import MockConnection, MockCursor


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_config(self) -> None:
        """Test default retry configuration."""
        from langgraph_checkpoint_snowflake import RetryConfig

        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.jitter is True

    def test_custom_config(self) -> None:
        """Test custom retry configuration."""
        from langgraph_checkpoint_snowflake import RetryConfig

        config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            jitter=False,
        )
        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.jitter is False


class TestMetrics:
    """Tests for Metrics collection."""

    def test_metrics_disabled_by_default(self) -> None:
        """Test that metrics are disabled by default."""
        from langgraph_checkpoint_snowflake import Metrics

        metrics = Metrics()
        assert metrics.enabled is False

    def test_metrics_record_when_enabled(self) -> None:
        """Test that metrics are recorded when enabled."""
        from langgraph_checkpoint_snowflake import Metrics

        metrics = Metrics(enabled=True)
        metrics.record("get_tuple", 0.5)
        metrics.record("get_tuple", 0.3)
        metrics.record("put", 1.0)

        stats = metrics.get_stats()
        assert stats["operation_counts"]["get_tuple"] == 2
        assert stats["operation_counts"]["put"] == 1
        assert stats["operation_durations"]["get_tuple"] == pytest.approx(0.8, rel=0.01)
        assert stats["average_durations"]["get_tuple"] == pytest.approx(0.4, rel=0.01)

    def test_metrics_record_errors(self) -> None:
        """Test that errors are recorded in metrics."""
        from langgraph_checkpoint_snowflake import Metrics

        metrics = Metrics(enabled=True)
        metrics.record("get_tuple", 0.5, ValueError("test error"))

        stats = metrics.get_stats()
        assert stats["error_counts"]["ValueError"] == 1

    def test_metrics_callback(self) -> None:
        """Test metrics callback function."""
        from langgraph_checkpoint_snowflake import Metrics

        callback_calls: list[tuple[str, float, Exception | None]] = []

        def callback(op: str, duration: float, error: Exception | None) -> None:
            callback_calls.append((op, duration, error))

        metrics = Metrics(enabled=True, on_operation=callback)
        metrics.record("test_op", 0.5)

        assert len(callback_calls) == 1
        assert callback_calls[0][0] == "test_op"
        assert callback_calls[0][1] == 0.5

    def test_metrics_reset(self) -> None:
        """Test metrics reset."""
        from langgraph_checkpoint_snowflake import Metrics

        metrics = Metrics(enabled=True)
        metrics.record("op1", 1.0)
        metrics.record("op2", 2.0)

        metrics.reset()
        stats = metrics.get_stats()

        assert stats["operation_counts"] == {}
        assert stats["operation_durations"] == {}
        assert stats["error_counts"] == {}


class TestSnowflakeSaverWithMetrics:
    """Tests for SnowflakeSaver with metrics enabled."""

    def test_saver_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that saver records metrics."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
        }

        saver.put(config, checkpoint, {}, {})

        stats = saver.get_metrics()
        assert stats is not None
        assert "put" in stats["operation_counts"]

    def test_saver_without_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that saver works without metrics."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        assert saver.get_metrics() is None


class TestSnowflakeSaverWithRetry:
    """Tests for SnowflakeSaver with retry configuration."""

    def test_saver_with_custom_retry(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that saver uses custom retry config."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(max_retries=5, base_delay=0.1)
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore
        saver.is_setup = True

        assert saver.retry_config.max_retries == 5
        assert saver.retry_config.base_delay == 0.1


class TestTTLExpiration:
    """Tests for TTL/expiration functionality."""

    def test_delete_before(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test delete_before removes old checkpoints."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Mock rowcount for delete
        mock_cursor._rowcount = 5

        saver.delete_before(timedelta(days=7))

        # Verify delete queries were executed
        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) >= 1

    def test_get_checkpoint_count(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_count returns count."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([(42,)])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        count = saver.get_checkpoint_count()
        assert count == 42

    def test_get_checkpoint_counts_by_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_counts_by_thread returns grouped counts."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([("thread-1", 10), ("thread-2", 5)])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        counts = saver.get_checkpoint_counts_by_thread(max_results=10)
        assert counts == [("thread-1", 10), ("thread-2", 5)]


class TestConnectionPool:
    """Tests for ConnectionPool."""

    def test_pool_creation(self) -> None:
        """Test connection pool creation."""
        from langgraph_checkpoint_snowflake._internal import (
            ConnectionPool,
            PoolConfig,
        )

        mock_factory = MagicMock()
        pool = ConnectionPool(mock_factory, PoolConfig(pool_size=3))

        assert pool.config.pool_size == 3
        assert pool.config.pool_timeout == 30.0

    def test_pool_get_and_return(self) -> None:
        """Test getting and returning connections."""
        from langgraph_checkpoint_snowflake._internal import ConnectionPool

        mock_conn = MagicMock()
        mock_factory = MagicMock(return_value=mock_conn)

        pool = ConnectionPool(mock_factory)

        conn = pool.get_connection()
        assert conn == mock_conn

        pool.return_connection(conn)

    def test_pool_context_manager(self) -> None:
        """Test pool connection context manager."""
        from langgraph_checkpoint_snowflake._internal import ConnectionPool

        mock_conn = MagicMock()
        mock_factory = MagicMock(return_value=mock_conn)

        pool = ConnectionPool(mock_factory)

        with pool.connection() as conn:
            assert conn == mock_conn

    def test_pool_close(self) -> None:
        """Test pool close."""
        from langgraph_checkpoint_snowflake._internal import ConnectionPool

        mock_conn = MagicMock()
        mock_factory = MagicMock(return_value=mock_conn)

        pool = ConnectionPool(mock_factory)
        conn = pool.get_connection()
        pool.return_connection(conn)

        pool.close()

        with pytest.raises(RuntimeError, match="closed"):
            pool.get_connection()


class TestTimedOperation:
    """Tests for timed_operation context manager."""

    def test_timed_operation_records_duration(self) -> None:
        """Test that timed_operation records duration."""
        from langgraph_checkpoint_snowflake._internal import Metrics, timed_operation

        metrics = Metrics(enabled=True)

        with timed_operation(metrics, "test_op"):
            time.sleep(0.01)

        stats = metrics.get_stats()
        assert "test_op" in stats["operation_counts"]
        assert stats["operation_durations"]["test_op"] >= 0.01

    def test_timed_operation_records_errors(self) -> None:
        """Test that timed_operation records errors."""
        from langgraph_checkpoint_snowflake._internal import Metrics, timed_operation

        metrics = Metrics(enabled=True)

        with pytest.raises(ValueError):
            with timed_operation(metrics, "test_op"):
                raise ValueError("test error")

        stats = metrics.get_stats()
        assert stats["error_counts"]["ValueError"] == 1

    def test_timed_operation_with_none_metrics(self) -> None:
        """Test that timed_operation works with None metrics."""
        from langgraph_checkpoint_snowflake._internal import timed_operation

        # Should not raise
        with timed_operation(None, "test_op"):
            pass


class TestAsyncFeatures:
    """Tests for async features."""

    @pytest.mark.asyncio
    async def test_async_saver_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async saver with metrics."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver
        from langgraph_checkpoint_snowflake._internal import Metrics

        metrics = Metrics(enabled=True)
        saver = AsyncSnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
        }

        await saver.aput(config, checkpoint, {}, {})

        stats = saver.get_metrics()
        assert stats is not None
        assert "aput" in stats["operation_counts"]

    @pytest.mark.asyncio
    async def test_async_delete_before(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async delete_before."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor._rowcount = 3

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        await saver.adelete_before(timedelta(days=7))

        # Verify delete queries were executed
        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) >= 1

    @pytest.mark.asyncio
    async def test_async_get_checkpoint_count(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async get_checkpoint_count."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([(100,)])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        count = await saver.aget_checkpoint_count()
        assert count == 100


class TestErrorGuidance:
    """Tests for actionable error messages with troubleshooting guidance."""

    def test_connection_error_with_guidance(self) -> None:
        """Test that SnowflakeConnectionError.with_guidance includes troubleshooting."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeConnectionError,
        )

        original = ConnectionError("Could not connect to Snowflake backend")
        err = SnowflakeConnectionError.with_guidance(original)

        assert isinstance(err, SnowflakeConnectionError)
        assert err.original_error is original
        assert "Troubleshooting" in err.message
        assert "SNOWFLAKE_ACCOUNT" in err.message
        assert "docs.snowflake.com" in err.message

    def test_authentication_error_with_guidance(self) -> None:
        """Test that SnowflakeAuthenticationError.with_guidance includes troubleshooting."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeAuthenticationError,
        )

        original = PermissionError("Invalid credentials")
        err = SnowflakeAuthenticationError.with_guidance(original)

        assert isinstance(err, SnowflakeAuthenticationError)
        assert err.original_error is original
        assert "Troubleshooting" in err.message
        assert "SNOWFLAKE_USER" in err.message
        assert "key pair auth" in err.message.lower()
        assert "RSA_PUBLIC_KEY" in err.message

    def test_warehouse_error_with_guidance(self) -> None:
        """Test that SnowflakeWarehouseError.with_guidance includes troubleshooting."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeWarehouseError,
        )

        original = RuntimeError("Warehouse suspended")
        err = SnowflakeWarehouseError.with_guidance(
            original, warehouse="COMPUTE_WH", retry_count=3
        )

        assert isinstance(err, SnowflakeWarehouseError)
        assert err.original_error is original
        assert err.retry_count == 3
        assert "COMPUTE_WH" in err.message
        assert "Troubleshooting" in err.message
        assert "ALTER WAREHOUSE" in err.message

    def test_warehouse_error_with_guidance_default_name(self) -> None:
        """Test warehouse error guidance with no warehouse name."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeWarehouseError,
        )

        original = RuntimeError("Warehouse error")
        err = SnowflakeWarehouseError.with_guidance(original)

        assert "<warehouse>" in err.message

    def test_configuration_error_with_guidance_list(self) -> None:
        """Test that SnowflakeConfigurationError.with_guidance formats missing params list."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeConfigurationError,
        )

        err = SnowflakeConfigurationError.with_guidance(
            ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"]
        )

        assert isinstance(err, SnowflakeConfigurationError)
        assert "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER" in err.message
        assert "Required environment variables" in err.message
        assert "SNOWFLAKE_PASSWORD" in err.message
        assert "SNOWFLAKE_PRIVATE_KEY_PATH" in err.message

    def test_configuration_error_with_guidance_string(self) -> None:
        """Test configuration error guidance with a string param."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeConfigurationError,
        )

        err = SnowflakeConfigurationError.with_guidance("missing auth method")

        assert "missing auth method" in err.message
        assert "Required environment variables" in err.message

    def test_configuration_error_with_guidance_none(self) -> None:
        """Test configuration error guidance with no params."""
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeConfigurationError,
        )

        err = SnowflakeConfigurationError.with_guidance()

        assert "unknown" in err.message

    def test_configuration_error_raised_for_missing_env(self) -> None:
        """Test that get_connection_params_from_env raises SnowflakeConfigurationError."""
        import os

        from langgraph_checkpoint_snowflake._internal import (
            get_connection_params_from_env,
        )
        from langgraph_checkpoint_snowflake.exceptions import (
            SnowflakeConfigurationError,
        )

        # Clear all Snowflake env vars
        env_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            "SNOWFLAKE_PRIVATE_KEY",
        ]
        saved = {k: os.environ.pop(k, None) for k in env_vars}

        try:
            with pytest.raises(SnowflakeConfigurationError) as exc_info:
                get_connection_params_from_env()
            assert "Required environment variables" in str(exc_info.value)
        finally:
            # Restore env vars
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v
