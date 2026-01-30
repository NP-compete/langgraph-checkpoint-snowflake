"""Unit tests for SnowflakeSaver (sync implementation)."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from tests.conftest import MockConnection, MockCursor, requires_snowflake

if TYPE_CHECKING:
    from langgraph_checkpoint_snowflake import SnowflakeSaver


class TestSnowflakeSaverInit:
    """Tests for SnowflakeSaver initialization."""

    def test_init_with_connection(self, mock_conn: MockConnection) -> None:
        """Test initializing with a connection object."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        assert saver.conn is mock_conn
        assert saver.is_setup is False

    def test_init_with_custom_serde(self, mock_conn: MockConnection) -> None:
        """Test initializing with a custom serializer."""
        from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

        from langgraph_checkpoint_snowflake import SnowflakeSaver

        custom_serde = JsonPlusSerializer()
        saver = SnowflakeSaver(mock_conn, serde=custom_serde)  # type: ignore
        assert saver.serde is not None

    def test_init_with_retry_config(self, mock_conn: MockConnection) -> None:
        """Test initializing with retry configuration."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(
            max_retries=5, base_delay=2.0, max_delay=120.0, jitter=False
        )
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.max_retries == 5
        assert saver.retry_config.base_delay == 2.0
        assert saver.retry_config.max_delay == 120.0
        assert saver.retry_config.jitter is False

    def test_init_with_metrics(self, mock_conn: MockConnection) -> None:
        """Test initializing with metrics collector."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore

        assert saver.metrics is metrics
        assert saver.metrics.enabled is True

    def test_init_default_retry_config(self, mock_conn: MockConnection) -> None:
        """Test that default retry config is applied."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore

        assert saver.retry_config.max_retries == 3
        assert saver.retry_config.base_delay == 1.0
        assert saver.retry_config.jitter is True


class TestSnowflakeSaverSetup:
    """Tests for SnowflakeSaver.setup()."""

    def test_setup_creates_tables(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that setup creates the required tables."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        # Set up mock to return no existing migrations
        mock_cursor.set_results([(None,)])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.setup()

        assert saver.is_setup is True
        # Verify migrations table was created
        assert any(
            "checkpoint_migrations" in q[0] for q in mock_cursor.executed_queries
        )

    def test_setup_is_idempotent(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that setup can be called multiple times safely."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([(None,)])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.setup()
        initial_query_count = len(mock_cursor.executed_queries)

        # Second setup should be a no-op
        saver.setup()
        assert len(mock_cursor.executed_queries) == initial_query_count


class TestSnowflakeSaverGetTuple:
    """Tests for SnowflakeSaver.get_tuple()."""

    def test_get_tuple_not_found(
        self, mock_saver: SnowflakeSaver, mock_cursor: MockCursor
    ) -> None:
        """Test get_tuple returns None when checkpoint not found."""
        mock_cursor.set_results([])

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        result = mock_saver.get_tuple(config)
        assert result is None

    def test_get_tuple_with_checkpoint_id(
        self, mock_saver: SnowflakeSaver, mock_cursor: MockCursor
    ) -> None:
        """Test get_tuple with specific checkpoint_id."""
        checkpoint_id = str(uuid4())
        checkpoint_data = json.dumps(
            {
                "v": 1,
                "id": checkpoint_id,
                "ts": "2024-01-01T00:00:00+00:00",
                "channel_values": {},
                "channel_versions": {},
                "versions_seen": {},
            }
        )

        mock_cursor.set_results(
            [
                (
                    "test-thread",
                    checkpoint_data,
                    "",
                    checkpoint_id,
                    None,
                    "{}",
                    None,
                    None,
                )
            ]
        )

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint_id,
            }
        }
        result = mock_saver.get_tuple(config)

        assert result is not None
        assert result.config["configurable"]["checkpoint_id"] == checkpoint_id


class TestSnowflakeSaverList:
    """Tests for SnowflakeSaver.list()."""

    def test_list_empty(
        self, mock_saver: SnowflakeSaver, mock_cursor: MockCursor
    ) -> None:
        """Test list returns empty iterator when no checkpoints."""
        mock_cursor.set_results([])

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        results = list(mock_saver.list(config))
        assert results == []

    def test_list_with_limit(
        self, mock_saver: SnowflakeSaver, mock_cursor: MockCursor
    ) -> None:
        """Test list respects limit parameter."""
        mock_cursor.set_results([])

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        list(mock_saver.list(config, limit=5))

        # Verify LIMIT was added to query
        last_query = mock_cursor.executed_queries[-1][0]
        assert "LIMIT 5" in last_query


class TestSnowflakeSaverPut:
    """Tests for SnowflakeSaver.put()."""

    def test_put_checkpoint(
        self,
        mock_saver: SnowflakeSaver,
        mock_cursor: MockCursor,
        test_config: dict[str, Any],
        test_checkpoint: dict[str, Any],
        test_metadata: dict[str, Any],
    ) -> None:
        """Test putting a checkpoint."""
        result = mock_saver.put(
            test_config,
            test_checkpoint,
            test_metadata,
            {"messages": "1.0"},
        )

        assert result["configurable"]["checkpoint_id"] == test_checkpoint["id"]
        assert (
            result["configurable"]["thread_id"]
            == test_config["configurable"]["thread_id"]
        )

    def test_put_with_blob_values(
        self,
        mock_saver: SnowflakeSaver,
        mock_cursor: MockCursor,
        test_config: dict[str, Any],
        test_metadata: dict[str, Any],
    ) -> None:
        """Test putting a checkpoint with blob values."""
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {
                "simple": "string",
                "complex": {"nested": "object"},
            },
            "channel_versions": {"simple": "1.0", "complex": "1.0"},
            "versions_seen": {},
        }

        mock_saver.put(
            test_config,
            checkpoint,
            test_metadata,
            {"simple": "1.0", "complex": "1.0"},
        )

        # Verify blob upsert was called for complex value
        blob_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_blobs" in q[0]
        ]
        assert len(blob_queries) > 0


class TestSnowflakeSaverPutWrites:
    """Tests for SnowflakeSaver.put_writes()."""

    def test_put_writes(
        self,
        mock_saver: SnowflakeSaver,
        mock_cursor: MockCursor,
    ) -> None:
        """Test putting writes."""
        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
                "checkpoint_id": str(uuid4()),
            }
        }
        writes = [("channel1", "value1"), ("channel2", "value2")]

        mock_saver.put_writes(config, writes, "task-1")

        # Verify writes were inserted
        write_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_writes" in q[0]
        ]
        assert len(write_queries) == 2


class TestSnowflakeSaverDeleteThread:
    """Tests for SnowflakeSaver.delete_thread()."""

    def test_delete_thread(
        self,
        mock_saver: SnowflakeSaver,
        mock_cursor: MockCursor,
    ) -> None:
        """Test deleting a thread."""
        mock_saver.delete_thread("test-thread")

        # Verify all three tables were deleted from
        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) == 3

        tables_deleted = [q[0] for q in delete_queries]
        assert any("checkpoint_writes" in t for t in tables_deleted)
        assert any("checkpoint_blobs" in t for t in tables_deleted)
        assert any("checkpoints" in t for t in tables_deleted)


class TestSnowflakeSaverVersioning:
    """Tests for version generation."""

    def test_get_next_version_from_none(self, mock_saver: SnowflakeSaver) -> None:
        """Test generating first version."""
        version = mock_saver.get_next_version(None, None)
        assert version.startswith("00000000000000000000000000000001.")

    def test_get_next_version_increments(self, mock_saver: SnowflakeSaver) -> None:
        """Test version increments correctly."""
        v1 = mock_saver.get_next_version(None, None)
        v2 = mock_saver.get_next_version(v1, None)

        # Extract version numbers
        v1_num = int(v1.split(".")[0])
        v2_num = int(v2.split(".")[0])

        assert v2_num == v1_num + 1


class TestSnowflakeSaverAsyncErrors:
    """Tests for async method error handling."""

    @pytest.mark.asyncio
    async def test_aget_tuple_raises(self, mock_saver: SnowflakeSaver) -> None:
        """Test that async methods raise NotImplementedError."""
        config = {"configurable": {"thread_id": "test"}}

        with pytest.raises(NotImplementedError) as exc_info:
            await mock_saver.aget_tuple(config)

        assert "AsyncSnowflakeSaver" in str(exc_info.value)


class TestSnowflakeSaverMetrics:
    """Tests for metrics collection."""

    def test_metrics_recorded_on_put(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that metrics are recorded on put operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "metrics-test",
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
        assert stats["operation_counts"]["put"] == 1

    def test_metrics_recorded_on_get_tuple(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that metrics are recorded on get_tuple operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor.set_results([])
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "metrics-get-test",
                "checkpoint_ns": "",
            }
        }

        saver.get_tuple(config)

        stats = saver.get_metrics()
        assert stats is not None
        assert "get_tuple" in stats["operation_counts"]

    def test_metrics_recorded_on_list(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that metrics are recorded on list operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor.set_results([])
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "metrics-list-test",
                "checkpoint_ns": "",
            }
        }

        list(saver.list(config))

        stats = saver.get_metrics()
        assert stats is not None
        assert "list" in stats["operation_counts"]

    def test_metrics_recorded_on_delete_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that metrics are recorded on delete_thread operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        saver.delete_thread("test-thread")

        stats = saver.get_metrics()
        assert stats is not None
        assert "delete_thread" in stats["operation_counts"]

    def test_get_metrics_returns_none_when_disabled(
        self, mock_conn: MockConnection
    ) -> None:
        """Test that get_metrics returns None when metrics disabled."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        assert saver.get_metrics() is None


class TestSnowflakeSaverTTL:
    """Tests for TTL/expiration functionality."""

    def test_delete_before(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test delete_before removes old checkpoints."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor._rowcount = 10
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        deleted = saver.delete_before(timedelta(days=7))

        # Verify delete queries were executed
        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) >= 1
        assert deleted == 10

    def test_delete_before_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test delete_before records metrics."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor._rowcount = 5
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        saver.delete_before(timedelta(days=30))

        stats = saver.get_metrics()
        assert stats is not None
        assert "delete_before" in stats["operation_counts"]

    def test_get_checkpoint_count(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_count returns total count."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([(42,)])
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        count = saver.get_checkpoint_count()
        assert count == 42

    def test_get_checkpoint_count_empty(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_count returns 0 when empty."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        count = saver.get_checkpoint_count()
        assert count == 0

    def test_get_checkpoint_counts_by_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_counts_by_thread returns grouped counts."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results(
            [
                ("thread-1", 100),
                ("thread-2", 50),
                ("thread-3", 25),
            ]
        )
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        counts = saver.get_checkpoint_counts_by_thread(max_results=10)

        assert len(counts) == 3
        assert counts[0] == ("thread-1", 100)
        assert counts[1] == ("thread-2", 50)
        assert counts[2] == ("thread-3", 25)


class TestSnowflakeSaverRetry:
    """Tests for retry functionality."""

    def test_custom_retry_config_used(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that custom retry config is applied to operations."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(max_retries=10, base_delay=0.5)
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore
        saver.is_setup = True

        # Verify retry config is stored
        assert saver.retry_config.max_retries == 10
        assert saver.retry_config.base_delay == 0.5


# Integration tests (require real Snowflake connection)


@requires_snowflake
class TestSnowflakeSaverIntegration:
    """Integration tests for SnowflakeSaver."""

    @pytest.mark.integration
    def test_full_checkpoint_lifecycle(self, integration_saver: SnowflakeSaver) -> None:
        """Test complete checkpoint lifecycle with real Snowflake."""
        thread_id = f"integration-test-{uuid4().hex[:8]}"
        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }

        # Create checkpoint
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"messages": ["hello"]},
            "channel_versions": {"messages": "1.0"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        # Put checkpoint
        result_config = integration_saver.put(
            config, checkpoint, metadata, {"messages": "1.0"}
        )

        # Get checkpoint
        result = integration_saver.get_tuple(result_config)
        assert result is not None
        assert result.checkpoint["id"] == checkpoint["id"]

        # List checkpoints
        checkpoints = list(integration_saver.list(config))
        assert len(checkpoints) >= 1

        # Delete thread
        integration_saver.delete_thread(thread_id)

        # Verify deletion
        result = integration_saver.get_tuple(config)
        assert result is None
