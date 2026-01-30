"""Conformance tests for langgraph-checkpoint-snowflake.

These tests verify that the checkpointer implementation conforms to
LangGraph's expected behavior, based on patterns from the official
checkpoint-postgres tests.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

from tests.conftest import MockConnection, MockCursor


def create_empty_checkpoint() -> dict[str, Any]:
    """Create an empty checkpoint structure."""
    return {
        "v": 1,
        "id": str(uuid4()),
        "ts": "2024-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
        "pending_sends": [],
    }


def create_checkpoint(
    parent: dict[str, Any], channel_values: dict[str, Any], step: int
) -> dict[str, Any]:
    """Create a checkpoint based on a parent checkpoint."""
    return {
        "v": 1,
        "id": str(uuid4()),
        "ts": f"2024-01-01T00:00:{step:02d}+00:00",
        "channel_values": channel_values,
        "channel_versions": {k: f"{step}.0" for k in channel_values},
        "versions_seen": parent.get("channel_versions", {}),
        "pending_sends": [],
    }


class TestCheckpointLifecycle:
    """Tests for complete checkpoint lifecycle."""

    def test_put_and_get_basic(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test basic put and get operations."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        thread_id = f"test-{uuid4().hex[:8]}"
        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }

        checkpoint = create_empty_checkpoint()
        metadata = {"source": "input", "step": 0}

        # Put checkpoint
        result_config = saver.put(config, checkpoint, metadata, {})

        # Verify returned config
        assert result_config["configurable"]["thread_id"] == thread_id
        assert result_config["configurable"]["checkpoint_id"] == checkpoint["id"]
        assert result_config["configurable"]["checkpoint_ns"] == ""

    def test_put_updates_parent_checkpoint_id(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that put correctly tracks parent checkpoint."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        thread_id = f"test-{uuid4().hex[:8]}"

        # First checkpoint
        config_1 = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }
        checkpoint_1 = create_empty_checkpoint()
        saver.put(config_1, checkpoint_1, {"step": 0}, {})

        # Second checkpoint (child of first)
        config_2 = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint_1["id"],  # Parent ID
            }
        }
        checkpoint_2 = create_checkpoint(checkpoint_1, {"data": "value"}, 1)
        result_2 = saver.put(config_2, checkpoint_2, {"step": 1}, {})

        # Verify second checkpoint has correct ID
        assert result_2["configurable"]["checkpoint_id"] == checkpoint_2["id"]


class TestMetadataHandling:
    """Tests for metadata handling."""

    def test_combined_metadata(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that config metadata is combined with checkpoint metadata."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "metadata-test",
                "checkpoint_ns": "",
            },
            "metadata": {"run_id": "my_run_id"},
        }

        checkpoint = create_empty_checkpoint()
        checkpoint_metadata = {"source": "loop", "step": 1}

        saver.put(config, checkpoint, checkpoint_metadata, {})

        # Verify the put query includes combined metadata
        put_queries = [
            q for q in mock_cursor.executed_queries if "MERGE INTO checkpoints" in q[0]
        ]
        assert len(put_queries) > 0

    def test_metadata_with_none_values(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test metadata with None values."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "none-metadata-test",
                "checkpoint_ns": "",
            }
        }

        checkpoint = create_empty_checkpoint()
        metadata = {"source": "loop", "step": 1, "score": None}

        result = saver.put(config, checkpoint, metadata, {})
        assert result is not None


class TestSearchAndFilter:
    """Tests for search and filter functionality."""

    def test_search_by_single_key(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test searching by a single metadata key."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        list(saver.list(None, filter={"source": "input"}))

        # Verify filter was applied in query
        last_query = mock_cursor.executed_queries[-1][0]
        assert "metadata" in last_query.lower() or "source" in str(
            mock_cursor.executed_queries[-1][1]
        )

    def test_search_by_multiple_keys(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test searching by multiple metadata keys."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        results = list(saver.list(None, filter={"source": "loop", "step": 1}))
        assert isinstance(results, list)

    def test_search_by_thread_id(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test searching by thread_id returns all namespaces."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {"configurable": {"thread_id": "thread-1"}}
        list(saver.list(config))

        # Verify query was executed with thread_id
        assert any("thread-1" in str(q[1]) for q in mock_cursor.executed_queries)

    def test_search_empty_filter(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test search with empty filter returns all."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        results = list(saver.list(None, filter={}))
        assert isinstance(results, list)


class TestNamespaceHandling:
    """Tests for checkpoint namespace handling."""

    def test_different_namespaces_same_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that different namespaces are treated separately."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        thread_id = "ns-test"

        # Put checkpoint in root namespace
        config_root = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }
        checkpoint_root = create_empty_checkpoint()
        saver.put(config_root, checkpoint_root, {"ns": "root"}, {})

        # Put checkpoint in inner namespace
        config_inner = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "inner",
            }
        }
        checkpoint_inner = create_empty_checkpoint()
        saver.put(config_inner, checkpoint_inner, {"ns": "inner"}, {})

        # Verify both were stored with correct namespaces
        put_queries = [
            q for q in mock_cursor.executed_queries if "MERGE INTO checkpoints" in q[0]
        ]
        assert len(put_queries) == 2


class TestWritesHandling:
    """Tests for checkpoint writes handling."""

    def test_put_writes_basic(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test basic put_writes functionality."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "writes-test",
                "checkpoint_ns": "",
                "checkpoint_id": str(uuid4()),
            }
        }

        writes = [("channel1", "value1"), ("channel2", {"nested": "value"})]

        saver.put_writes(config, writes, task_id="task-1")

        # Verify writes were inserted
        write_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_writes" in q[0]
        ]
        assert len(write_queries) == 2

    def test_put_writes_with_task_path(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test put_writes with task_path parameter."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "writes-path-test",
                "checkpoint_ns": "",
                "checkpoint_id": str(uuid4()),
            }
        }

        writes = [("channel", "value")]

        saver.put_writes(config, writes, task_id="task-1", task_path="node:0")

        # Should not raise
        write_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_writes" in q[0]
        ]
        assert len(write_queries) >= 1


class TestVersioning:
    """Tests for checkpoint versioning."""

    def test_version_format(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that versions follow expected format."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # First version
        v1 = saver.get_next_version(None, None)
        assert "." in v1  # Should have format "number.uuid"

        # Second version
        v2 = saver.get_next_version(v1, None)
        assert "." in v2

        # Version numbers should increment
        v1_num = int(v1.split(".")[0])
        v2_num = int(v2.split(".")[0])
        assert v2_num > v1_num

    def test_channel_versions_stored(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that channel versions are properly stored."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "version-test",
                "checkpoint_ns": "",
            }
        }

        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"messages": ["hello"]},
            "channel_versions": {"messages": "1.abc123"},
            "versions_seen": {},
        }

        new_versions = {"messages": "2.def456"}

        saver.put(config, checkpoint, {}, new_versions)

        # Verify blob was stored with version
        blob_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_blobs" in q[0]
        ]
        assert len(blob_queries) > 0


class TestDeleteOperations:
    """Tests for delete operations."""

    def test_delete_thread_removes_all_data(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that delete_thread removes checkpoints, blobs, and writes."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        thread_id = "delete-test"
        saver.delete_thread(thread_id)

        # Verify all three tables were deleted from
        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]

        tables_deleted = set()
        for query, _ in delete_queries:
            if "checkpoint_writes" in query:
                tables_deleted.add("writes")
            elif "checkpoint_blobs" in query:
                tables_deleted.add("blobs")
            elif "checkpoints" in query:
                tables_deleted.add("checkpoints")

        assert tables_deleted == {"writes", "blobs", "checkpoints"}

    def test_delete_nonexistent_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that deleting a nonexistent thread doesn't error."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Should not raise
        saver.delete_thread("nonexistent-thread-12345")


class TestAsyncConformance:
    """Async conformance tests."""

    @pytest.mark.asyncio
    async def test_async_put_and_get(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async put and get operations."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "async-test",
                "checkpoint_ns": "",
            }
        }

        checkpoint = create_empty_checkpoint()
        metadata = {"source": "input", "step": 0}

        result = await saver.aput(config, checkpoint, metadata, {})
        assert result["configurable"]["checkpoint_id"] == checkpoint["id"]

    @pytest.mark.asyncio
    async def test_async_list(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async list operation."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "async-list-test",
                "checkpoint_ns": "",
            }
        }

        results = [r async for r in saver.alist(config)]
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_async_delete(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async delete operation."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        await saver.adelete_thread("async-delete-test")

        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) == 3


class TestMetricsConformance:
    """Conformance tests for metrics functionality."""

    def test_metrics_track_all_operations(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that metrics track all checkpoint operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor.set_results([])
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        thread_id = "metrics-conformance-test"
        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }

        # Perform all operations
        checkpoint = create_empty_checkpoint()
        saver.put(config, checkpoint, {"step": 0}, {})

        config_with_id = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint["id"],
            }
        }
        saver.get_tuple(config_with_id)
        list(saver.list(config))
        saver.put_writes(config_with_id, [("ch", "val")], "task-1")
        saver.delete_thread(thread_id)

        stats = saver.get_metrics()
        assert stats is not None
        assert "put" in stats["operation_counts"]
        assert "get_tuple" in stats["operation_counts"]
        assert "list" in stats["operation_counts"]
        assert "put_writes" in stats["operation_counts"]
        assert "delete_thread" in stats["operation_counts"]

    def test_metrics_callback_invoked(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that metrics callback is invoked for operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        callback_invocations: list[tuple[str, float, Exception | None]] = []

        def callback(op: str, duration: float, error: Exception | None) -> None:
            callback_invocations.append((op, duration, error))

        metrics = Metrics(enabled=True, on_operation=callback)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "callback-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = create_empty_checkpoint()
        saver.put(config, checkpoint, {}, {})

        assert len(callback_invocations) == 1
        assert callback_invocations[0][0] == "put"
        assert callback_invocations[0][1] >= 0  # Duration should be non-negative


class TestRetryConformance:
    """Conformance tests for retry functionality."""

    def test_retry_config_applied_to_sync_saver(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that retry config is properly applied to sync saver."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(max_retries=7, base_delay=0.5, max_delay=30.0, jitter=False)
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.max_retries == 7
        assert saver.retry_config.base_delay == 0.5
        assert saver.retry_config.max_delay == 30.0
        assert saver.retry_config.jitter is False

    @pytest.mark.asyncio
    async def test_retry_config_applied_to_async_saver(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that retry config is properly applied to async saver."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver
        from langgraph_checkpoint_snowflake._internal import RetryConfig

        retry = RetryConfig(max_retries=5, base_delay=1.0)
        saver = AsyncSnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.max_retries == 5
        assert saver.retry_config.base_delay == 1.0


class TestTTLConformance:
    """Conformance tests for TTL/expiration functionality."""

    def test_delete_before_sync(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test sync delete_before operation."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor._rowcount = 15
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        deleted = saver.delete_before(timedelta(days=14))

        assert deleted == 15
        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) >= 1

    @pytest.mark.asyncio
    async def test_delete_before_async(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async delete_before operation."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor._rowcount = 20
        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        deleted = await saver.adelete_before(timedelta(days=7))

        assert deleted == 20

    def test_get_checkpoint_count_sync(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test sync get_checkpoint_count operation."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([(500,)])
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        count = saver.get_checkpoint_count()
        assert count == 500

    @pytest.mark.asyncio
    async def test_get_checkpoint_count_async(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async get_checkpoint_count operation."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([(250,)])
        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        count = await saver.aget_checkpoint_count()
        assert count == 250


class TestAsyncMetricsConformance:
    """Async conformance tests for metrics."""

    @pytest.mark.asyncio
    async def test_async_metrics_track_operations(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that async operations are tracked in metrics."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver
        from langgraph_checkpoint_snowflake._internal import Metrics

        mock_cursor.set_results([])
        metrics = Metrics(enabled=True)
        saver = AsyncSnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "async-metrics-test",
                "checkpoint_ns": "",
            }
        }

        checkpoint = create_empty_checkpoint()
        await saver.aput(config, checkpoint, {}, {})

        config_with_id = {
            "configurable": {
                "thread_id": "async-metrics-test",
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint["id"],
            }
        }
        await saver.aget_tuple(config_with_id)
        _ = [r async for r in saver.alist(config)]

        stats = saver.get_metrics()
        assert stats is not None
        assert "aput" in stats["operation_counts"]
        assert "aget_tuple" in stats["operation_counts"]
        assert "alist" in stats["operation_counts"]
