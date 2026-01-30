"""Unit tests for AsyncSnowflakeSaver."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tests.conftest import MockConnection, MockCursor, requires_snowflake

if TYPE_CHECKING:
    from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver


class TestAsyncSnowflakeSaverInit:
    """Tests for AsyncSnowflakeSaver initialization."""

    def test_init_with_connection(self, mock_conn: MockConnection) -> None:
        """Test initializing with a connection object."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        assert saver.conn is mock_conn
        assert saver.is_setup is False


class TestAsyncSnowflakeSaverSetup:
    """Tests for AsyncSnowflakeSaver.setup()."""

    @pytest.mark.asyncio
    async def test_setup_creates_tables(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that setup creates the required tables."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([(None,)])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        await saver.setup()

        assert saver.is_setup is True

    @pytest.mark.asyncio
    async def test_setup_is_idempotent(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that setup can be called multiple times safely."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([(None,)])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        await saver.setup()
        initial_query_count = len(mock_cursor.executed_queries)

        await saver.setup()
        assert len(mock_cursor.executed_queries) == initial_query_count


class TestAsyncSnowflakeSaverGetTuple:
    """Tests for AsyncSnowflakeSaver.aget_tuple()."""

    @pytest.mark.asyncio
    async def test_aget_tuple_not_found(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test aget_tuple returns None when checkpoint not found."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        result = await saver.aget_tuple(config)
        assert result is None

    @pytest.mark.asyncio
    async def test_aget_tuple_with_checkpoint_id(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test aget_tuple with specific checkpoint_id."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

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

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint_id,
            }
        }
        result = await saver.aget_tuple(config)

        assert result is not None
        assert result.config["configurable"]["checkpoint_id"] == checkpoint_id


class TestAsyncSnowflakeSaverList:
    """Tests for AsyncSnowflakeSaver.alist()."""

    @pytest.mark.asyncio
    async def test_alist_empty(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test alist returns empty when no checkpoints."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        results = [r async for r in saver.alist(config)]
        assert results == []

    @pytest.mark.asyncio
    async def test_alist_with_limit(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test alist respects limit parameter."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }
        [r async for r in saver.alist(config, limit=5)]

        last_query = mock_cursor.executed_queries[-1][0]
        assert "LIMIT 5" in last_query


class TestAsyncSnowflakeSaverPut:
    """Tests for AsyncSnowflakeSaver.aput()."""

    @pytest.mark.asyncio
    async def test_aput_checkpoint(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test putting a checkpoint asynchronously."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
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
            "channel_values": {"messages": ["hello"]},
            "channel_versions": {"messages": "1.0"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = await saver.aput(config, checkpoint, metadata, {"messages": "1.0"})

        assert result["configurable"]["checkpoint_id"] == checkpoint["id"]


class TestAsyncSnowflakeSaverPutWrites:
    """Tests for AsyncSnowflakeSaver.aput_writes()."""

    @pytest.mark.asyncio
    async def test_aput_writes(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test putting writes asynchronously."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
                "checkpoint_id": str(uuid4()),
            }
        }
        writes = [("channel1", "value1"), ("channel2", "value2")]

        await saver.aput_writes(config, writes, "task-1")

        write_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_writes" in q[0]
        ]
        assert len(write_queries) == 2


class TestAsyncSnowflakeSaverDeleteThread:
    """Tests for AsyncSnowflakeSaver.adelete_thread()."""

    @pytest.mark.asyncio
    async def test_adelete_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test deleting a thread asynchronously."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        await saver.adelete_thread("test-thread")

        delete_queries = [q for q in mock_cursor.executed_queries if "DELETE" in q[0]]
        assert len(delete_queries) == 3


class TestAsyncSnowflakeSaverConcurrency:
    """Tests for AsyncSnowflakeSaver concurrency handling."""

    @pytest.mark.asyncio
    async def test_concurrent_operations(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that concurrent operations are properly serialized."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
                "checkpoint_ns": "",
            }
        }

        # Run multiple operations concurrently
        tasks = [
            saver.aget_tuple(config),
            saver.aget_tuple(config),
            saver.aget_tuple(config),
        ]

        results = await asyncio.gather(*tasks)

        # All should complete without error
        assert len(results) == 3


# Integration tests


@requires_snowflake
class TestAsyncSnowflakeSaverIntegration:
    """Integration tests for AsyncSnowflakeSaver."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_full_checkpoint_lifecycle(
        self, async_integration_saver: AsyncSnowflakeSaver
    ) -> None:
        """Test complete checkpoint lifecycle with real Snowflake."""
        thread_id = f"async-integration-test-{uuid4().hex[:8]}"
        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }

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
        result_config = await async_integration_saver.aput(
            config, checkpoint, metadata, {"messages": "1.0"}
        )

        # Get checkpoint
        result = await async_integration_saver.aget_tuple(result_config)
        assert result is not None
        assert result.checkpoint["id"] == checkpoint["id"]

        # List checkpoints
        checkpoints = [c async for c in async_integration_saver.alist(config)]
        assert len(checkpoints) >= 1

        # Delete thread
        await async_integration_saver.adelete_thread(thread_id)

        # Verify deletion
        result = await async_integration_saver.aget_tuple(config)
        assert result is None
