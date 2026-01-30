"""Edge case tests for langgraph-checkpoint-snowflake.

Tests for:
- Unicode and special characters
- Large payloads
- Concurrent operations
- Boundary conditions
- Error handling
- Metrics edge cases
- Retry edge cases
- TTL edge cases
"""

from __future__ import annotations

import asyncio
import threading
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

from tests.conftest import MockConnection, MockCursor


class TestUnicodeAndSpecialCharacters:
    """Tests for unicode and special character handling."""

    def test_unicode_in_channel_values(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that unicode characters are preserved in channel values."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "unicode-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {
                "japanese": "こんにちは世界",
                "chinese": "你好世界",
                "korean": "안녕하세요",
                "emoji": "🎉🚀💻🔥",
                "arabic": "مرحبا بالعالم",
                "mixed": "Hello 世界 🌍",
            },
            "channel_versions": {},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, {})
        assert result["configurable"]["checkpoint_id"] == checkpoint["id"]

    def test_unicode_in_thread_id(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that unicode thread IDs work correctly."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "スレッド-123-🧵",
                "checkpoint_ns": "",
            }
        }

        result = saver.get_tuple(config)
        assert result is None

        # Verify the query was executed with unicode thread_id
        assert any("スレッド-123-🧵" in str(q[1]) for q in mock_cursor.executed_queries)

    def test_special_characters_in_metadata(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test special characters in metadata."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "special-chars-test",
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
        metadata = {
            "source": "input",
            "step": 0,
            "special": "quotes: \"'` and backslash: \\ and newline: \n",
            "json_like": '{"nested": "value"}',
        }

        result = saver.put(config, checkpoint, metadata, {})
        assert result is not None

    def test_null_character_handling(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that null characters are handled (stripped or escaped)."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "null-char-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"with_null": "before\x00after"},
            "channel_versions": {},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        # Should not raise
        result = saver.put(config, checkpoint, metadata, {})
        assert result is not None


class TestLargePayloads:
    """Tests for large payload handling."""

    def test_large_channel_values(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test handling of large channel values (>1MB)."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Create a large payload (~2MB)
        large_data = "x" * (2 * 1024 * 1024)

        config = {
            "configurable": {
                "thread_id": "large-payload-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"large": large_data},
            "channel_versions": {"large": "1.0"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, {"large": "1.0"})
        assert result is not None

    def test_many_channel_values(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test handling of many channel values."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Create many channels
        channel_values = {f"channel_{i}": f"value_{i}" for i in range(100)}
        channel_versions = {f"channel_{i}": "1.0" for i in range(100)}

        config = {
            "configurable": {
                "thread_id": "many-channels-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": channel_values,
            "channel_versions": channel_versions,
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, channel_versions)
        assert result is not None

    def test_deeply_nested_data(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test handling of deeply nested data structures."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Create deeply nested structure
        nested: dict[str, Any] = {"level": 0}
        current = nested
        for i in range(50):
            current["nested"] = {"level": i + 1}
            current = current["nested"]

        config = {
            "configurable": {
                "thread_id": "nested-data-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"deep": nested},
            "channel_versions": {"deep": "1.0"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, {"deep": "1.0"})
        assert result is not None


class TestConcurrentOperations:
    """Tests for concurrent operation handling."""

    def test_concurrent_puts_same_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test concurrent puts to the same thread."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        thread_id = "concurrent-test"
        results: list[dict[str, Any]] = []
        errors: list[Exception] = []

        def put_checkpoint(index: int) -> None:
            try:
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
                    "channel_values": {"index": index},
                    "channel_versions": {},
                    "versions_seen": {},
                }
                result = saver.put(config, checkpoint, {"step": index}, {})
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Run concurrent puts
        threads = [threading.Thread(target=put_checkpoint, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed
        assert len(errors) == 0
        assert len(results) == 5

    def test_concurrent_gets_same_thread(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test concurrent gets from the same thread."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        thread_id = "concurrent-get-test"
        results: list[Any] = []
        errors: list[Exception] = []

        def get_checkpoint() -> None:
            try:
                config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": "",
                    }
                }
                result = saver.get_tuple(config)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Run concurrent gets
        threads = [threading.Thread(target=get_checkpoint) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed
        assert len(errors) == 0
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_async_concurrent_operations(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async concurrent operations."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

        mock_cursor.set_results([])

        saver = AsyncSnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        async def get_checkpoint(thread_id: str) -> Any:
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                }
            }
            return await saver.aget_tuple(config)

        # Run many concurrent async operations
        tasks = [get_checkpoint(f"thread-{i}") for i in range(20)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should succeed (return None since no data)
        errors = [r for r in results if isinstance(r, Exception)]
        assert len(errors) == 0


class TestBoundaryConditions:
    """Tests for boundary conditions."""

    def test_empty_channel_values(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test checkpoint with empty channel values."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "empty-channels-test",
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
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, {})
        assert result is not None

    def test_empty_metadata(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test checkpoint with empty metadata."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "empty-metadata-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"data": "value"},
            "channel_versions": {},
            "versions_seen": {},
        }

        result = saver.put(config, checkpoint, {}, {})
        assert result is not None

    def test_very_long_thread_id(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test with very long thread ID."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # 500 character thread ID
        long_thread_id = "t" * 500

        config = {
            "configurable": {
                "thread_id": long_thread_id,
                "checkpoint_ns": "",
            }
        }

        result = saver.get_tuple(config)
        assert result is None  # No data, but should not error

    def test_checkpoint_ns_variations(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test various checkpoint namespace values."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        namespaces = ["", "inner", "outer:inner", "a:b:c:d:e", "ns-with-dash"]

        for ns in namespaces:
            config = {
                "configurable": {
                    "thread_id": "ns-test",
                    "checkpoint_ns": ns,
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

            result = saver.put(config, checkpoint, {}, {})
            assert result["configurable"]["checkpoint_ns"] == ns

    def test_list_with_zero_limit(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test list with limit=0 (should return empty)."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "limit-test",
                "checkpoint_ns": "",
            }
        }

        # Note: limit=0 behavior depends on implementation
        # Some implementations treat 0 as "no limit"
        results = list(saver.list(config, limit=0))
        # Should not error
        assert isinstance(results, list)

    def test_list_with_none_config(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test list with None config (list all)."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        results = list(saver.list(None))
        assert isinstance(results, list)


class TestDataTypeHandling:
    """Tests for various data type handling."""

    def test_various_python_types(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test handling of various Python types in channel values."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "types-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {
                "string": "hello",
                "int": 42,
                "float": 3.14159,
                "bool_true": True,
                "bool_false": False,
                "none": None,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
                "tuple_as_list": [1, 2, 3],  # Tuples become lists in JSON
            },
            "channel_versions": {},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, {})
        assert result is not None

    def test_binary_data_in_blobs(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test that binary-like data is handled via blob storage."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "binary-test",
                "checkpoint_ns": "",
            }
        }
        # Complex object that will be serialized to blob
        complex_obj = {
            "data": list(range(1000)),
            "nested": {"more": {"data": [1, 2, 3]}},
        }

        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"complex": complex_obj},
            "channel_versions": {"complex": "1.0"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = saver.put(config, checkpoint, metadata, {"complex": "1.0"})
        assert result is not None

        # Verify blob query was executed
        blob_queries = [
            q for q in mock_cursor.executed_queries if "checkpoint_blobs" in q[0]
        ]
        assert len(blob_queries) > 0


class TestFilterAndSearch:
    """Tests for filter and search functionality."""

    def test_list_with_metadata_filter(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test list with metadata filter."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "filter-test",
                "checkpoint_ns": "",
            }
        }

        results = list(saver.list(config, filter={"source": "input", "step": 1}))
        assert isinstance(results, list)

    def test_list_with_before_config(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test list with before parameter."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "before-test",
                "checkpoint_ns": "",
            }
        }
        before = {
            "configurable": {
                "thread_id": "before-test",
                "checkpoint_ns": "",
                "checkpoint_id": "some-id",
            }
        }

        results = list(saver.list(config, before=before))
        assert isinstance(results, list)


class TestMetricsEdgeCases:
    """Edge case tests for metrics functionality."""

    def test_metrics_with_many_operations(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test metrics with many operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor.set_results([])
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        # Perform many operations
        for i in range(100):
            config = {
                "configurable": {
                    "thread_id": f"metrics-stress-{i}",
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
        assert stats["operation_counts"]["put"] == 100

    def test_metrics_reset(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test metrics reset functionality."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "reset-test",
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

        # Reset metrics
        metrics.reset()

        stats = saver.get_metrics()
        assert stats is not None
        assert stats["operation_counts"] == {}

    def test_metrics_average_duration(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test metrics average duration calculation."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        for i in range(5):
            config = {
                "configurable": {
                    "thread_id": f"avg-test-{i}",
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
        assert "put" in stats["average_durations"]
        assert stats["average_durations"]["put"] >= 0

    def test_metrics_concurrent_updates(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test metrics with concurrent operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        errors: list[Exception] = []

        def do_put(thread_id: str) -> None:
            try:
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
                    "channel_values": {},
                    "channel_versions": {},
                    "versions_seen": {},
                }
                saver.put(config, checkpoint, {}, {})
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=do_put, args=(f"concurrent-{i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        stats = saver.get_metrics()
        assert stats is not None
        assert stats["operation_counts"]["put"] == 10


class TestRetryEdgeCases:
    """Edge case tests for retry functionality."""

    def test_retry_config_with_zero_delay(self, mock_conn: MockConnection) -> None:
        """Test retry config with zero base delay."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(max_retries=3, base_delay=0.0)
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.base_delay == 0.0

    def test_retry_config_with_single_retry(self, mock_conn: MockConnection) -> None:
        """Test retry config with single retry."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(max_retries=1)
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.max_retries == 1

    def test_retry_config_no_jitter(self, mock_conn: MockConnection) -> None:
        """Test retry config without jitter."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(jitter=False)
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.jitter is False


class TestTTLEdgeCases:
    """Edge case tests for TTL/expiration functionality."""

    def test_delete_before_zero_days(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test delete_before with zero days (delete all)."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor._rowcount = 100
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        deleted = saver.delete_before(timedelta(days=0))
        assert deleted == 100

    def test_delete_before_very_old(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test delete_before with very old cutoff."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor._rowcount = 0
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        deleted = saver.delete_before(timedelta(days=365 * 10))  # 10 years
        assert deleted == 0

    def test_delete_before_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test delete_before records metrics correctly."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor._rowcount = 50
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        saver.delete_before(timedelta(days=7))

        stats = saver.get_metrics()
        assert stats is not None
        assert "delete_before" in stats["operation_counts"]
        assert stats["operation_durations"]["delete_before"] >= 0

    def test_get_checkpoint_count_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_count records metrics."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        mock_cursor.set_results([(1000,)])
        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        count = saver.get_checkpoint_count()

        assert count == 1000
        stats = saver.get_metrics()
        assert stats is not None
        assert "get_checkpoint_count" in stats["operation_counts"]

    def test_get_checkpoint_counts_by_thread_empty(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test get_checkpoint_counts_by_thread with no data."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        counts = saver.get_checkpoint_counts_by_thread()
        assert counts == []


class TestAsyncEdgeCases:
    """Async edge case tests for new features."""

    @pytest.mark.asyncio
    async def test_async_delete_before_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async delete_before with metrics."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver
        from langgraph_checkpoint_snowflake._internal import Metrics

        mock_cursor._rowcount = 25
        metrics = Metrics(enabled=True)
        saver = AsyncSnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        deleted = await saver.adelete_before(timedelta(days=14))

        assert deleted == 25
        stats = saver.get_metrics()
        assert stats is not None
        assert "adelete_before" in stats["operation_counts"]

    @pytest.mark.asyncio
    async def test_async_get_checkpoint_count_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async get_checkpoint_count with metrics."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver
        from langgraph_checkpoint_snowflake._internal import Metrics

        mock_cursor.set_results([(750,)])
        metrics = Metrics(enabled=True)
        saver = AsyncSnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        count = await saver.aget_checkpoint_count()

        assert count == 750
        stats = saver.get_metrics()
        assert stats is not None
        assert "aget_checkpoint_count" in stats["operation_counts"]

    @pytest.mark.asyncio
    async def test_async_concurrent_with_metrics(
        self, mock_conn: MockConnection, mock_cursor: MockCursor
    ) -> None:
        """Test async concurrent operations with metrics."""
        from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver
        from langgraph_checkpoint_snowflake._internal import Metrics

        mock_cursor.set_results([])
        metrics = Metrics(enabled=True)
        saver = AsyncSnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        async def do_put(thread_id: str) -> dict[str, Any]:
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
                "channel_values": {},
                "channel_versions": {},
                "versions_seen": {},
            }
            return await saver.aput(config, checkpoint, {}, {})

        tasks = [do_put(f"async-concurrent-{i}") for i in range(10)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 10
        stats = saver.get_metrics()
        assert stats is not None
        assert stats["operation_counts"]["aput"] == 10
