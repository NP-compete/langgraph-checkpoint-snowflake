"""Property-based tests for langgraph-checkpoint-snowflake using Hypothesis.

These tests use property-based testing to discover edge cases that
might not be covered by example-based tests.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

try:
    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False

from tests.conftest import MockConnection, MockCursor

pytestmark = pytest.mark.skipif(
    not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed"
)


# Custom strategies for generating test data
if HYPOTHESIS_AVAILABLE:
    # Strategy for generating valid thread IDs
    thread_id_strategy = st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "N", "P", "S"),
            blacklist_characters="\x00",
        ),
        min_size=1,
        max_size=100,
    )

    # Strategy for generating checkpoint namespaces
    checkpoint_ns_strategy = st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "N"),
            whitelist_characters=":_-",
        ),
        min_size=0,
        max_size=50,
    )

    # Strategy for generating simple JSON-serializable values
    json_value_strategy = st.recursive(
        st.none()
        | st.booleans()
        | st.integers(min_value=-(2**31), max_value=2**31 - 1)
        | st.floats(allow_nan=False, allow_infinity=False)
        | st.text(max_size=100),
        lambda children: (
            st.lists(children, max_size=10)
            | st.dictionaries(st.text(min_size=1, max_size=20), children, max_size=10)
        ),
        max_leaves=50,
    )

    # Strategy for generating metadata
    metadata_strategy = st.fixed_dictionaries(
        {
            "source": st.sampled_from(["input", "loop", "update"]),
            "step": st.integers(min_value=0, max_value=1000),
        },
        optional={
            "score": st.none() | st.floats(allow_nan=False, allow_infinity=False),
            "custom": st.text(max_size=50),
        },
    )


@pytest.fixture
def property_saver(mock_conn: MockConnection) -> Any:
    """Fixture providing a SnowflakeSaver for property tests."""
    from langgraph_checkpoint_snowflake import SnowflakeSaver

    saver = SnowflakeSaver(mock_conn)  # type: ignore
    saver.is_setup = True
    return saver


class TestThreadIdProperties:
    """Property tests for thread ID handling."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(thread_id=thread_id_strategy)
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_thread_id_roundtrip(
        self, mock_conn: MockConnection, thread_id: str
    ) -> None:
        """Test that any valid thread_id can be used."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

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

        result = saver.put(config, checkpoint, {}, {})
        assert result["configurable"]["thread_id"] == thread_id


class TestNamespaceProperties:
    """Property tests for namespace handling."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(ns=checkpoint_ns_strategy)
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_namespace_roundtrip(self, mock_conn: MockConnection, ns: str) -> None:
        """Test that any valid namespace can be used."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "test-thread",
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


class TestChannelValueProperties:
    """Property tests for channel value handling."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(value=json_value_strategy)
    @settings(
        max_examples=100,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_channel_value_serialization(
        self, mock_conn: MockConnection, value: Any
    ) -> None:
        """Test that any JSON-serializable value can be stored."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "value-test",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": str(uuid4()),
            "ts": "2024-01-01T00:00:00+00:00",
            "channel_values": {"test_channel": value},
            "channel_versions": {"test_channel": "1.0"},
            "versions_seen": {},
        }

        # Should not raise
        result = saver.put(config, checkpoint, {}, {"test_channel": "1.0"})
        assert result is not None


class TestMetadataProperties:
    """Property tests for metadata handling."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(metadata=metadata_strategy)
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_metadata_serialization(
        self, mock_conn: MockConnection, metadata: dict[str, Any]
    ) -> None:
        """Test that various metadata structures can be stored."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        config = {
            "configurable": {
                "thread_id": "metadata-test",
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

        # Should not raise
        result = saver.put(config, checkpoint, metadata, {})
        assert result is not None


class TestVersionProperties:
    """Property tests for version handling."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(count=st.integers(min_value=1, max_value=100))
    @settings(
        max_examples=20,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_version_monotonicity(self, mock_conn: MockConnection, count: int) -> None:
        """Test that versions are monotonically increasing."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        versions = []
        current = None
        for _ in range(count):
            current = saver.get_next_version(current, None)
            versions.append(current)

        # Extract version numbers
        version_nums = [int(v.split(".")[0]) for v in versions]

        # Should be strictly increasing
        for i in range(1, len(version_nums)):
            assert version_nums[i] > version_nums[i - 1]


class TestWritesProperties:
    """Property tests for writes handling."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(
        channels=st.lists(
            st.tuples(
                st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz"),
                json_value_strategy,
            ),
            min_size=1,
            max_size=20,
        )
    )
    @settings(
        max_examples=30,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_writes_multiple_channels(
        self, mock_conn: MockConnection, channels: list[tuple[str, Any]]
    ) -> None:
        """Test that multiple channel writes work correctly."""
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

        # Should not raise
        saver.put_writes(config, channels, task_id="task-1")


class TestListProperties:
    """Property tests for list operations."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(limit=st.integers(min_value=1, max_value=1000))
    @settings(
        max_examples=20,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_list_limit_parameter(
        self, mock_conn: MockConnection, mock_cursor: MockCursor, limit: int
    ) -> None:
        """Test that list respects various limit values."""
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

        list(saver.list(config, limit=limit))

        # Verify LIMIT was applied
        last_query = mock_cursor.executed_queries[-1][0]
        assert f"LIMIT {limit}" in last_query


class TestFilterProperties:
    """Property tests for filter operations."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(
        filter_dict=st.dictionaries(
            st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz"),
            st.text(max_size=50) | st.integers() | st.none(),
            min_size=0,
            max_size=5,
        )
    )
    @settings(
        max_examples=30,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_filter_various_structures(
        self, mock_conn: MockConnection, mock_cursor: MockCursor, filter_dict: dict
    ) -> None:
        """Test that various filter structures are handled."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Should not raise
        list(saver.list(None, filter=filter_dict))


class TestConcurrencyProperties:
    """Property tests for concurrent operations."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(num_threads=st.integers(min_value=2, max_value=10))
    @settings(
        max_examples=5,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_concurrent_puts_different_threads(
        self, mock_conn: MockConnection, num_threads: int
    ) -> None:
        """Test concurrent puts to different thread IDs."""
        import threading

        from langgraph_checkpoint_snowflake import SnowflakeSaver

        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        results: list[dict] = []
        errors: list[Exception] = []

        def put_checkpoint(thread_id: str) -> None:
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
                result = saver.put(config, checkpoint, {}, {})
                results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=put_checkpoint, args=(f"thread-{i}",))
            for i in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == num_threads


class TestRetryConfigProperties:
    """Property tests for retry configuration."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(
        max_retries=st.integers(min_value=1, max_value=10),  # Pydantic constraint: 1-10
        base_delay=st.floats(
            min_value=0.0, max_value=60.0, allow_nan=False
        ),  # Pydantic: 0-60
        max_delay=st.floats(
            min_value=0.0, max_value=300.0, allow_nan=False
        ),  # Pydantic: 0-300
        jitter=st.booleans(),
    )
    @settings(
        max_examples=30,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_retry_config_properties(
        self,
        mock_conn: MockConnection,
        max_retries: int,
        base_delay: float,
        max_delay: float,
        jitter: bool,
    ) -> None:
        """Test that any valid retry config can be used."""
        from langgraph_checkpoint_snowflake import RetryConfig, SnowflakeSaver

        retry = RetryConfig(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
            jitter=jitter,
        )
        saver = SnowflakeSaver(mock_conn, retry_config=retry)  # type: ignore

        assert saver.retry_config.max_retries == max_retries
        assert saver.retry_config.base_delay == base_delay
        assert saver.retry_config.max_delay == max_delay
        assert saver.retry_config.jitter == jitter


class TestMetricsProperties:
    """Property tests for metrics functionality."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(num_operations=st.integers(min_value=1, max_value=50))
    @settings(
        max_examples=10,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_metrics_count_accuracy(
        self, mock_conn: MockConnection, mock_cursor: MockCursor, num_operations: int
    ) -> None:
        """Test that metrics accurately count operations."""
        from langgraph_checkpoint_snowflake import Metrics, SnowflakeSaver

        metrics = Metrics(enabled=True)
        saver = SnowflakeSaver(mock_conn, metrics=metrics)  # type: ignore
        saver.is_setup = True

        for i in range(num_operations):
            config = {
                "configurable": {
                    "thread_id": f"prop-metrics-{i}",
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
        assert stats["operation_counts"]["put"] == num_operations

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(
        durations=st.lists(
            st.floats(min_value=0.001, max_value=1.0, allow_nan=False),
            min_size=1,
            max_size=20,
        )
    )
    @settings(max_examples=20)
    def test_metrics_average_calculation(self, durations: list[float]) -> None:
        """Test that metrics correctly calculate averages."""
        from langgraph_checkpoint_snowflake._internal import Metrics

        metrics = Metrics(enabled=True)

        for duration in durations:
            metrics.record("test_op", duration)

        stats = metrics.get_stats()
        expected_avg = sum(durations) / len(durations)

        assert stats["operation_counts"]["test_op"] == len(durations)
        assert abs(stats["average_durations"]["test_op"] - expected_avg) < 0.0001


class TestTTLProperties:
    """Property tests for TTL/expiration functionality."""

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(days=st.integers(min_value=0, max_value=365))
    @settings(
        max_examples=20,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_delete_before_various_ages(
        self, mock_conn: MockConnection, mock_cursor: MockCursor, days: int
    ) -> None:
        """Test delete_before with various age values."""
        from datetime import timedelta

        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor._rowcount = days  # Use days as rowcount for variety
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        deleted = saver.delete_before(timedelta(days=days))
        assert deleted == days

    @pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
    @given(max_results=st.integers(min_value=1, max_value=1000))
    @settings(
        max_examples=20,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_get_checkpoint_counts_by_thread_limit(
        self, mock_conn: MockConnection, mock_cursor: MockCursor, max_results: int
    ) -> None:
        """Test get_checkpoint_counts_by_thread with various limits."""
        from langgraph_checkpoint_snowflake import SnowflakeSaver

        mock_cursor.set_results([])
        saver = SnowflakeSaver(mock_conn)  # type: ignore
        saver.is_setup = True

        # Should not raise
        counts = saver.get_checkpoint_counts_by_thread(max_results=max_results)
        assert isinstance(counts, list)
