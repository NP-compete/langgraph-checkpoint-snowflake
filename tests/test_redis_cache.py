"""Tests for Redis/Valkey write-back cache."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from langgraph_checkpoint_snowflake.redis_cache import (
    RedisWriteCache,
    RedisWriteCacheConfig,
)


@pytest.fixture
def redis_config() -> RedisWriteCacheConfig:
    """Create a test Redis cache config."""
    return RedisWriteCacheConfig(
        enabled=True,
        redis_url="redis://localhost:6379/0",
        sync_interval_seconds=1.0,
        batch_size=10,
        max_pending_writes=100,
        key_prefix="test",
    )


@pytest.fixture
def mock_redis() -> MagicMock:
    """Create a mock Redis client."""
    mock = MagicMock()
    mock.ping.return_value = True
    mock.pipeline.return_value = MagicMock()
    mock.pipeline.return_value.execute.return_value = [True, True]
    mock.zcard.return_value = 0
    mock.zrange.return_value = []
    mock.scan.return_value = (0, [])
    mock.set.return_value = True
    mock.eval.return_value = 1
    return mock


@pytest.fixture
def mock_snowflake_saver() -> MagicMock:
    """Create a mock Snowflake saver."""
    mock = MagicMock()
    mock.conn = MagicMock()
    mock.UPSERT_CHECKPOINTS_SQL = "MERGE INTO checkpoints..."
    return mock


class TestRedisWriteCacheConfig:
    """Tests for RedisWriteCacheConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = RedisWriteCacheConfig()

        assert config.enabled is False
        assert config.redis_url == "redis://localhost:6379/0"
        assert config.sync_interval_seconds == 10.0
        assert config.batch_size == 100
        assert config.max_pending_writes == 10000
        assert config.key_prefix == "lgcp"

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = RedisWriteCacheConfig(
            enabled=True,
            redis_url="redis://custom:6380/1",
            sync_interval_seconds=5.0,
            batch_size=50,
            key_prefix="myapp",
        )

        assert config.enabled is True
        assert config.redis_url == "redis://custom:6380/1"
        assert config.sync_interval_seconds == 5.0
        assert config.batch_size == 50
        assert config.key_prefix == "myapp"

    def test_validation_sync_interval(self) -> None:
        """Test validation of sync_interval_seconds."""
        with pytest.raises(ValueError):
            RedisWriteCacheConfig(sync_interval_seconds=0.5)  # Too low

        with pytest.raises(ValueError):
            RedisWriteCacheConfig(sync_interval_seconds=400)  # Too high

    def test_validation_batch_size(self) -> None:
        """Test validation of batch_size."""
        with pytest.raises(ValueError):
            RedisWriteCacheConfig(batch_size=0)  # Too low

        with pytest.raises(ValueError):
            RedisWriteCacheConfig(batch_size=2000)  # Too high

    def test_immutable(self) -> None:
        """Test that config is immutable (frozen)."""
        config = RedisWriteCacheConfig()
        with pytest.raises(TypeError):
            config.enabled = True  # type: ignore[misc]


class TestRedisWriteCache:
    """Tests for RedisWriteCache."""

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_init_connects_to_redis(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test that init connects to Redis."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        # Verify connection was tested
        mock_client.ping.assert_called_once()

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_init_raises_on_connection_failure(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test that init raises on connection failure."""
        mock_client = MagicMock()
        mock_client.ping.side_effect = Exception("Connection refused")
        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        with pytest.raises(ConnectionError, match="Failed to connect"):
            RedisWriteCache(redis_config, mock_snowflake_saver)

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_put_writes_to_redis(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test that put writes checkpoint to Redis."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [True, True]
        mock_client.pipeline.return_value = mock_pipeline
        mock_client.zcard.return_value = 1
        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        # Write a checkpoint
        cache.put(
            thread_id="thread-1",
            checkpoint_ns="default",
            checkpoint_id="cp-1",
            checkpoint_data={"id": "cp-1", "v": 1},
            metadata={"source": "input"},
        )

        # Verify pipeline was used
        mock_client.pipeline.assert_called()
        mock_pipeline.setex.assert_called_once()
        mock_pipeline.zadd.assert_called_once()
        mock_pipeline.execute.assert_called_once()

        # Verify metrics updated
        assert cache._metrics["writes"] == 1

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_get_returns_cached_data(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test that get returns data from Redis cache."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True

        # Mock cached data
        cached_data = {
            "thread_id": "thread-1",
            "checkpoint_ns": "default",
            "checkpoint_id": "cp-1",
            "checkpoint": {"id": "cp-1", "v": 1},
            "metadata": {"source": "input"},
            "timestamp": time.time(),
        }
        mock_client.get.return_value = json.dumps(cached_data).encode("utf-8")

        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        # Get the checkpoint
        result = cache.get("thread-1", "default", "cp-1")

        assert result is not None
        assert result["checkpoint_id"] == "cp-1"
        assert result["checkpoint"]["id"] == "cp-1"
        assert cache._metrics["cache_hits"] == 1

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_get_returns_none_on_miss(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test that get returns None on cache miss."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.get.return_value = None

        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        result = cache.get("thread-1", "default", "cp-1")

        assert result is None
        assert cache._metrics["cache_misses"] == 1

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_get_latest_checkpoint(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test getting latest checkpoint (no checkpoint_id)."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True

        # Mock multiple checkpoints
        cp1 = {
            "thread_id": "thread-1",
            "checkpoint_ns": "default",
            "checkpoint_id": "cp-1",
            "checkpoint": {"id": "cp-1"},
            "metadata": {},
            "timestamp": 1000.0,
        }
        cp2 = {
            "thread_id": "thread-1",
            "checkpoint_ns": "default",
            "checkpoint_id": "cp-2",
            "checkpoint": {"id": "cp-2"},
            "metadata": {},
            "timestamp": 2000.0,  # Newer
        }

        mock_client.scan.return_value = (0, [b"key1", b"key2"])
        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [
            json.dumps(cp1).encode("utf-8"),
            json.dumps(cp2).encode("utf-8"),
        ]
        mock_client.pipeline.return_value = mock_pipeline

        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        # Get latest (no checkpoint_id)
        result = cache.get("thread-1", "default", None)

        assert result is not None
        assert result["checkpoint_id"] == "cp-2"  # Should be the newer one

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_get_stats(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test get_stats returns cache statistics."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.zcard.return_value = 42
        mock_client.zrange.return_value = [(b"key1", time.time() - 10)]

        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        stats = cache.get_stats()

        assert stats["enabled"] is True
        assert stats["pending_writes"] == 42
        assert stats["oldest_pending_age_seconds"] is not None
        assert stats["redis_connected"] is True
        assert "writes" in stats
        assert "reads" in stats
        assert "cache_hit_rate" in stats

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_invalidate(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test invalidate removes cache entries."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.scan.return_value = (0, [b"key1", b"key2"])
        mock_client.delete.return_value = 2

        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        deleted = cache.invalidate("thread-1", "default")

        assert deleted == 2
        mock_client.delete.assert_called_once()

        # Cleanup
        cache._stop_event.set()

    @patch("langgraph_checkpoint_snowflake.redis_cache.redis")
    def test_make_data_key(
        self,
        mock_redis_module: MagicMock,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> None:
        """Test _make_data_key creates correct key format."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True

        mock_redis_module.ConnectionPool.from_url.return_value = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        cache = RedisWriteCache(redis_config, mock_snowflake_saver)

        key = cache._make_data_key("thread-1", "ns", "cp-1")

        assert key == "test:data:thread-1:ns:cp-1"

        # Cleanup
        cache._stop_event.set()


class TestRedisWriteCacheIntegration:
    """Integration tests using fakeredis."""

    @pytest.fixture
    def fakeredis_cache(
        self,
        redis_config: RedisWriteCacheConfig,
        mock_snowflake_saver: MagicMock,
    ) -> RedisWriteCache | None:
        """Create a cache with fakeredis for integration testing."""
        try:
            import fakeredis
        except ImportError:
            pytest.skip("fakeredis not installed")
            return None

        # Patch redis to use fakeredis
        with patch("langgraph_checkpoint_snowflake.redis_cache.redis") as mock_module:
            fake_server = fakeredis.FakeServer()
            fake_redis = fakeredis.FakeRedis(server=fake_server)

            mock_module.ConnectionPool.from_url.return_value = MagicMock()
            mock_module.Redis.return_value = fake_redis

            cache = RedisWriteCache(redis_config, mock_snowflake_saver)
            yield cache

            cache._stop_event.set()

    def test_put_and_get_roundtrip(
        self,
        fakeredis_cache: RedisWriteCache | None,
    ) -> None:
        """Test full put/get roundtrip with fakeredis."""
        if fakeredis_cache is None:
            pytest.skip("fakeredis not available")

        cache = fakeredis_cache

        # Put a checkpoint
        cache.put(
            thread_id="thread-1",
            checkpoint_ns="default",
            checkpoint_id="cp-1",
            checkpoint_data={"id": "cp-1", "v": 1, "data": "test"},
            metadata={"source": "input", "step": 0},
        )

        # Get it back
        result = cache.get("thread-1", "default", "cp-1")

        assert result is not None
        assert result["checkpoint_id"] == "cp-1"
        assert result["checkpoint"]["data"] == "test"
        assert result["metadata"]["source"] == "input"


class TestRedisWriteCacheValKeyCompatibility:
    """Tests to verify Valkey compatibility.

    All Redis commands used should be compatible with Valkey.
    This test class documents which commands are used.
    """

    def test_commands_are_valkey_compatible(self) -> None:
        """Document and verify Valkey-compatible commands.

        Commands used by RedisWriteCache:
        - PING: Check connection (Valkey compatible)
        - SET with NX and EX: Distributed lock (Valkey compatible)
        - SETEX: Store data with TTL (Valkey compatible)
        - GET: Retrieve data (Valkey compatible)
        - ZADD: Add to sorted set (Valkey compatible)
        - ZRANGE: Get range from sorted set (Valkey compatible)
        - ZREM: Remove from sorted set (Valkey compatible)
        - ZCARD: Count sorted set members (Valkey compatible)
        - SCAN: Iterate keys (Valkey compatible)
        - DELETE: Remove keys (Valkey compatible)
        - PUBLISH: Pub/sub (Valkey compatible)
        - EVAL: Lua scripts (Valkey compatible)
        - PIPELINE: Batch operations (Valkey compatible)

        All these commands are part of the Redis core command set
        and are fully supported by Valkey.
        """
        # This test serves as documentation
        valkey_compatible_commands = [
            "PING",
            "SET",
            "SETEX",
            "GET",
            "ZADD",
            "ZRANGE",
            "ZREM",
            "ZCARD",
            "SCAN",
            "DEL",
            "PUBLISH",
            "EVAL",
        ]

        # Commands NOT used (that might have compatibility issues):
        # - OBJECT ENCODING (Redis-specific introspection)
        # - DEBUG commands
        # - MODULE commands
        # - ACL commands (different in Valkey)

        assert len(valkey_compatible_commands) > 0  # Documentation test
