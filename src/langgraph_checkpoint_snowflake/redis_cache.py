"""Redis/Valkey-based write-back cache for multi-pod deployments.

This module provides a write-back cache that dramatically improves write
performance by accepting writes in Redis/Valkey (1-3ms) and asynchronously
syncing to Snowflake in the background.

Features:
    - Fast writes (1-3ms vs 150-400ms direct to Snowflake)
    - Durable persistence (Redis AOF)
    - Multi-pod coordination (distributed locks)
    - Background sync to Snowflake
    - Graceful shutdown with flush

Note:
    This module uses only Redis commands that are compatible with Valkey.
    You can use either Redis or Valkey as the backend.

Example:
    >>> from langgraph_checkpoint_snowflake import (
    ...     SnowflakeSaver,
    ...     RedisWriteCacheConfig,
    ... )
    >>> cache_config = RedisWriteCacheConfig(
    ...     enabled=True,
    ...     redis_url="redis://localhost:6379/0",
    ... )
    >>> with SnowflakeSaver.from_env(
    ...     redis_cache_config=cache_config,
    ... ) as saver:
    ...     saver.setup()
    ...     # Writes now go to Redis first (fast!)
    ...     # Background worker syncs to Snowflake
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from redis import Redis

    from langgraph_checkpoint_snowflake.base import BaseSnowflakeSaver

logger = logging.getLogger(__name__)


class RedisWriteCacheConfig(BaseModel):
    """Configuration for Redis/Valkey write-back cache.

    This cache accepts writes in Redis (1-3ms) and asynchronously syncs
    to Snowflake in the background, providing 100x faster write performance.

    Attributes:
        enabled: Whether write-back caching is enabled.
        redis_url: Redis/Valkey connection URL.
        sync_interval_seconds: How often to sync cache to Snowflake.
        batch_size: Number of checkpoints to sync per batch.
        max_pending_writes: Max pending writes before warning.
        max_connections: Max Redis connections per pod.
        socket_timeout: Socket timeout for Redis operations.
        lock_timeout_seconds: Distributed lock timeout (prevents deadlock).
        key_prefix: Prefix for all Redis keys (for namespacing).

    Example:
        >>> config = RedisWriteCacheConfig(
        ...     enabled=True,
        ...     redis_url="redis://redis-cluster:6379/0",
        ...     sync_interval_seconds=10.0,
        ...     batch_size=100,
        ... )
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    enabled: bool = Field(
        default=False,
        description="Enable write-back caching via Redis/Valkey",
    )

    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis/Valkey connection URL (redis://host:port/db)",
    )

    sync_interval_seconds: float = Field(
        default=10.0,
        ge=1.0,
        le=300.0,
        description="How often to sync cache to Snowflake (seconds)",
    )

    batch_size: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Number of checkpoints to sync per batch",
    )

    max_pending_writes: int = Field(
        default=10000,
        ge=100,
        le=100000,
        description="Max pending writes before warning (not a hard limit)",
    )

    max_connections: int = Field(
        default=50,
        ge=10,
        le=1000,
        description="Max Redis connections per pod",
    )

    socket_timeout: float = Field(
        default=5.0,
        ge=1.0,
        le=30.0,
        description="Socket timeout for Redis operations (seconds)",
    )

    socket_connect_timeout: float = Field(
        default=5.0,
        ge=1.0,
        le=30.0,
        description="Socket connect timeout for Redis (seconds)",
    )

    lock_timeout_seconds: int = Field(
        default=30,
        ge=10,
        le=300,
        description="Distributed lock timeout (prevents deadlock)",
    )

    key_prefix: str = Field(
        default="lgcp",
        description="Prefix for all Redis keys (for namespacing)",
    )

    retry_on_timeout: bool = Field(
        default=True,
        description="Retry Redis operations on timeout",
    )


class RedisWriteCache:
    """Write-back cache using Redis/Valkey for multi-pod deployments.

    This implementation provides:
    - Fast writes (1-3ms) via Redis/Valkey
    - Durable persistence (AOF)
    - Multi-pod coordination (distributed locks)
    - Background sync to Snowflake
    - Graceful shutdown with flush

    All Redis commands used are compatible with Valkey.

    Example:
        >>> cache = RedisWriteCache(config, snowflake_saver)
        >>> cache.start()
        >>>
        >>> # Fast write to cache
        >>> cache.put(thread_id, checkpoint_ns, checkpoint_id, data)
        >>>
        >>> # Read from cache (or None if not cached)
        >>> result = cache.get(thread_id, checkpoint_ns, checkpoint_id)
        >>>
        >>> # Graceful shutdown
        >>> cache.shutdown()
    """

    def __init__(
        self,
        config: RedisWriteCacheConfig,
        snowflake_saver: BaseSnowflakeSaver,
    ) -> None:
        """Initialize Redis write cache.

        Args:
            config: Redis cache configuration.
            snowflake_saver: SnowflakeSaver instance for sync operations.

        Raises:
            ImportError: If redis package is not installed.
            ConnectionError: If cannot connect to Redis.
        """
        self.config = config
        self.snowflake_saver = snowflake_saver

        # Key patterns (using config prefix)
        self._prefix = config.key_prefix
        self._pending_key = f"{self._prefix}:pending"  # Sorted set
        self._data_prefix = f"{self._prefix}:data"  # Hash prefix
        self._lock_key = f"{self._prefix}:sync_lock"
        self._stats_key = f"{self._prefix}:stats"

        # Import redis
        try:
            import redis
        except ImportError as e:
            raise ImportError(
                "Redis is required for write-back cache. "
                "Install with: pip install 'langgraph-checkpoint-snowflake[redis]'"
            ) from e

        # Create connection pool
        self._pool = redis.ConnectionPool.from_url(
            config.redis_url,
            max_connections=config.max_connections,
            socket_timeout=config.socket_timeout,
            socket_connect_timeout=config.socket_connect_timeout,
            retry_on_timeout=config.retry_on_timeout,
            decode_responses=False,  # We handle encoding
        )

        # Create Redis client
        self._redis: Redis = redis.Redis(connection_pool=self._pool)

        # Verify connection
        try:
            self._redis.ping()
            logger.info("Connected to Redis at %s", config.redis_url)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}") from e

        # Metrics (thread-safe)
        self._metrics = {
            "writes": 0,
            "reads": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "syncs": 0,
            "sync_errors": 0,
            "checkpoints_synced": 0,
        }
        self._metrics_lock = threading.Lock()

        # Background sync worker
        self._stop_event = threading.Event()
        self._sync_thread: threading.Thread | None = None
        self._started = False

    def start(self) -> None:
        """Start the background sync worker.

        This method starts a background thread that periodically syncs
        pending writes from Redis to Snowflake.

        Safe to call multiple times (idempotent).
        """
        if self._started:
            return

        self._sync_thread = threading.Thread(
            target=self._sync_worker,
            name="redis-sync-worker",
            daemon=True,
        )
        self._sync_thread.start()
        self._started = True

        logger.info(
            "Redis write cache started (sync_interval=%.1fs, batch_size=%d)",
            self.config.sync_interval_seconds,
            self.config.batch_size,
        )

    def put(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        checkpoint_data: dict[str, Any],
        metadata: dict[str, Any],
        parent_checkpoint_id: str | None = None,
    ) -> None:
        """Write checkpoint to Redis cache (returns in 1-3ms).

        This is the main write path - fast and durable (with AOF).

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Checkpoint namespace.
            checkpoint_id: Checkpoint ID.
            checkpoint_data: Checkpoint data dictionary.
            metadata: Checkpoint metadata dictionary.
            parent_checkpoint_id: Parent checkpoint ID (optional).
        """
        # Create unique key for this checkpoint
        data_key = self._make_data_key(thread_id, checkpoint_ns, checkpoint_id)

        # Prepare data for storage
        data = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "checkpoint": checkpoint_data,
            "metadata": metadata,
            "timestamp": time.time(),
            "write_id": str(uuid.uuid4()),  # For deduplication
        }

        try:
            serialized = json.dumps(data, default=str).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error("Failed to serialize checkpoint: %s", e)
            raise

        # Atomic write using pipeline
        # Commands used: SET, ZADD, PUBLISH - all Valkey compatible
        pipe = self._redis.pipeline()

        # 1. Store checkpoint data (with 24-hour TTL for cleanup)
        pipe.setex(data_key, 86400, serialized)

        # 2. Add to pending sync queue (sorted set, scored by timestamp)
        pipe.zadd(self._pending_key, {data_key: time.time()})

        try:
            pipe.execute()

            with self._metrics_lock:
                self._metrics["writes"] += 1

            # Publish invalidation event (for cross-pod cache invalidation)
            # Other pods can subscribe to this channel
            self._redis.publish(
                f"{self._prefix}:invalidate",
                f"{thread_id}:{checkpoint_ns}".encode(),
            )

            logger.debug(
                "Wrote checkpoint to cache: thread=%s id=%s",
                thread_id,
                checkpoint_id,
            )

            # Check queue size (warning only, not blocking)
            queue_size: int | None = self._redis.zcard(self._pending_key)  # type: ignore[assignment]
            if queue_size and queue_size > self.config.max_pending_writes:
                logger.warning(
                    "Write cache backlog: %d pending writes! "
                    "Sync may be falling behind.",
                    queue_size,
                )

        except Exception as e:
            logger.error("Failed to write to Redis: %s", e, exc_info=True)
            raise

    def get(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Get checkpoint from Redis cache.

        If not in cache, returns None (caller should query Snowflake).

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Checkpoint namespace.
            checkpoint_id: Checkpoint ID (None for latest).

        Returns:
            Checkpoint data dict if found in cache, None otherwise.
        """
        with self._metrics_lock:
            self._metrics["reads"] += 1

        try:
            if checkpoint_id:
                # Get specific checkpoint
                data_key = self._make_data_key(thread_id, checkpoint_ns, checkpoint_id)
                data: bytes | None = self._redis.get(data_key)  # type: ignore[assignment]

                if data:
                    with self._metrics_lock:
                        self._metrics["cache_hits"] += 1
                    result: dict[str, Any] = json.loads(data)
                    return result
                else:
                    with self._metrics_lock:
                        self._metrics["cache_misses"] += 1
                    return None

            else:
                # Get latest checkpoint for this thread
                # Use SCAN instead of KEYS for production safety
                pattern = self._make_data_key(thread_id, checkpoint_ns, "*")
                keys: list[bytes] = []

                # SCAN is Valkey compatible
                cursor: int = 0
                while True:
                    scan_result: tuple[int, list[bytes]] = self._redis.scan(  # type: ignore[assignment]
                        cursor=cursor,
                        match=pattern,
                        count=100,
                    )
                    cursor, batch = scan_result
                    keys.extend(batch)
                    if cursor == 0:
                        break

                if not keys:
                    with self._metrics_lock:
                        self._metrics["cache_misses"] += 1
                    return None

                # Get all matching checkpoints
                pipe = self._redis.pipeline()
                for key in keys:
                    pipe.get(key)
                values = pipe.execute()

                # Find latest by timestamp
                checkpoints: list[dict[str, Any]] = []
                for v in values:
                    if v:
                        try:
                            checkpoints.append(json.loads(v))
                        except json.JSONDecodeError:
                            continue

                if not checkpoints:
                    with self._metrics_lock:
                        self._metrics["cache_misses"] += 1
                    return None

                # Return checkpoint with latest timestamp
                latest = max(checkpoints, key=lambda c: c.get("timestamp", 0))
                with self._metrics_lock:
                    self._metrics["cache_hits"] += 1
                return latest

        except Exception as e:
            logger.error("Failed to read from Redis: %s", e, exc_info=True)
            # On error, return None (caller will query Snowflake)
            return None

    def invalidate(
        self,
        thread_id: str,
        checkpoint_ns: str | None = None,
    ) -> int:
        """Invalidate cache entries for a thread.

        Note: This only removes from the data cache, not from the pending
        sync queue. Pending writes will still be synced to Snowflake.

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Optional checkpoint namespace (None for all).

        Returns:
            Number of entries invalidated.
        """
        if checkpoint_ns is not None:
            pattern = self._make_data_key(thread_id, checkpoint_ns, "*")
        else:
            pattern = self._make_data_key(thread_id, "*", "*")

        # Use SCAN + DELETE (Valkey compatible)
        deleted = 0
        cursor: int = 0
        while True:
            scan_result: tuple[int, list[bytes]] = self._redis.scan(  # type: ignore[assignment]
                cursor=cursor,
                match=pattern,
                count=100,
            )
            cursor, keys = scan_result
            if keys:
                del_count: int = self._redis.delete(*keys)  # type: ignore[assignment]
                deleted += del_count
            if cursor == 0:
                break

        if deleted:
            logger.debug(
                "Invalidated %d cache entries for thread=%s",
                deleted,
                thread_id,
            )

        return deleted

    def _sync_worker(self) -> None:
        """Background worker that syncs Redis -> Snowflake.

        Uses distributed locking to ensure only ONE pod syncs at a time.
        This prevents race conditions and duplicate writes to Snowflake.
        """
        logger.info("Redis sync worker started")

        while not self._stop_event.is_set():
            try:
                # Try to acquire distributed lock
                # SET with NX and EX is Valkey compatible
                lock_value = str(uuid.uuid4()).encode("utf-8")
                lock_acquired = self._redis.set(
                    self._lock_key,
                    lock_value,
                    nx=True,  # Only set if doesn't exist
                    ex=self.config.lock_timeout_seconds,  # Auto-expire
                )

                if not lock_acquired:
                    # Another pod is syncing, sleep and retry
                    logger.debug("Sync lock held by another pod, waiting...")
                    self._stop_event.wait(self.config.sync_interval_seconds)
                    continue

                # We have the lock! Sync pending checkpoints
                logger.debug("Acquired sync lock, starting batch sync...")

                try:
                    synced_count = self._sync_batch()

                    if synced_count > 0:
                        pending = self._redis.zcard(self._pending_key)
                        logger.info(
                            "Synced %d checkpoints to Snowflake (pending=%s)",
                            synced_count,
                            pending,
                        )

                        with self._metrics_lock:
                            self._metrics["syncs"] += 1
                            self._metrics["checkpoints_synced"] += synced_count

                except Exception as e:
                    logger.error("Sync batch failed: %s", e, exc_info=True)
                    with self._metrics_lock:
                        self._metrics["sync_errors"] += 1

                finally:
                    # Release lock (only if we still own it)
                    # Use Lua script for atomic check-and-delete (Valkey compatible)
                    self._release_lock(lock_value)
                    logger.debug("Released sync lock")

            except Exception as e:
                logger.error("Sync worker error: %s", e, exc_info=True)

            # Sleep until next sync interval
            self._stop_event.wait(self.config.sync_interval_seconds)

        logger.info("Redis sync worker stopped")

    def _release_lock(self, lock_value: bytes) -> bool:
        """Release distributed lock if we still own it.

        Uses a Lua script for atomic check-and-delete.
        This is Valkey compatible.

        Args:
            lock_value: The value we set when acquiring the lock.

        Returns:
            True if lock was released, False if we didn't own it.
        """
        # Lua script: only delete if value matches (Valkey compatible)
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result = self._redis.eval(script, 1, self._lock_key, lock_value)
        return bool(result)

    def _sync_batch(self) -> int:
        """Sync a batch of pending checkpoints to Snowflake.

        Returns:
            Number of checkpoints synced.
        """
        # Get oldest pending checkpoints from queue
        # ZRANGE with WITHSCORES is Valkey compatible
        items: list[tuple[bytes, float]] = self._redis.zrange(  # type: ignore[assignment]
            self._pending_key,
            0,  # Start index
            self.config.batch_size - 1,  # End index
            withscores=True,
        )

        if not items:
            return 0  # Nothing to sync

        # Fetch checkpoint data for each item
        pipe = self._redis.pipeline()
        for key, _score in items:
            pipe.get(key)
        values: list[bytes | None] = pipe.execute()  # type: ignore[assignment]

        # Deserialize checkpoints
        checkpoints: list[tuple[bytes, dict[str, Any]]] = []
        for i, value in enumerate(values):
            if value:
                try:
                    data = json.loads(value)
                    checkpoints.append((items[i][0], data))
                except json.JSONDecodeError as e:
                    logger.error("Failed to deserialize checkpoint: %s", e)
                    # Remove corrupted entry from queue
                    self._redis.zrem(self._pending_key, items[i][0])

        if not checkpoints:
            return 0

        # Sync to Snowflake
        try:
            self._batch_write_to_snowflake(checkpoints)

            # Mark as synced (atomic operation)
            pipe = self._redis.pipeline()
            for key, _data in checkpoints:
                # Remove from pending queue
                pipe.zrem(self._pending_key, key)
                # Delete data (it's now in Snowflake)
                pipe.delete(key)

            pipe.execute()

            return len(checkpoints)

        except Exception as e:
            # Sync failed - items remain in queue for retry
            logger.error(
                "Failed to sync batch to Snowflake: %s",
                e,
                exc_info=True,
            )
            raise

    def _batch_write_to_snowflake(
        self,
        checkpoints: list[tuple[bytes, dict[str, Any]]],
    ) -> None:
        """Write batch of checkpoints to Snowflake.

        Args:
            checkpoints: List of (redis_key, checkpoint_data) tuples.
        """
        from langgraph_checkpoint_snowflake._internal import Conn, get_cursor

        # Get connection from saver (type assertion for mypy)
        conn: Conn = self.snowflake_saver.conn  # type: ignore[attr-defined]

        with get_cursor(conn) as cur:
            for _key, data in checkpoints:
                # Prepare checkpoint for Snowflake
                checkpoint_json = json.dumps(data["checkpoint"])
                metadata_json = json.dumps(data["metadata"])

                # Execute MERGE (upsert)
                cur.execute(
                    self.snowflake_saver.UPSERT_CHECKPOINTS_SQL,
                    (
                        data["thread_id"],
                        data["checkpoint_ns"],
                        data["checkpoint_id"],
                        data["parent_checkpoint_id"],
                        checkpoint_json,
                        metadata_json,
                    ),
                )

        logger.debug("Wrote %d checkpoints to Snowflake", len(checkpoints))

    def flush(self, timeout: float = 30.0) -> int:
        """Force immediate sync of all pending writes.

        Blocks until all pending writes are synced to Snowflake or timeout.

        Args:
            timeout: Max time to wait for sync completion (seconds).

        Returns:
            int: Total number of checkpoints synced.

        Raises:
            TimeoutError: If flush doesn't complete within timeout.
        """
        logger.info("Flushing write cache to Snowflake...")
        start = time.time()
        total_synced = 0

        # Try to acquire lock and sync
        while time.time() - start < timeout:
            lock_value = str(uuid.uuid4()).encode("utf-8")
            lock_acquired = self._redis.set(
                self._lock_key,
                lock_value,
                nx=True,
                ex=30,
            )

            if lock_acquired:
                try:
                    # Sync all pending
                    while True:
                        synced = self._sync_batch()
                        if synced == 0:
                            break  # All done
                        total_synced += synced

                    logger.info(
                        "Flushed %d checkpoints to Snowflake",
                        total_synced,
                    )
                    return total_synced

                finally:
                    self._release_lock(lock_value)

            # Lock held by another pod, wait
            time.sleep(0.5)

        # Timeout
        pending = self._redis.zcard(self._pending_key)
        raise TimeoutError(
            f"Failed to flush within {timeout}s ({pending} checkpoints still pending)"
        )

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats including:
            - pending_writes: Number of checkpoints awaiting sync
            - oldest_pending_age_seconds: Age of oldest pending write
            - writes: Total writes to cache
            - reads: Total reads from cache
            - cache_hit_rate: Percentage of cache hits
            - syncs: Number of successful sync operations
            - sync_errors: Number of failed sync operations
            - checkpoints_synced: Total checkpoints synced to Snowflake
            - redis_connected: Whether Redis connection is healthy
        """
        pending_count = self._redis.zcard(self._pending_key) or 0

        # Get oldest pending timestamp
        oldest: list[tuple[bytes, float]] = self._redis.zrange(  # type: ignore[assignment]
            self._pending_key,
            0,
            0,
            withscores=True,
        )
        oldest_age: float | None = None
        if oldest:
            oldest_timestamp = oldest[0][1]
            oldest_age = time.time() - oldest_timestamp

        with self._metrics_lock:
            stats = self._metrics.copy()

        # Calculate hit rate
        total_reads = stats["reads"]
        hit_rate = stats["cache_hits"] / total_reads if total_reads > 0 else 0.0

        # Check Redis connection
        try:
            redis_connected = bool(self._redis.ping())
        except Exception:
            redis_connected = False

        return {
            "enabled": self.config.enabled,
            "pending_writes": pending_count,
            "oldest_pending_age_seconds": oldest_age,
            "writes": stats["writes"],
            "reads": stats["reads"],
            "cache_hits": stats["cache_hits"],
            "cache_misses": stats["cache_misses"],
            "cache_hit_rate": hit_rate,
            "syncs": stats["syncs"],
            "sync_errors": stats["sync_errors"],
            "checkpoints_synced": stats["checkpoints_synced"],
            "redis_connected": redis_connected,
        }

    def shutdown(self, flush_timeout: float = 30.0) -> None:
        """Shutdown background worker and flush pending writes.

        Args:
            flush_timeout: Max time to wait for flush (seconds).
        """
        logger.info("Shutting down Redis write cache...")

        # Stop background worker
        self._stop_event.set()
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=5.0)

        # Flush remaining writes
        try:
            self.flush(timeout=flush_timeout)
        except TimeoutError as e:
            logger.error("Failed to flush all writes during shutdown: %s", e)

        # Close Redis connection pool
        self._pool.disconnect()

        logger.info("Redis write cache shutdown complete")

    def _make_data_key(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> str:
        """Create a Redis key for checkpoint data.

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Checkpoint namespace.
            checkpoint_id: Checkpoint ID (can be "*" for patterns).

        Returns:
            Redis key string.
        """
        return f"{self._data_prefix}:{thread_id}:{checkpoint_ns}:{checkpoint_id}"

    @contextmanager
    def _timed_operation(self, operation: str) -> Iterator[None]:
        """Context manager for timing operations.

        Args:
            operation: Name of the operation.

        Yields:
            None
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            duration = time.perf_counter() - start
            logger.debug("Operation %s completed in %.3fs", operation, duration)


__all__ = [
    "RedisWriteCache",
    "RedisWriteCacheConfig",
]
