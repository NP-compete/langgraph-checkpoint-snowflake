"""Snowflake checkpoint saver for LangGraph.

This module provides a Snowflake-based checkpoint saver for LangGraph,
enabling durable, stateful AI agents with Snowflake persistence.

Features:
    - Retry with exponential backoff for transient errors
    - TTL/expiration for automatic cleanup of old checkpoints
    - Metrics/observability support
    - Optional in-memory LRU cache for hot checkpoints

Example:
    >>> from langgraph_checkpoint_snowflake import SnowflakeSaver
    >>> with SnowflakeSaver.from_conn_string(
    ...     account="your_account",
    ...     user="your_user",
    ...     password="your_password",
    ...     warehouse="your_warehouse",
    ...     database="your_database",
    ...     schema="your_schema",
    ... ) as checkpointer:
    ...     checkpointer.setup()
    ...     graph = builder.compile(checkpointer=checkpointer)
"""

from __future__ import annotations

import builtins
import json
import threading
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from langgraph_checkpoint_snowflake._internal import (
    BenchmarkResult,
    CacheConfig,
    CheckpointCache,
    Conn,
    Metrics,
    RetryConfig,
    _load_private_key,
    benchmark_operation,
    configure_logging,
    create_connection,
    execute_with_retry,
    get_connection_params_from_env,
    get_cursor,
    logger,
    set_log_level,
    timed_operation,
)
from langgraph_checkpoint_snowflake.base import BaseSnowflakeSaver
from langgraph_checkpoint_snowflake.exceptions import (
    SnowflakeAuthenticationError,
    SnowflakeCheckpointError,
    SnowflakeConfigurationError,
    SnowflakeConnectionError,
    SnowflakeQueryError,
    SnowflakeSchemaError,
    SnowflakeSerializationError,
    SnowflakeTransientError,
    SnowflakeWarehouseError,
)

__version__ = "0.1.0"


def __getattr__(name: str) -> Any:
    """Lazy import for AsyncSnowflakeSaver to avoid circular imports."""
    if name == "AsyncSnowflakeSaver":
        from langgraph_checkpoint_snowflake.aio import AsyncSnowflakeSaver

        return AsyncSnowflakeSaver
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class SnowflakeSaver(BaseSnowflakeSaver):
    """Synchronous checkpoint saver that stores checkpoints in Snowflake.

    This class provides a synchronous interface for storing and retrieving
    LangGraph checkpoints in Snowflake. For async usage, see AsyncSnowflakeSaver.

    Features:
        - Retry with exponential backoff for transient errors
        - TTL/expiration for automatic cleanup of old checkpoints
        - Metrics/observability support
        - Optional in-memory LRU cache for hot checkpoints

    Args:
        conn: A Snowflake connection object.
        serde: Optional serializer for checkpoint data.
        retry_config: Optional retry configuration.
        metrics: Optional metrics collector.
        cache_config: Optional cache configuration for in-memory caching.

    Example:
        >>> import snowflake.connector
        >>> conn = snowflake.connector.connect(...)
        >>> checkpointer = SnowflakeSaver(conn)
        >>> checkpointer.setup()
        >>> graph = builder.compile(checkpointer=checkpointer)

    Example with retry and metrics:
        >>> from langgraph_checkpoint_snowflake import SnowflakeSaver, RetryConfig, Metrics
        >>> metrics = Metrics(enabled=True)
        >>> retry = RetryConfig(max_retries=5, base_delay=2.0)
        >>> checkpointer = SnowflakeSaver(conn, retry_config=retry, metrics=metrics)

    Example with caching:
        >>> from langgraph_checkpoint_snowflake import SnowflakeSaver, CacheConfig
        >>> cache = CacheConfig(enabled=True, max_size=100, ttl_seconds=300)
        >>> checkpointer = SnowflakeSaver(conn, cache_config=cache)
    """

    conn: Conn
    lock: threading.Lock
    is_setup: bool
    retry_config: RetryConfig
    metrics: Metrics | None
    _cache: CheckpointCache

    def __init__(
        self,
        conn: Conn,
        *,
        serde: SerializerProtocol | None = None,
        retry_config: RetryConfig | None = None,
        metrics: Metrics | None = None,
        cache_config: CacheConfig | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.conn = conn
        self.lock = threading.Lock()
        self.is_setup = False
        self.retry_config = retry_config or RetryConfig()
        self.metrics = metrics
        self._cache = CheckpointCache(cache_config)

    @classmethod
    @contextmanager
    def from_conn_string(
        cls,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str,
        *,
        role: str | None = None,
        authenticator: str | None = None,
        serde: SerializerProtocol | None = None,
        **kwargs: Any,
    ) -> Iterator[SnowflakeSaver]:
        """Create a SnowflakeSaver from connection parameters.

        Args:
            account: Snowflake account identifier.
            user: Username for authentication.
            password: Password for authentication.
            warehouse: Warehouse to use.
            database: Database to use.
            schema: Schema to use.
            role: Optional role to use.
            authenticator: Optional authenticator method.
            serde: Optional serializer for checkpoint data.
            **kwargs: Additional connection parameters.

        Yields:
            A configured SnowflakeSaver instance.

        Example:
            >>> with SnowflakeSaver.from_conn_string(
            ...     account="my_account",
            ...     user="my_user",
            ...     password="my_password",
            ...     warehouse="my_warehouse",
            ...     database="my_database",
            ...     schema="my_schema",
            ... ) as checkpointer:
            ...     checkpointer.setup()
            ...     # Use checkpointer...
        """
        conn = create_connection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            authenticator=authenticator,
            **kwargs,
        )
        try:
            yield cls(conn, serde=serde)
        finally:
            conn.close()

    @classmethod
    @contextmanager
    def from_env(
        cls,
        *,
        serde: SerializerProtocol | None = None,
    ) -> Iterator[SnowflakeSaver]:
        """Create a SnowflakeSaver from environment variables.

        Required environment variables:
            - SNOWFLAKE_ACCOUNT
            - SNOWFLAKE_USER
            - SNOWFLAKE_WAREHOUSE
            - SNOWFLAKE_DATABASE
            - SNOWFLAKE_SCHEMA

        Authentication (one of):
            - SNOWFLAKE_PASSWORD (password auth)
            - SNOWFLAKE_PRIVATE_KEY_PATH (key pair auth - path to PEM file)
            - SNOWFLAKE_PRIVATE_KEY (key pair auth - PEM string)

        Optional environment variables:
            - SNOWFLAKE_ROLE
            - SNOWFLAKE_AUTHENTICATOR
            - SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (for encrypted private keys)

        Yields:
            A configured SnowflakeSaver instance.
        """
        params = get_connection_params_from_env()
        conn = create_connection(**params)
        try:
            yield cls(conn, serde=serde)
        finally:
            conn.close()

    @classmethod
    @contextmanager
    def from_key_pair(
        cls,
        account: str,
        user: str,
        warehouse: str,
        database: str,
        schema: str,
        private_key_path: str | None = None,
        private_key: str | bytes | None = None,
        private_key_passphrase: str | None = None,
        *,
        role: str | None = None,
        serde: SerializerProtocol | None = None,
        **kwargs: Any,
    ) -> Iterator[SnowflakeSaver]:
        """Create a SnowflakeSaver using key pair authentication.

        This is the recommended authentication method for production and CI/CD
        environments as it avoids storing passwords.

        Args:
            account: Snowflake account identifier.
            user: Username for authentication.
            warehouse: Warehouse to use.
            database: Database to use.
            schema: Schema to use.
            private_key_path: Path to PEM-encoded private key file.
            private_key: PEM-encoded private key as string or bytes.
            private_key_passphrase: Passphrase for encrypted private keys.
            role: Optional role to use.
            serde: Optional serializer for checkpoint data.
            **kwargs: Additional connection parameters.

        Yields:
            A configured SnowflakeSaver instance.

        Example:
            >>> with SnowflakeSaver.from_key_pair(
            ...     account="my_account",
            ...     user="my_user",
            ...     warehouse="my_warehouse",
            ...     database="my_database",
            ...     schema="my_schema",
            ...     private_key_path="/path/to/rsa_key.p8",
            ... ) as checkpointer:
            ...     checkpointer.setup()
            ...     # Use checkpointer...
        """
        private_key_bytes = _load_private_key(
            private_key_path=private_key_path,
            private_key=private_key,
            private_key_passphrase=private_key_passphrase,
        )
        conn = create_connection(
            account=account,
            user=user,
            warehouse=warehouse,
            database=database,
            schema=schema,
            private_key=private_key_bytes,
            role=role,
            **kwargs,
        )
        try:
            yield cls(conn, serde=serde)
        finally:
            conn.close()

    def setup(self) -> None:
        """Set up the checkpoint database tables.

        This method creates the necessary tables in Snowflake if they don't
        already exist and runs any pending migrations. It MUST be called
        before using the checkpointer for the first time.

        This method is idempotent and safe to call multiple times.
        """
        if self.is_setup:
            return

        with self.lock:
            if self.is_setup:
                return

            with get_cursor(self.conn) as cur:
                # Create migrations table first
                cur.execute(self.MIGRATIONS[0])

                # Get current migration version
                cur.execute("SELECT MAX(v) as v FROM checkpoint_migrations")
                row = cur.fetchone()
                version = row[0] if row and row[0] is not None else -1

                # Run pending migrations
                for v, migration in enumerate(
                    self.MIGRATIONS[version + 1 :], start=version + 1
                ):
                    try:
                        cur.execute(migration)
                        cur.execute(
                            "INSERT INTO checkpoint_migrations (v) VALUES (%s)",
                            (v,),
                        )
                    except Exception as e:
                        # Handle case where migration already applied
                        if "already exists" in str(e).lower():
                            cur.execute(
                                "INSERT INTO checkpoint_migrations (v) "
                                "SELECT %s WHERE NOT EXISTS "
                                "(SELECT 1 FROM checkpoint_migrations WHERE v = %s)",
                                (v, v),
                            )
                        else:
                            raise

            self.is_setup = True

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Get a checkpoint tuple from the database.

        If caching is enabled, checks the cache first before querying the database.

        Args:
            config: Configuration specifying which checkpoint to retrieve.
                Must contain `thread_id` in configurable. Optionally contains
                `checkpoint_ns` and `checkpoint_id`.

        Returns:
            The checkpoint tuple if found, None otherwise.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Check cache first
        cached: CheckpointTuple | None = self._cache.get(
            thread_id, checkpoint_ns, checkpoint_id
        )
        if cached is not None:
            logger.debug(
                "Cache hit for thread=%s, checkpoint=%s", thread_id, checkpoint_id
            )
            return cached

        with timed_operation(self.metrics, "get_tuple"):
            with self.lock, get_cursor(self.conn) as cur:
                if checkpoint_id:
                    execute_with_retry(
                        cur,
                        self.SELECT_SQL
                        + "WHERE c.thread_id = %s AND c.checkpoint_ns = %s AND c.checkpoint_id = %s",
                        (thread_id, checkpoint_ns, checkpoint_id),
                        self.retry_config,
                    )
                else:
                    execute_with_retry(
                        cur,
                        self.SELECT_SQL
                        + "WHERE c.thread_id = %s AND c.checkpoint_ns = %s "
                        "ORDER BY c.checkpoint_id DESC LIMIT 1",
                        (thread_id, checkpoint_ns),
                        self.retry_config,
                    )

                row = cur.fetchone()
                if row is None:
                    return None

                result = self._row_to_checkpoint_tuple(row)

                # Cache the result
                self._cache.put(thread_id, checkpoint_ns, checkpoint_id, result)
                # Also cache with actual checkpoint_id if we fetched "latest"
                if checkpoint_id is None and result.checkpoint:
                    actual_id = result.checkpoint.get("id")
                    if actual_id:
                        self._cache.put(thread_id, checkpoint_ns, actual_id, result)

                return result

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints matching the given criteria.

        Args:
            config: Configuration for filtering by thread_id and checkpoint_ns.
            filter: Additional metadata filters.
            before: Only return checkpoints before this one.
            limit: Maximum number of checkpoints to return.

        Yields:
            Matching checkpoint tuples, ordered by checkpoint_id descending.
        """
        where, params = self._search_where(config, filter, before)
        query = self.SELECT_SQL + where + " ORDER BY c.checkpoint_id DESC"

        if limit is not None:
            query += f" LIMIT {int(limit)}"

        with timed_operation(self.metrics, "list"):
            with self.lock, get_cursor(self.conn) as cur:
                execute_with_retry(cur, query, params, self.retry_config)
                for row in cur:
                    yield self._row_to_checkpoint_tuple(cast(tuple[Any, ...], row))

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint in the database.

        Invalidates cached entries for this thread to ensure consistency.

        Args:
            config: Configuration for the checkpoint.
            checkpoint: The checkpoint data to store.
            metadata: Metadata associated with the checkpoint.
            new_versions: New channel versions as of this write.

        Returns:
            Updated configuration with the new checkpoint_id.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        parent_checkpoint_id = config["configurable"].get("checkpoint_id")

        # Invalidate cache for this thread (new checkpoint means "latest" changes)
        self._cache.invalidate(thread_id, checkpoint_ns)

        # Prepare checkpoint data - separate blobs from inline values
        checkpoint_copy = checkpoint.copy()
        checkpoint_copy["channel_values"] = checkpoint_copy["channel_values"].copy()

        blob_values = {}
        for k, v in checkpoint["channel_values"].items():
            if v is None or isinstance(v, str | int | float | bool):
                pass  # Keep inline
            else:
                blob_values[k] = checkpoint_copy["channel_values"].pop(k)

        # Serialize checkpoint and metadata
        checkpoint_json = json.dumps(checkpoint_copy)
        metadata_json = json.dumps(get_checkpoint_metadata(config, metadata))

        with timed_operation(self.metrics, "put"):
            with self.lock, get_cursor(self.conn) as cur:
                # Store blobs (batch if multiple)
                if blob_versions := {
                    k: v for k, v in new_versions.items() if k in blob_values
                }:
                    blob_params = self._dump_blobs(
                        thread_id, checkpoint_ns, blob_values, blob_versions
                    )
                    for params in blob_params:
                        execute_with_retry(
                            cur,
                            self.UPSERT_CHECKPOINT_BLOBS_SQL,
                            params,
                            self.retry_config,
                        )

                # Store checkpoint
                execute_with_retry(
                    cur,
                    self.UPSERT_CHECKPOINTS_SQL,
                    (
                        thread_id,
                        checkpoint_ns,
                        checkpoint["id"],
                        parent_checkpoint_id,
                        checkpoint_json,
                        metadata_json,
                    ),
                    self.retry_config,
                )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint.

        Uses batch operations (executemany) for improved performance when
        storing multiple writes.

        Args:
            config: Configuration of the related checkpoint.
            writes: List of (channel, value) pairs to store.
            task_id: Identifier for the task creating the writes.
            task_path: Path of the task creating the writes.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]

        # Determine which SQL to use based on write types
        use_upsert = all(w[0] in WRITES_IDX_MAP for w in writes)

        write_params = self._dump_writes(
            thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, writes
        )

        with timed_operation(self.metrics, "put_writes"):
            with self.lock, get_cursor(self.conn) as cur:
                if use_upsert:
                    # Batch upsert for better performance
                    if len(write_params) > 1:
                        for params in write_params:
                            execute_with_retry(
                                cur,
                                self.UPSERT_CHECKPOINT_WRITES_SQL,
                                params,
                                self.retry_config,
                            )
                    elif write_params:
                        execute_with_retry(
                            cur,
                            self.UPSERT_CHECKPOINT_WRITES_SQL,
                            write_params[0],
                            self.retry_config,
                        )
                else:
                    for params in write_params:
                        # For INSERT with duplicate check, we need to pass params twice
                        full_params = params + (
                            params[0],  # thread_id
                            params[1],  # checkpoint_ns
                            params[2],  # checkpoint_id
                            params[3],  # task_id
                            params[5],  # idx
                        )
                        execute_with_retry(
                            cur,
                            self.INSERT_CHECKPOINT_WRITES_SQL,
                            full_params,
                            self.retry_config,
                        )

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes for a thread.

        Also invalidates any cached entries for this thread.

        Args:
            thread_id: The thread ID to delete.
        """
        # Invalidate cache for this thread
        self._cache.invalidate(thread_id)

        with timed_operation(self.metrics, "delete_thread"):
            with self.lock, get_cursor(self.conn) as cur:
                execute_with_retry(
                    cur,
                    "DELETE FROM checkpoint_writes WHERE thread_id = %s",
                    (thread_id,),
                    self.retry_config,
                )
                execute_with_retry(
                    cur,
                    "DELETE FROM checkpoint_blobs WHERE thread_id = %s",
                    (thread_id,),
                    self.retry_config,
                )
                execute_with_retry(
                    cur,
                    "DELETE FROM checkpoints WHERE thread_id = %s",
                    (thread_id,),
                    self.retry_config,
                )

    def delete_before(self, max_age: timedelta) -> int:
        """Delete checkpoints older than max_age.

        This method implements TTL/expiration functionality by removing
        checkpoints that are older than the specified duration.

        Args:
            max_age: Maximum age of checkpoints to keep.

        Returns:
            Number of checkpoints deleted.

        Example:
            >>> from datetime import timedelta
            >>> # Delete checkpoints older than 7 days
            >>> deleted = checkpointer.delete_before(timedelta(days=7))
            >>> print(f"Deleted {deleted} old checkpoints")
        """
        cutoff_id = self._get_cutoff_checkpoint_id(max_age)
        logger.info(
            "Deleting checkpoints older than %s (cutoff: %s)", max_age, cutoff_id
        )

        with timed_operation(self.metrics, "delete_before"):
            with self.lock, get_cursor(self.conn) as cur:
                # Delete writes for old checkpoints
                execute_with_retry(
                    cur,
                    self.DELETE_OLD_WRITES_SQL,
                    None,
                    self.retry_config,
                )

                # Delete orphaned blobs
                execute_with_retry(
                    cur,
                    self.DELETE_OLD_BLOBS_SQL,
                    (cutoff_id,),
                    self.retry_config,
                )

                # Delete old checkpoints and get count
                execute_with_retry(
                    cur,
                    self.DELETE_OLD_CHECKPOINTS_SQL,
                    (cutoff_id, cutoff_id),
                    self.retry_config,
                )
                deleted = cur.rowcount or 0

        logger.info("Deleted %d checkpoints", deleted)
        return deleted

    def get_checkpoint_count(self) -> int:
        """Get the total number of checkpoints.

        Returns:
            Total number of checkpoints in the database.
        """
        with timed_operation(self.metrics, "get_checkpoint_count"):
            with self.lock, get_cursor(self.conn) as cur:
                execute_with_retry(
                    cur,
                    self.COUNT_CHECKPOINTS_SQL,
                    None,
                    self.retry_config,
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0

    def get_checkpoint_counts_by_thread(
        self, max_results: int = 100
    ) -> builtins.list[tuple[str, int]]:
        """Get checkpoint counts grouped by thread.

        Args:
            max_results: Maximum number of threads to return.

        Returns:
            List of (thread_id, count) tuples, ordered by count descending.
        """
        with timed_operation(self.metrics, "get_checkpoint_counts_by_thread"):
            with self.lock, get_cursor(self.conn) as cur:
                execute_with_retry(
                    cur,
                    self.COUNT_CHECKPOINTS_BY_THREAD_SQL,
                    (max_results,),
                    self.retry_config,
                )
                return [(row[0], row[1]) for row in cur.fetchall()]

    def delete_threads(self, thread_ids: builtins.list[str]) -> int:
        """Delete all checkpoints and writes for multiple threads.

        This is more efficient than calling delete_thread multiple times
        as it batches the delete operations.

        Args:
            thread_ids: List of thread IDs to delete.

        Returns:
            Number of threads deleted.
        """
        if not thread_ids:
            return 0

        # Invalidate cache for all threads
        for thread_id in thread_ids:
            self._cache.invalidate(thread_id)

        with timed_operation(self.metrics, "delete_threads"):
            with self.lock, get_cursor(self.conn) as cur:
                # Use IN clause for batch delete
                placeholders = ", ".join(["%s"] * len(thread_ids))

                execute_with_retry(
                    cur,
                    f"DELETE FROM checkpoint_writes WHERE thread_id IN ({placeholders})",
                    tuple(thread_ids),
                    self.retry_config,
                )
                execute_with_retry(
                    cur,
                    f"DELETE FROM checkpoint_blobs WHERE thread_id IN ({placeholders})",
                    tuple(thread_ids),
                    self.retry_config,
                )
                execute_with_retry(
                    cur,
                    f"DELETE FROM checkpoints WHERE thread_id IN ({placeholders})",
                    tuple(thread_ids),
                    self.retry_config,
                )

        logger.info("Deleted %d threads", len(thread_ids))
        return len(thread_ids)

    def get_metrics(self) -> dict[str, Any] | None:
        """Get current metrics statistics.

        Returns:
            Dictionary of metrics if enabled, None otherwise.
        """
        if self.metrics:
            return self.metrics.get_stats()
        return None

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache hit/miss counts and hit rate.

        Example:
            >>> stats = checkpointer.get_cache_stats()
            >>> print(f"Cache hit rate: {stats['hit_rate']:.1%}")
        """
        return self._cache.get_stats()

    def clear_cache(self) -> None:
        """Clear the in-memory cache.

        This forces subsequent get_tuple calls to query the database.
        """
        self._cache.clear()

    def benchmark(
        self,
        thread_id: str = "benchmark-thread",
        iterations: int = 100,
        warmup: int = 5,
        payload_size: int = 1000,
    ) -> dict[str, BenchmarkResult]:
        """Run performance benchmarks on checkpoint operations.

        This method benchmarks get_tuple and put operations to help
        identify performance characteristics and bottlenecks.

        Args:
            thread_id: Thread ID to use for benchmark (will be cleaned up).
            iterations: Number of iterations per operation.
            warmup: Number of warmup iterations.
            payload_size: Size of test payload in bytes.

        Returns:
            Dictionary mapping operation names to BenchmarkResult objects.

        Example:
            >>> results = checkpointer.benchmark(iterations=50)
            >>> for name, result in results.items():
            ...     print(result)
        """
        import uuid

        # Ensure setup
        self.setup()

        # Create test checkpoint data
        test_checkpoint: dict[str, Any] = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "ts": "2024-01-01T00:00:00Z",
            "channel_values": {"data": "x" * payload_size},
            "channel_versions": {},
            "versions_seen": {},
            "pending_sends": [],
        }
        test_metadata: dict[str, Any] = {"source": "input", "step": 0}
        test_config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            }
        }

        results: dict[str, BenchmarkResult] = {}

        # Benchmark put
        def do_put() -> None:
            test_checkpoint["id"] = str(uuid.uuid4())
            self.put(
                test_config,
                cast(Checkpoint, test_checkpoint),
                cast(CheckpointMetadata, test_metadata),
                {},
            )

        results["put"] = benchmark_operation(
            do_put, iterations=iterations, warmup=warmup, name="put"
        )

        # Benchmark get_tuple (with data in DB)
        # First, put a checkpoint to retrieve
        self.put(
            test_config,
            cast(Checkpoint, test_checkpoint),
            cast(CheckpointMetadata, test_metadata),
            {},
        )
        get_config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": test_checkpoint["id"],
            }
        }

        # Clear cache to benchmark DB access
        self.clear_cache()

        def do_get_uncached() -> None:
            self.clear_cache()
            self.get_tuple(get_config)

        results["get_tuple_uncached"] = benchmark_operation(
            do_get_uncached,
            iterations=iterations,
            warmup=warmup,
            name="get_tuple_uncached",
        )

        # Benchmark get_tuple with cache
        if self._cache.config.enabled:
            # Prime the cache
            self.get_tuple(get_config)

            def do_get_cached() -> None:
                self.get_tuple(get_config)

            results["get_tuple_cached"] = benchmark_operation(
                do_get_cached,
                iterations=iterations,
                warmup=warmup,
                name="get_tuple_cached",
            )

        # Cleanup
        self.delete_thread(thread_id)

        return results

    def _row_to_checkpoint_tuple(self, row: tuple[Any, ...]) -> CheckpointTuple:
        """Convert a database row to a CheckpointTuple.

        Args:
            row: A row from the checkpoints query.

        Returns:
            A CheckpointTuple with the checkpoint data.
        """
        (
            thread_id,
            checkpoint_data,
            checkpoint_ns,
            checkpoint_id,
            parent_checkpoint_id,
            metadata,
            channel_values,
            pending_writes,
        ) = row

        # Parse checkpoint JSON if it's a string
        if isinstance(checkpoint_data, str):
            checkpoint_dict = json.loads(checkpoint_data)
        else:
            checkpoint_dict = checkpoint_data

        # Parse metadata JSON if it's a string
        if isinstance(metadata, str):
            metadata_dict = json.loads(metadata)
        else:
            metadata_dict = metadata or {}

        # Load blob values
        loaded_blobs = self._load_blobs(channel_values)

        # Merge inline values with blob values
        checkpoint_dict["channel_values"] = {
            **(checkpoint_dict.get("channel_values") or {}),
            **loaded_blobs,
        }

        # Load pending writes
        loaded_writes = self._load_writes(pending_writes)

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint=checkpoint_dict,
            metadata=metadata_dict,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_checkpoint_id,
                    }
                }
                if parent_checkpoint_id
                else None
            ),
            pending_writes=loaded_writes,
        )

    # Async methods raise NotImplementedError to guide users to AsyncSnowflakeSaver
    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Async version - use AsyncSnowflakeSaver instead."""
        raise NotImplementedError(
            "SnowflakeSaver does not support async methods. "
            "Use AsyncSnowflakeSaver instead:\n"
            "from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver"
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Async version - use AsyncSnowflakeSaver instead."""
        raise NotImplementedError(
            "SnowflakeSaver does not support async methods. "
            "Use AsyncSnowflakeSaver instead:\n"
            "from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver"
        )
        # Make this an async generator to satisfy the return type
        yield  # pragma: no cover

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Async version - use AsyncSnowflakeSaver instead."""
        raise NotImplementedError(
            "SnowflakeSaver does not support async methods. "
            "Use AsyncSnowflakeSaver instead:\n"
            "from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver"
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Async version - use AsyncSnowflakeSaver instead."""
        raise NotImplementedError(
            "SnowflakeSaver does not support async methods. "
            "Use AsyncSnowflakeSaver instead:\n"
            "from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver"
        )

    async def adelete_thread(self, thread_id: str) -> None:
        """Async version - use AsyncSnowflakeSaver instead."""
        raise NotImplementedError(
            "SnowflakeSaver does not support async methods. "
            "Use AsyncSnowflakeSaver instead:\n"
            "from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver"
        )


__all__ = [
    "AsyncSnowflakeSaver",
    "BaseSnowflakeSaver",
    "BenchmarkResult",
    "CacheConfig",
    "Metrics",
    "RetryConfig",
    "SnowflakeAuthenticationError",
    "SnowflakeCheckpointError",
    "SnowflakeConfigurationError",
    "SnowflakeConnectionError",
    "SnowflakeQueryError",
    "SnowflakeSaver",
    "SnowflakeSchemaError",
    "SnowflakeSerializationError",
    "SnowflakeTransientError",
    "SnowflakeWarehouseError",
    "__version__",
    "benchmark_operation",
    "configure_logging",
    "set_log_level",
]
