"""Async Snowflake checkpoint saver for LangGraph.

This module provides an async wrapper around the sync Snowflake connector,
using asyncio.to_thread() for non-blocking execution.

Features:
    - Retry with exponential backoff for transient errors
    - TTL/expiration for automatic cleanup of old checkpoints
    - Metrics/observability support
    - Optional Redis/Valkey write-back cache for 100x faster writes
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager
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
    Conn,
    Metrics,
    RetryConfig,
    _load_private_key,
    create_connection,
    execute_with_retry,
    get_connection_params_from_env,
    get_cursor,
    logger,
    timed_operation,
)
from langgraph_checkpoint_snowflake.base import BaseSnowflakeSaver
from langgraph_checkpoint_snowflake.redis_cache import (
    RedisWriteCache,
    RedisWriteCacheConfig,
)


class AsyncSnowflakeSaver(BaseSnowflakeSaver):
    """Async checkpoint saver that stores checkpoints in Snowflake.

    IMPORTANT: This implementation wraps synchronous Snowflake connector operations
    with ``asyncio.to_thread()``. It provides an async-compatible API but does NOT
    deliver true async I/O performance benefits.

    Implications:
        - Operations still block thread pool threads
        - No performance benefit under high concurrency (>10 concurrent ops)
        - Works fine for moderate async workloads
        - Provides async-compatible API for async applications

    For high-concurrency workloads requiring true async I/O, consider using
    ``langgraph-checkpoint-postgres`` with ``asyncpg`` instead.

    Features:
        - Retry with exponential backoff for transient errors
        - TTL/expiration for automatic cleanup of old checkpoints
        - Metrics/observability support
        - Optional nest_asyncio support for sync fallbacks from async context
        - Optional Redis/Valkey write-back cache for 100x faster writes

    Args:
        conn: A Snowflake connection object.
        serde: Optional serializer for checkpoint data.
        retry_config: Optional retry configuration.
        metrics: Optional metrics collector.
        allow_sync_from_async: If True, enables sync method calls from within
            an async context using nest_asyncio. Defaults to False.
        redis_cache_config: Optional Redis write-back cache configuration.

    Example:
        >>> async with AsyncSnowflakeSaver.from_conn_string(
        ...     account="my_account",
        ...     user="my_user",
        ...     password="my_password",
        ...     warehouse="my_warehouse",
        ...     database="my_database",
        ...     schema="my_schema",
        ... ) as checkpointer:
        ...     await checkpointer.asetup()
        ...     graph = builder.compile(checkpointer=checkpointer)
        ...     result = await graph.ainvoke({"input": "hello"}, config)

    Example with Redis write-back cache:
        >>> from langgraph_checkpoint_snowflake import RedisWriteCacheConfig
        >>> redis_config = RedisWriteCacheConfig(
        ...     enabled=True,
        ...     redis_url="redis://localhost:6379/0",
        ... )
        >>> async with AsyncSnowflakeSaver.from_env(
        ...     redis_cache_config=redis_config,
        ... ) as checkpointer:
        ...     await checkpointer.asetup()
        ...     # Writes go to Redis (1-3ms), background sync to Snowflake
    """

    conn: Conn
    lock: asyncio.Lock
    is_setup: bool
    retry_config: RetryConfig
    metrics: Metrics | None
    _loop: asyncio.AbstractEventLoop | None
    _allow_sync_from_async: bool
    _nest_asyncio_applied: bool
    _redis_cache: RedisWriteCache | None

    def __init__(
        self,
        conn: Conn,
        *,
        serde: SerializerProtocol | None = None,
        retry_config: RetryConfig | None = None,
        metrics: Metrics | None = None,
        allow_sync_from_async: bool = False,
        redis_cache_config: RedisWriteCacheConfig | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.conn = conn
        self.lock = asyncio.Lock()
        self.is_setup = False
        self.retry_config = retry_config or RetryConfig()
        self.metrics = metrics
        self._loop = None
        self._allow_sync_from_async = allow_sync_from_async
        self._nest_asyncio_applied = False

        # Initialize Redis write cache if enabled
        self._redis_cache = None
        if redis_cache_config and redis_cache_config.enabled:
            from langgraph_checkpoint_snowflake.redis_cache import RedisWriteCache

            self._redis_cache = RedisWriteCache(
                config=redis_cache_config,
                snowflake_saver=self,
            )
            self._redis_cache.start()
            logger.info("Redis write-back cache enabled (async)")

    def _ensure_nest_asyncio(self) -> None:
        """Apply nest_asyncio if enabled and not already applied."""
        if self._allow_sync_from_async and not self._nest_asyncio_applied:
            try:
                import nest_asyncio

                nest_asyncio.apply()
                self._nest_asyncio_applied = True
                logger.debug("nest_asyncio applied for sync-from-async support")
            except ImportError as e:
                raise ImportError(
                    "nest_asyncio is required for allow_sync_from_async=True. "
                    "Install it with: pip install nest_asyncio"
                ) from e

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Get the event loop, caching it for sync method access."""
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    @classmethod
    @asynccontextmanager
    async def from_conn_string(
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
    ) -> AsyncIterator[AsyncSnowflakeSaver]:
        """Create an AsyncSnowflakeSaver from connection parameters.

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
            A configured AsyncSnowflakeSaver instance.
        """
        conn = await asyncio.to_thread(
            create_connection,
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
            await asyncio.to_thread(conn.close)

    @classmethod
    @asynccontextmanager
    async def from_env(
        cls,
        *,
        serde: SerializerProtocol | None = None,
    ) -> AsyncIterator[AsyncSnowflakeSaver]:
        """Create an AsyncSnowflakeSaver from environment variables.

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
            A configured AsyncSnowflakeSaver instance.
        """
        params = get_connection_params_from_env()
        conn = await asyncio.to_thread(create_connection, **params)
        try:
            yield cls(conn, serde=serde)
        finally:
            await asyncio.to_thread(conn.close)

    @classmethod
    @asynccontextmanager
    async def from_key_pair(
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
    ) -> AsyncIterator[AsyncSnowflakeSaver]:
        """Create an AsyncSnowflakeSaver using key pair authentication.

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
            A configured AsyncSnowflakeSaver instance.

        Example:
            >>> async with AsyncSnowflakeSaver.from_key_pair(
            ...     account="my_account",
            ...     user="my_user",
            ...     warehouse="my_warehouse",
            ...     database="my_database",
            ...     schema="my_schema",
            ...     private_key_path="/path/to/rsa_key.p8",
            ... ) as checkpointer:
            ...     await checkpointer.setup()
            ...     # Use checkpointer...
        """
        private_key_bytes = _load_private_key(
            private_key_path=private_key_path,
            private_key=private_key,
            private_key_passphrase=private_key_passphrase,
        )
        conn = await asyncio.to_thread(
            create_connection,
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
            await asyncio.to_thread(conn.close)

    async def setup(self) -> None:
        """Set up the checkpoint database tables asynchronously.

        This method creates the necessary tables in Snowflake if they don't
        already exist and runs any pending migrations.

        Note: This is an alias for asetup() for backwards compatibility.
        """
        await self.asetup()

    async def asetup(self) -> None:
        """Set up the checkpoint database tables asynchronously.

        This method creates the necessary tables in Snowflake if they don't
        already exist and runs any pending migrations.

        This follows the LangGraph naming convention for async methods (asetup).
        """
        if self.is_setup:
            return

        async with self.lock:
            if self.is_setup:
                return

            await asyncio.to_thread(self._setup_sync)
            self.is_setup = True

    def _setup_sync(self) -> None:
        """Synchronous setup implementation."""
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
                    if "already exists" in str(e).lower():
                        cur.execute(
                            "INSERT INTO checkpoint_migrations (v) "
                            "SELECT %s WHERE NOT EXISTS "
                            "(SELECT 1 FROM checkpoint_migrations WHERE v = %s)",
                            (v, v),
                        )
                    else:
                        raise

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Get a checkpoint tuple from the database asynchronously.

        Checks Redis write cache first if enabled, then queries Snowflake.

        Args:
            config: Configuration specifying which checkpoint to retrieve.

        Returns:
            The checkpoint tuple if found, None otherwise.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Check Redis write cache first (sync operation, but fast)
        if self._redis_cache:
            redis_data = self._redis_cache.get(thread_id, checkpoint_ns, checkpoint_id)
            if redis_data:
                logger.debug(
                    "Redis cache hit for thread=%s, checkpoint=%s",
                    thread_id,
                    checkpoint_id,
                )
                return self._build_checkpoint_tuple_from_redis(redis_data, config)

        # Query Snowflake
        async with self.lock:
            return await asyncio.to_thread(self._get_tuple_sync, config)

    def _get_tuple_sync(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Synchronous get_tuple implementation."""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        with timed_operation(self.metrics, "aget_tuple"):
            with get_cursor(self.conn) as cur:
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

                return self._row_to_checkpoint_tuple(row)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints matching the given criteria asynchronously.

        Args:
            config: Configuration for filtering by thread_id and checkpoint_ns.
            filter: Additional metadata filters.
            before: Only return checkpoints before this one.
            limit: Maximum number of checkpoints to return.

        Yields:
            Matching checkpoint tuples, ordered by checkpoint_id descending.
        """
        # Fetch all results in a thread, then yield them
        results = await asyncio.to_thread(
            self._list_sync, config, filter, before, limit
        )
        for result in results:
            yield result

    def _list_sync(
        self,
        config: RunnableConfig | None,
        filter: dict[str, Any] | None,
        before: RunnableConfig | None,
        limit: int | None,
    ) -> list[CheckpointTuple]:
        """Synchronous list implementation."""
        where, params = self._search_where(config, filter, before)
        query = self.SELECT_SQL + where + " ORDER BY c.checkpoint_id DESC"

        if limit is not None:
            query += f" LIMIT {int(limit)}"

        results = []
        with timed_operation(self.metrics, "alist"):
            with get_cursor(self.conn) as cur:
                execute_with_retry(cur, query, params, self.retry_config)
                for row in cur:
                    results.append(
                        self._row_to_checkpoint_tuple(cast(tuple[Any, ...], row))
                    )
        return results

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint in the database asynchronously.

        If Redis write-back cache is enabled, writes go to Redis first (1-3ms)
        and are asynchronously synced to Snowflake in the background.

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

        # If Redis write cache is enabled, write there (fast path)
        if self._redis_cache:
            checkpoint_data = dict(checkpoint)
            metadata_dict = dict(get_checkpoint_metadata(config, metadata))

            self._redis_cache.put(
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                checkpoint_id=checkpoint["id"],
                checkpoint_data=checkpoint_data,
                metadata=metadata_dict,
                parent_checkpoint_id=parent_checkpoint_id,
            )

            # Return immediately (background sync to Snowflake)
            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint["id"],
                }
            }

        # Direct write to Snowflake (slow path)
        async with self.lock:
            return await asyncio.to_thread(
                self._put_sync, config, checkpoint, metadata, new_versions
            )

    def _put_sync(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Synchronous put implementation."""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        parent_checkpoint_id = config["configurable"].get("checkpoint_id")

        # Prepare checkpoint data
        checkpoint_copy = checkpoint.copy()
        checkpoint_copy["channel_values"] = checkpoint_copy["channel_values"].copy()

        blob_values = {}
        for k, v in checkpoint["channel_values"].items():
            if v is None or isinstance(v, str | int | float | bool):
                pass
            else:
                blob_values[k] = checkpoint_copy["channel_values"].pop(k)

        checkpoint_json = json.dumps(checkpoint_copy)
        metadata_json = json.dumps(get_checkpoint_metadata(config, metadata))

        with timed_operation(self.metrics, "aput"):
            with get_cursor(self.conn) as cur:
                # Store blobs
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

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint asynchronously.

        Args:
            config: Configuration of the related checkpoint.
            writes: List of (channel, value) pairs to store.
            task_id: Identifier for the task creating the writes.
            task_path: Path of the task creating the writes.
        """
        async with self.lock:
            await asyncio.to_thread(
                self._put_writes_sync, config, writes, task_id, task_path
            )

    def _put_writes_sync(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str,
    ) -> None:
        """Synchronous put_writes implementation."""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]

        use_upsert = all(w[0] in WRITES_IDX_MAP for w in writes)
        write_params = self._dump_writes(
            thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, writes
        )

        with timed_operation(self.metrics, "aput_writes"):
            with get_cursor(self.conn) as cur:
                if use_upsert:
                    for params in write_params:
                        execute_with_retry(
                            cur,
                            self.UPSERT_CHECKPOINT_WRITES_SQL,
                            params,
                            self.retry_config,
                        )
                else:
                    for params in write_params:
                        full_params = params + (
                            params[0],
                            params[1],
                            params[2],
                            params[3],
                            params[5],
                        )
                        execute_with_retry(
                            cur,
                            self.INSERT_CHECKPOINT_WRITES_SQL,
                            full_params,
                            self.retry_config,
                        )

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes for a thread asynchronously.

        Args:
            thread_id: The thread ID to delete.
        """
        async with self.lock:
            await asyncio.to_thread(self._delete_thread_sync, thread_id)

    def _delete_thread_sync(self, thread_id: str) -> None:
        """Synchronous delete_thread implementation."""
        with timed_operation(self.metrics, "adelete_thread"):
            with get_cursor(self.conn) as cur:
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

    async def adelete_before(self, max_age: timedelta) -> int:
        """Delete checkpoints older than max_age asynchronously.

        Args:
            max_age: Maximum age of checkpoints to keep.

        Returns:
            Number of checkpoints deleted.
        """
        async with self.lock:
            return await asyncio.to_thread(self._delete_before_sync, max_age)

    def _delete_before_sync(self, max_age: timedelta) -> int:
        """Synchronous delete_before implementation."""
        cutoff_id = self._get_cutoff_checkpoint_id(max_age)
        logger.info(
            "Deleting checkpoints older than %s (cutoff: %s)", max_age, cutoff_id
        )

        with timed_operation(self.metrics, "adelete_before"):
            with get_cursor(self.conn) as cur:
                execute_with_retry(
                    cur,
                    self.DELETE_OLD_WRITES_SQL,
                    None,
                    self.retry_config,
                )
                execute_with_retry(
                    cur,
                    self.DELETE_OLD_BLOBS_SQL,
                    (cutoff_id,),
                    self.retry_config,
                )
                execute_with_retry(
                    cur,
                    self.DELETE_OLD_CHECKPOINTS_SQL,
                    (cutoff_id, cutoff_id),
                    self.retry_config,
                )
                deleted = cur.rowcount or 0

        logger.info("Deleted %d checkpoints", deleted)
        return deleted

    async def aget_checkpoint_count(self) -> int:
        """Get the total number of checkpoints asynchronously.

        Returns:
            Total number of checkpoints in the database.
        """
        async with self.lock:
            return await asyncio.to_thread(self._get_checkpoint_count_sync)

    def _get_checkpoint_count_sync(self) -> int:
        """Synchronous get_checkpoint_count implementation."""
        with timed_operation(self.metrics, "aget_checkpoint_count"):
            with get_cursor(self.conn) as cur:
                execute_with_retry(
                    cur,
                    self.COUNT_CHECKPOINTS_SQL,
                    None,
                    self.retry_config,
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0

    def get_metrics(self) -> dict[str, Any] | None:
        """Get current metrics statistics.

        Returns:
            Dictionary of metrics if enabled, None otherwise.
        """
        if self.metrics:
            return self.metrics.get_stats()
        return None

    def get_redis_cache_stats(self) -> dict[str, Any] | None:
        """Get Redis write-back cache statistics.

        Returns:
            Dictionary with Redis cache stats if enabled, None otherwise.
        """
        if self._redis_cache:
            return self._redis_cache.get_stats()
        return None

    async def aflush_redis_cache(self, timeout: float = 30.0) -> int:
        """Flush Redis write cache to Snowflake asynchronously.

        Forces immediate sync of all pending writes to Snowflake.

        Args:
            timeout: Max time to wait for flush (seconds).

        Returns:
            Number of checkpoints flushed.
        """
        if self._redis_cache:
            return await asyncio.to_thread(self._redis_cache.flush, timeout)
        logger.warning("Redis write cache not enabled, nothing to flush")
        return 0

    async def ashutdown_redis_cache(self, flush_timeout: float = 30.0) -> None:
        """Shutdown Redis write cache gracefully.

        Args:
            flush_timeout: Max time to wait for flush (seconds).
        """
        if self._redis_cache:
            await asyncio.to_thread(self._redis_cache.shutdown, flush_timeout)
            self._redis_cache = None

    def _build_checkpoint_tuple_from_redis(
        self,
        redis_data: dict[str, Any],
        config: RunnableConfig,
    ) -> CheckpointTuple:
        """Build a CheckpointTuple from Redis cache data.

        Args:
            redis_data: Data retrieved from Redis cache.
            config: Original config for the request.

        Returns:
            CheckpointTuple constructed from the cached data.
        """
        thread_id = redis_data["thread_id"]
        checkpoint_ns = redis_data["checkpoint_ns"]
        checkpoint_id = redis_data["checkpoint_id"]
        parent_checkpoint_id = redis_data.get("parent_checkpoint_id")
        checkpoint_dict = redis_data["checkpoint"]
        metadata_dict = redis_data.get("metadata", {})

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
            pending_writes=[],
        )

    def _row_to_checkpoint_tuple(self, row: tuple[Any, ...]) -> CheckpointTuple:
        """Convert a database row to a CheckpointTuple."""
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

        if isinstance(checkpoint_data, str):
            checkpoint_dict = json.loads(checkpoint_data)
        else:
            checkpoint_dict = checkpoint_data

        if isinstance(metadata, str):
            metadata_dict = json.loads(metadata)
        else:
            metadata_dict = metadata or {}

        loaded_blobs = self._load_blobs(channel_values)
        checkpoint_dict["channel_values"] = {
            **(checkpoint_dict.get("channel_values") or {}),
            **loaded_blobs,
        }

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

    # Sync methods delegate to async
    # By default, uses run_coroutine_threadsafe (requires different thread)
    # With allow_sync_from_async=True, uses nest_asyncio (works from same thread)

    def _run_sync(self, coro: Any) -> Any:
        """Run an async coroutine synchronously.

        If allow_sync_from_async is True, uses nest_asyncio to allow
        calling from within an async context. Otherwise, uses
        run_coroutine_threadsafe which requires a different thread.
        """
        if self._allow_sync_from_async:
            self._ensure_nest_asyncio()
            # With nest_asyncio, we can use asyncio.run or get_event_loop().run_until_complete
            try:
                loop = asyncio.get_running_loop()
                return loop.run_until_complete(coro)
            except RuntimeError:
                # No running loop, create one
                return asyncio.run(coro)
        else:
            # Default behavior: use run_coroutine_threadsafe
            try:
                if asyncio.get_running_loop() is self.loop:
                    raise asyncio.InvalidStateError(
                        "Synchronous calls to AsyncSnowflakeSaver are only allowed from a "
                        "different thread. Set allow_sync_from_async=True to enable sync "
                        "calls from within an async context (requires nest_asyncio)."
                    )
            except RuntimeError:
                pass
            return asyncio.run_coroutine_threadsafe(coro, self.loop).result()

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Get a checkpoint tuple (sync wrapper for async implementation).

        Note: By default, this method is for use from background threads only.
        Set allow_sync_from_async=True to enable calls from async context.
        """
        result: CheckpointTuple | None = self._run_sync(self.aget_tuple(config))
        return result

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints (sync wrapper for async implementation).

        Note: By default, this method is for use from background threads only.
        Set allow_sync_from_async=True to enable calls from async context.
        """
        if self._allow_sync_from_async:
            self._ensure_nest_asyncio()
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            aiter_ = self.alist(config, filter=filter, before=before, limit=limit)
            while True:
                try:
                    yield loop.run_until_complete(anext(aiter_))
                except StopAsyncIteration:
                    break
        else:
            try:
                if asyncio.get_running_loop() is self.loop:
                    raise asyncio.InvalidStateError(
                        "Synchronous calls to AsyncSnowflakeSaver are only allowed from a "
                        "different thread. Set allow_sync_from_async=True to enable sync "
                        "calls from within an async context (requires nest_asyncio)."
                    )
            except RuntimeError:
                pass

            aiter_ = self.alist(config, filter=filter, before=before, limit=limit)
            while True:
                try:
                    coro = anext(aiter_)
                    yield asyncio.run_coroutine_threadsafe(
                        coro,
                        self.loop,
                    ).result()
                except StopAsyncIteration:
                    break

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint (sync wrapper for async implementation).

        Note: By default, this method is for use from background threads only.
        Set allow_sync_from_async=True to enable calls from async context.
        """
        result: RunnableConfig = self._run_sync(
            self.aput(config, checkpoint, metadata, new_versions)
        )
        return result

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store writes (sync wrapper for async implementation).

        Note: By default, this method is for use from background threads only.
        Set allow_sync_from_async=True to enable calls from async context.
        """
        self._run_sync(self.aput_writes(config, writes, task_id, task_path))

    def delete_thread(self, thread_id: str) -> None:
        """Delete a thread (sync wrapper for async implementation).

        Note: By default, this method is for use from background threads only.
        Set allow_sync_from_async=True to enable calls from async context.
        """
        self._run_sync(self.adelete_thread(thread_id))


__all__ = ["AsyncSnowflakeSaver"]
