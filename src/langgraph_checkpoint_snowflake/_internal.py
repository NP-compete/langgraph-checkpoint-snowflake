"""Internal utilities for Snowflake connection management."""

from __future__ import annotations

import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor

if TYPE_CHECKING:
    # Type alias for connection types we accept
    Conn = SnowflakeConnection
else:
    Conn = "SnowflakeConnection"

# Module logger
logger = logging.getLogger(__name__)


def configure_logging(
    level: int | str = logging.INFO,
    format_string: str | None = None,
    handler: logging.Handler | None = None,
) -> logging.Logger:
    """Configure logging for the langgraph-checkpoint-snowflake package.

    This function provides a convenient way to set up logging with
    configurable verbosity levels.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            Can be an integer or string like "DEBUG".
        format_string: Custom format string for log messages.
            Defaults to "%(asctime)s - %(name)s - %(levelname)s - %(message)s".
        handler: Custom handler. Defaults to StreamHandler(sys.stderr).

    Returns:
        The configured logger instance.

    Example:
        >>> from langgraph_checkpoint_snowflake import configure_logging
        >>> import logging
        >>> # Enable debug logging
        >>> configure_logging(logging.DEBUG)
        >>> # Or use string level
        >>> configure_logging("DEBUG")
        >>> # Custom format
        >>> configure_logging(
        ...     level="INFO",
        ...     format_string="%(levelname)s: %(message)s"
        ... )
    """
    # Convert string level to int if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    # Get the package logger
    pkg_logger = logging.getLogger("langgraph_checkpoint_snowflake")
    pkg_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    pkg_logger.handlers.clear()

    # Create handler if not provided
    if handler is None:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)

    # Set format
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)

    pkg_logger.addHandler(handler)

    return pkg_logger


def set_log_level(level: int | str) -> None:
    """Set the logging level for the package.

    A simpler alternative to configure_logging() when you just want
    to change the verbosity level.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            Can be an integer or string like "DEBUG".

    Example:
        >>> from langgraph_checkpoint_snowflake import set_log_level
        >>> # Enable debug logging
        >>> set_log_level("DEBUG")
        >>> # Reduce to warnings only
        >>> set_log_level("WARNING")
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    pkg_logger = logging.getLogger("langgraph_checkpoint_snowflake")
    pkg_logger.setLevel(level)

    # Also update handlers
    for handler in pkg_logger.handlers:
        handler.setLevel(level)


def _load_private_key(
    private_key_path: str | None = None,
    private_key: str | bytes | None = None,
    private_key_passphrase: str | None = None,
) -> bytes:
    """Load and decode a private key for Snowflake key pair authentication.

    Args:
        private_key_path: Path to a PEM-encoded private key file.
        private_key: PEM-encoded private key as string or bytes.
        private_key_passphrase: Passphrase for encrypted private keys.

    Returns:
        DER-encoded private key bytes suitable for Snowflake connector.

    Raises:
        ValueError: If neither private_key_path nor private_key is provided.
    """
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    if private_key_path:
        with open(private_key_path, "rb") as key_file:
            key_data = key_file.read()
    elif private_key:
        key_data = (
            private_key.encode("utf-8") if isinstance(private_key, str) else private_key
        )
    else:
        raise ValueError("Either private_key_path or private_key must be provided")

    passphrase = (
        private_key_passphrase.encode("utf-8") if private_key_passphrase else None
    )

    p_key = serialization.load_pem_private_key(
        key_data,
        password=passphrase,
        backend=default_backend(),
    )

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection_params_from_env() -> dict[str, Any]:
    """Get Snowflake connection parameters from environment variables.

    Supports both password and key pair authentication:
    - Password auth: Set SNOWFLAKE_PASSWORD
    - Key pair auth: Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PRIVATE_KEY

    Returns:
        Dictionary of connection parameters.

    Raises:
        ValueError: If required environment variables are not set.
    """
    # Base required vars (always needed)
    base_required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]

    missing_base = [var for var in base_required if not os.environ.get(var)]
    if missing_base:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_base)}"
        )

    # Check for authentication method
    has_password = bool(os.environ.get("SNOWFLAKE_PASSWORD"))
    has_key_path = bool(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"))
    has_key_data = bool(os.environ.get("SNOWFLAKE_PRIVATE_KEY"))

    if not has_password and not has_key_path and not has_key_data:
        raise ValueError(
            "Missing authentication: Set SNOWFLAKE_PASSWORD for password auth, "
            "or SNOWFLAKE_PRIVATE_KEY_PATH / SNOWFLAKE_PRIVATE_KEY for key pair auth"
        )

    params: dict[str, Any] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
    }

    # Set up authentication
    if has_key_path or has_key_data:
        # Key pair authentication
        params["private_key"] = _load_private_key(
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key=os.environ.get("SNOWFLAKE_PRIVATE_KEY"),
            private_key_passphrase=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        )
    else:
        # Password authentication
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]

    # Optional parameters
    if role := os.environ.get("SNOWFLAKE_ROLE"):
        params["role"] = role
    if authenticator := os.environ.get("SNOWFLAKE_AUTHENTICATOR"):
        params["authenticator"] = authenticator

    return params


def create_connection(
    account: str,
    user: str,
    warehouse: str,
    database: str,
    schema: str,
    password: str | None = None,
    private_key: bytes | None = None,
    private_key_path: str | None = None,
    private_key_passphrase: str | None = None,
    role: str | None = None,
    authenticator: str | None = None,
    **kwargs: Any,
) -> SnowflakeConnection:
    """Create a new Snowflake connection.

    Supports both password and key pair authentication:
    - Password auth: Provide `password`
    - Key pair auth: Provide `private_key` (DER bytes) or `private_key_path`

    Args:
        account: Snowflake account identifier.
        user: Username for authentication.
        warehouse: Warehouse to use.
        database: Database to use.
        schema: Schema to use.
        password: Password for authentication (mutually exclusive with private_key).
        private_key: DER-encoded private key bytes for key pair auth.
        private_key_path: Path to PEM-encoded private key file.
        private_key_passphrase: Passphrase for encrypted private keys.
        role: Optional role to use.
        authenticator: Optional authenticator method.
        **kwargs: Additional connection parameters.

    Returns:
        A new Snowflake connection.

    Raises:
        ValueError: If neither password nor private key is provided.
    """
    import snowflake.connector

    conn_params: dict[str, Any] = {
        "account": account,
        "user": user,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        **kwargs,
    }

    # Handle authentication
    if private_key:
        conn_params["private_key"] = private_key
    elif private_key_path:
        conn_params["private_key"] = _load_private_key(
            private_key_path=private_key_path,
            private_key_passphrase=private_key_passphrase,
        )
    elif password:
        conn_params["password"] = password
    else:
        raise ValueError(
            "Authentication required: provide password, private_key, or private_key_path"
        )

    if role:
        conn_params["role"] = role
    if authenticator:
        conn_params["authenticator"] = authenticator

    return snowflake.connector.connect(**conn_params)


@contextmanager
def get_cursor(conn: SnowflakeConnection) -> Iterator[SnowflakeCursor]:
    """Get a cursor from a connection as a context manager.

    Args:
        conn: Snowflake connection.

    Yields:
        A cursor for executing queries.
    """
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


def execute_with_retry(
    cursor: SnowflakeCursor,
    query: str,
    params: tuple[Any, ...] | list[Any] | None = None,
    retry_config: RetryConfig | None = None,
) -> SnowflakeCursor:
    """Execute a query with retry logic for transient errors.

    Args:
        cursor: Snowflake cursor.
        query: SQL query to execute.
        params: Query parameters.
        retry_config: Retry configuration. Defaults to RetryConfig().

    Returns:
        The cursor after successful execution.

    Raises:
        SnowflakeTransientError: If all retries fail for transient errors.
        SnowflakeQueryError: For non-retryable query errors.
        SnowflakeCheckpointError: For other Snowflake errors.
    """
    import random

    import snowflake.connector.errors as sf_errors

    from langgraph_checkpoint_snowflake.exceptions import wrap_snowflake_error

    config = retry_config or RetryConfig()
    last_error: Exception | None = None
    retryable_errors = (
        sf_errors.ServiceUnavailableError,
        sf_errors.OperationalError,
        sf_errors.DatabaseError,
    )

    for attempt in range(config.max_retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except retryable_errors as e:
            last_error = e
            if attempt < config.max_retries - 1:
                delay = min(config.base_delay * (2**attempt), config.max_delay)
                if config.jitter:
                    delay *= 0.5 + random.random()
                logger.warning(
                    "Retrying query after error (attempt %d/%d): %s",
                    attempt + 1,
                    config.max_retries,
                    e,
                )
                time.sleep(delay)
            continue
        except Exception as e:
            # Wrap non-retryable errors
            raise wrap_snowflake_error(e, query=query[:100] if query else None) from e

    if last_error:
        # Wrap the final error with retry count
        raise wrap_snowflake_error(
            last_error,
            message=f"Query failed after {config.max_retries} retries",
            query=query[:100] if query else None,
            retry_count=config.max_retries,
        ) from last_error
    raise RuntimeError("Unexpected state in execute_with_retry")


def executemany_with_retry(
    cursor: SnowflakeCursor,
    query: str,
    params_list: list[tuple[Any, ...]],
    retry_config: RetryConfig | None = None,
) -> SnowflakeCursor:
    """Execute a query with multiple parameter sets with retry logic.

    Args:
        cursor: Snowflake cursor.
        query: SQL query to execute.
        params_list: List of parameter tuples.
        retry_config: Retry configuration. Defaults to RetryConfig().

    Returns:
        The cursor after successful execution.

    Raises:
        SnowflakeTransientError: If all retries fail for transient errors.
        SnowflakeQueryError: For non-retryable query errors.
        SnowflakeCheckpointError: For other Snowflake errors.
    """
    import random

    import snowflake.connector.errors as sf_errors

    from langgraph_checkpoint_snowflake.exceptions import wrap_snowflake_error

    config = retry_config or RetryConfig()
    last_error: Exception | None = None
    retryable_errors = (
        sf_errors.ServiceUnavailableError,
        sf_errors.OperationalError,
        sf_errors.DatabaseError,
    )

    for attempt in range(config.max_retries):
        try:
            cursor.executemany(query, params_list)
            return cursor
        except retryable_errors as e:
            last_error = e
            if attempt < config.max_retries - 1:
                delay = min(config.base_delay * (2**attempt), config.max_delay)
                if config.jitter:
                    delay *= 0.5 + random.random()
                logger.warning(
                    "Retrying executemany after error (attempt %d/%d): %s",
                    attempt + 1,
                    config.max_retries,
                    e,
                )
                time.sleep(delay)
            continue
        except Exception as e:
            # Wrap non-retryable errors
            raise wrap_snowflake_error(e, query=query[:100] if query else None) from e

    if last_error:
        # Wrap the final error with retry count
        raise wrap_snowflake_error(
            last_error,
            message=f"Batch query failed after {config.max_retries} retries",
            query=query[:100] if query else None,
            retry_count=config.max_retries,
        ) from last_error
    raise RuntimeError("Unexpected state in executemany_with_retry")


class RetryConfig(BaseModel):
    """Configuration for retry behavior.

    Attributes:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay in seconds between retries.
        jitter: Whether to add random jitter to delays.

    Example:
        >>> from langgraph_checkpoint_snowflake import RetryConfig
        >>> config = RetryConfig(max_retries=5, base_delay=2.0)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_retries: int = Field(
        default=3, ge=1, le=10, description="Maximum retry attempts"
    )
    base_delay: float = Field(
        default=1.0, ge=0.0, le=60.0, description="Base delay in seconds"
    )
    max_delay: float = Field(
        default=60.0, ge=0.0, le=300.0, description="Maximum delay in seconds"
    )
    jitter: bool = Field(default=True, description="Add random jitter to delays")


class PoolConfig(BaseModel):
    """Configuration for connection pooling.

    Attributes:
        pool_size: Number of connections in the pool.
        pool_timeout: Timeout in seconds when waiting for a connection.
        pool_recycle: Time in seconds before a connection is recycled.

    Example:
        >>> from langgraph_checkpoint_snowflake import PoolConfig
        >>> config = PoolConfig(pool_size=10, pool_timeout=60.0)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    pool_size: int = Field(default=5, ge=1, le=100, description="Number of connections")
    pool_timeout: float = Field(
        default=30.0, ge=1.0, le=300.0, description="Connection timeout"
    )
    pool_recycle: float = Field(
        default=3600.0, ge=60.0, le=86400.0, description="Recycle time"
    )


class CacheConfig(BaseModel):
    """Configuration for in-memory LRU cache.

    The cache stores recently accessed checkpoints to reduce database queries
    for frequently accessed threads.

    Attributes:
        enabled: Whether caching is enabled.
        max_size: Maximum number of checkpoints to cache.
        ttl_seconds: Time-to-live for cached entries in seconds.
            Set to 0 or None for no expiration.

    Example:
        >>> from langgraph_checkpoint_snowflake import CacheConfig, SnowflakeSaver
        >>> # Cache up to 100 checkpoints for 5 minutes
        >>> cache_config = CacheConfig(enabled=True, max_size=100, ttl_seconds=300)
        >>> saver = SnowflakeSaver(conn, cache_config=cache_config)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    enabled: bool = Field(default=False, description="Enable caching")
    max_size: int = Field(
        default=128, ge=1, le=10000, description="Maximum cache entries"
    )
    ttl_seconds: float | None = Field(
        default=300.0, ge=0, description="TTL in seconds (None=no expiry)"
    )


class SerializationConfig(BaseModel):
    """Configuration for checkpoint serialization.

    Attributes:
        use_msgpack: If True, use MessagePack for smaller payloads.
            Falls back to JSON if msgpack is not installed.
        compression: Optional compression method ('gzip', 'lz4', or None).
            Requires the corresponding library to be installed.

    Example:
        >>> from langgraph_checkpoint_snowflake import SerializationConfig
        >>> # Use MessagePack for smaller payloads
        >>> config = SerializationConfig(use_msgpack=True)
        >>> # With compression
        >>> config = SerializationConfig(use_msgpack=True, compression="gzip")
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    use_msgpack: bool = Field(
        default=False, description="Use MessagePack serialization"
    )
    compression: str | None = Field(
        default=None,
        pattern=r"^(gzip|lz4)$",
        description="Compression method (gzip, lz4, or None)",
    )


class FastSerializer:
    """Optimized serializer with optional MessagePack and compression support.

    This serializer provides faster serialization and smaller payloads compared
    to the default JSON serializer, especially for binary data.
    """

    def __init__(self, config: SerializationConfig | None = None) -> None:
        """Initialize the serializer.

        Args:
            config: Serialization configuration.
        """
        self.config = config or SerializationConfig()
        self._msgpack_available: bool | None = None
        self._compression_available: dict[str, bool] = {}

    def _check_msgpack(self) -> bool:
        """Check if msgpack is available."""
        if self._msgpack_available is None:
            try:
                import msgpack  # noqa: F401

                self._msgpack_available = True
            except ImportError:
                self._msgpack_available = False
                if self.config.use_msgpack:
                    logger.warning(
                        "msgpack not installed, falling back to JSON. "
                        "Install with: pip install msgpack"
                    )
        return self._msgpack_available

    def _check_compression(self, method: str) -> bool:
        """Check if a compression method is available."""
        if method not in self._compression_available:
            if method == "gzip":
                self._compression_available[method] = True  # Built-in
            elif method == "lz4":
                try:
                    import lz4.frame  # noqa: F401

                    self._compression_available[method] = True
                except ImportError:
                    self._compression_available[method] = False
                    logger.warning(
                        "lz4 not installed, compression disabled. "
                        "Install with: pip install lz4"
                    )
            else:
                self._compression_available[method] = False
        return self._compression_available[method]

    def dumps(self, obj: Any) -> bytes:
        """Serialize an object to bytes.

        Args:
            obj: Object to serialize.

        Returns:
            Serialized bytes.
        """
        import json

        # Serialize
        if self.config.use_msgpack and self._check_msgpack():
            import msgpack

            data = msgpack.packb(obj, use_bin_type=True)
        else:
            data = json.dumps(obj).encode("utf-8")

        # Compress
        if self.config.compression and self._check_compression(self.config.compression):
            if self.config.compression == "gzip":
                import gzip

                data = gzip.compress(data)
            elif self.config.compression == "lz4":
                import lz4.frame

                data = lz4.frame.compress(data)

        return bytes(data)

    def loads(self, data: bytes) -> Any:
        """Deserialize bytes to an object.

        Args:
            data: Serialized bytes.

        Returns:
            Deserialized object.
        """
        import json

        # Decompress
        if self.config.compression and self._check_compression(self.config.compression):
            if self.config.compression == "gzip":
                import gzip

                data = gzip.decompress(data)
            elif self.config.compression == "lz4":
                import lz4.frame

                data = lz4.frame.decompress(data)

        # Deserialize
        if self.config.use_msgpack and self._check_msgpack():
            import msgpack

            return msgpack.unpackb(data, raw=False)
        else:
            return json.loads(data.decode("utf-8"))


class CheckpointCache:
    """Thread-safe LRU cache for checkpoint tuples.

    This cache reduces database round-trips for frequently accessed checkpoints.
    It uses an OrderedDict for LRU eviction and supports optional TTL expiration.
    """

    def __init__(self, config: CacheConfig | None = None) -> None:
        """Initialize the cache.

        Args:
            config: Cache configuration. Defaults to disabled cache.
        """
        import threading
        from collections import OrderedDict

        self.config = config or CacheConfig()
        self._cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def _make_key(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str | None
    ) -> str:
        """Create a cache key from checkpoint identifiers."""
        return f"{thread_id}:{checkpoint_ns}:{checkpoint_id or 'latest'}"

    def get(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str | None
    ) -> Any | None:
        """Get a checkpoint from the cache.

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Checkpoint namespace.
            checkpoint_id: Checkpoint ID, or None for latest.

        Returns:
            Cached checkpoint tuple if found and not expired, None otherwise.
        """
        if not self.config.enabled:
            return None

        key = self._make_key(thread_id, checkpoint_ns, checkpoint_id)

        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            value, timestamp = self._cache[key]

            # Check TTL expiration
            if self.config.ttl_seconds:
                if time.time() - timestamp > self.config.ttl_seconds:
                    del self._cache[key]
                    self._misses += 1
                    return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            self._hits += 1
            return value

    def put(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str | None,
        value: Any,
    ) -> None:
        """Store a checkpoint in the cache.

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Checkpoint namespace.
            checkpoint_id: Checkpoint ID.
            value: Checkpoint tuple to cache.
        """
        if not self.config.enabled:
            return

        key = self._make_key(thread_id, checkpoint_ns, checkpoint_id)

        with self._lock:
            # Remove oldest entries if at capacity
            while len(self._cache) >= self.config.max_size:
                self._cache.popitem(last=False)

            self._cache[key] = (value, time.time())

    def invalidate(self, thread_id: str, checkpoint_ns: str | None = None) -> int:
        """Invalidate cached entries for a thread.

        Args:
            thread_id: Thread identifier.
            checkpoint_ns: Optional namespace to limit invalidation.

        Returns:
            Number of entries invalidated.
        """
        if not self.config.enabled:
            return 0

        prefix = f"{thread_id}:"
        if checkpoint_ns is not None:
            prefix = f"{thread_id}:{checkpoint_ns}:"

        with self._lock:
            keys_to_remove = [k for k in self._cache if k.startswith(prefix)]
            for key in keys_to_remove:
                del self._cache[key]
            return len(keys_to_remove)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with hit/miss counts and hit rate.
        """
        with self._lock:
            total = self._hits + self._misses
            return {
                "enabled": self.config.enabled,
                "size": len(self._cache),
                "max_size": self.config.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total > 0 else 0.0,
            }


class Metrics(BaseModel):
    """Simple metrics collector for observability.

    Attributes:
        enabled: Whether metrics collection is enabled.
        operation_counts: Count of operations by type.
        operation_durations: Total duration of operations by type.
        error_counts: Count of errors by type.
        on_operation: Optional callback for operation events.

    Example:
        >>> from langgraph_checkpoint_snowflake import Metrics
        >>> metrics = Metrics(enabled=True)
        >>> saver = SnowflakeSaver(conn, metrics=metrics)
        >>> # After operations
        >>> print(metrics.get_stats())
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    enabled: bool = Field(default=False, description="Enable metrics collection")
    operation_counts: dict[str, int] = Field(default_factory=dict)
    operation_durations: dict[str, float] = Field(default_factory=dict)
    error_counts: dict[str, int] = Field(default_factory=dict)
    on_operation: Callable[[str, float, Exception | None], None] | None = Field(
        default=None, description="Callback for operation events"
    )

    def record(
        self, operation: str, duration: float, error: Exception | None = None
    ) -> None:
        """Record an operation metric."""
        if not self.enabled:
            return

        self.operation_counts[operation] = self.operation_counts.get(operation, 0) + 1
        self.operation_durations[operation] = (
            self.operation_durations.get(operation, 0.0) + duration
        )

        if error:
            error_type = type(error).__name__
            self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

        if self.on_operation:
            self.on_operation(operation, duration, error)

    def get_stats(self) -> dict[str, Any]:
        """Get current metrics statistics."""
        return {
            "operation_counts": dict(self.operation_counts),
            "operation_durations": dict(self.operation_durations),
            "error_counts": dict(self.error_counts),
            "average_durations": {
                op: self.operation_durations.get(op, 0) / count
                for op, count in self.operation_counts.items()
                if count > 0
            },
        }

    def reset(self) -> None:
        """Reset all metrics."""
        self.operation_counts.clear()
        self.operation_durations.clear()
        self.error_counts.clear()


@contextmanager
def timed_operation(
    metrics: Metrics | None, operation: str
) -> Iterator[dict[str, Any]]:
    """Context manager for timing operations and recording metrics.

    Args:
        metrics: Optional metrics collector.
        operation: Name of the operation being timed.

    Yields:
        A context dict that can be used to store operation metadata.
    """
    context: dict[str, Any] = {"start_time": time.perf_counter()}
    error: Exception | None = None
    try:
        yield context
    except Exception as e:
        error = e
        raise
    finally:
        duration = time.perf_counter() - context["start_time"]
        if metrics:
            metrics.record(operation, duration, error)
        logger.debug(
            "Operation %s completed in %.3fs%s",
            operation,
            duration,
            f" with error: {error}" if error else "",
        )


class BenchmarkResult(BaseModel):
    """Results from a benchmark run.

    Attributes:
        operation: Name of the operation benchmarked.
        iterations: Number of iterations run.
        total_time: Total time in seconds.
        avg_time: Average time per operation in seconds.
        min_time: Minimum time for a single operation.
        max_time: Maximum time for a single operation.
        ops_per_second: Operations per second.

    Example:
        >>> result = BenchmarkResult(
        ...     operation="get_tuple",
        ...     iterations=100,
        ...     total_time=5.0,
        ...     avg_time=0.05,
        ...     min_time=0.03,
        ...     max_time=0.1,
        ...     ops_per_second=20.0,
        ... )
        >>> print(result)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: str = Field(description="Name of the operation")
    iterations: int = Field(ge=1, description="Number of iterations")
    total_time: float = Field(ge=0, description="Total time in seconds")
    avg_time: float = Field(ge=0, description="Average time per operation")
    min_time: float = Field(ge=0, description="Minimum time for single operation")
    max_time: float = Field(ge=0, description="Maximum time for single operation")
    ops_per_second: float = Field(ge=0, description="Operations per second")

    def __str__(self) -> str:
        """Format benchmark results as a string."""
        return (
            f"{self.operation}: {self.iterations} iterations\n"
            f"  Total: {self.total_time:.3f}s\n"
            f"  Avg: {self.avg_time * 1000:.2f}ms\n"
            f"  Min: {self.min_time * 1000:.2f}ms\n"
            f"  Max: {self.max_time * 1000:.2f}ms\n"
            f"  Ops/sec: {self.ops_per_second:.1f}"
        )


def benchmark_operation(
    operation: Callable[[], Any],
    iterations: int = 100,
    warmup: int = 5,
    name: str = "operation",
) -> BenchmarkResult:
    """Benchmark a single operation.

    Args:
        operation: Callable to benchmark.
        iterations: Number of iterations to run.
        warmup: Number of warmup iterations (not counted).
        name: Name for the operation in results.

    Returns:
        BenchmarkResult with timing statistics.

    Example:
        >>> from langgraph_checkpoint_snowflake._internal import benchmark_operation
        >>> result = benchmark_operation(
        ...     lambda: checkpointer.get_tuple(config),
        ...     iterations=100,
        ...     name="get_tuple"
        ... )
        >>> print(result)
    """
    # Warmup
    for _ in range(warmup):
        operation()

    # Benchmark
    times: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        operation()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    total_time = sum(times)
    return BenchmarkResult(
        operation=name,
        iterations=iterations,
        total_time=total_time,
        avg_time=total_time / iterations,
        min_time=min(times),
        max_time=max(times),
        ops_per_second=iterations / total_time if total_time > 0 else 0,
    )


class ConnectionPool:
    """Simple connection pool for Snowflake connections.

    This pool manages a fixed number of connections and provides
    thread-safe access to them.
    """

    def __init__(
        self,
        connection_factory: Callable[[], SnowflakeConnection],
        config: PoolConfig | None = None,
    ) -> None:
        """Initialize the connection pool.

        Args:
            connection_factory: Callable that creates new connections.
            config: Pool configuration.
        """
        import threading
        from collections import deque

        self.config = config or PoolConfig()
        self._connection_factory = connection_factory
        self._pool: deque[tuple[SnowflakeConnection, float]] = deque()
        self._lock = threading.Lock()
        self._semaphore = threading.Semaphore(self.config.pool_size)
        self._closed = False

    def get_connection(self) -> SnowflakeConnection:
        """Get a connection from the pool.

        Returns:
            A Snowflake connection.

        Raises:
            TimeoutError: If no connection is available within the timeout.
            RuntimeError: If the pool is closed.
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        if not self._semaphore.acquire(timeout=self.config.pool_timeout):
            raise TimeoutError(
                f"Could not acquire connection within {self.config.pool_timeout}s"
            )

        try:
            with self._lock:
                now = time.time()
                # Try to get an existing connection
                while self._pool:
                    conn, created_at = self._pool.popleft()
                    # Check if connection needs recycling
                    if now - created_at > self.config.pool_recycle:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        continue
                    # Check if connection is still valid
                    try:
                        conn.cursor().execute("SELECT 1")
                        return conn
                    except Exception:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        continue

            # Create new connection
            return self._connection_factory()
        except Exception:
            self._semaphore.release()
            raise

    def return_connection(self, conn: SnowflakeConnection) -> None:
        """Return a connection to the pool.

        Args:
            conn: The connection to return.
        """
        if self._closed:
            try:
                conn.close()
            except Exception:
                pass
            return

        with self._lock:
            self._pool.append((conn, time.time()))
        self._semaphore.release()

    @contextmanager
    def connection(self) -> Iterator[SnowflakeConnection]:
        """Context manager for getting and returning a connection.

        Yields:
            A Snowflake connection.
        """
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    def close(self) -> None:
        """Close all connections in the pool."""
        self._closed = True
        with self._lock:
            while self._pool:
                conn, _ = self._pool.popleft()
                try:
                    conn.close()
                except Exception:
                    pass


__all__ = [
    "BenchmarkResult",
    "CacheConfig",
    "CheckpointCache",
    "Conn",
    "ConnectionPool",
    "FastSerializer",
    "Metrics",
    "PoolConfig",
    "RetryConfig",
    "SerializationConfig",
    "_load_private_key",
    "benchmark_operation",
    "configure_logging",
    "create_connection",
    "execute_with_retry",
    "executemany_with_retry",
    "get_connection_params_from_env",
    "get_cursor",
    "logger",
    "set_log_level",
    "timed_operation",
]
