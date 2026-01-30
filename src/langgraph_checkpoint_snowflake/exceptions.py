"""Custom exceptions for langgraph-checkpoint-snowflake.

This module provides specific exception classes for different types of
Snowflake errors, making it easier to handle and debug issues.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class SnowflakeCheckpointError(Exception):
    """Base exception for all Snowflake checkpoint errors.

    This is the base class for all exceptions raised by the Snowflake
    checkpoint saver. Catch this to handle any checkpoint-related error.

    Attributes:
        message: Human-readable error message.
        original_error: The original exception that caused this error.
    """

    def __init__(
        self,
        message: str,
        original_error: Exception | None = None,
    ) -> None:
        self.message = message
        self.original_error = original_error
        super().__init__(message)

    def __str__(self) -> str:
        if self.original_error:
            return f"{self.message}: {self.original_error}"
        return self.message


class SnowflakeConnectionError(SnowflakeCheckpointError):
    """Error connecting to Snowflake.

    Raised when the connection to Snowflake fails. Common causes:
    - Invalid credentials (password or key pair)
    - Network issues
    - Invalid account identifier
    - Warehouse not available

    Example:
        >>> try:
        ...     with SnowflakeSaver.from_conn_string(...) as saver:
        ...         pass
        ... except SnowflakeConnectionError as e:
        ...     print(f"Connection failed: {e}")
    """

    pass


class SnowflakeAuthenticationError(SnowflakeConnectionError):
    """Authentication failed.

    Raised when authentication to Snowflake fails. Common causes:
    - Invalid username or password
    - Invalid or expired private key
    - User account locked or disabled
    - Missing required authentication parameters

    Example:
        >>> try:
        ...     with SnowflakeSaver.from_key_pair(...) as saver:
        ...         pass
        ... except SnowflakeAuthenticationError as e:
        ...     print(f"Auth failed: {e}")
    """

    pass


class SnowflakeQueryError(SnowflakeCheckpointError):
    """Error executing a query.

    Raised when a SQL query fails to execute. Common causes:
    - Syntax errors in SQL
    - Missing tables or columns
    - Permission denied
    - Data type mismatches

    Attributes:
        query: The SQL query that failed (may be truncated for security).
    """

    def __init__(
        self,
        message: str,
        original_error: Exception | None = None,
        query: str | None = None,
    ) -> None:
        super().__init__(message, original_error)
        # Truncate query for security (avoid logging sensitive data)
        self.query = query[:200] + "..." if query and len(query) > 200 else query

    def __str__(self) -> str:
        base = super().__str__()
        if self.query:
            return f"{base} [Query: {self.query}]"
        return base


class SnowflakeTransientError(SnowflakeCheckpointError):
    """Transient error that may succeed on retry.

    Raised for errors that are likely temporary and may succeed if retried.
    Common causes:
    - Warehouse starting up (cold start)
    - Network timeout
    - Service temporarily unavailable
    - Resource contention

    The retry logic in the checkpointer automatically handles these errors,
    but you may catch them for custom retry handling.

    Attributes:
        retry_count: Number of retries attempted before giving up.
    """

    def __init__(
        self,
        message: str,
        original_error: Exception | None = None,
        retry_count: int = 0,
    ) -> None:
        super().__init__(message, original_error)
        self.retry_count = retry_count

    def __str__(self) -> str:
        base = super().__str__()
        if self.retry_count > 0:
            return f"{base} (after {self.retry_count} retries)"
        return base


class SnowflakeWarehouseError(SnowflakeTransientError):
    """Warehouse-related error.

    Raised when there's an issue with the Snowflake warehouse. Common causes:
    - Warehouse suspended and taking time to resume
    - Warehouse does not exist
    - Insufficient credits
    - Warehouse size too small for query

    Example:
        >>> try:
        ...     saver.setup()
        ... except SnowflakeWarehouseError as e:
        ...     print(f"Warehouse issue: {e}")
        ...     print("Consider using a larger warehouse or auto-resume")
    """

    pass


class SnowflakeSchemaError(SnowflakeQueryError):
    """Schema-related error.

    Raised when there's an issue with database schema. Common causes:
    - Table does not exist (setup() not called)
    - Column missing (migration needed)
    - Schema does not exist
    - Permission denied on schema

    Example:
        >>> try:
        ...     saver.get_tuple(config)
        ... except SnowflakeSchemaError as e:
        ...     print(f"Schema issue: {e}")
        ...     print("Did you call setup() first?")
    """

    pass


class SnowflakeSerializationError(SnowflakeCheckpointError):
    """Error serializing or deserializing checkpoint data.

    Raised when checkpoint data cannot be serialized to JSON or
    deserialized from the database. Common causes:
    - Non-serializable Python objects in state
    - Corrupted data in database
    - Incompatible data types

    Example:
        >>> try:
        ...     saver.put(config, checkpoint, metadata, versions)
        ... except SnowflakeSerializationError as e:
        ...     print(f"Serialization failed: {e}")
        ...     print("Check that all state values are JSON-serializable")
    """

    pass


class SnowflakeConfigurationError(SnowflakeCheckpointError):
    """Configuration error.

    Raised when the checkpointer is misconfigured. Common causes:
    - Missing required environment variables
    - Invalid connection parameters
    - Invalid retry or pool configuration

    Example:
        >>> try:
        ...     saver = SnowflakeSaver.from_env()
        ... except SnowflakeConfigurationError as e:
        ...     print(f"Config error: {e}")
        ...     print("Check your environment variables")
    """

    pass


def wrap_snowflake_error(
    error: Exception,
    message: str | None = None,
    query: str | None = None,
    retry_count: int = 0,
) -> SnowflakeCheckpointError:
    """Wrap a Snowflake connector error in a custom exception.

    This function examines the original error and wraps it in the most
    appropriate custom exception class.

    Args:
        error: The original Snowflake connector error.
        message: Optional custom message (defaults to error message).
        query: Optional query that caused the error.
        retry_count: Number of retries attempted.

    Returns:
        A SnowflakeCheckpointError subclass appropriate for the error type.
    """
    error_msg = message or str(error)
    error_str = str(error).lower()

    # Try to import Snowflake error types
    try:
        import snowflake.connector.errors as sf_errors

        # Authentication errors
        if isinstance(error, sf_errors.ForbiddenError) or "authentication" in error_str:
            return SnowflakeAuthenticationError(error_msg, error)

        # Connection errors
        if isinstance(
            error, sf_errors.InterfaceError | sf_errors.OperationalError
        ) and any(
            x in error_str for x in ["connection", "network", "timeout", "refused"]
        ):
            return SnowflakeConnectionError(error_msg, error)

        # Warehouse errors
        if "warehouse" in error_str or "suspended" in error_str:
            return SnowflakeWarehouseError(error_msg, error, retry_count)

        # Transient errors (service unavailable, etc.)
        if isinstance(error, sf_errors.ServiceUnavailableError) or any(
            x in error_str for x in ["unavailable", "retry", "temporary"]
        ):
            return SnowflakeTransientError(error_msg, error, retry_count)

        # Schema errors
        if isinstance(error, sf_errors.ProgrammingError) and any(
            x in error_str
            for x in ["does not exist", "not found", "unknown", "invalid identifier"]
        ):
            return SnowflakeSchemaError(error_msg, error, query)

        # Query errors
        if isinstance(error, sf_errors.ProgrammingError):
            return SnowflakeQueryError(error_msg, error, query)

        # Database errors (catch-all for DB issues)
        if isinstance(error, sf_errors.DatabaseError):
            return SnowflakeQueryError(error_msg, error, query)

    except ImportError:
        pass

    # Default: wrap in base exception
    return SnowflakeCheckpointError(error_msg, error)


__all__ = [
    "SnowflakeAuthenticationError",
    "SnowflakeCheckpointError",
    "SnowflakeConfigurationError",
    "SnowflakeConnectionError",
    "SnowflakeQueryError",
    "SnowflakeSchemaError",
    "SnowflakeSerializationError",
    "SnowflakeTransientError",
    "SnowflakeWarehouseError",
    "wrap_snowflake_error",
]
