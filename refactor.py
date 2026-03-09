import re
import os

def update_docstrings(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    # Simple dictionary for known types based on the prompt's desired state
    replacements = [
        (
            "Args:\n            config: Configuration specifying",
            "Args:\n            config (RunnableConfig): Configuration specifying"
        ),
        (
            "Returns:\n            The checkpoint tuple if found",
            "Returns:\n            CheckpointTuple | None: The checkpoint tuple if found"
        ),
        (
            "Returns:\n            Updated configuration with the new",
            "Returns:\n            RunnableConfig: Updated configuration with the new"
        ),
        (
            "Yields:\n            Matching checkpoint tuples",
            "Yields:\n            Iterator[CheckpointTuple]: Matching checkpoint tuples"
        ),
        (
            "Yields:\n            A configured SnowflakeSaver",
            "Yields:\n            Iterator[SnowflakeSaver]: A configured SnowflakeSaver"
        ),
        (
            "Args:\n            conn: A Snowflake connection",
            "Args:\n            conn (Conn): A Snowflake connection"
        ),
        (
            "            serde: Optional serializer",
            "            serde (SerializerProtocol | None): Optional serializer"
        ),
        (
            "            retry_config: Optional retry",
            "            retry_config (RetryConfig | None): Optional retry"
        ),
        (
            "            metrics: Optional metrics",
            "            metrics (Metrics | None): Optional metrics"
        ),
        (
            "            cache_config: Optional cache",
            "            cache_config (CacheConfig | None): Optional cache"
        ),
        (
            "            redis_cache_config: Optional Redis",
            "            redis_cache_config (RedisWriteCacheConfig | None): Optional Redis"
        ),
        (
            "            thread_id: The thread ID",
            "            thread_id (str): The thread ID"
        ),
        (
            "            thread_ids: List of thread IDs",
            "            thread_ids (list[str]): List of thread IDs"
        ),
        (
            "            max_age: Maximum age",
            "            max_age (timedelta): Maximum age"
        ),
        (
            "            max_results: Maximum number",
            "            max_results (int): Maximum number"
        ),
        (
            "Returns:\n            Total number of checkpoints",
            "Returns:\n            int: Total number of checkpoints"
        ),
        (
            "Returns:\n            Number of checkpoints deleted",
            "Returns:\n            int: Number of checkpoints deleted"
        ),
        (
            "Returns:\n            Number of threads deleted",
            "Returns:\n            int: Number of threads deleted"
        ),
        (
            "Returns:\n            Dictionary of metrics if enabled",
            "Returns:\n            dict[str, Any] | None: Dictionary of metrics if enabled"
        ),
        (
            "Returns:\n            Dictionary with cache hit/miss",
            "Returns:\n            dict[str, Any]: Dictionary with cache hit/miss"
        ),
        (
            "Returns:\n            List of (thread_id, count)",
            "Returns:\n            list[tuple[str, int]]: List of (thread_id, count)"
        )
    ]

    new_content = content
    for old, new in replacements:
        new_content = new_content.replace(old, new)
        
    with open(filepath, 'w') as f:
        f.write(new_content)

for root, _, files in os.walk('/root/.openclaw/workspace/agents/workspace-flocky/langgraph-checkpoint-snowflake/src/langgraph_checkpoint_snowflake'):
    for f in files:
        if f.endswith('.py'):
            update_docstrings(os.path.join(root, f))
