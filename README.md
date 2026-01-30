# LangGraph Checkpoint Snowflake

[![PyPI version](https://badge.fury.io/py/langgraph-checkpoint-snowflake.svg)](https://badge.fury.io/py/langgraph-checkpoint-snowflake)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/sodutta/langgraph-checkpoint-snowflake/actions/workflows/ci.yml/badge.svg)](https://github.com/sodutta/langgraph-checkpoint-snowflake/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/sodutta/langgraph-checkpoint-snowflake/branch/main/graph/badge.svg)](https://codecov.io/gh/sodutta/langgraph-checkpoint-snowflake)

A Snowflake checkpoint saver for [LangGraph](https://github.com/langchain-ai/langgraph), enabling durable, stateful AI agents with Snowflake persistence. This library provides both synchronous and asynchronous implementations for persisting agent state, supporting features like human-in-the-loop workflows, time travel debugging, and crash recovery.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

You need the following to use this library:

- Python 3.10 or higher
- A Snowflake account with appropriate permissions
- A warehouse, database, and schema configured in Snowflake

For authentication, you need one of:

```bash
# Password authentication
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"

# OR Key pair authentication (recommended for production)
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/rsa_key.p8"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your_passphrase"  # if key is encrypted
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
```

To generate a key pair for authentication:

```bash
# Generate encrypted private key (recommended)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -v2 aes256

# Extract public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Register public key with Snowflake user
# ALTER USER your_user SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

### Installing

Install the package from PyPI:

```bash
pip install langgraph-checkpoint-snowflake
```

For development, clone the repository and install with dev dependencies:

```bash
git clone https://github.com/sodutta/langgraph-checkpoint-snowflake.git
cd langgraph-checkpoint-snowflake
pip install -e ".[dev]"
```

Verify the installation by running a simple example:

```python
from langgraph.graph import StateGraph
from langgraph_checkpoint_snowflake import SnowflakeSaver

# Create checkpointer from environment variables
with SnowflakeSaver.from_env() as checkpointer:
    # Run migrations (creates tables if they don't exist)
    checkpointer.setup()

    # Build a simple graph
    builder = StateGraph(dict)
    builder.add_node("node", lambda state: {"count": state.get("count", 0) + 1})
    builder.set_entry_point("node")
    builder.set_finish_point("node")

    # Compile with checkpointer
    graph = builder.compile(checkpointer=checkpointer)

    # Run with thread_id for persistence
    config = {"configurable": {"thread_id": "test-thread"}}
    result = graph.invoke({"count": 0}, config)
    print(f"Result: {result}")
```

For async usage:

```python
from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver

async with AsyncSnowflakeSaver.from_env() as checkpointer:
    await checkpointer.setup()
    graph = builder.compile(checkpointer=checkpointer)
    result = await graph.ainvoke({"count": 0}, config)
```

> **Note on Async Implementation**: `AsyncSnowflakeSaver` wraps synchronous Snowflake connector
> operations with `asyncio.to_thread()`. This provides an async-compatible API but does **not**
> deliver true async I/O performance benefits. Operations still block thread pool threads.
> For high-concurrency workloads requiring true async I/O, consider using
> [langgraph-checkpoint-postgres](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-postgres)
> with `asyncpg` instead.

Using the prebuilt ReAct agent with tools:

```python
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from langgraph_checkpoint_snowflake import SnowflakeSaver


# Define a tool
@tool
def get_weather(city: str) -> str:
    """Get the current weather for a city."""
    return f"The weather in {city} is sunny, 72F."


# Create the agent with Snowflake persistence
with SnowflakeSaver.from_env() as checkpointer:
    checkpointer.setup()

    llm = ChatOpenAI(model="gpt-4o-mini")
    agent = create_react_agent(llm, tools=[get_weather], checkpointer=checkpointer)

    # Run the agent
    config = {"configurable": {"thread_id": "weather-thread"}}
    response = agent.invoke(
        {"messages": [{"role": "user", "content": "What's the weather in Tokyo?"}]},
        config,
    )
    print(response["messages"][-1].content)

    # Continue the conversation (state is persisted)
    response = agent.invoke(
        {"messages": [{"role": "user", "content": "How about New York?"}]},
        config,
    )
    print(response["messages"][-1].content)
```

See the [examples](examples/) directory for more complete examples including human-in-the-loop workflows and multi-turn conversations.

## Running the tests

The test suite includes unit tests, property-based tests, and integration tests.

### Unit and property tests

Run the full test suite (excluding integration tests that require a live Snowflake connection):

```bash
make test
```

Or run specific test files:

```bash
# Unit tests
pytest tests/test_sync.py -v

# Property-based tests using Hypothesis
pytest tests/test_property.py -v

# Edge case tests
pytest tests/test_edge_cases.py -v

# Feature tests (retry, metrics, TTL, connection pooling)
pytest tests/test_features.py -v
```

### Integration tests

Integration tests require a live Snowflake connection. Set up your environment variables and run:

```bash
pytest tests/test_integration.py -v
```

### Coding style tests

The project uses ruff for linting and formatting, and mypy for type checking:

```bash
# Run all linters
make lint

# Run formatters
make format

# Run individual tools
ruff check src tests
ruff format src tests
mypy src/langgraph_checkpoint_snowflake
```

## Performance and Benchmarking

### Performance Characteristics

Snowflake is optimized for analytical workloads (OLAP), not transactional (OLTP). Expected performance:

| Operation          | Latency (uncached) | Latency (cached) | Notes                                        |
| ------------------ | ------------------ | ---------------- | -------------------------------------------- |
| `get_tuple`      | 100-300ms          | <1ms             | Cache dramatically improves read performance |
| `put`            | 150-400ms          | N/A              | Writes always go to Snowflake                |
| `list`           | 100-500ms          | N/A              | Depends on result set size                   |
| `delete_thread`  | 100-300ms          | N/A              | Single thread deletion                       |
| `delete_threads` | 150-500ms          | N/A              | Batch deletion (more efficient)              |

Additional considerations:

| Aspect     | Expected Behavior                            |
| ---------- | -------------------------------------------- |
| Cold Start | 5-30s if warehouse is suspended              |
| Cost       | Frequent small queries keep warehouse active |
| Throughput | ~5-20 ops/sec (varies by warehouse size)     |

For low-latency requirements (<10ms), consider using [langgraph-checkpoint-postgres](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-postgres) instead.

### Running Benchmarks

The library includes a built-in benchmark utility to measure performance in your environment:

```python
from langgraph_checkpoint_snowflake import SnowflakeSaver, CacheConfig

# Without caching
with SnowflakeSaver.from_env() as checkpointer:
    checkpointer.setup()
    results = checkpointer.benchmark(iterations=50)

    for name, result in results.items():
        print(result)

# Example output:
# put: 50 iterations
#   Total: 12.345s
#   Avg: 246.90ms
#   Min: 180.23ms
#   Max: 412.56ms
#   Ops/sec: 4.1
#
# get_tuple_uncached: 50 iterations
#   Total: 8.234s
#   Avg: 164.68ms
#   Min: 120.45ms
#   Max: 298.12ms
#   Ops/sec: 6.1
```

### Enabling Caching

For read-heavy workloads, enable the in-memory LRU cache to dramatically improve `get_tuple` performance:

```python
from langgraph_checkpoint_snowflake import SnowflakeSaver, CacheConfig

# Configure cache: 100 entries, 5 minute TTL
cache_config = CacheConfig(
    enabled=True,
    max_size=100,
    ttl_seconds=300,
)

with SnowflakeSaver.from_env() as checkpointer:
    saver = SnowflakeSaver(checkpointer.conn, cache_config=cache_config)
    saver.setup()

    # First call hits database (~150ms)
    result1 = saver.get_tuple(config)

    # Second call hits cache (<1ms)
    result2 = saver.get_tuple(config)

    # Check cache statistics
    stats = saver.get_cache_stats()
    print(f"Cache hit rate: {stats['hit_rate']:.1%}")
    # Cache hit rate: 50.0%
```

Cache invalidation happens automatically on:

- `put()` - New checkpoint invalidates "latest" cache entry
- `delete_thread()` - All entries for thread are invalidated
- `delete_threads()` - All entries for specified threads are invalidated

### Benchmark with Caching

```python
# With caching enabled
cache_config = CacheConfig(enabled=True, max_size=100, ttl_seconds=300)

with SnowflakeSaver.from_env() as checkpointer:
    saver = SnowflakeSaver(checkpointer.conn, cache_config=cache_config)
    saver.setup()

    results = saver.benchmark(iterations=50)

    for name, result in results.items():
        print(result)

# Example output with caching:
# get_tuple_uncached: 50 iterations
#   Avg: 165.23ms
#   Ops/sec: 6.1
#
# get_tuple_cached: 50 iterations
#   Avg: 0.02ms
#   Ops/sec: 50000.0
```

## Deployment

### Database Schema

The checkpointer creates three tables automatically when you call `setup()`:

- `checkpoints` - Stores checkpoint data and metadata
- `checkpoint_blobs` - Stores serialized channel values
- `checkpoint_writes` - Stores pending writes for crash recovery

### Production Configuration

For production deployments, use key pair authentication and configure retry and metrics:

```python
from langgraph_checkpoint_snowflake import (
    SnowflakeSaver,
    RetryConfig,
    Metrics,
)

# Configure retry behavior
retry_config = RetryConfig(
    max_retries=5,
    base_delay=1.0,
    max_delay=30.0,
    jitter=True,
)

# Enable metrics collection
metrics = Metrics()

with SnowflakeSaver.from_key_pair(
    account="your_account",
    user="your_user",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    private_key_path="/path/to/rsa_key.p8",
    retry_config=retry_config,
    metrics=metrics,
) as checkpointer:
    checkpointer.setup()
    # ... use checkpointer ...

# Review metrics after use
print(f"Operations: {metrics.operation_count}")
print(f"Avg latency: {metrics.average_duration_ms:.2f}ms")
```

### Connection Parameters

| Parameter                  | Description                   | Required                    |
| -------------------------- | ----------------------------- | --------------------------- |
| `account`                | Snowflake account identifier  | Yes                         |
| `user`                   | Username                      | Yes                         |
| `warehouse`              | Warehouse name                | Yes                         |
| `database`               | Database name                 | Yes                         |
| `schema`                 | Schema name                   | Yes                         |
| `password`               | Password (for password auth)  | One of password/private_key |
| `private_key_path`       | Path to PEM private key file  | One of password/private_key |
| `private_key`            | PEM private key content       | One of password/private_key |
| `private_key_passphrase` | Passphrase for encrypted keys | No                          |
| `role`                   | Role to use                   | No                          |

## Built With

* [LangGraph](https://github.com/langchain-ai/langgraph) - Framework for building stateful, multi-actor agents
* [snowflake-connector-python](https://github.com/snowflakedb/snowflake-connector-python) - Snowflake Python connector
* [langgraph-checkpoint](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint) - Base checkpoint interfaces

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests.

Before submitting a pull request:

1. Run the linters: `make lint`
2. Run the formatters: `make format`
3. Run the tests: `make test`
4. Ensure all checks pass

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/sodutta/langgraph-checkpoint-snowflake/tags).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

* The LangGraph team for the excellent checkpoint interface design
* The Snowflake team for the Python connector
* Inspired by the official LangGraph checkpoint implementations (PostgreSQL, SQLite)
