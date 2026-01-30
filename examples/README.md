# Examples

This directory contains examples demonstrating how to use `langgraph-checkpoint-snowflake`.

## Prerequisites

1. Install the package:
   ```bash
   pip install langgraph-checkpoint-snowflake
   ```

2. Install your preferred LLM provider:
   ```bash
   # Choose one:
   pip install langchain-openai           # OpenAI
   pip install langchain-anthropic        # Anthropic Claude
   pip install langchain-google-genai     # Google AI Studio
   pip install langchain-google-vertexai  # Google Vertex AI
   pip install langchain-ollama           # Ollama (local models)
   pip install langchain-openai           # vLLM (uses OpenAI-compatible API)
   ```

3. Set up environment variables:
   ```bash
   # Snowflake connection
   export SNOWFLAKE_ACCOUNT="your_account"
   export SNOWFLAKE_USER="your_user"
   export SNOWFLAKE_PASSWORD="your_password"
   export SNOWFLAKE_WAREHOUSE="your_warehouse"
   export SNOWFLAKE_DATABASE="your_database"
   export SNOWFLAKE_SCHEMA="your_schema"

   # LLM provider (set one based on your choice)
   export OPENAI_API_KEY="your_key"       # For OpenAI
   export ANTHROPIC_API_KEY="your_key"    # For Anthropic
   export GOOGLE_API_KEY="your_key"       # For Google AI Studio
   export GOOGLE_CLOUD_PROJECT="your_project"  # For Vertex AI
   # Ollama doesn't require an API key

   # For vLLM (OpenAI-compatible API)
   export VLLM_BASE_URL="http://localhost:8000/v1"
   export VLLM_MODEL="meta-llama/Llama-3.1-8B-Instruct"  # optional
   ```

## Examples

### 1. Basic Usage (`basic_usage.py`)

Demonstrates fundamental checkpointer operations:
- Setting up the Snowflake checkpointer
- Building a conversational agent
- Persisting state across invocations
- Listing checkpoint history
- Resuming from specific checkpoints

```bash
python examples/basic_usage.py
```

### 2. Async Usage (`async_usage.py`)

Shows async patterns:
- Using `AsyncSnowflakeSaver`
- Streaming responses
- Concurrent conversations

```bash
python examples/async_usage.py
```

### 3. Human-in-the-Loop (`human_in_the_loop.py`)

Demonstrates approval workflows:
- Interrupting execution for human review
- Modifying state before resuming
- Building approval-based agents

```bash
python examples/human_in_the_loop.py
```

### 4. ReAct Agent (`react_agent.py`)

Shows how to use `create_react_agent` with custom tools:
- Using LangGraph's prebuilt ReAct agent
- Defining custom tools (weather, calculator, search)
- Tool execution with state persistence
- Context recall across interactions

```bash
python examples/react_agent.py
```

### 5. State Management (`state_management.py`)

Demonstrates advanced state inspection and manipulation:
- `get_state()` - Inspect current state snapshot
- `get_state_history()` - View all checkpoints (time travel)
- `update_state()` - Modify state manually
- Replay from a specific checkpoint

```bash
python examples/state_management.py
```

## Key Concepts

### Thread IDs

Each conversation is identified by a `thread_id`. All checkpoints for a conversation share the same thread ID:

```python
config = {"configurable": {"thread_id": "my-conversation-123"}}
```

### Checkpoint Namespaces

For complex graphs with subgraphs, use `checkpoint_ns` to namespace checkpoints:

```python
config = {
    "configurable": {
        "thread_id": "my-thread",
        "checkpoint_ns": "subgraph-1",
    }
}
```

### Connection Management

The checkpointer supports three connection patterns:

```python
# 1. From environment variables
with SnowflakeSaver.from_env() as checkpointer:
    ...

# 2. From explicit parameters
with SnowflakeSaver.from_conn_string(
    account="...",
    user="...",
    password="...",
    warehouse="...",
    database="...",
    schema="...",
) as checkpointer:
    ...

# 3. From existing connection
import snowflake.connector
conn = snowflake.connector.connect(...)
checkpointer = SnowflakeSaver(conn)
```

## Performance Notes

Snowflake is optimized for analytical workloads, not transactional. Expect:
- 100-500ms latency per checkpoint operation
- 5-30s cold start if warehouse is suspended
- Higher costs with frequent small queries

For low-latency requirements, consider PostgreSQL or SQLite checkpointers instead.
