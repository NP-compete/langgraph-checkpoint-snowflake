"""Basic usage example for langgraph-checkpoint-snowflake.

This example demonstrates:
1. Setting up the Snowflake checkpointer
2. Building a simple conversational agent
3. Persisting state across invocations
4. Resuming conversations from checkpoints
5. Listing checkpoint history
6. Using RetryConfig for resilient database operations
7. Using Metrics for observability
8. Using TTL to clean up old checkpoints

Prerequisites:
    pip install langgraph-checkpoint-snowflake

    For the LLM, install one of:
    - pip install langchain-openai           (OpenAI)
    - pip install langchain-anthropic        (Anthropic)
    - pip install langchain-google-genai     (Google AI Studio)
    - pip install langchain-google-vertexai  (Google Vertex AI)
    - pip install langchain-ollama           (Ollama/local)
    - pip install langchain-openai           (vLLM via OpenAI-compatible API)

Environment variables required:
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA

    Plus your LLM provider's API key (e.g., OPENAI_API_KEY, ANTHROPIC_API_KEY)
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Annotated, Any, TypedDict

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from langgraph_checkpoint_snowflake import Metrics, RetryConfig, SnowflakeSaver


def get_llm() -> BaseChatModel:
    """Get an LLM instance based on available providers.

    Configure this function to use your preferred LLM provider.
    Uncomment the provider you want to use.
    """
    # Option 1: OpenAI
    # from langchain_openai import ChatOpenAI
    # return ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Option 2: Anthropic
    # from langchain_anthropic import ChatAnthropic
    # return ChatAnthropic(model="claude-3-haiku-20240307", temperature=0)

    # Option 3: Google AI Studio
    # from langchain_google_genai import ChatGoogleGenerativeAI
    # return ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0)

    # Option 4: Google Vertex AI
    # from langchain_google_vertexai import ChatVertexAI
    # return ChatVertexAI(model="gemini-1.5-flash", temperature=0)

    # Option 5: Ollama (local)
    # from langchain_ollama import ChatOllama
    # return ChatOllama(model="llama3.2", temperature=0)

    # Option 6: vLLM (OpenAI-compatible API)
    # from langchain_openai import ChatOpenAI
    # return ChatOpenAI(
    #     model="meta-llama/Llama-3.1-8B-Instruct",
    #     base_url="http://localhost:8000/v1",  # vLLM server URL
    #     api_key="EMPTY",  # vLLM doesn't require a real key
    #     temperature=0,
    # )

    # Option 7: Azure OpenAI
    # from langchain_openai import AzureChatOpenAI
    # return AzureChatOpenAI(
    #     azure_deployment="your-deployment",
    #     api_version="2024-02-15-preview",
    # )

    # Default: Try to auto-detect based on environment variables
    if os.environ.get("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model="gpt-4o-mini", temperature=0)
    elif os.environ.get("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model="claude-3-haiku-20240307", temperature=0)
    elif os.environ.get("GOOGLE_API_KEY"):
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0)
    elif os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("VERTEXAI_PROJECT"):
        from langchain_google_vertexai import ChatVertexAI

        return ChatVertexAI(model="gemini-1.5-flash", temperature=0)
    elif os.environ.get("VLLM_BASE_URL"):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=os.environ.get("VLLM_MODEL", "meta-llama/Llama-3.1-8B-Instruct"),
            base_url=os.environ["VLLM_BASE_URL"],
            api_key=os.environ.get("VLLM_API_KEY", "EMPTY"),
            temperature=0,
        )
    else:
        raise ValueError(
            "No LLM provider configured. Set one of: "
            "OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, "
            "GOOGLE_CLOUD_PROJECT/VERTEXAI_PROJECT, VLLM_BASE_URL, "
            "or modify get_llm() to use your preferred provider."
        )


class State(TypedDict):
    """State for the conversational agent."""

    messages: Annotated[list[BaseMessage], add_messages]


def create_agent(llm: BaseChatModel) -> StateGraph:
    """Create a simple conversational agent."""

    def chatbot(state: State) -> dict[str, Any]:
        """Process messages and generate a response."""
        response = llm.invoke(state["messages"])
        return {"messages": [response]}

    # Build the graph
    builder = StateGraph(State)
    builder.add_node("chatbot", chatbot)
    builder.add_edge(START, "chatbot")
    builder.add_edge("chatbot", END)

    return builder


def main() -> None:
    """Run the example."""
    # Get the LLM
    llm = get_llm()
    print(f"Using LLM: {llm.__class__.__name__}")

    # Configure retry behavior for resilient database operations
    retry_config = RetryConfig(
        max_retries=5,  # Retry up to 5 times
        base_delay=1.0,  # Start with 1 second delay
        max_delay=60.0,  # Cap delay at 60 seconds
        jitter=True,  # Add randomness to prevent thundering herd
    )

    # Configure metrics for observability
    def on_operation(op: str, duration: float, error: Exception | None) -> None:
        """Callback for each database operation."""
        status = "OK" if error is None else f"ERROR: {error}"
        print(f"  [Metrics] {op}: {duration:.3f}s - {status}")

    metrics = Metrics(enabled=True, on_operation=on_operation)

    # Create the checkpointer with retry and metrics
    with SnowflakeSaver.from_env(
        retry_config=retry_config,
        metrics=metrics,
    ) as checkpointer:
        # Run migrations (creates tables if they don't exist)
        print("Setting up Snowflake tables...")
        checkpointer.setup()
        print("Setup complete!")

        # Create and compile the agent with the checkpointer
        builder = create_agent(llm)
        graph = builder.compile(checkpointer=checkpointer)

        # Define a thread ID for this conversation
        thread_id = "example-conversation-1"
        config: dict[str, Any] = {"configurable": {"thread_id": thread_id}}

        # First interaction
        print("\n--- First Interaction ---")
        result = graph.invoke(
            {"messages": [("user", "Hi! My name is Alice.")]},
            config,
        )
        print(f"Assistant: {result['messages'][-1].content}")

        # Second interaction (continues the conversation)
        print("\n--- Second Interaction ---")
        result = graph.invoke(
            {"messages": [("user", "What's my name?")]},
            config,
        )
        print(f"Assistant: {result['messages'][-1].content}")

        # List all checkpoints for this thread
        print("\n--- Checkpoint History ---")
        checkpoints = list(checkpointer.list(config))
        print(f"Total checkpoints: {len(checkpoints)}")
        for i, cp in enumerate(checkpoints[:3]):  # Show first 3
            print(f"  {i + 1}. ID: {cp.config['configurable']['checkpoint_id'][:8]}...")

        # Demonstrate resuming from a specific checkpoint
        if len(checkpoints) > 1:
            print("\n--- Resume from Earlier Checkpoint ---")
            earlier_checkpoint = checkpoints[1]  # Second most recent
            resume_config = earlier_checkpoint.config

            # Continue from that point
            result = graph.invoke(
                {"messages": [("user", "Actually, call me Bob.")]},
                resume_config,
            )
            print(f"Assistant: {result['messages'][-1].content}")

        # Show checkpoint statistics
        print("\n--- Checkpoint Statistics ---")
        total_count = checkpointer.get_checkpoint_count()
        print(f"Total checkpoints in database: {total_count}")

        counts_by_thread = checkpointer.get_checkpoint_counts_by_thread(max_results=5)
        if counts_by_thread:
            print("Checkpoints by thread:")
            for tid, count in counts_by_thread:
                print(f"  {tid}: {count}")

        # Show collected metrics
        print("\n--- Metrics Summary ---")
        stats = checkpointer.get_metrics()
        if stats:
            print(f"Operations: {stats['operation_counts']}")
            print(f"Avg durations: {stats['average_durations']}")

        # Demonstrate TTL cleanup (delete checkpoints older than 30 days)
        print("\n--- TTL Cleanup Demo ---")
        deleted = checkpointer.delete_before(timedelta(days=30))
        print(f"Deleted {deleted} checkpoints older than 30 days")

        # Clean up (optional - delete the thread)
        print("\n--- Cleanup ---")
        checkpointer.delete_thread(thread_id)
        print(f"Deleted thread: {thread_id}")


def example_with_explicit_connection() -> None:
    """Example using explicit connection parameters with retry and metrics."""
    llm = get_llm()

    # Create retry config for production use
    retry_config = RetryConfig(max_retries=3, base_delay=0.5)

    # Create metrics (disabled callback for cleaner output)
    metrics = Metrics(enabled=True)

    with SnowflakeSaver.from_conn_string(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        retry_config=retry_config,
        metrics=metrics,
    ) as checkpointer:
        checkpointer.setup()

        builder = create_agent(llm)
        graph = builder.compile(checkpointer=checkpointer)

        config: dict[str, Any] = {"configurable": {"thread_id": "explicit-example"}}
        result = graph.invoke(
            {"messages": [("user", "Hello!")]},
            config,
        )
        print(f"Response: {result['messages'][-1].content}")

        # Print metrics summary
        stats = checkpointer.get_metrics()
        if stats:
            print(f"Operations performed: {stats['operation_counts']}")


if __name__ == "__main__":
    main()
