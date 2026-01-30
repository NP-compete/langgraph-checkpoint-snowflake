"""Async usage example for langgraph-checkpoint-snowflake.

This example demonstrates:
1. Using AsyncSnowflakeSaver for async workflows
2. Building an async agent with streaming
3. Concurrent checkpoint operations
4. Using RetryConfig for resilient async operations
5. Using Metrics for async observability
6. Using async TTL cleanup (adelete_before)

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

import asyncio
import os
from datetime import timedelta
from typing import Annotated, Any, TypedDict

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from langgraph_checkpoint_snowflake import AsyncSnowflakeSaver, Metrics, RetryConfig


def get_llm() -> BaseChatModel:
    """Get an LLM instance based on available providers.

    Configure this function to use your preferred LLM provider.
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
    """Create a simple conversational agent with async support."""

    async def chatbot(state: State) -> dict[str, Any]:
        """Process messages and generate a response asynchronously."""
        response = await llm.ainvoke(state["messages"])
        return {"messages": [response]}

    builder = StateGraph(State)
    builder.add_node("chatbot", chatbot)
    builder.add_edge(START, "chatbot")
    builder.add_edge("chatbot", END)

    return builder


async def main() -> None:
    """Run the async example."""
    llm = get_llm()
    print(f"Using LLM: {llm.__class__.__name__}")

    # Configure retry behavior for resilient async operations
    retry_config = RetryConfig(
        max_retries=5,
        base_delay=1.0,
        max_delay=60.0,
        jitter=True,
    )

    # Configure metrics with async-friendly callback
    def on_operation(op: str, duration: float, error: Exception | None) -> None:
        status = "OK" if error is None else f"ERROR: {error}"
        print(f"  [Async Metrics] {op}: {duration:.3f}s - {status}")

    metrics = Metrics(enabled=True, on_operation=on_operation)

    async with AsyncSnowflakeSaver.from_env(
        retry_config=retry_config,
        metrics=metrics,
    ) as checkpointer:
        print("Setting up Snowflake tables...")
        await checkpointer.setup()
        print("Setup complete!")

        builder = create_agent(llm)
        graph = builder.compile(checkpointer=checkpointer)

        thread_id = "async-example-1"
        config: dict[str, Any] = {"configurable": {"thread_id": thread_id}}

        # First interaction
        print("\n--- First Async Interaction ---")
        result = await graph.ainvoke(
            {"messages": [("user", "Hi! I'm learning about async programming.")]},
            config,
        )
        print(f"Assistant: {result['messages'][-1].content}")

        # Second interaction with streaming
        print("\n--- Streaming Response ---")
        print("Assistant: ", end="", flush=True)
        async for event in graph.astream_events(
            {"messages": [("user", "Give me a quick tip about async/await.")]},
            config,
            version="v2",
        ):
            if event["event"] == "on_chat_model_stream":
                chunk = event["data"]["chunk"]
                if hasattr(chunk, "content") and chunk.content:
                    print(chunk.content, end="", flush=True)
        print()  # Newline after streaming

        # List checkpoints asynchronously
        print("\n--- Async Checkpoint Listing ---")
        count = 0
        async for checkpoint in checkpointer.alist(config):
            count += 1
            print(
                f"  Checkpoint {count}: "
                f"{checkpoint.config['configurable']['checkpoint_id'][:8]}..."
            )
            if count >= 3:
                break

        # Show checkpoint statistics (async)
        print("\n--- Async Checkpoint Statistics ---")
        total_count = await checkpointer.aget_checkpoint_count()
        print(f"Total checkpoints in database: {total_count}")

        # Show collected metrics
        print("\n--- Async Metrics Summary ---")
        stats = checkpointer.get_metrics()
        if stats:
            print(f"Operations: {stats['operation_counts']}")
            print(f"Avg durations: {stats['average_durations']}")

        # Demonstrate async TTL cleanup
        print("\n--- Async TTL Cleanup Demo ---")
        deleted = await checkpointer.adelete_before(timedelta(days=30))
        print(f"Deleted {deleted} checkpoints older than 30 days")

        # Cleanup
        print("\n--- Cleanup ---")
        await checkpointer.adelete_thread(thread_id)
        print(f"Deleted thread: {thread_id}")


async def concurrent_conversations() -> None:
    """Example showing concurrent conversations with metrics."""
    llm = get_llm()

    # Track metrics across concurrent operations
    metrics = Metrics(enabled=True)
    retry_config = RetryConfig(max_retries=3)

    async with AsyncSnowflakeSaver.from_env(
        retry_config=retry_config,
        metrics=metrics,
    ) as checkpointer:
        await checkpointer.setup()

        builder = create_agent(llm)
        graph = builder.compile(checkpointer=checkpointer)

        # Run multiple conversations concurrently
        async def run_conversation(thread_id: str, message: str) -> str:
            config: dict[str, Any] = {"configurable": {"thread_id": thread_id}}
            result = await graph.ainvoke(
                {"messages": [("user", message)]},
                config,
            )
            content = result["messages"][-1].content
            return str(content) if content else ""

        print("Running 3 concurrent conversations...")
        results = await asyncio.gather(
            run_conversation("thread-1", "What is Python?"),
            run_conversation("thread-2", "What is JavaScript?"),
            run_conversation("thread-3", "What is Rust?"),
        )

        for i, result in enumerate(results, 1):
            print(f"\nThread {i} response: {result[:100]}...")

        # Show metrics from concurrent operations
        print("\n--- Concurrent Operations Metrics ---")
        stats = checkpointer.get_metrics()
        if stats:
            print(f"Total operations: {sum(stats['operation_counts'].values())}")
            print(f"Operations breakdown: {stats['operation_counts']}")

        # Cleanup all threads
        for i in range(1, 4):
            await checkpointer.adelete_thread(f"thread-{i}")


if __name__ == "__main__":
    asyncio.run(main())
