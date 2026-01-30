"""ReAct agent example with Snowflake checkpointer.

This example demonstrates:
1. Using create_react_agent from langgraph.prebuilt
2. Adding custom tools to the agent
3. Persisting agent state with Snowflake
4. Resuming conversations with tool history
5. Using RetryConfig for reliable tool execution persistence
6. Using Metrics to track agent performance
7. Using checkpoint statistics and TTL cleanup

Prerequisites:
    pip install langgraph-checkpoint-snowflake langgraph

    For the LLM, install one of:
    - pip install langchain-openai           (OpenAI)
    - pip install langchain-anthropic        (Anthropic)
    - pip install langchain-google-genai     (Google AI Studio)
    - pip install langchain-google-vertexai  (Google Vertex AI)
    - pip install langchain-ollama           (Ollama/local)

Environment variables required:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or key pair auth)
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

    Plus your LLM provider's API key
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

from langgraph_checkpoint_snowflake import Metrics, RetryConfig, SnowflakeSaver


def get_llm() -> BaseChatModel:
    """Get an LLM instance based on available providers."""
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
            "GOOGLE_CLOUD_PROJECT/VERTEXAI_PROJECT, VLLM_BASE_URL"
        )


# Define custom tools for the agent
@tool
def get_weather(city: str) -> str:
    """Get the current weather for a city.

    Args:
        city: The name of the city to get weather for.

    Returns:
        A string describing the current weather.
    """
    # Simulated weather data
    weather_data = {
        "new york": "72°F, Partly Cloudy",
        "london": "58°F, Rainy",
        "tokyo": "68°F, Clear",
        "paris": "65°F, Overcast",
        "sydney": "75°F, Sunny",
    }
    city_lower = city.lower()
    if city_lower in weather_data:
        return f"The weather in {city} is {weather_data[city_lower]}"
    return f"Weather data not available for {city}"


@tool
def get_time(timezone: str = "UTC") -> str:
    """Get the current time.

    Args:
        timezone: The timezone to get time for (simplified - just returns UTC).

    Returns:
        The current time as a string.
    """
    now = datetime.now()
    return f"The current time is {now.strftime('%Y-%m-%d %H:%M:%S')} ({timezone})"


@tool
def calculate(expression: str) -> str:
    """Evaluate a mathematical expression.

    Args:
        expression: A mathematical expression to evaluate (e.g., "2 + 2", "10 * 5").

    Returns:
        The result of the calculation.
    """
    try:
        # Simple and safe evaluation for basic math
        allowed_chars = set("0123456789+-*/.() ")
        if not all(c in allowed_chars for c in expression):
            return "Error: Only basic math operations are allowed"
        result = eval(expression)  # noqa: S307
        return f"{expression} = {result}"
    except Exception as e:
        return f"Error calculating: {e}"


@tool
def search_knowledge_base(query: str) -> str:
    """Search a knowledge base for information.

    Args:
        query: The search query.

    Returns:
        Relevant information from the knowledge base.
    """
    # Simulated knowledge base
    knowledge = {
        "langgraph": "LangGraph is a library for building stateful, multi-actor "
        "applications with LLMs. It extends LangChain with graph-based workflows.",
        "snowflake": "Snowflake is a cloud-based data warehousing platform that "
        "provides data storage, processing, and analytics solutions.",
        "checkpoint": "A checkpoint is a saved state of an agent's execution that "
        "allows resuming from that point later.",
        "react": "ReAct (Reasoning and Acting) is a paradigm where LLMs interleave "
        "reasoning traces with actions to solve tasks.",
    }
    query_lower = query.lower()
    for key, value in knowledge.items():
        if key in query_lower:
            return value
    return f"No specific information found for '{query}'. Try a different query."


def main() -> None:
    """Run the ReAct agent example."""
    llm = get_llm()
    print(f"Using LLM: {llm.__class__.__name__}")

    # Define the tools available to the agent
    tools = [get_weather, get_time, calculate, search_knowledge_base]

    # Configure retry for reliable persistence during tool execution
    retry_config = RetryConfig(
        max_retries=5,
        base_delay=1.0,
        max_delay=60.0,
        jitter=True,
    )

    # Track metrics for agent performance analysis
    def on_operation(op: str, duration: float, error: Exception | None) -> None:
        if error:
            print(f"  [ERROR] {op}: {error}")

    metrics = Metrics(enabled=True, on_operation=on_operation)

    with SnowflakeSaver.from_env(
        retry_config=retry_config,
        metrics=metrics,
    ) as checkpointer:
        checkpointer.setup()

        # Create the ReAct agent with tools and checkpointer
        # The prompt parameter sets a system message for the agent
        agent = create_react_agent(
            model=llm,
            tools=tools,
            checkpointer=checkpointer,
            prompt="You are a helpful assistant with access to weather, time, "
            "calculator, and knowledge base tools. Be concise in your responses.",
        )

        thread_id = "react-agent-demo"
        config: dict[str, Any] = {"configurable": {"thread_id": thread_id}}

        # First interaction - ask about weather
        print("\n--- Query 1: Weather ---")
        result = agent.invoke(
            {"messages": [("user", "What's the weather like in Tokyo and London?")]},
            config,
        )
        print(f"Agent: {result['messages'][-1].content}")

        # Second interaction - do some calculations
        print("\n--- Query 2: Calculations ---")
        result = agent.invoke(
            {"messages": [("user", "What is 15 * 7 + 23?")]},
            config,
        )
        print(f"Agent: {result['messages'][-1].content}")

        # Third interaction - search knowledge base
        print("\n--- Query 3: Knowledge Search ---")
        result = agent.invoke(
            {
                "messages": [
                    ("user", "What is LangGraph and how does it relate to checkpoints?")
                ]
            },
            config,
        )
        print(f"Agent: {result['messages'][-1].content}")

        # Fourth interaction - agent remembers context
        print("\n--- Query 4: Context Recall ---")
        result = agent.invoke(
            {
                "messages": [
                    ("user", "Which city had better weather - Tokyo or London?")
                ]
            },
            config,
        )
        print(f"Agent: {result['messages'][-1].content}")

        # Show checkpoint history
        print("\n--- Checkpoint History ---")
        checkpoints = list(checkpointer.list(config))
        print(f"Total checkpoints: {len(checkpoints)}")
        for i, cp in enumerate(checkpoints[:5]):
            metadata = cp.metadata or {}
            print(
                f"  {i + 1}. Step: {metadata.get('step', 'N/A')}, "
                f"Source: {metadata.get('source', 'N/A')}"
            )

        # Show checkpoint statistics
        print("\n--- Checkpoint Statistics ---")
        total_count = checkpointer.get_checkpoint_count()
        print(f"Total checkpoints in database: {total_count}")

        counts_by_thread = checkpointer.get_checkpoint_counts_by_thread(max_results=5)
        if counts_by_thread:
            print("Top threads by checkpoint count:")
            for tid, count in counts_by_thread:
                print(f"  {tid}: {count}")

        # Show agent performance metrics
        print("\n--- Agent Performance Metrics ---")
        stats = checkpointer.get_metrics()
        if stats:
            print(f"Checkpoint operations: {stats['operation_counts']}")
            print(f"Average durations: {stats['average_durations']}")
            if stats["error_counts"]:
                print(f"Errors: {stats['error_counts']}")

        # Demonstrate resuming from a checkpoint
        if len(checkpoints) > 2:
            print("\n--- Resume from Earlier Checkpoint ---")
            earlier_config = checkpoints[2].config
            result = agent.invoke(
                {"messages": [("user", "What time is it now?")]},
                earlier_config,
            )
            print(f"Agent: {result['messages'][-1].content}")

        # Demonstrate TTL cleanup
        print("\n--- TTL Cleanup Demo ---")
        deleted = checkpointer.delete_before(timedelta(days=30))
        print(f"Deleted {deleted} checkpoints older than 30 days")

        # Cleanup
        print("\n--- Cleanup ---")
        checkpointer.delete_thread(thread_id)
        print(f"Deleted thread: {thread_id}")


if __name__ == "__main__":
    main()
