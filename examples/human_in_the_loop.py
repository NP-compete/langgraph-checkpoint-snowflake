"""Human-in-the-loop example with Snowflake checkpointer.

This example demonstrates:
1. Interrupting agent execution for human review
2. Modifying state before resuming
3. Using checkpoints for approval workflows
4. Using Metrics to track approval workflow operations
5. Using RetryConfig for reliable state persistence

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
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

    Plus your LLM provider's API key
"""

from __future__ import annotations

import os
from typing import Annotated, Any, Literal, TypedDict

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from langgraph_checkpoint_snowflake import Metrics, RetryConfig, SnowflakeSaver


def get_llm() -> BaseChatModel:
    """Get an LLM instance based on available providers."""
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


class State(TypedDict, total=False):
    """State with approval tracking."""

    messages: Annotated[list[BaseMessage], add_messages]
    pending_action: str | None
    approved: bool | None


def create_approval_agent(llm: BaseChatModel) -> StateGraph:
    """Create an agent that requires human approval for actions."""

    def analyze_request(state: State) -> dict[str, Any]:
        """Analyze the user request and propose an action."""
        response = llm.invoke(
            [
                (
                    "system",
                    "You are a helpful assistant. When the user asks you to do "
                    "something, describe what action you would take. Be specific.",
                ),
                *state["messages"],
            ]
        )
        content = response.content if hasattr(response, "content") else str(response)
        return {
            "messages": [response],
            "pending_action": str(content),
            "approved": None,
        }

    def execute_action(state: State) -> dict[str, Any]:
        """Execute the approved action."""
        if state.get("approved"):
            return {
                "messages": [
                    ("assistant", f"Action completed: {state.get('pending_action')}")
                ],
                "pending_action": None,
            }
        else:
            return {
                "messages": [
                    (
                        "assistant",
                        "Action was not approved. Let me know if you'd like to try "
                        "something else.",
                    )
                ],
                "pending_action": None,
            }

    def should_continue(state: State) -> Literal["execute", "wait"]:
        """Check if we should execute or wait for approval."""
        if state.get("approved") is None:
            return "wait"  # Interrupt here for human review
        return "execute"

    # Build the graph
    builder = StateGraph(State)
    builder.add_node("analyze", analyze_request)
    builder.add_node("execute", execute_action)

    builder.add_edge(START, "analyze")
    builder.add_conditional_edges(
        "analyze",
        should_continue,
        {"execute": "execute", "wait": END},
    )
    builder.add_edge("execute", END)

    return builder


def main() -> None:
    """Run the human-in-the-loop example."""
    llm = get_llm()
    print(f"Using LLM: {llm.__class__.__name__}")

    # Configure retry for reliable state persistence during approval workflow
    retry_config = RetryConfig(
        max_retries=5,
        base_delay=1.0,
        jitter=True,
    )

    # Track metrics for the approval workflow
    workflow_metrics: list[tuple[str, float, str]] = []

    def on_operation(op: str, duration: float, error: Exception | None) -> None:
        status = "success" if error is None else f"error: {error}"
        workflow_metrics.append((op, duration, status))

    metrics = Metrics(enabled=True, on_operation=on_operation)

    with SnowflakeSaver.from_env(
        retry_config=retry_config,
        metrics=metrics,
    ) as checkpointer:
        checkpointer.setup()

        builder = create_approval_agent(llm)
        # Compile with interrupt_before to pause before execution
        graph = builder.compile(
            checkpointer=checkpointer,
            interrupt_before=["execute"],
        )

        thread_id = "approval-workflow-1"
        config: dict[str, Any] = {"configurable": {"thread_id": thread_id}}

        # Step 1: User makes a request
        print("--- Step 1: User Request ---")
        result = graph.invoke(
            {
                "messages": [
                    (
                        "user",
                        "Please send an email to the team about the project update.",
                    )
                ]
            },
            config,
        )
        print(f"Proposed action: {result.get('pending_action', 'N/A')}")

        # Step 2: Human reviews and approves/rejects
        print("\n--- Step 2: Human Review ---")
        approval = input("Approve this action? (yes/no): ").strip().lower()

        # Step 3: Update state with approval decision
        print("\n--- Step 3: Resume with Decision ---")

        # Get the current checkpoint
        checkpoint_tuple = checkpointer.get_tuple(config)
        if checkpoint_tuple:
            # Update the state with approval
            graph.update_state(
                config,
                {"approved": approval == "yes"},
            )

            # Resume execution
            result = graph.invoke(None, config)
            print(f"Final response: {result['messages'][-1].content}")

        # Show checkpoint history
        print("\n--- Checkpoint History ---")
        for i, cp in enumerate(checkpointer.list(config)):
            metadata = cp.metadata or {}
            print(
                f"  {i + 1}. Step: {metadata.get('step', 'N/A')}, "
                f"Source: {metadata.get('source', 'N/A')}"
            )
            if i >= 4:
                break

        # Show workflow metrics
        print("\n--- Workflow Metrics ---")
        stats = checkpointer.get_metrics()
        if stats:
            print(
                f"Total checkpoint operations: {sum(stats['operation_counts'].values())}"
            )
            print(f"Operations: {stats['operation_counts']}")
            total_time = sum(stats["operation_durations"].values())
            print(f"Total checkpoint time: {total_time:.3f}s")

        # Show detailed operation log
        print("\n--- Operation Log ---")
        for op, duration, status in workflow_metrics[:10]:
            print(f"  {op}: {duration:.3f}s ({status})")

        # Cleanup
        checkpointer.delete_thread(thread_id)


if __name__ == "__main__":
    main()
