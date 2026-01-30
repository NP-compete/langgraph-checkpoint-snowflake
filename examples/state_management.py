"""State management example with Snowflake checkpointer.

This example demonstrates advanced state management features:
1. Inspecting current state with get_state()
2. Viewing checkpoint history with get_state_history() (time travel)
3. Modifying state with update_state()
4. Replaying from a specific checkpoint

These features enable powerful debugging, auditing, and human-in-the-loop
workflows where you need fine-grained control over agent state.

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
from typing import Annotated

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from langgraph_checkpoint_snowflake import SnowflakeSaver


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
    else:
        raise ValueError(
            "No LLM provider configured. Set one of: "
            "OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, "
            "GOOGLE_CLOUD_PROJECT/VERTEXAI_PROJECT"
        )


# Define the state schema
class State(TypedDict):
    """State for the conversation graph."""

    messages: Annotated[list, add_messages]


def chatbot(state: State) -> dict:
    """Process messages and generate a response."""
    llm = get_llm()
    response = llm.invoke(state["messages"])
    return {"messages": [response]}


def build_graph(checkpointer: SnowflakeSaver) -> StateGraph:
    """Build a simple chatbot graph."""
    builder = StateGraph(State)
    builder.add_node("chatbot", chatbot)
    builder.add_edge(START, "chatbot")
    builder.add_edge("chatbot", END)
    return builder.compile(checkpointer=checkpointer)


def demo_get_state(graph: StateGraph, config: dict) -> None:
    """Demonstrate get_state() - inspect current state snapshot."""
    print("\n" + "=" * 60)
    print("DEMO: get_state() - Inspect Current State")
    print("=" * 60)

    # Get the current state
    state = graph.get_state(config)

    if state.values:
        print("\nCurrent state values:")
        print(f"  - Number of messages: {len(state.values.get('messages', []))}")

        print("\nMessages in state:")
        for i, msg in enumerate(state.values.get("messages", [])):
            role = "Human" if isinstance(msg, HumanMessage) else "AI"
            content = (
                msg.content[:100] + "..." if len(msg.content) > 100 else msg.content
            )
            print(f"  [{i}] {role}: {content}")

        print(f"\nCheckpoint ID: {state.config['configurable'].get('checkpoint_id')}")
        print(f"Parent checkpoint: {state.parent_config}")
        print(f"Next nodes: {state.next}")
    else:
        print("\nNo state found for this thread.")


def demo_get_state_history(graph: StateGraph, config: dict) -> None:
    """Demonstrate get_state_history() - view all checkpoints (time travel)."""
    print("\n" + "=" * 60)
    print("DEMO: get_state_history() - Time Travel")
    print("=" * 60)

    # Get all checkpoints for this thread
    history = list(graph.get_state_history(config))

    print(f"\nFound {len(history)} checkpoints in history:")
    print("-" * 40)

    for i, state in enumerate(history):
        checkpoint_id = state.config["configurable"].get("checkpoint_id", "N/A")
        num_messages = len(state.values.get("messages", []))
        created_by = (
            state.metadata.get("source", "unknown") if state.metadata else "unknown"
        )

        print(f"\n[{i}] Checkpoint: {checkpoint_id[:20]}...")
        print(f"    Messages: {num_messages}")
        print(f"    Created by: {created_by}")

        # Show the last message at this checkpoint
        messages = state.values.get("messages", [])
        if messages:
            last_msg = messages[-1]
            role = "Human" if isinstance(last_msg, HumanMessage) else "AI"
            content = (
                last_msg.content[:50] + "..."
                if len(last_msg.content) > 50
                else last_msg.content
            )
            print(f"    Last message ({role}): {content}")

    return history


def demo_update_state(graph: StateGraph, config: dict) -> None:
    """Demonstrate update_state() - modify state manually."""
    print("\n" + "=" * 60)
    print("DEMO: update_state() - Modify State Manually")
    print("=" * 60)

    # Get current state
    current_state = graph.get_state(config)
    print(f"\nCurrent message count: {len(current_state.values.get('messages', []))}")

    # Add a correction message
    correction_message = HumanMessage(
        content="Actually, I want to clarify: my name is spelled differently."
    )

    print("\nAdding a correction message to state...")
    new_config = graph.update_state(
        config,
        values={"messages": [correction_message]},
    )

    print(
        f"State updated. New checkpoint: {new_config['configurable'].get('checkpoint_id', 'N/A')[:20]}..."
    )

    # Verify the update
    updated_state = graph.get_state(config)
    print(f"Updated message count: {len(updated_state.values.get('messages', []))}")

    # Show the added message
    messages = updated_state.values.get("messages", [])
    if messages:
        last_msg = messages[-1]
        print(f"Last message: {last_msg.content}")

    return new_config


def demo_replay_from_checkpoint(
    graph: StateGraph, config: dict, checkpoint_id: str
) -> None:
    """Demonstrate replaying from a specific checkpoint."""
    print("\n" + "=" * 60)
    print("DEMO: Replay from Specific Checkpoint")
    print("=" * 60)

    # Create config pointing to specific checkpoint
    replay_config = {
        "configurable": {
            "thread_id": config["configurable"]["thread_id"],
            "checkpoint_id": checkpoint_id,
        }
    }

    print(f"\nReplaying from checkpoint: {checkpoint_id[:20]}...")

    # Get state at that checkpoint
    state_at_checkpoint = graph.get_state(replay_config)
    print(
        f"Messages at checkpoint: {len(state_at_checkpoint.values.get('messages', []))}"
    )

    # Continue from that checkpoint with a new message
    print("\nContinuing conversation from this checkpoint...")
    result = graph.invoke(
        {"messages": [HumanMessage(content="What did I just tell you?")]},
        replay_config,
    )

    # Show the response
    if result.get("messages"):
        last_msg = result["messages"][-1]
        print(f"\nAI response: {last_msg.content}")


def main() -> None:
    """Run all state management demos."""
    print("State Management Example")
    print("========================")
    print("This example demonstrates advanced state inspection and manipulation.")

    with SnowflakeSaver.from_env() as checkpointer:
        checkpointer.setup()

        # Build the graph
        graph = build_graph(checkpointer)

        # Use a unique thread ID for this demo
        thread_id = "state-management-demo"
        config = {"configurable": {"thread_id": thread_id}}

        # First, have a short conversation to create some checkpoints
        print("\n" + "=" * 60)
        print("SETUP: Creating a conversation with multiple checkpoints")
        print("=" * 60)

        messages_to_send = [
            "Hi! My name is Alice.",
            "I work as a software engineer.",
            "What do you remember about me?",
        ]

        for msg in messages_to_send:
            print(f"\nUser: {msg}")
            result = graph.invoke({"messages": [HumanMessage(content=msg)]}, config)
            if result.get("messages"):
                ai_response = result["messages"][-1].content
                print(f"AI: {ai_response[:200]}...")

        # Demo 1: Inspect current state
        demo_get_state(graph, config)

        # Demo 2: View checkpoint history (time travel)
        history = demo_get_state_history(graph, config)

        # Demo 3: Update state manually
        demo_update_state(graph, config)

        # Continue the conversation after the update
        print("\n" + "=" * 60)
        print("Continuing conversation after state update...")
        print("=" * 60)
        result = graph.invoke(
            {"messages": [HumanMessage(content="Do you understand my clarification?")]},
            config,
        )
        if result.get("messages"):
            print(f"\nAI: {result['messages'][-1].content}")

        # Demo 4: Replay from an earlier checkpoint
        if len(history) >= 3:
            # Get an earlier checkpoint (not the latest)
            earlier_checkpoint = history[2].config["configurable"]["checkpoint_id"]
            demo_replay_from_checkpoint(graph, config, earlier_checkpoint)

        # Final summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print("""
State management features demonstrated:

1. get_state(config)
   - Inspect the current state snapshot
   - See all messages, checkpoint ID, and metadata
   - Useful for debugging and monitoring

2. get_state_history(config)
   - View all checkpoints for a thread
   - Enables "time travel" debugging
   - Useful for auditing and rollback

3. update_state(config, values)
   - Modify state manually
   - Add, remove, or change messages
   - Useful for human-in-the-loop corrections

4. Replay from checkpoint
   - Continue from any historical checkpoint
   - Branch conversations from past states
   - Useful for "what if" scenarios
""")


if __name__ == "__main__":
    main()
