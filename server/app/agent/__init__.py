"""
Agent package — LangGraph-based agentic query pipeline.

Modules:
  state.py    – AgentState type definition
  llm.py      – Azure OpenAI client singleton
  tools/      – individual tool modules
  graph.py    – graph construction + agent node
"""
from app.agent.graph import run_agent_query  # noqa: F401

__all__ = ["run_agent_query"]
