"""Azure OpenAI LangChain clients — thread-safe singletons.

Two deployments:
  get_llm()       → gpt-4o      (primary, used on turn 1)
  get_llm_mini()  → gpt-4o-mini (cheaper, used on follow-up turns 2+;
                                   graph_builder falls back to get_llm() on RateLimitError)
"""
from __future__ import annotations

import threading

from langchain_openai import AzureChatOpenAI

from app.core.config import get_settings

_llm: AzureChatOpenAI | None = None
_llm_mini: AzureChatOpenAI | None = None
_lock = threading.Lock()


def _make_llm(deployment: str, max_tokens: int = 1500) -> AzureChatOpenAI:
    s = get_settings()
    endpoint = s.AZURE_OPENAI_ENDPOINT or s.AZURE_OPENAI_API_BASE
    api_key = s.AZURE_OPENAI_KEY or s.AZURE_OPENAI_API_KEY
    api_version = s.AZURE_OPENAI_API_VERSION
    return AzureChatOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        azure_deployment=deployment,
        api_version=api_version,
        temperature=0,
        max_completion_tokens=max_tokens,
        timeout=60,
        max_retries=2,
    )


def get_llm() -> AzureChatOpenAI:
    """Return the gpt-4o singleton (primary model, turn 1)."""
    global _llm
    if _llm is None:
        with _lock:
            if _llm is None:
                _llm = _make_llm(get_settings().AZURE_OPENAI_DEPLOYMENT)
    return _llm


def get_llm_mini() -> AzureChatOpenAI:
    """Return the gpt-4o-mini singleton (follow-up turns 2+)."""
    global _llm_mini
    if _llm_mini is None:
        with _lock:
            if _llm_mini is None:
                _llm_mini = _make_llm(get_settings().AZURE_OPENAI_DEPLOYMENT_MINI)
    return _llm_mini
