"""Azure OpenAI LangChain client — thread-safe singleton."""
from __future__ import annotations

import threading

from langchain_openai import AzureChatOpenAI

from app.core.config import get_settings

_llm: AzureChatOpenAI | None = None
_llm_lock = threading.Lock()


def get_llm() -> AzureChatOpenAI:
    global _llm
    if _llm is None:
        with _llm_lock:
            if _llm is None:
                s = get_settings()
                endpoint = s.AZURE_OPENAI_ENDPOINT or s.AZURE_OPENAI_API_BASE
                api_key = s.AZURE_OPENAI_KEY or s.AZURE_OPENAI_API_KEY
                deployment = (
                    s.AZURE_OPENAI_DEPLOYMENT
                    if s.AZURE_OPENAI_DEPLOYMENT != "gpt-4"
                    else s.AZURE_OPENAI_MODEL
                ) or s.AZURE_OPENAI_DEPLOYMENT
                api_version = s.AZURE_OPENAI_API_VERSION or "2024-02-01"
                _llm = AzureChatOpenAI(
                    azure_endpoint=endpoint,
                    api_key=api_key,
                    azure_deployment=deployment,
                    api_version=api_version,
                    temperature=0,
                    max_completion_tokens=1500,
                    timeout=60,
                    max_retries=2,
                )
    return _llm
