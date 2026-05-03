"""Azure OpenAI client singleton for backend LLM tasks."""
from __future__ import annotations

import threading

from openai import AzureOpenAI

from app.core.config import get_settings

_ai_client: AzureOpenAI | None = None
_ai_deployment: str | None = None
_client_lock = threading.Lock()


def get_client() -> tuple[AzureOpenAI, str]:
    """Get (or lazily create) a process-wide Azure OpenAI client and deployment name."""
    global _ai_client, _ai_deployment
    if _ai_client is None:
        with _client_lock:
            if _ai_client is None:
                settings = get_settings()
                endpoint = settings.AZURE_OPENAI_ENDPOINT or settings.AZURE_OPENAI_API_BASE
                api_key = settings.AZURE_OPENAI_KEY or settings.AZURE_OPENAI_API_KEY
                deployment = (
                    settings.AZURE_OPENAI_DEPLOYMENT
                    if settings.AZURE_OPENAI_DEPLOYMENT != "gpt-4"
                    else settings.AZURE_OPENAI_MODEL
                ) or settings.AZURE_OPENAI_DEPLOYMENT

                _ai_client = AzureOpenAI(
                    azure_endpoint=endpoint,
                    api_key=api_key,
                    api_version=settings.AZURE_OPENAI_API_VERSION,
                )
                _ai_deployment = deployment
    return _ai_client, _ai_deployment
