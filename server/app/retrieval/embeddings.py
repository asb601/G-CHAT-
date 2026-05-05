"""
Embedding service — generates and batches text embeddings via Azure OpenAI.

Uses `text-embedding-3-small` (1 536 dims) on the same Azure OpenAI resource
as the rest of the app.  The embedding deployment name is configured via
`AZURE_OPENAI_EMBEDDING_DEPLOYMENT` in .env (defaults to "text-embedding-3-small").

Cost reference (Azure OpenAI pricing, 2026):
  text-embedding-3-small  $0.02 / 1M tokens
  1M file descriptions × ~300 tok  ≈  $6 total one-time backfill
  Per user query                   ≈  $0.00001

Public functions
----------------
embed_text(text)          → list[float]           (1 536 dims, single string)
embed_batch(texts)        → list[list[float]]      (up to 100 strings)
build_search_text(obj)    → str                    (canonical text for a file)
"""
from __future__ import annotations

import threading
from typing import Union

from openai import AsyncAzureOpenAI

from app.core.config import get_settings

# ---------------------------------------------------------------------------
# Lazy async client — created once, reused across the process lifetime.
# Protected by a lock because the lifespan event is the only writer.
# ---------------------------------------------------------------------------
_embedding_client: AsyncAzureOpenAI | None = None
_embedding_deployment: str | None = None
_lock = threading.Lock()

_EMBED_DIMS = 1536


def _get_embedding_client() -> tuple[AsyncAzureOpenAI, str]:
    global _embedding_client, _embedding_deployment
    if _embedding_client is None:
        with _lock:
            if _embedding_client is None:
                s = get_settings()
                endpoint = s.AZURE_OPENAI_ENDPOINT or s.AZURE_OPENAI_API_BASE
                api_key = s.AZURE_OPENAI_KEY or s.AZURE_OPENAI_API_KEY
                _embedding_client = AsyncAzureOpenAI(
                    azure_endpoint=endpoint,
                    api_key=api_key,
                    api_version=s.AZURE_OPENAI_API_VERSION,
                )
                _embedding_deployment = s.AZURE_OPENAI_EMBEDDING_DEPLOYMENT
    return _embedding_client, _embedding_deployment  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def embed_text(text: str) -> list[float]:
    """Embed a single string.  Returns a 1 536-dim float list."""
    if not text or not text.strip():
        return [0.0] * _EMBED_DIMS
    results = await embed_batch([text])
    return results[0]


async def embed_batch(texts: list[str]) -> list[list[float]]:
    """Embed up to 100 strings in one API call.

    Returns embeddings in the same order as `texts`.
    Empty / whitespace strings are replaced with zero vectors without an API call.
    """
    if not texts:
        return []

    # Separate real texts from blanks (blanks get zero vectors, no API charges)
    indices_to_embed: list[int] = []
    cleaned: list[str] = []
    for i, t in enumerate(texts):
        if t and t.strip():
            indices_to_embed.append(i)
            cleaned.append(t.strip())

    result: list[list[float]] = [[0.0] * _EMBED_DIMS for _ in texts]

    if cleaned:
        try:
            client, deployment = _get_embedding_client()
            resp = await client.embeddings.create(
                model=deployment,
                input=cleaned,
            )
            for pos, item in enumerate(resp.data):
                result[indices_to_embed[pos]] = item.embedding
        except Exception as exc:
            # DeploymentNotFound → deployment not yet created in Azure portal.
            # Other errors (rate limit, transient) also handled here.
            # Ingestion continues — embedding columns stay NULL until backfill.
            _log_embedding_failure(exc)

    return result


def _log_embedding_failure(exc: Exception) -> None:
    """Emit a single structured warning when embeddings are unavailable."""
    try:
        from app.core.logger import ingest_logger  # lazy import avoids circular dep
        ingest_logger.warning(
            "embedding_unavailable",
            error=str(exc)[:200],
            hint=(
                "Deploy 'text-embedding-3-small' in Azure OpenAI Studio "
                "and set AZURE_OPENAI_EMBEDDING_DEPLOYMENT in .env"
            ),
        )
    except Exception:
        pass  # logging must never crash the ingestion pipeline


# ---------------------------------------------------------------------------
# Canonical search-text builder
# ---------------------------------------------------------------------------

# Accepted input types: SQLAlchemy FileMetadata row OR plain dict with same keys.
_AnyMetadata = Union["FileMetadata", dict]  # type: ignore[name-defined]


def build_search_text(metadata: _AnyMetadata) -> str:
    """Build the canonical text string indexed for BM25, trgm, and embeddings.

    Concatenates in order:
      file_name · ai_description · column names · good_for topics · key_metrics

    Works with both a FileMetadata ORM row and a dict (catalog entry format).
    Pure function — no I/O, no side effects.
    """
    def _get(key: str, default="") -> str:
        if isinstance(metadata, dict):
            return str(metadata.get(key, default) or default)
        return str(getattr(metadata, key, default) or default)

    def _get_list(key: str) -> list:
        if isinstance(metadata, dict):
            val = metadata.get(key) or []
        else:
            val = getattr(metadata, key, None) or []
        return val if isinstance(val, list) else []

    parts: list[str] = []

    # File name (strip blob-path prefix if present)
    blob = _get("blob_path")
    if blob:
        parts.append(blob.rsplit("/", 1)[-1])  # filename only
    file_name = _get("file_name") or _get("name")
    if file_name:
        parts.append(file_name)

    # AI description
    desc = _get("ai_description")
    if desc:
        parts.append(desc)

    # Column names — accept either the heavy `columns_info` shape (list of
    # {name, type, sample_values, ...} dicts) used by the FileMetadata ORM
    # row or the lean `column_names` list-of-strings shape used by the
    # cached catalog entries.
    col_names: list[str] = []
    for c in _get_list("columns_info"):
        if isinstance(c, dict):
            col_names.append(c.get("name", ""))
        elif isinstance(c, str):
            col_names.append(c)
    if not col_names:
        for c in _get_list("column_names"):
            if isinstance(c, str):
                col_names.append(c)
    if col_names:
        parts.append(" ".join(n for n in col_names if n))

    # good_for topics
    good_for = _get_list("good_for")
    if good_for:
        parts.append(" ".join(str(g) for g in good_for))

    # key_metrics
    key_metrics = _get_list("key_metrics")
    if key_metrics:
        parts.append(" ".join(str(m) for m in key_metrics))

    # key_dimensions
    key_dims = _get_list("key_dimensions")
    if key_dims:
        parts.append(" ".join(str(d) for d in key_dims))

    return " ".join(p.strip() for p in parts if p.strip())
