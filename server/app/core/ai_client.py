import asyncio
import json
import threading
import time

import tiktoken
from openai import AzureOpenAI

from app.core.config import get_settings
from app.core.logger import ingest_logger, llm_logger


# ── Singleton Azure OpenAI client ─────────────────────────────────────────────
# Constructing AzureOpenAI creates an HTTP session — reuse it across all calls.
_ai_client: AzureOpenAI | None = None
_ai_deployment: str | None = None
_client_lock = threading.Lock()


def _get_client() -> tuple[AzureOpenAI, str]:
    global _ai_client, _ai_deployment
    if _ai_client is None:
        with _client_lock:
            if _ai_client is None:
                settings = get_settings()
                # Support both naming conventions in .env:
                #   AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_KEY / AZURE_OPENAI_DEPLOYMENT
                #   AZURE_OPENAI_API_BASE / AZURE_OPENAI_API_KEY / AZURE_OPENAI_MODEL
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
                    api_version=settings.AZURE_OPENAI_API_VERSION if hasattr(settings, "AZURE_OPENAI_API_VERSION") else "2024-02-01",
                )
                _ai_deployment = deployment
    return _ai_client, _ai_deployment


# ── Tiktoken + cost calculation ───────────────────────────────────────────────
# Prices per 1K tokens (USD). Update if deployment changes.
_PRICE_PER_1K: dict[str, dict[str, float]] = {
    "gpt-4o-mini":  {"prompt": 0.000150, "completion": 0.000600},
    "gpt-4o":       {"prompt": 0.005000, "completion": 0.015000},
    "gpt-4":        {"prompt": 0.030000, "completion": 0.060000},
    "gpt-4-turbo":  {"prompt": 0.010000, "completion": 0.030000},
    "gpt-35-turbo": {"prompt": 0.000500, "completion": 0.001500},
}

# Session accumulator — resets on process restart
_session_cost_usd: float = 0.0
_session_tokens: dict[str, int] = {"prompt": 0, "completion": 0, "total": 0}
_cost_lock = threading.Lock()

_tiktoken_encodings: dict[str, tiktoken.Encoding] = {}


def _get_encoding(model: str) -> tiktoken.Encoding:
    if model not in _tiktoken_encodings:
        try:
            _tiktoken_encodings[model] = tiktoken.encoding_for_model(model)
        except KeyError:
            _tiktoken_encodings[model] = tiktoken.get_encoding("cl100k_base")
    return _tiktoken_encodings[model]


def _count_tokens(text: str, model: str) -> int:
    try:
        return len(_get_encoding(model).encode(text))
    except Exception:
        return len(text) // 4


def _calc_cost(prompt_tokens: int, completion_tokens: int, model: str) -> float:
    """Return cost in USD for one LLM call."""
    key = model.lower()
    for k, prices in _PRICE_PER_1K.items():
        if k in key:
            return round(
                (prompt_tokens / 1000 * prices["prompt"])
                + (completion_tokens / 1000 * prices["completion"]),
                8,
            )
    prices = _PRICE_PER_1K["gpt-4o-mini"]
    return round(
        (prompt_tokens / 1000 * prices["prompt"])
        + (completion_tokens / 1000 * prices["completion"]),
        8,
    )


def _track_and_log(
    function: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    duration_ms: float,
    extra: dict | None = None,
) -> None:
    """Emit one structured line to llm_calls.log with full token + cost breakdown."""
    global _session_cost_usd
    total_tokens = prompt_tokens + completion_tokens
    call_cost = _calc_cost(prompt_tokens, completion_tokens, model)

    with _cost_lock:
        _session_cost_usd += call_cost
        _session_tokens["prompt"] += prompt_tokens
        _session_tokens["completion"] += completion_tokens
        _session_tokens["total"] += total_tokens
        session_cost_snapshot = round(_session_cost_usd, 6)
        session_tokens_snapshot = dict(_session_tokens)

    log_fields: dict = {
        "function": function,
        "model": model,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cost_usd": call_cost,
        "session_cost_usd": session_cost_snapshot,
        "session_tokens": session_tokens_snapshot,
        "duration_ms": duration_ms,
    }
    if extra:
        log_fields.update(extra)
    llm_logger.info("llm_call", **log_fields)

    # Also emit to the unified costs.log so LLM + Azure appear in one file
    from app.core.cost_tracker import track_llm as _track_llm_cost
    _track_llm_cost(function, model, prompt_tokens, completion_tokens, call_cost, duration_ms)


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


def _safe_parse_json(text: str) -> dict:
    """Strip markdown fences and parse JSON. Returns fallback on failure."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        return {}


async def generate_file_description(
    columns_info: list, sample_rows: list, filename: str
) -> dict:
    def _run() -> dict:
        client, deployment = _get_client()
        cols_for_prompt = [
            {
                "name": c["name"],
                "type": c["type"],
                "samples": c["sample_values"],
                "unique": c["unique_values"],
            }
            for c in columns_info
        ]
        prompt = f"""You are analyzing a data file named "{filename}".
Return ONLY this JSON with no preamble no markdown:
{{
  "summary": "one sentence what this file contains and what questions it answers",
  "good_for": ["query type 1", "query type 2"],
  "key_metrics": ["numeric columns for aggregation"],
  "key_dimensions": ["categorical columns for grouping"],
  "date_range_start": "YYYY-MM-DD or null",
  "date_range_end": "YYYY-MM-DD or null"
}}

Columns: {json.dumps(cols_for_prompt, default=str)}
Sample rows: {json.dumps(sample_rows[:3], default=str)}"""

        prompt_tokens = _count_tokens(prompt, deployment)
        t = time.perf_counter()
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0,
        )
        duration = _ms(t)
        raw = response.choices[0].message.content

        api = response.usage
        p_tok = api.prompt_tokens if api else prompt_tokens
        c_tok = api.completion_tokens if api else _count_tokens(raw, deployment)

        parsed = _safe_parse_json(raw)
        if not parsed.get("summary"):
            parsed = {
                "summary": filename, "good_for": [], "key_metrics": [],
                "key_dimensions": [], "date_range_start": None, "date_range_end": None,
            }
        parsed["_p_tok"] = p_tok
        parsed["_c_tok"] = c_tok
        parsed["_duration"] = duration
        parsed["_deployment"] = deployment
        return parsed

    ingest_logger.info("llm_call", function="generate_file_description",
                       status="started", filename=filename,
                       column_count=len(columns_info))
    result = await asyncio.to_thread(_run)
    _track_and_log(
        function="generate_file_description",
        model=result.pop("_deployment"),
        prompt_tokens=result.pop("_p_tok"),
        completion_tokens=result.pop("_c_tok"),
        duration_ms=result.pop("_duration"),
        extra={"filename": filename, "summary": result.get("summary", "")[:120]},
    )
    ingest_logger.info("llm_call", function="generate_file_description",
                       status="done", filename=filename,
                       summary=result.get("summary", "")[:150],
                       good_for=result.get("good_for", []))
    return result
