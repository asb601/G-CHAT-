"""Token counting and per-call/session LLM cost tracking utilities."""
from __future__ import annotations

import threading
import time

import tiktoken

from app.core.logger import llm_logger

_PRICE_PER_1K: dict[str, dict[str, float]] = {
    "gpt-4o-mini": {"prompt": 0.000150, "completion": 0.000600},
    "gpt-4o": {"prompt": 0.005000, "completion": 0.015000},
    "gpt-4": {"prompt": 0.030000, "completion": 0.060000},
    "gpt-4-turbo": {"prompt": 0.010000, "completion": 0.030000},
    "gpt-35-turbo": {"prompt": 0.000500, "completion": 0.001500},
}

_session_cost_usd: float = 0.0
_session_tokens: dict[str, int] = {"prompt": 0, "completion": 0, "total": 0}
_cost_lock = threading.Lock()

_tiktoken_encodings: dict[str, tiktoken.Encoding] = {}


def get_encoding(model: str) -> tiktoken.Encoding:
    if model not in _tiktoken_encodings:
        try:
            _tiktoken_encodings[model] = tiktoken.encoding_for_model(model)
        except KeyError:
            _tiktoken_encodings[model] = tiktoken.get_encoding("cl100k_base")
    return _tiktoken_encodings[model]


def count_tokens(text: str, model: str) -> int:
    try:
        return len(get_encoding(model).encode(text))
    except Exception:
        return len(text) // 4


def calc_cost(prompt_tokens: int, completion_tokens: int, model: str) -> float:
    """Return cost in USD for one LLM call."""
    key = model.lower()
    for k, prices in _PRICE_PER_1K.items():
        if k in key:
            return round(
                (prompt_tokens / 1000 * prices["prompt"]) + (completion_tokens / 1000 * prices["completion"]),
                8,
            )

    prices = _PRICE_PER_1K["gpt-4o-mini"]
    return round(
        (prompt_tokens / 1000 * prices["prompt"]) + (completion_tokens / 1000 * prices["completion"]),
        8,
    )


def track_and_log(
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
    call_cost = calc_cost(prompt_tokens, completion_tokens, model)

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

    from app.core.cost_tracker import track_llm as _track_llm_cost

    _track_llm_cost(function, model, prompt_tokens, completion_tokens, call_cost, duration_ms)


def elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)
