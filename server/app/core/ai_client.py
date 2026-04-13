import asyncio
import hashlib
import json
import threading
import time

import tiktoken
from openai import AzureOpenAI

from app.core.config import get_settings
from app.core.logger import chat_logger, ingest_logger, llm_logger


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


# ── LLM response cache ────────────────────────────────────────────────────────
# Caches file-selection and SQL responses so repeat/similar queries skip the LLM.
# Key = hash of inputs; evicts oldest entry when full.
_llm_cache: dict[str, dict] = {}
_LLM_CACHE_MAX = 500


def _cache_get(key: str) -> dict | None:
    return _llm_cache.get(key)


def _cache_set(key: str, value: dict) -> None:
    if len(_llm_cache) >= _LLM_CACHE_MAX:
        del _llm_cache[next(iter(_llm_cache))]
    _llm_cache[key] = value


def _hash_key(*parts) -> str:
    raw = json.dumps(parts, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


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


async def select_relevant_files(
    query: str,
    file_summaries: list,
    relationships: list,
    extra_context: str = "",
) -> dict:
    def _run() -> dict:
        client, deployment = _get_client()
        summaries_text = "\n\n".join(
            [
                f"blob_path: {f['blob_path']}\n"
                f"description: {f['ai_description']}\n"
                f"good_for: {', '.join(f.get('good_for', []))}\n"
                f"columns: {', '.join([c['name'] for c in f['columns_info']])}\n"
                f"metrics: {', '.join(f.get('key_metrics', []))}\n"
                f"dimensions: {', '.join(f.get('key_dimensions', []))}"
                for f in file_summaries
            ]
        )
        rel_text = (
            "\n".join(
                [
                    f"{r['file_a_path']} <-> {r['file_b_path']} via {r['shared_column']} (score: {r['confidence_score']})"
                    for r in relationships
                    if r.get("confidence_score", 0) > 0.6
                ]
            )
            or "None"
        )

        prompt = f"""You are a data analyst. User asked: "{query}"
{extra_context}

Available files:
{summaries_text}

Known relationships:
{rel_text}

Return ONLY this JSON:
{{
  "files": ["blob_path_1", "blob_path_2"],
  "joins": [{{"file_a": "path", "file_b": "path", "on_column": "col", "join_type": "LEFT JOIN"}}],
  "confidence": 0.95,
  "reasoning": "brief explanation"
}}
If no files relevant: {{"files": [], "joins": [], "confidence": 0, "reasoning": "what data is missing"}}
Return ONLY valid JSON."""

        prompt_tokens = _count_tokens(prompt, deployment)
        t = time.perf_counter()
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0,
        )
        duration = _ms(t)
        raw = response.choices[0].message.content

        api = response.usage
        p_tok = api.prompt_tokens if api else prompt_tokens
        c_tok = api.completion_tokens if api else _count_tokens(raw, deployment)

        parsed = _safe_parse_json(raw)
        if not parsed:
            parsed = {"files": [], "joins": [], "confidence": 0,
                      "reasoning": "parse error", "_raw": raw[:300]}
        parsed["_p_tok"] = p_tok
        parsed["_c_tok"] = c_tok
        parsed["_duration"] = duration
        parsed["_deployment"] = deployment
        return parsed

    cache_key = _hash_key(
        "select_relevant_files",
        query,
        sorted((f["blob_path"], (f.get("ai_description") or "")[:80]) for f in file_summaries),
    )
    cached = _cache_get(cache_key)
    if cached:
        chat_logger.info("llm_call", function="select_relevant_files", status="cache_hit",
                         selected_files=cached.get("files", []),
                         confidence=cached.get("confidence", 0))
        llm_logger.info("llm_call", function="select_relevant_files", status="cache_hit",
                        selected_files=cached.get("files", []))
        return cached

    start = time.perf_counter()
    chat_logger.info("llm_call", function="select_relevant_files",
                     status="started",
                     query=query[:150],
                     candidates=[f["blob_path"] for f in file_summaries])
    result = await asyncio.to_thread(_run)
    if result.get("_raw"):
        chat_logger.warning("llm_call", function="select_relevant_files",
                            issue="json_parse_failed", raw_preview=result["_raw"])
    _track_and_log(
        function="select_relevant_files",
        model=result.pop("_deployment"),
        prompt_tokens=result.pop("_p_tok"),
        completion_tokens=result.pop("_c_tok"),
        duration_ms=result.pop("_duration"),
        extra={
            "query": query[:150],
            "selected_files": result.get("files", []),
            "confidence": result.get("confidence", 0),
            "reasoning": result.get("reasoning", "")[:200],
        },
    )
    chat_logger.info("llm_call", function="select_relevant_files",
                     status="done",
                     selected_files=result.get("files", []),
                     confidence=result.get("confidence", 0),
                     reasoning=result.get("reasoning", ""),
                     duration_ms=_ms(start))
    result.pop("_raw", None)
    _cache_set(cache_key, result)
    return result


async def generate_sql(
    query: str,
    relevant_files: list,
    joins: list,
    container_name: str,
    error_feedback: str | None = None,
) -> str:
    # Cache fresh SQL (not retries — error_feedback means the SQL was wrong)
    sql_cache_key = None
    if not error_feedback:
        sql_cache_key = _hash_key(
            "generate_sql",
            query,
            sorted(f["blob_path"] for f in relevant_files),
            container_name,
        )
        cached_sql = _cache_get(sql_cache_key)
        if cached_sql:
            chat_logger.info("llm_call", function="generate_sql", status="cache_hit",
                             sql=cached_sql["sql"][:200])
            llm_logger.info("llm_call", function="generate_sql", status="cache_hit",
                            sql_preview=cached_sql["sql"][:200])
            return cached_sql["sql"]

    def _run() -> tuple[str, int, int, str, float]:
        client, deployment = _get_client()
        schema_text = "\n\n".join(
            [
                f"File: az://{container_name}/{f['blob_path']}\n"
                f"Columns: {json.dumps([{'name': c['name'], 'type': c['type'], 'all_values' if len(c.get('unique_values', [])) <= 20 else 'sample_values': c.get('unique_values', c['sample_values'])} for c in f['columns_info']])}"
                for f in relevant_files
            ]
        )
        joins_text = (
            "\n".join(
                [
                    f"JOIN az://{container_name}/{j['file_b']} ON {j['on_column']} ({j['join_type']})"
                    for j in joins
                ]
            )
            or "No joins needed"
        )
        error_section = (
            f"\nPrevious SQL failed: {error_feedback}\nFix it using actual schema above."
            if error_feedback else ""
        )
        prompt = f"""You are a DuckDB SQL expert. Answer: "{query}"

Schemas:
{schema_text}

Joins:
{joins_text}
{error_section}

Rules:
- Use read_csv_auto('az://...') for each file
- Use TRY_CAST for date columns to handle format issues
- LIMIT 1000 rows
- Return ONLY the SQL query. No markdown. No backticks. No explanation."""

        prompt_tokens = _count_tokens(prompt, deployment)
        t = time.perf_counter()
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0,
        )
        duration = _ms(t)
        sql = response.choices[0].message.content.strip()

        api = response.usage
        p_tok = api.prompt_tokens if api else prompt_tokens
        c_tok = api.completion_tokens if api else _count_tokens(sql, deployment)

        if sql.startswith("```"):
            lines = sql.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            sql = "\n".join(lines).strip()
        return sql, p_tok, c_tok, deployment, duration

    start = time.perf_counter()
    chat_logger.info("llm_call", function="generate_sql", status="started",
                     query=query[:150],
                     files=[f["blob_path"] for f in relevant_files],
                     is_retry=error_feedback is not None)
    sql, p_tok, c_tok, deployment, llm_duration = await asyncio.to_thread(_run)
    _track_and_log(
        function="generate_sql",
        model=deployment,
        prompt_tokens=p_tok,
        completion_tokens=c_tok,
        duration_ms=llm_duration,
        extra={
            "query": query[:150],
            "files": [f["blob_path"] for f in relevant_files],
            "is_retry": error_feedback is not None,
            "sql_preview": sql[:300],
        },
    )
    chat_logger.info("llm_call", function="generate_sql", status="done",
                     sql=sql[:500], duration_ms=_ms(start))
    if sql_cache_key:
        _cache_set(sql_cache_key, {"sql": sql})
    return sql


async def format_response(
    query: str, sql_result: list, selected_files: list
) -> dict:
    def _run() -> dict:
        client, deployment = _get_client()
        if not sql_result:
            return {"answer": "No data found matching your query.", "chart_type": None,
                    "_p_tok": 0, "_c_tok": 0, "_duration": 0.0, "_deployment": deployment}

        prompt = f"""User asked: "{query}"
Query returned {len(sql_result)} rows. First 10:
{json.dumps(sql_result[:10], default=str)}

Return ONLY this JSON:
{{
  "answer": "concise natural language summary of key findings",
  "chart_type": "bar|line|pie|table|none",
  "chart_title": "title if chart",
  "x_column": "column for x axis",
  "y_column": "column for y axis"
}}
Return ONLY valid JSON."""

        prompt_tokens = _count_tokens(prompt, deployment)
        t = time.perf_counter()
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.2,
        )
        duration = _ms(t)
        raw = response.choices[0].message.content

        api = response.usage
        p_tok = api.prompt_tokens if api else prompt_tokens
        c_tok = api.completion_tokens if api else _count_tokens(raw, deployment)

        parsed = _safe_parse_json(raw)
        if not parsed:
            parsed = {"answer": "Query completed.", "chart_type": "table"}
        parsed["_p_tok"] = p_tok
        parsed["_c_tok"] = c_tok
        parsed["_duration"] = duration
        parsed["_deployment"] = deployment
        return parsed

    start = time.perf_counter()
    chat_logger.info("llm_call", function="format_response", status="started",
                     row_count=len(sql_result))
    result = await asyncio.to_thread(_run)
    _track_and_log(
        function="format_response",
        model=result.pop("_deployment"),
        prompt_tokens=result.pop("_p_tok"),
        completion_tokens=result.pop("_c_tok"),
        duration_ms=result.pop("_duration"),
        extra={
            "row_count": len(sql_result),
            "chart_type": result.get("chart_type"),
            "answer_preview": result.get("answer", "")[:150],
        },
    )
    chat_logger.info("llm_call", function="format_response", status="done",
                     answer=result.get("answer", "")[:200],
                     chart_type=result.get("chart_type"),
                     duration_ms=_ms(start))
    return result


async def suggest_rephrase(query: str, relevant_files: list) -> str:
    def _run() -> tuple[str, int, int, str, float]:
        client, deployment = _get_client()
        all_cols = [c["name"] for f in relevant_files for c in f["columns_info"]]
        prompt = f"""A user asked "{query}" but the query failed multiple times.
Available columns: {all_cols}
Suggest a clearer way to ask this question in one sentence.
Return only the suggested question."""
        prompt_tokens = _count_tokens(prompt, deployment)
        t = time.perf_counter()
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.3,
        )
        duration = _ms(t)
        text = response.choices[0].message.content.strip()
        api = response.usage
        p_tok = api.prompt_tokens if api else prompt_tokens
        c_tok = api.completion_tokens if api else _count_tokens(text, deployment)
        return text, p_tok, c_tok, deployment, duration

    text, p_tok, c_tok, deployment, llm_duration = await asyncio.to_thread(_run)
    _track_and_log(
        function="suggest_rephrase",
        model=deployment,
        prompt_tokens=p_tok,
        completion_tokens=c_tok,
        duration_ms=llm_duration,
        extra={"query": query[:150], "suggestion": text[:200]},
    )
    chat_logger.info("llm_call", function="suggest_rephrase", status="done",
                     suggestion=text[:200])
    return text
