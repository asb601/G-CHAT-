"""High-level LLM tasks used by backend services."""
from __future__ import annotations

import asyncio
import json
import time

from app.core.logger import ingest_logger
from app.core.openai_client import get_client
from app.core.token_counter import count_tokens, elapsed_ms, track_and_log


def safe_parse_json(text: str) -> dict:
    """Strip markdown fences and parse JSON. Returns fallback on failure."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [line for line in lines if not line.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        return {}


async def generate_file_description(columns_info: list, sample_rows: list, filename: str) -> dict:
    def _run() -> dict:
        client, deployment = get_client()
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

        prompt_tokens = count_tokens(prompt, deployment)

        t = time.perf_counter()
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0,
        )
        duration = elapsed_ms(t)
        raw = response.choices[0].message.content

        api = response.usage
        p_tok = api.prompt_tokens if api else prompt_tokens
        c_tok = api.completion_tokens if api else count_tokens(raw, deployment)

        parsed = safe_parse_json(raw)
        if not parsed.get("summary"):
            parsed = {
                "summary": filename,
                "good_for": [],
                "key_metrics": [],
                "key_dimensions": [],
                "date_range_start": None,
                "date_range_end": None,
            }
        parsed["_p_tok"] = p_tok
        parsed["_c_tok"] = c_tok
        parsed["_duration"] = duration
        parsed["_deployment"] = deployment
        return parsed

    ingest_logger.info(
        "llm_call",
        function="generate_file_description",
        status="started",
        filename=filename,
        column_count=len(columns_info),
    )
    result = await asyncio.to_thread(_run)
    track_and_log(
        function="generate_file_description",
        model=result.pop("_deployment"),
        prompt_tokens=result.pop("_p_tok"),
        completion_tokens=result.pop("_c_tok"),
        duration_ms=result.pop("_duration"),
        extra={"filename": filename, "summary": result.get("summary", "")[:120]},
    )
    ingest_logger.info(
        "llm_call",
        function="generate_file_description",
        status="done",
        filename=filename,
        summary=result.get("summary", "")[:150],
        good_for=result.get("good_for", []),
    )
    return result
