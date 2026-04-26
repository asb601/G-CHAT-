"""Catalog & schema tools — search files and inspect columns."""
from __future__ import annotations

import json
from langchain_core.tools import tool

from app.agent.search_normalization import (
    is_lookup_file,
    tokenize_search_query,
)
from app.core.logger import pipeline_logger
from app.retrieval.embeddings import build_search_text

# Hard cap on how many files search_catalog returns to the LLM.
_MAX_RESULTS = 15
# How many lookup / master / dimension files to surface unconditionally,
# even if they did not score on the user's query tokens.  Protects against
# vocabulary mismatches between the user's question (e.g. SAP "customer
# master") and the file's metadata (e.g. Oracle EBS "parties / party_name").
_LOOKUP_PAD_SLOTS = 5


def _match_score(query: str, file_entry: dict) -> tuple[int, list[str]]:
    query_tokens = tokenize_search_query(query)
    if not query_tokens:
        return 0, []
    

    search_text = build_search_text(file_entry).lower()
    matched_tokens = [token for token in query_tokens if token in search_text]
    score = len(matched_tokens)

    column_names = [c.get("name", "") for c in (file_entry.get("columns_info") or []) if isinstance(c, dict)]
    column_text = " ".join(column_names).lower()
    score += sum(2 for token in query_tokens if token in column_text)

    blob_path = (file_entry.get("blob_path") or "").lower()
    score += sum(1 for token in query_tokens if token in blob_path)

    return score, sorted(set(matched_tokens))


def build_catalog_tools(
    catalog: list[dict],
    parquet_paths: dict[str, str] | None = None,
    container_name: str = "",
) -> list:
    """Return search_catalog and get_file_schema tools bound to the catalog."""

    def _sql_path(blob_path: str) -> str:
        """Return the SQL-ready expression for a blob_path."""
        if parquet_paths and blob_path in parquet_paths:
            return f"read_parquet('az://{container_name}/{parquet_paths[blob_path]}')"
        if container_name and blob_path:
            return f"read_csv_auto('az://{container_name}/{blob_path}', sample_size=500, null_padding=true, ignore_errors=true)"
        return blob_path

    @tool
    def search_catalog(query: str) -> str:
        """Search the ingested file catalog to find files relevant to the user's question.
        Returns file paths, descriptions, columns, and what they are good for.
        Use when you need to discover which file to query or what columns are available.
        This searches file metadata only; it does not search actual row values inside the data."""
        if not catalog:
            return json.dumps({"error": "No files have been ingested yet."})

        def _entry(f: dict, score: int, matched_terms: list[str]) -> dict:
            return {
                "match_score": score,
                "matched_terms": matched_terms,
                "blob_path": f["blob_path"],
                "sql_path": _sql_path(f["blob_path"]),
                "description": f.get("ai_description", ""),
                "columns": [c["name"] for c in (f.get("columns_info") or [])],
                "key_metrics": f.get("key_metrics") or [],
                "key_dimensions": f.get("key_dimensions") or [],
                "good_for": f.get("good_for") or [],
                "date_range": f"{f.get('date_range_start')} → {f.get('date_range_end')}",
            }

        # Score every file once.  Keep all files (do NOT drop score==0) so
        # vocabulary-mismatched lookup tables remain reachable.  Sort by score
        # descending; lookup-files with score 0 are then promoted ahead of
        # other zero-score files via a stable secondary key.
        scored: list[tuple[int, bool, dict, list[str]]] = []
        for f in catalog:
            score, matched_terms = _match_score(query, f)
            scored.append((score, is_lookup_file(f), f, matched_terms))

        # Sort: higher score first; among equal scores, prefer lookup tables
        # (they are usually the discovery target when the user asks about an
        # entity by name).  Final tiebreaker = blob_path for determinism.
        scored.sort(
            key=lambda x: (-x[0], 0 if x[1] else 1, x[2]["blob_path"]),
        )

        matched = [_entry(f, s, mt) for (s, _is_lk, f, mt) in scored if s > 0]
        matched_blobs = {r["blob_path"] for r in matched}

        # Always surface up to _LOOKUP_PAD_SLOTS lookup-style files that the
        # token-match step missed.  Generic, query-agnostic — handles ERP
        # vocabulary gaps (customer ↔ party, account ↔ supplier, etc.).
        lookup_pad = []
        for s, is_lk, f, mt in scored:
            if not is_lk:
                continue
            if f["blob_path"] in matched_blobs:
                continue
            lookup_pad.append(_entry(f, s, mt))
            if len(lookup_pad) >= _LOOKUP_PAD_SLOTS:
                break

        results = matched + lookup_pad

        # Final fallback: nothing matched and no lookup files exist either —
        # fall back to a generic top-of-catalog slice so the agent at least
        # sees something.
        if not results:
            results = [_entry(f, 0, []) for f in catalog[: _MAX_RESULTS]]

        results = results[: _MAX_RESULTS]

        pipeline_logger.info(
            "search_catalog",
            query=query,
            files_found=len(results),
            matched_files=[r["blob_path"] for r in results],
            lookup_padded=[r["blob_path"] for r in lookup_pad],
            result_descriptions=[r.get("description", "")[:120] for r in results],
        )

        return json.dumps({"files": results, "total": len(results)}, default=str)

    @tool
    def get_file_schema(blob_path: str) -> str:
        """Get the full column schema, sample values, and data types for a specific file.
        Use this to understand exact column names and types before writing SQL."""
        # Exact match first
        match = next((f for f in catalog if f["blob_path"] == blob_path), None)

        # Fuzzy fallback: strip az://container/ prefix and extension, then match on stem
        if not match:
            q = blob_path.lower()
            # Strip az://container_name/ prefix if present
            q_stem = q
            if q_stem.startswith("az://"):
                q_stem = q_stem.split("/", 3)[-1]  # strip az://container/
            # Strip extension (.parquet, .csv, etc.)
            if "." in q_stem:
                q_stem = q_stem.rsplit(".", 1)[0]
            # Match against catalog blob_path stems (also strip extension)
            def _stem(bp: str) -> str:
                s = bp.lower()
                return s.rsplit(".", 1)[0] if "." in s else s

            match = next(
                (f for f in catalog if q_stem == _stem(f["blob_path"]) or q_stem in _stem(f["blob_path"])),
                None,
            )

        # Fuzzy fallback: try matching against description
        if not match:
            match = next(
                (f for f in catalog if q_stem in (f.get("ai_description") or "").lower()),
                None,
            )

        if not match:
            available = [f["blob_path"] for f in catalog[:15]]
            pipeline_logger.info(
                "get_file_schema",
                blob_path=blob_path,
                found=False,
                available_files=available,
            )
            return json.dumps({
                "error": f"File '{blob_path}' not found.",
                "available_files": available,
                "hint": "Use one of the blob_path values above, or call search_catalog to find the right file.",
            })

        cols = []
        for c in (match.get("columns_info") or []):
            cols.append({
                "name": c["name"],
                "type": c.get("type", "unknown"),
                "sample_values": c.get("sample_values", [])[:5],
                "unique_count": len(c.get("unique_values", [])),
            })

        pipeline_logger.info(
            "get_file_schema",
            blob_path=blob_path,
            resolved_blob_path=match["blob_path"],
            found=True,
            column_count=len(cols),
            columns=[c["name"] for c in cols],
            column_types={c["name"]: c["type"] for c in cols},
            sample_values={c["name"]: c["sample_values"] for c in cols},
        )

        return json.dumps({
            "blob_path": match["blob_path"],
            "sql_path": _sql_path(match["blob_path"]),
            "sql_hint": "Use the sql_path value directly in your SQL FROM clause.",
            "columns": cols,
            "key_metrics": match.get("key_metrics") or [],
            "key_dimensions": match.get("key_dimensions") or [],
            "date_range": {
                "start": match.get("date_range_start"),
                "end": match.get("date_range_end"),
            },
        }, default=str)

    return [search_catalog, get_file_schema]
