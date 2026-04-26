"""Shared normalization utilities for lightweight metadata search."""
from __future__ import annotations

import re

_TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:[&'][a-z0-9]+)*", re.IGNORECASE)

# Only words that survive the length filter (≥4 chars) but carry zero catalog
# signal — verbs/question-words used in question phrasing, never in metadata.
_QUERY_INTENT_WORDS: frozenset[str] = frozenset({
    "show", "give", "find", "list", "tell", "fetch", "display",
    "what", "when", "where", "which", "with", "from", "have",
    "does", "that", "this", "them", "they", "will", "been",
    "were", "much", "many", "also", "just", "more", "than",
})


def tokenize_search_query(text: str, *, min_length: int = 4) -> list[str]:
    """Tokenize and normalize a search query for lightweight metadata matching.

    Strategy:
    - Require tokens to be ≥ 4 characters.  The vast majority of English
      function words (at, in, of, by, to, a, an, is, the, for, and, but,
      not, all, any, ...) are ≤ 3 chars and drop out automatically.
    - Exception: punctuated compound entities (AT&T, O'Brien) whose
      normalized form is shorter than the raw match token are always kept
      regardless of length — e.g. "AT&T" → raw="at&t" → normalized="att".
    - Drop a small set of query-intent words that are ≥ 4 chars but never
      appear in catalog file descriptions or column names.
    """
    tokens: list[str] = []
    for match in _TOKEN_PATTERN.findall(text.casefold()):
        normalized = re.sub(r"[^a-z0-9]+", "", match)
        is_compound = len(normalized) < len(match)  # e.g. AT&T → att
        if not is_compound and len(normalized) < min_length:
            continue
        if normalized in _QUERY_INTENT_WORDS:
            continue
        tokens.append(normalized)
    return tokens


# ── Lookup / master / dimension file detection ────────────────────────────────
#
# Used by both the prompt shortlist construction (graph.py) and the
# search_catalog tool (tools/catalog.py) to make sure entity-name-bearing
# tables stay reachable to the agent even when the user's query vocabulary
# (e.g. SAP "customer / master / account") does not literally overlap the
# file's vocabulary (e.g. Oracle EBS "party / parties").  Pure structural
# heuristic — no query-specific or filename-specific knowledge.

LOOKUP_KEYWORDS: tuple[str, ...] = (
    "master", "masters", "parties", "party", "accounts", "account",
    "lookup", "directory", "reference", "dimension",
)

# Column-name suffixes that mark a column as an entity-name / label column —
# the kind of column you would resolve a literal user-supplied value against.
NAME_COLUMN_SUFFIXES: tuple[str, ...] = (
    "_name", "name", "_desc", "_description", "_label", "_title",
)


def is_lookup_file(entry: dict) -> bool:
    """Heuristic: does this catalog entry look like a master / lookup / dim table?

    Qualifies if ANY of:
      - blob_path contains a lookup keyword (master, parties, lookup, dim_, ...)
      - ai_description contains a lookup keyword
      - it has at least one column whose name ends in _NAME / _DESC / _LABEL
        (universal markers of an entity-name column)
    """
    blob = (entry.get("blob_path") or "").lower()
    if any(kw in blob for kw in LOOKUP_KEYWORDS):
        return True
    desc = (entry.get("ai_description") or "").lower()
    if any(kw in desc for kw in LOOKUP_KEYWORDS):
        return True
    for col in (entry.get("columns_info") or []):
        if not isinstance(col, dict):
            continue
        name = (col.get("name") or "").lower()
        if any(name.endswith(sfx) for sfx in NAME_COLUMN_SUFFIXES):
            return True
    return False