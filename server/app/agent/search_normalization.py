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