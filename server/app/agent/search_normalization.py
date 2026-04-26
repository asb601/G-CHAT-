"""Shared normalization utilities for lightweight metadata search."""
from __future__ import annotations

import re
from functools import lru_cache

import stopwordsiso as stopwords


_TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:[&'][a-z0-9]+)*", re.IGNORECASE)
_QUERY_INTENT_STOPWORDS = {
    "show",
    "given",
}


@lru_cache(maxsize=1)
def _english_stopwords() -> frozenset[str]:
    library_words = {word.casefold() for word in stopwords.stopwords("en")}
    return frozenset(library_words | _QUERY_INTENT_STOPWORDS)


def tokenize_search_query(text: str, *, min_length: int = 2) -> list[str]:
    """Tokenize and normalize user search text for lightweight metadata matching.

    Uses a library-backed English stopword set and preserves mixed punctuation
    entities like "AT&T" by normalizing them to "att" instead of splitting them
    into noisy tokens like "at" and "t".
    """
    stopword_set = _english_stopwords()
    tokens: list[str] = []

    for match in _TOKEN_PATTERN.findall(text.casefold()):
        normalized = re.sub(r"[^a-z0-9]+", "", match)
        if len(normalized) < min_length:
            continue
        if normalized in stopword_set:
            continue
        tokens.append(normalized)

    return tokens