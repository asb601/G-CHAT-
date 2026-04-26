"""Shared normalization utilities for lightweight metadata search."""
from __future__ import annotations

import re

_TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:[&'][a-z0-9]+)*", re.IGNORECASE)

# Standard English stopwords (inlined — no external dependency required).
# Covers common function words, prepositions, pronouns, auxiliary verbs, and
# query-intent words that add no discriminating signal in metadata search.
_STOPWORDS: frozenset[str] = frozenset({
    # articles / determiners
    "a", "an", "the",
    # conjunctions
    "and", "but", "or", "nor", "for", "yet", "so",
    # prepositions
    "at", "by", "in", "of", "on", "to", "up", "as", "into", "from",
    "with", "about", "above", "after", "before", "between", "during",
    "out", "through", "under", "over", "per",
    # pronouns
    "i", "me", "my", "we", "our", "you", "your", "he", "she", "it",
    "they", "them", "their", "this", "that", "these", "those",
    # auxiliary verbs
    "am", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall", "can",
    # common adverbs / quantifiers
    "all", "any", "both", "each", "few", "more", "most", "no", "not",
    "only", "other", "same", "some", "such", "too", "very", "just",
    "than", "then", "so",
    # question words
    "how", "what", "when", "where", "which", "who", "why",
    # query-intent verbs that carry no domain signal
    "show", "give", "get", "find", "list", "tell", "given", "fetch",
})


def tokenize_search_query(text: str, *, min_length: int = 2) -> list[str]:
    """Tokenize and normalize user search text for lightweight metadata matching.

    Uses an inlined English stopword set and preserves mixed punctuation
    entities like "AT&T" by normalizing them to "att" instead of splitting them
    into noisy tokens like "at" and "t".
    """
    tokens: list[str] = []

    for match in _TOKEN_PATTERN.findall(text.casefold()):
        normalized = re.sub(r"[^a-z0-9]+", "", match)
        if len(normalized) < min_length:
            continue
        if normalized in _STOPWORDS:
            continue
        tokens.append(normalized)

    return tokens