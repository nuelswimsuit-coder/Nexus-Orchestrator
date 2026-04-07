"""
Code-level anti-parrot / de-duplication for swarm LLM lines.

Uses stdlib string similarity (SequenceMatcher) — no extra dependencies.
"""

from __future__ import annotations

import difflib
import re

# Last N outgoing texts to compare against (per group).
RECENT_SENT_CAP = 15

# Reject candidate if similarity ratio to any stored line is strictly greater than this (0–1).
SIMILARITY_REJECT_RATIO = 0.85

# Initial generation + this many retries (e.g. 2 => 3 attempts total).
MAX_REGENERATION_RETRIES = 2


def normalize_for_similarity(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).casefold()


def string_similarity_ratio(a: str, b: str) -> float:
    na = normalize_for_similarity(a)
    nb = normalize_for_similarity(b)
    if not na and not nb:
        return 1.0
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    return float(difflib.SequenceMatcher(None, na, nb).ratio())


def is_too_similar_to_recent(candidate: str, recent: list[str], *, threshold: float | None = None) -> bool:
    """True if candidate should be rejected (exact match or ratio > threshold vs any recent line)."""
    thr = SIMILARITY_REJECT_RATIO if threshold is None else threshold
    c = (candidate or "").strip()
    if not c:
        return False
    for prev in recent:
        p = (prev or "").strip()
        if not p:
            continue
        if string_similarity_ratio(c, p) > thr:
            return True
    return False
