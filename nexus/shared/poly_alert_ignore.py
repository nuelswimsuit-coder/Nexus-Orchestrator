"""
Stable fingerprints for Polymarket AI Telegram alerts — ignore list in Redis.

Used by the API (SADD/SMEMBERS) and ``start_telegram_bot`` (skip + callback).
"""

from __future__ import annotations

import hashlib
import re

# Redis SET of 16-char lowercase hex strings
POLY_AI_ALERT_IGNORE_REDIS_KEY = "nexus:poly_ai_alert:ignore"

_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{16}$")


def is_valid_ignore_fingerprint(s: str) -> bool:
    return bool(s and _FINGERPRINT_RE.match(s.strip().lower()))


def fingerprint_cx(
    *,
    yes_token: str,
    slug: str,
    market_id: str,
    signal: str,
) -> str:
    """Cross-exchange high-confidence alert — one row per market anchor + signal label."""
    anchor = (yes_token or "").strip() or (slug or "").strip() or (market_id or "").strip()
    sig = (signal or "").strip().upper()
    raw = f"cx|{anchor}|{sig}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def fingerprint_position(
    *,
    token_id: str,
    slug: str,
    outcome: str,
    action: str,
) -> str:
    """Portfolio-based AI rec — token (or slug+outcome) + BUY/SELL."""
    tid = (token_id or "").strip()
    anchor = tid or f"{(slug or '').strip()}|{(outcome or 'YES').strip().upper()}"
    act = (action or "").strip().upper()
    raw = f"pos|{anchor}|{act}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
