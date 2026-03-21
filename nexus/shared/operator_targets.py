"""
Operator watch-list → Strategy Brain swarm regex patterns.

Loads ``nexus/data/targets.json`` (shipped defaults for zero-touch setup).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_NEXUS_ROOT = Path(__file__).resolve().parents[1]
_TARGETS_PATH = _NEXUS_ROOT / "data" / "targets.json"


def _default_targets_document() -> dict[str, Any]:
    return {
        "targets": [
            {
                "category": "Crypto Regulation",
                "keywords": ["SEC", "ETF", "Gensler", "Ban", "Approval"],
                "priority": "high",
                "auto_trade": True,
            },
            {
                "category": "Whale Alerts",
                "keywords": [
                    "Whale",
                    "transferred to Binance",
                    "minted at Tether Treasury",
                    "dump",
                    "liquidation",
                ],
                "priority": "high",
                "auto_trade": True,
            },
            {
                "category": "Market Makers",
                "keywords": ["Elon Musk", "Fed rate", "Powell", "CPI"],
                "priority": "medium",
                "auto_trade": False,
            },
        ]
    }


def _ensure_targets_file() -> None:
    if _TARGETS_PATH.is_file():
        return
    _TARGETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    doc = _default_targets_document()
    _TARGETS_PATH.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")


def load_operator_targets_raw() -> list[dict[str, Any]]:
    """Return target rows with category, keywords, priority, auto_trade."""
    _ensure_targets_file()
    try:
        doc = json.loads(_TARGETS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        doc = _default_targets_document()
    rows = doc.get("targets")
    return list(rows) if isinstance(rows, list) else []


def keyword_to_swarm_pattern(keyword: str) -> str:
    """Build a case-insensitive regex that tolerates punctuation between words."""
    parts = [p for p in re.split(r"\s+", keyword.strip()) if p]
    if not parts:
        return ""
    escaped = [re.escape(p) for p in parts]
    if len(escaped) == 1:
        return escaped[0]
    sep = r"[\s\W_]*"
    return sep.join(escaped)


def load_operator_target_patterns() -> tuple[tuple[str, str], ...]:
    """
    Map JSON targets to ``(regex, label)`` tuples for :data:`SWARM_TRIGGER_PATTERNS`.
    """
    out: list[tuple[str, str]] = []
    for row in load_operator_targets_raw():
        category = str(row.get("category") or "Watch").strip() or "Watch"
        kws = row.get("keywords") or []
        if not isinstance(kws, list):
            continue
        for kw in kws:
            if not isinstance(kw, str):
                continue
            pat = keyword_to_swarm_pattern(kw)
            if not pat:
                continue
            label = f"{category}: {kw.strip()}"
            out.append((pat, label))
    return tuple(out)
