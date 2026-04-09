# #region agent log
"""Append-only NDJSON for debug sessions (no secrets)."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

_DEBUG_LOG = Path(__file__).resolve().parent.parent / "debug-6bcb28.log"
_SESSION = "6bcb28"


def agent_log(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        payload = {
            "sessionId": _SESSION,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with _DEBUG_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


# #endregion
