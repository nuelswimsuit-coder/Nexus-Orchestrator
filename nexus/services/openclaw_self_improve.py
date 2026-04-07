"""
OpenClaw self-improvement loop: subscribe to ``nexus:swarm:logs``, patch persona constraints,
and emit a verification event.

Enable on the API host with ``OPENCLAW_SELF_IMPROVE_ENABLED=1`` (see ``nexus.api.main`` lifespan),
or run: ``python -m nexus.services.openclaw_self_improve``
"""

from __future__ import annotations

import ast
import asyncio
import json
import re
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.cc_events import publish_cc_event
from nexus.shared.personas import PERSONA_ARCHETYPES, deterministic_archetype_index
from nexus.shared.swarm_logs_redis import (
    ISSUE_HALLUCINATION,
    ISSUE_OPENCLAW_VERIFY,
    ISSUE_PARROT_BUG,
    SWARM_LOGS_CHANNEL,
    publish_swarm_log_event,
)

log = structlog.get_logger(__name__)

_PERSONAS_PATH = Path(__file__).resolve().parents[1] / "shared" / "personas.py"
_JSON_ASSIGN_RE = re.compile(
    r"^(OPENCLAW_ARCHETYPE_EXTRA_JSON\s*=\s*)(.+?)\s*$",
    re.MULTILINE,
)


def _personas_path() -> Path:
    return _PERSONAS_PATH


def _derive_negative_constraint(issue: str, sample: str) -> str:
    low = (sample or "").lower()
    if "ynet" in low or "- ynet" in low:
        return "Never append '- Ynet', '-ynet', or other Ynet attributions to your lines."
    if any(t in low for t in ("n12", "כאן", "גלובס", "מעריב", "כלכליסט", "וואלה", "calcalist", "walla")):
        return "Do not end messages with Israeli news outlet names or dash attributions."
    if issue == ISSUE_PARROT_BUG:
        return "Do not parrot or near-copy recent group lines; use distinct wording every time."
    if issue == ISSUE_HALLUCINATION:
        return "Do not fabricate attributions or headline-style credits; stay in authentic short chat voice."
    return "Avoid repeating the bad pattern shown in recent logs; stay persona-true and non-repetitive."


def _load_extra_map_from_personas_text(text: str) -> dict[str, Any]:
    m = _JSON_ASSIGN_RE.search(text)
    if not m:
        return {}
    lit = m.group(2).strip()
    try:
        inner = ast.literal_eval(lit)
    except (ValueError, SyntaxError):
        return {}
    if not isinstance(inner, str):
        return {}
    try:
        data = json.loads(inner)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def patch_personas_openclaw_json(archetype_index: int, constraint: str) -> bool:
    """
    Append ``constraint`` to the JSON map on ``OPENCLAW_ARCHETYPE_EXTRA_JSON`` in personas.py.
    Returns True if the file was rewritten.
    """
    path = _personas_path()
    if not path.is_file():
        log.warning("openclaw_personas_missing", path=str(path))
        return False
    text = path.read_text(encoding="utf-8")
    m = _JSON_ASSIGN_RE.search(text)
    if not m:
        log.warning("openclaw_json_marker_missing", path=str(path))
        return False
    data = _load_extra_map_from_personas_text(text)
    key = str(int(archetype_index) % max(1, len(PERSONA_ARCHETYPES)))
    cur = data.get(key)
    lines: list[str]
    if isinstance(cur, list):
        lines = [str(x).strip() for x in cur if str(x).strip()]
    elif isinstance(cur, str) and cur.strip():
        lines = [cur.strip()]
    else:
        lines = []
    c = (constraint or "").strip()
    if c and not any(c.lower() in x.lower() or x.lower() in c.lower() for x in lines):
        lines.append(c)
    data[key] = lines
    new_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    new_line = f"OPENCLAW_ARCHETYPE_EXTRA_JSON = {repr(new_json)}"
    updated = text[: m.start()] + new_line + text[m.end() :]
    if updated == text:
        return False
    path.write_text(updated, encoding="utf-8")
    log.info("openclaw_personas_patched", archetype_index=int(key), constraints=len(lines))
    return True


async def _handle_swarm_log_payload(redis: Any, payload: dict[str, Any]) -> None:
    issue = str(payload.get("issue") or payload.get("type") or "").strip()
    if issue not in (ISSUE_PARROT_BUG, ISSUE_HALLUCINATION):
        return
    sample = str(payload.get("sample") or payload.get("message") or "")
    session_base = str(payload.get("session_base") or "")
    try:
        ai = int(payload.get("archetype_index"))
    except (TypeError, ValueError):
        ai = deterministic_archetype_index(session_base) if session_base else 0
    constraint = _derive_negative_constraint(issue, sample)
    if not patch_personas_openclaw_json(ai, constraint):
        log.debug("openclaw_skip_patch", issue=issue, archetype_index=ai)
    verify_msg = (
        f"[OpenClawVerify] archetype={ai} constraint={constraint[:120]} "
        f"— reload worker/API process to load updated nexus.shared.personas."
    )
    await publish_cc_event(
        redis,
        "openclaw_persona_verify",
        {
            "archetype_index": ai,
            "constraint": constraint,
            "issue": issue,
            "sample_preview": sample[:160],
        },
    )
    await publish_swarm_log_event(
        redis,
        {
            "issue": ISSUE_OPENCLAW_VERIFY,
            "message": verify_msg,
            "archetype_index": ai,
            "constraint": constraint,
        },
    )


async def run_openclaw_self_improve_loop(redis: Any) -> None:
    """Blocking subscribe loop until cancelled."""
    pubsub = redis.pubsub()
    await pubsub.subscribe(SWARM_LOGS_CHANNEL)
    log.info("openclaw_self_improve_subscribed", channel=SWARM_LOGS_CHANNEL)
    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=30.0)
            if not msg or msg.get("type") != "message":
                continue
            raw = msg.get("data", "")
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            try:
                await _handle_swarm_log_payload(redis, payload)
            except Exception as exc:
                log.warning("openclaw_self_improve_handler_error", error=str(exc))
    finally:
        try:
            await pubsub.unsubscribe(SWARM_LOGS_CHANNEL)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass


def main() -> None:
    import os

    import redis.asyncio as aioredis

    from nexus.shared.config import settings

    async def _run() -> None:
        url = os.getenv("OPENCLAW_SELF_IMPROVE_REDIS_URL", settings.redis_url)
        client = aioredis.from_url(url, decode_responses=True)
        try:
            await run_openclaw_self_improve_loop(client)
        finally:
            await client.aclose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
