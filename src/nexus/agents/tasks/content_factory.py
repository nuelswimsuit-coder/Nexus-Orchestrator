"""
telegram.content_factory — Autonomous AI Content Generation & Publishing.

Pipeline
--------
1. Pre-flight: CPU check (same threshold as auto_scrape).

2. Niche discovery: query telefix.db for the target group's niche/topic.
   Falls back to a configurable default topic if the DB has no niche data.

3. Text generation (Gemini 2.0 Flash):
   - Prompt: "Generate a viral, high-engagement Telegram post for a {niche}
     community. Include relevant emojis. Keep it under 280 characters."
   - The generated text is scanned for outbound links and financial CTAs.

4. Image generation (Imagen 4.0 via Gemini API):
   - Prompt derived from the niche and post text.
   - Saved as a temp PNG in the Mangement Ahu data directory.

5. HITL gate:
   - If the generated text contains an outbound link (http/https) OR a
     financial CTA keyword (buy, invest, profit, earn, ROI, etc.), the task
     returns with `requires_hitl=True` and stores a ContentPreview in Redis.
   - The master's dispatcher surfaces this as a HITL task.
   - If clean, the task proceeds directly to posting.

6. Posting (Telethon subprocess):
   - Calls _content_post_helper.py which imports the Mangement Ahu project
     and uses the manager's session to send the message + photo.

7. Status: writes to Redis nexus:content:status and appends to agent log.

Stealth Mode
------------
In stealth mode the dashboard hides all content previews, but the task
still runs and writes to the agent log. The log entry includes the full
generated text so the operator can review it in the terminal.

Redis keys
----------
nexus:content:status          — current task state (running/idle/etc.)
nexus:content:previews        — list of ContentPreview JSON objects
nexus:content:factory:active  — "1" while a content factory job is running
                                (read by ClusterStatus for monitor glow)
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import psutil
import structlog

from nexus.agents.task_registry import registry
from nexus.agents.tasks.auto_scrape import CPU_THRESHOLD, RESCHEDULE_DELAY_S, TELEFIX_DB

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

TELEFIX_PROJECT = r"C:\Users\Yarin\Desktop\Mangement Ahu"
SESSIONS_DIR    = os.path.join(TELEFIX_PROJECT, "sessions")

CONTENT_STATUS_KEY   = "nexus:content:status"
CONTENT_PREVIEWS_KEY = "nexus:content:previews"
CONTENT_ACTIVE_KEY   = "nexus:content:factory:active"
CONTENT_HASHES_KEY   = "nexus:content:hashes"   # duplicate detection
CONTENT_STATUS_TTL   = 3600
CONTENT_PREVIEWS_MAX = 20
CONTENT_HASHES_MAX   = 500   # remember last 500 posts per group
CONTENT_HASHES_TTL   = 86400 * 30  # 30 days

# Similarity threshold for duplicate detection (0–1, lower = stricter)
DUPLICATE_SIMILARITY_THRESHOLD = 0.85

# Financial CTA keywords that trigger the HITL gate
FINANCIAL_CTA_KEYWORDS = re.compile(
    r"\b(buy|invest|profit|earn|roi|revenue|income|money|cash|fund|trade|"
    r"crypto|bitcoin|token|nft|dividend|return|yield|bonus|promo|discount|"
    r"sale|offer|deal|free|win|prize|reward)\b",
    re.IGNORECASE,
)

# Outbound link pattern
OUTBOUND_LINK_RE = re.compile(r"https?://", re.IGNORECASE)

# Default niches per project_id prefix (extend as needed)
DEFAULT_NICHES: dict[str, str] = {
    "telefix":   "Telegram growth and community management",
    "crypto":    "cryptocurrency and DeFi trends",
    "fitness":   "fitness, health, and wellness",
    "fashion":   "fashion and lifestyle trends",
    "tech":      "technology and AI innovations",
    "default":   "trending topics and viral content",
}


# ── Redis helpers ──────────────────────────────────────────────────────────────

async def _write_status(redis: Any, status: str, detail: str = "") -> None:
    if redis is None:
        return
    payload = json.dumps({
        "status": status,
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    await redis.set(CONTENT_STATUS_KEY, payload, ex=CONTENT_STATUS_TTL)


async def _set_active(redis: Any, active: bool) -> None:
    """Set the content-factory-active flag (read by the monitor glow)."""
    if redis is None:
        return
    if active:
        await redis.set(CONTENT_ACTIVE_KEY, "1", ex=CONTENT_STATUS_TTL)
    else:
        await redis.delete(CONTENT_ACTIVE_KEY)


async def _store_preview(redis: Any, preview: dict[str, Any]) -> None:
    """Push a ContentPreview to the Redis list for the dashboard."""
    if redis is None:
        return
    await redis.lpush(CONTENT_PREVIEWS_KEY, json.dumps(preview))
    await redis.ltrim(CONTENT_PREVIEWS_KEY, 0, CONTENT_PREVIEWS_MAX - 1)
    await redis.expire(CONTENT_PREVIEWS_KEY, 86400)


# ── Niche discovery ────────────────────────────────────────────────────────────

async def _get_niche(project_id: str, target_group_id: str) -> str:
    """
    Look up the niche/topic for the target group from telefix.db.
    Falls back to DEFAULT_NICHES[project_id] or "trending topics".
    """
    if not os.path.exists(TELEFIX_DB):
        return DEFAULT_NICHES.get(project_id, DEFAULT_NICHES["default"])

    try:
        uri = f"file:{TELEFIX_DB.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")
            # Try to find the group title/niche from managed_groups or targets
            async with db.execute(
                "SELECT title FROM managed_groups WHERE group_id = ? LIMIT 1",
                (target_group_id,),
            ) as c:
                row = await c.fetchone()
                if row and row["title"]:
                    return row["title"]

            async with db.execute(
                "SELECT title FROM targets WHERE link LIKE ? LIMIT 1",
                (f"%{target_group_id}%",),
            ) as c:
                row = await c.fetchone()
                if row and row["title"]:
                    return row["title"]
    except Exception as exc:
        log.warning("content_factory_niche_lookup_error", error=str(exc))

    # Fall back to project_id prefix
    for prefix, niche in DEFAULT_NICHES.items():
        if project_id.lower().startswith(prefix):
            return niche
    return DEFAULT_NICHES["default"]


# ── HITL detection ─────────────────────────────────────────────────────────────

def _needs_hitl(text: str) -> tuple[bool, str]:
    """
    Return (True, reason) if the text requires human approval before posting.
    Triggers on: outbound links OR financial CTA keywords.
    """
    if OUTBOUND_LINK_RE.search(text):
        return True, "Post contains an outbound link"
    match = FINANCIAL_CTA_KEYWORDS.search(text)
    if match:
        return True, f"Post contains financial CTA keyword: '{match.group()}'"
    return False, ""


def _content_fingerprint(text: str) -> str:
    """
    Generate a short fingerprint of post text for duplicate detection.

    Uses a normalised word-set hash so minor variations (punctuation,
    extra spaces) don't bypass the check.
    """
    import hashlib
    # Normalise: lowercase, strip punctuation, sort words
    words = sorted(re.sub(r"[^\w\s]", "", text.lower()).split())
    normalised = " ".join(words[:30])  # first 30 words
    return hashlib.sha256(normalised.encode()).hexdigest()[:16]


def _jaccard_similarity(a: str, b: str) -> float:
    """Compute word-level Jaccard similarity between two texts (0–1)."""
    set_a = set(re.sub(r"[^\w\s]", "", a.lower()).split())
    set_b = set(re.sub(r"[^\w\s]", "", b.lower()).split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


async def _is_duplicate(
    redis: Any,
    target_group_id: str,
    post_text: str,
) -> tuple[bool, str]:
    """
    Check if the generated post is too similar to recent posts for this group.

    Uses a two-stage check:
    1. Exact fingerprint match (fast Redis SET lookup).
    2. Jaccard similarity against the last 20 posts (slower but catches paraphrases).

    Returns (is_duplicate, reason).
    """
    if redis is None:
        return False, ""

    fingerprint = _content_fingerprint(post_text)
    hash_key = f"{CONTENT_HASHES_KEY}:{target_group_id}"

    # Stage 1: exact fingerprint check
    existing = await redis.lrange(hash_key, 0, CONTENT_HASHES_MAX - 1)
    if fingerprint in existing:
        return True, f"Exact duplicate detected (fingerprint={fingerprint})"

    # Stage 2: Jaccard similarity against stored texts
    # We store "fingerprint|text_snippet" so we can do similarity checks
    for entry in existing[:20]:
        parts = entry.split("|", 1)
        if len(parts) == 2:
            stored_text = parts[1]
            sim = _jaccard_similarity(post_text, stored_text)
            if sim >= DUPLICATE_SIMILARITY_THRESHOLD:
                return True, f"Near-duplicate detected (similarity={sim:.0%})"

    return False, ""


async def _register_content(
    redis: Any,
    target_group_id: str,
    post_text: str,
) -> None:
    """Store the post fingerprint + snippet for future duplicate checks."""
    if redis is None:
        return
    fingerprint = _content_fingerprint(post_text)
    snippet = post_text[:100].replace("|", " ")  # pipe is our delimiter
    entry = f"{fingerprint}|{snippet}"
    hash_key = f"{CONTENT_HASHES_KEY}:{target_group_id}"
    await redis.lpush(hash_key, entry)
    await redis.ltrim(hash_key, 0, CONTENT_HASHES_MAX - 1)
    await redis.expire(hash_key, CONTENT_HASHES_TTL)


def _apply_group_voice(text: str, niche: str, target_group_id: str) -> str:
    """
    Style Consistency — apply a consistent voice/tone for each group.

    Each group gets a deterministic style based on its ID hash:
      - formal:   Professional, data-driven tone
      - casual:   Friendly, emoji-heavy
      - hype:     Energetic, FOMO-driven
      - minimal:  Clean, no emoji, short sentences

    This ensures each group maintains its own unique personality.
    """
    import hashlib
    style_idx = int(hashlib.md5(target_group_id.encode()).hexdigest(), 16) % 4
    styles = ["formal", "casual", "hype", "minimal"]
    style = styles[style_idx]

    if style == "formal":
        # Remove excessive emoji, keep professional
        text = re.sub(r"[🔥💥⚡🚀]+", "", text).strip()
        if not text.endswith("."):
            text += "."
    elif style == "casual":
        # Ensure at least one emoji
        if not re.search(r"[\U0001F300-\U0001FFFF]", text):
            text = "✨ " + text
    elif style == "hype":
        # Add energy markers
        if "!" not in text:
            text = text.rstrip(".") + "! 🔥"
    elif style == "minimal":
        # Strip all emoji
        text = re.sub(r"[\U0001F300-\U0001FFFF\U00002700-\U000027BF]+", "", text).strip()

    log.debug("content_style_applied", style=style, group=target_group_id)
    return text


# ── Subprocess: generate + post ────────────────────────────────────────────────

def _run_content_subprocess(
    niche: str,
    target_group_id: str,
    gemini_api_key: str,
    post_text: str,
    image_path: str | None,
) -> dict[str, Any]:
    """
    Run the Mangement Ahu posting logic in a subprocess.
    Returns {"success": bool, "message_id": int | None, "error": str | None}.
    """
    helper = os.path.join(os.path.dirname(__file__), "_content_post_helper.py")
    cmd = [
        sys.executable, helper,
        "--project",    TELEFIX_PROJECT,
        "--group",      target_group_id,
        "--text",       post_text,
        "--image",      image_path or "",
        "--api-key",    gemini_api_key,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=120, cwd=TELEFIX_PROJECT,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().splitlines()
            if lines:
                return json.loads(lines[-1])
        return {
            "success": False,
            "message_id": None,
            "error": result.stderr[-300:] or "subprocess failed",
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "message_id": None, "error": "post subprocess timed out"}
    except Exception as exc:
        return {"success": False, "message_id": None, "error": str(exc)}


# ── Gemini text generation (direct httpx call) ─────────────────────────────────

async def _generate_text(niche: str, api_key: str) -> str:
    """
    Call Gemini 2.0 Flash to generate a viral Telegram post for the niche.
    Falls back to a template if the API key is missing or the call fails.
    """
    if not api_key:
        log.warning("content_factory_no_gemini_key", using="template")
        return _template_post(niche)

    try:
        import httpx
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.0-flash:generateContent?key={api_key}"
        )
        prompt = (
            f"Generate a short, viral, high-engagement Telegram post for a "
            f"'{niche}' community. "
            "Use 2-4 relevant emojis. Keep it under 280 characters. "
            "Be engaging, conversational, and avoid financial promises or outbound links. "
            "Output ONLY the post text, nothing else."
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": 150, "temperature": 0.9},
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            text = (
                data["candidates"][0]["content"]["parts"][0]["text"]
                .strip()
                .strip('"')
            )
            return text[:280]
    except Exception as exc:
        log.error("content_factory_gemini_text_error", error=str(exc))
        return _template_post(niche)


def _template_post(niche: str) -> str:
    """Fallback post template when Gemini is unavailable."""
    templates = [
        f"🚀 Big things are happening in the world of {niche}! Stay tuned for updates. 💡",
        f"✨ Your daily dose of {niche} inspiration is here! What's your take? 👇",
        f"🔥 The {niche} community is growing fast. Join the conversation! 💬",
        f"💎 Quality content about {niche} — because you deserve the best. 🌟",
    ]
    import random
    return random.choice(templates)


# ── Gemini image generation ────────────────────────────────────────────────────

async def _generate_image(niche: str, post_text: str, api_key: str) -> str | None:
    """
    Call Imagen 4.0 to generate a visual for the post.
    Returns the local file path, or None on failure.
    """
    if not api_key:
        return None

    try:
        import httpx
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"imagen-4.0-generate-001:predict?key={api_key}"
        )
        prompt = (
            f"Professional, high-quality social media image for a {niche} community. "
            "Vibrant colors, modern aesthetic, no text overlay, 16:9 aspect ratio."
        )
        payload = {
            "instances": [{"prompt": prompt}],
            "parameters": {"sampleCount": 1},
        }
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            b64 = data["predictions"][0]["bytesBase64Encoded"]

        import base64
        out_dir = os.path.join(TELEFIX_PROJECT, "data")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"content_{uuid.uuid4().hex[:8]}.png")
        with open(out_path, "wb") as f:
            f.write(base64.b64decode(b64))
        return out_path

    except Exception as exc:
        log.warning("content_factory_imagen_error", error=str(exc))
        return None


# ── Main task handler ──────────────────────────────────────────────────────────

@registry.register("telegram.content_factory")
async def content_factory(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Autonomous AI content generation and publishing task.

    Parameters
    ----------
    project_id      : str  — project identifier (used for niche lookup)
    target_group_id : str  — Telegram group ID or username
    force           : bool — skip CPU check (default False)
    post_now        : bool — skip HITL gate even if CTA detected (default False)
    custom_text     : str  — use this text instead of generating (optional)
    gemini_api_key  : str  — override vault key (optional)

    Returns
    -------
    dict with keys: status, post_text, image_path, requires_hitl, hitl_reason,
                    message_id, duration_s, error
    """
    started_at = time.monotonic()
    project_id      = str(parameters.get("project_id", "telefix"))
    target_group_id = str(parameters.get("target_group_id", ""))
    force           = bool(parameters.get("force", False))
    post_now        = bool(parameters.get("post_now", False))
    custom_text     = str(parameters.get("custom_text", "")).strip()
    redis           = parameters.get("__redis__")
    secrets         = parameters.get("__secrets__", {})

    gemini_api_key = (
        parameters.get("gemini_api_key")
        or secrets.get("GEMINI_API_KEY")
        or os.getenv("GEMINI_API_KEY", "")
    )

    # ── Pre-flight ─────────────────────────────────────────────────────────────
    cpu_now = psutil.cpu_percent(interval=1.0)
    if cpu_now > CPU_THRESHOLD and not force:
        await _write_status(redis, "low_resources",
            f"CPU {cpu_now:.0f}% > {CPU_THRESHOLD:.0f}%")
        return {
            "status": "low_resources",
            "post_text": None,
            "image_path": None,
            "requires_hitl": False,
            "hitl_reason": "",
            "message_id": None,
            "duration_s": round(time.monotonic() - started_at, 2),
            "reschedule_in_s": RESCHEDULE_DELAY_S,
            "error": None,
        }

    if not target_group_id:
        return {
            "status": "failed",
            "post_text": None,
            "image_path": None,
            "requires_hitl": False,
            "hitl_reason": "",
            "message_id": None,
            "duration_s": round(time.monotonic() - started_at, 2),
            "error": "target_group_id is required",
        }

    await _set_active(redis, True)
    await _write_status(redis, "running", f"Generating content for group {target_group_id}")

    # ── Niche discovery ────────────────────────────────────────────────────────
    niche = await _get_niche(project_id, target_group_id)
    log.info("content_factory_niche", niche=niche, group=target_group_id)

    # ── Text generation ────────────────────────────────────────────────────────
    if custom_text:
        post_text = custom_text
    else:
        await _write_status(redis, "generating_text", f"Asking Gemini about '{niche}'")
        post_text = await _generate_text(niche, gemini_api_key)

    log.info("content_factory_text_generated", length=len(post_text))

    # ── Style consistency ──────────────────────────────────────────────────────
    post_text = _apply_group_voice(post_text, niche, target_group_id)

    # ── Duplicate detection ────────────────────────────────────────────────────
    if not custom_text:  # only check AI-generated content
        is_dup, dup_reason = await _is_duplicate(redis, target_group_id, post_text)
        if is_dup:
            log.warning(
                "content_factory_duplicate_detected",
                group=target_group_id,
                reason=dup_reason,
            )
            # Regenerate once with a different seed
            await _write_status(redis, "regenerating", f"Duplicate detected: {dup_reason}")
            post_text = await _generate_text(f"{niche} (fresh angle)", gemini_api_key)
            post_text = _apply_group_voice(post_text, niche, target_group_id)
            is_dup2, _ = await _is_duplicate(redis, target_group_id, post_text)
            if is_dup2:
                log.warning("content_factory_duplicate_on_retry", group=target_group_id)
                # Accept it — better to post near-duplicate than nothing

    # ── Image generation ───────────────────────────────────────────────────────
    await _write_status(redis, "generating_image", "Generating visual asset")
    image_path = await _generate_image(niche, post_text, gemini_api_key)

    # ── HITL detection ─────────────────────────────────────────────────────────
    requires_hitl, hitl_reason = _needs_hitl(post_text)

    preview_id = str(uuid.uuid4())
    preview = {
        "preview_id":      preview_id,
        "project_id":      project_id,
        "target_group_id": target_group_id,
        "niche":           niche,
        "post_text":       post_text,
        "image_path":      image_path,
        "requires_hitl":   requires_hitl,
        "hitl_reason":     hitl_reason,
        "status":          "pending_approval" if requires_hitl else "ready",
        "created_at":      datetime.now(timezone.utc).isoformat(),
    }
    await _store_preview(redis, preview)

    if requires_hitl and not post_now:
        await _write_status(redis, "awaiting_approval",
            f"HITL required: {hitl_reason}")
        await _set_active(redis, False)
        log.info("content_factory_hitl_required",
            reason=hitl_reason, preview_id=preview_id)
        return {
            "status": "awaiting_approval",
            "post_text": post_text,
            "image_path": image_path,
            "requires_hitl": True,
            "hitl_reason": hitl_reason,
            "preview_id": preview_id,
            "message_id": None,
            "duration_s": round(time.monotonic() - started_at, 2),
            "error": None,
        }

    # ── Post via subprocess ────────────────────────────────────────────────────
    await _write_status(redis, "posting", f"Publishing to {target_group_id}")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _run_content_subprocess,
        niche, target_group_id, gemini_api_key, post_text, image_path,
    )
    duration = round(time.monotonic() - started_at, 2)

    await _set_active(redis, False)

    if result["success"]:
        # Register content fingerprint to prevent future duplicates
        await _register_content(redis, target_group_id, post_text)
        await _write_status(redis, "completed",
            f"Posted to {target_group_id} in {duration:.0f}s")
        return {
            "status": "completed",
            "post_text": post_text,
            "image_path": image_path,
            "requires_hitl": False,
            "hitl_reason": "",
            "preview_id": preview_id,
            "message_id": result.get("message_id"),
            "duration_s": duration,
            "error": None,
        }
    else:
        await _write_status(redis, "failed", result.get("error", "unknown"))
        return {
            "status": "failed",
            "post_text": post_text,
            "image_path": image_path,
            "requires_hitl": False,
            "hitl_reason": "",
            "preview_id": preview_id,
            "message_id": None,
            "duration_s": duration,
            "error": result.get("error"),
        }
