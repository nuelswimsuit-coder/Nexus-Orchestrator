"""
swarm.poll_generator — daily native Telegram poll from cached news + LLM (Hebrew).

After posting, publishes the new message id on Redis and enqueues ``swarm.poll.cast_vote``
jobs for 15–25 non-poster vault sessions, staggered over up to 3 hours via ARQ ``_defer_by``.
"""

from __future__ import annotations

import base64
import json
import os
import random
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import structlog

from nexus.modules.community_vibe import _gemini_json  # noqa: SLF001 — shared Gemini JSON helper
from nexus.services.recent_news_digest import get_tick_news_bundle_for_consumer
from nexus.worker.services.tg_session import async_telegram_client
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

SWARM_POLL_REDIS_CHANNEL = "nexus:swarm:native_poll"
SWARM_POLL_LATEST_KEY = "nexus:swarm:native_poll:latest"
_VOTE_SPREAD_S = 3 * 3600
_VOTE_JOB_EXPIRES_S = 4 * 3600
_MAX_QUESTION_LEN = 255
_MAX_OPTION_LEN = 100


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_sessions_dir(explicit: str | None) -> Path:
    if explicit and str(explicit).strip():
        return Path(str(explicit).strip()).expanduser().resolve()
    env = (os.getenv("VAULT_SESSIONS_DIR") or "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (_project_root() / "vault" / "sessions").resolve()


def _discover_session_bases(sessions_dir: Path) -> list[str]:
    if not sessions_dir.is_dir():
        return []
    files = sorted(sessions_dir.glob("*.session"), key=lambda p: p.as_posix().lower())
    return [str(p.with_suffix("").resolve()) for p in files]


def _norm_base(path: str) -> str:
    try:
        return Path(path).resolve().as_posix().lower()
    except OSError:
        return (path or "").strip().lower()


def _resolve_gemini_key(parameters: dict[str, Any]) -> str:
    sec = parameters.get("__secrets__", {})
    return (
        str(parameters.get("gemini_api_key", "")).strip()
        or str(sec.get("GEMINI_API_KEY", "") or "")
        or os.getenv("GEMINI_API_KEY", "")
    )


def _top_headline_for_poll(bundle: Any) -> str:
    for raw in (getattr(bundle, "digest_text", None) or "").splitlines():
        s = raw.strip()
        if s:
            return s[:800]
    ah = (getattr(bundle, "anchor_title", None) or "").strip()
    if ah:
        return ah[:800]
    return "חדשות היום בישראל"


def _parse_poll_spec(raw: dict[str, Any]) -> tuple[str, list[str]]:
    q = str(raw.get("question") or "").strip()
    opts_raw = raw.get("options")
    opts: list[str] = []
    if isinstance(opts_raw, list):
        for x in opts_raw:
            t = str(x).strip()[:_MAX_OPTION_LEN]
            if t:
                opts.append(t)
    opts = opts[:4]
    if len(opts) < 2:
        opts = ["כן", "לא", "תלוי"]
    q = q[:_MAX_QUESTION_LEN] if q else "מה דעתכם?"
    return q, opts[:4]


async def _llm_poll_json(api_key: str, headline: str) -> dict[str, Any]:
    sys_prompt = (
        "You output JSON only, no markdown. Keys: question (string), options (array of strings). "
        "The question must be in Hebrew: cynical, witty, engaging, suitable for a Telegram group poll — "
        "inspired by the news headline the user sends. "
        "options: 2 to 4 short Hebrew answers (max 4). No outlet names in the question."
    )
    user = json.dumps({"headline": headline}, ensure_ascii=False)
    return await _gemini_json(api_key, sys_prompt, user, temperature=0.88, max_tokens=384)


async def _enqueue_cast_vote(
    *,
    parameters: dict[str, Any],
    defer_by: timedelta,
) -> bool:
    try:
        import arq
        from arq.connections import RedisSettings

        from nexus.shared.config import settings
        from nexus.shared.schemas import TaskPayload

        task = TaskPayload(
            task_type="swarm.poll.cast_vote",
            parameters=parameters,
            project_id="swarm-poll",
            priority=3,
            job_expires_seconds=_VOTE_JOB_EXPIRES_S,
        )
        pool = await arq.create_pool(
            RedisSettings.from_dsn(settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        await pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=str(uuid.uuid4()),
            _queue_name="nexus:tasks",
            _defer_by=defer_by,
            _expires=_VOTE_JOB_EXPIRES_S,
        )
        await pool.aclose()
        return True
    except Exception as exc:
        log.error("poll_cast_vote_enqueue_failed", error=str(exc))
        return False


@registry.register("swarm.poll_generator")
async def poll_generator(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Post a native poll to ``chat`` using ``poster_session`` (vault base path without .suffix).

    Parameters
    ----------
    chat : str | int — username, t.me link, or numeric group id (as int or str).
    poster_session : str — session base path (optional if POLL_GENERATOR_POSTER_SESSION is set).
    sessions_dir : str — override vault sessions directory.
    """
    redis = parameters.get("__redis__")
    chat = parameters.get("chat")
    if chat is None:
        return {"status": "failed", "error": "missing_chat"}
    poster = (
        str(parameters.get("poster_session") or "").strip()
        or (os.getenv("POLL_GENERATOR_POSTER_SESSION") or "").strip()
    )
    if not poster:
        return {"status": "failed", "error": "missing_poster_session"}

    sessions_dir = _resolve_sessions_dir(str(parameters.get("sessions_dir") or "") or None)
    all_bases = _discover_session_bases(sessions_dir)
    poster_n = _norm_base(poster)
    dormant_pool = [b for b in all_bases if _norm_base(b) != poster_n]
    random.shuffle(dormant_pool)

    bundle = await get_tick_news_bundle_for_consumer(redis)
    headline = _top_headline_for_poll(bundle)

    api_key = _resolve_gemini_key(parameters)
    if not api_key:
        spec = {
            "question": f"על הכותרת: {headline[:120]}… — מה מרגיש לכם נכון?",
            "options": ["מוגזם", "לגיטימי", "לא מעניין אותי", "תלוי מי מדבר"],
        }
        log.warning("poll_generator_no_gemini_key_fallback")
    else:
        try:
            spec = await _llm_poll_json(api_key, headline)
        except Exception as exc:
            log.warning("poll_generator_llm_failed", error=str(exc))
            spec = {
                "question": "מה הכי מצחיק בחדשות היום?",
                "options": ["הפוליטיקאים", "הפרשנים", "הכל", "אני לא עוקב"],
            }

    question, option_texts = _parse_poll_spec(spec if isinstance(spec, dict) else {})

    from telethon.tl.types import InputMediaPoll, Poll, PollAnswer, TextPlain  # type: ignore[import-untyped]

    answers: list[Any] = []
    option_bytes: list[bytes] = []
    for i, text in enumerate(option_texts):
        b = bytes([i])
        option_bytes.append(b)
        answers.append(PollAnswer(text=TextPlain(text), option=b))

    poll_id = random.getrandbits(63) or 1
    media = InputMediaPoll(
        poll=Poll(
            id=poll_id,
            question=TextPlain(question),
            answers=answers,
            closed=False,
            public_voters=False,
            multiple_choice=False,
            quiz=False,
        ),
        correct_answers=None,
    )

    message_id: int | None = None
    async with async_telegram_client(poster, parameters) as client:
        entity = await client.get_entity(chat)
        sent = await client.send_message(entity, file=media)
        message_id = int(sent.id) if sent and getattr(sent, "id", None) else None

    if message_id is None:
        return {"status": "failed", "error": "send_failed", "headline_preview": headline[:160]}

    opt_b64_list = [base64.b64encode(b).decode("ascii") for b in option_bytes]
    pub_payload = {
        "schema": "nexus.swarm.native_poll.v1",
        "chat": chat,
        "message_id": message_id,
        "headline": headline[:500],
        "question": question,
        "option_b64": opt_b64_list,
        "poster_session": poster,
    }
    payload_json = json.dumps(pub_payload, ensure_ascii=False)
    if redis is not None:
        try:
            await redis.set(SWARM_POLL_LATEST_KEY, payload_json, ex=86400 * 2)
            await redis.publish(SWARM_POLL_REDIS_CHANNEL, payload_json)
        except Exception as exc:
            log.warning("poll_redis_publish_failed", error=str(exc))

    want_voters = random.randint(15, 25)
    n_voters = min(want_voters, len(dormant_pool))
    chosen = dormant_pool[:n_voters]
    enqueued = 0
    for base in chosen:
        idx = random.randrange(len(option_bytes))
        blob = option_bytes[idx]
        defer_s = random.randint(0, _VOTE_SPREAD_S)
        ok = await _enqueue_cast_vote(
            parameters={
                "session_base": base,
                "chat": chat,
                "message_id": message_id,
                "option_b64": base64.b64encode(blob).decode("ascii"),
            },
            defer_by=timedelta(seconds=defer_s),
        )
        if ok:
            enqueued += 1

    return {
        "status": "ok",
        "message_id": message_id,
        "headline_preview": headline[:200],
        "question": question,
        "options_n": len(option_texts),
        "dormant_candidates": len(dormant_pool),
        "vote_jobs_enqueued": enqueued,
        "redis_channel": SWARM_POLL_REDIS_CHANNEL,
    }


@registry.register("swarm.poll.cast_vote")
async def poll_cast_vote(parameters: dict[str, Any]) -> dict[str, Any]:
    """Cast one Telethon vote using ``functions.messages.SendVoteRequest``."""
    from telethon.tl.functions.messages import SendVoteRequest  # type: ignore[import-untyped]

    base = str(parameters.get("session_base") or "").strip()
    chat = parameters.get("chat")
    mid_raw = parameters.get("message_id")
    b64 = str(parameters.get("option_b64") or "").strip()
    if not base or chat is None or mid_raw is None or not b64:
        return {"status": "failed", "error": "missing_parameters"}
    try:
        msg_id = int(mid_raw)
    except (TypeError, ValueError):
        return {"status": "failed", "error": "bad_message_id"}
    try:
        opt = base64.b64decode(b64)
    except Exception:
        return {"status": "failed", "error": "bad_option_b64"}

    async with async_telegram_client(base, parameters) as client:
        peer = await client.get_input_entity(chat)
        await client(SendVoteRequest(peer=peer, msg_id=msg_id, options=[opt]))

    return {"status": "ok", "message_id": msg_id}
