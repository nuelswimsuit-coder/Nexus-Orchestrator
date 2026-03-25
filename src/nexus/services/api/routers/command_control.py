"""
Command & Control suite — project visions, intelligence modules, finance,
infrastructure actions, and a WebSocket feed for live worker/task events.

REST prefix: ``/api/cc`` · WebSocket: ``/api/cc/ws``
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from nexus.services.api.dependencies import RedisDep
from nexus.shared.cc_events import CC_EVENTS_CHANNEL, publish_cc_event
from nexus.shared.cc_hub_store import get_project, list_projects, patch_project_metadata
from nexus.shared.kill_switch import PANIC_CHANNEL
from nexus.shared.swarm_signals import SWARM_KEYWORD_HASH, SWARM_SIGNAL_KEY
from nexus.agents.trading.poly_bot_state import POLY_BOT_PNL_KEY, POLY_BOT_STATUS_KEY

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/cc", tags=["command-control"])

HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"

ROI_REDIS_KEY = "nexus:cc:roi"
NUEL_SNAPSHOT_KEY = "nexus:cc:nuel:snapshot"
WALLET_BALANCE_KEY = "nexus:cc:crypto:wallet_snapshot"


# ── Schemas ────────────────────────────────────────────────────────────────────


class LeadScoreRequest(BaseModel):
    name: str = ""
    company: str = ""
    email: str = ""
    source: str = ""
    message_count: int = 0
    days_since_contact: int | None = None
    tags: list[str] = Field(default_factory=list)


class LeadScoreResponse(BaseModel):
    score: int
    tier: str
    factors: dict[str, Any]


class PredictReplyRequest(BaseModel):
    context: str = ""
    last_messages: list[str] = Field(default_factory=list)
    goal: str = "close_deal"  # close_deal | expose_scam | rapport


class DigitalShadowRequest(BaseModel):
    target_id: str
    messages: list[dict[str, str]] = Field(default_factory=list)


class RecoverWorkerRequest(BaseModel):
    node_id: str = Field(default="*", description="Worker NODE_ID, or '*' for all workers")
    mode: str = "restart_process"  # restart_process | signal_only


class PatchMetadataBody(BaseModel):
    metadata: dict[str, Any]


# ── Helpers ────────────────────────────────────────────────────────────────────


def _lead_score(req: LeadScoreRequest) -> LeadScoreResponse:
    score = 42
    factors: dict[str, Any] = {}

    src = (req.source or "").lower()
    if "linkedin" in src or "apollo" in src:
        score += 14
        factors["source_quality"] = "+14"
    elif "telegram" in src or "scrape" in src:
        score += 8
        factors["source_quality"] = "+8"

    if req.company.strip():
        score += 6
        factors["company_present"] = "+6"
    if "@" in req.email:
        score += 4
        factors["email_present"] = "+4"

    mc = max(0, req.message_count)
    if mc >= 5:
        score += 12
        factors["engagement"] = "+12"
    elif mc >= 2:
        score += 6
        factors["engagement"] = "+6"

    if req.days_since_contact is not None:
        if req.days_since_contact <= 3:
            score += 10
            factors["recency"] = "+10"
        elif req.days_since_contact <= 14:
            score += 4
            factors["recency"] = "+4"
        elif req.days_since_contact > 45:
            score -= 8
            factors["recency"] = "-8"

    low_tags = [t.lower() for t in req.tags]
    if any("hot" in t or "warm" in t for t in low_tags):
        score += 10
        factors["tags"] = "+10"
    if any("spam" in t or "cold" in t for t in low_tags):
        score -= 12
        factors["tags"] = "-12"

    score = max(1, min(100, score))
    tier = "A" if score >= 75 else "B" if score >= 50 else "C"
    return LeadScoreResponse(score=score, tier=tier, factors=factors)


def _shadow_from_messages(messages: list[dict[str, str]]) -> dict[str, Any]:
    texts = [m.get("text", "") for m in messages if m.get("text")]
    joined = " ".join(texts).lower()
    words = re.findall(r"[a-zא-ת]{4,}", joined)
    stop = frozenset(
        "that this with from have were been they will your what when "
        "about which their there would could".split()
    )
    top = [w for w, _ in Counter(w for w in words if w not in stop).most_common(8)]

    emoji_happy = joined.count("😊") + joined.count("🙏") + joined.count("❤")
    emoji_neg = joined.count("😠") + joined.count("💀")

    tone = "neutral"
    if emoji_happy > emoji_neg + 1:
        tone = "warm"
    elif emoji_neg > emoji_happy + 1:
        tone = "adversarial"

    return {
        "target_surface_traits": {
            "lexical_fingerprint": top,
            "estimated_tone": tone,
            "message_volume": len(texts),
        },
        "operational_notes": (
            "Heuristic shadow only — pair with live LLM + Telefix DB for production."
        ),
    }


def _predict_replies(req: PredictReplyRequest) -> dict[str, Any]:
    goal = (req.goal or "close_deal").lower()
    tail = (req.last_messages[-1] if req.last_messages else "")[:200]

    if goal == "expose_scam":
        replies = [
            "בקשתי רישיון/הוכחת פיקדון מפוקח — בלי זה אין המשך.",
            "נשמח לוודא את הישות המשפטית לפני כל העברה.",
            "שלחו כתובת חוזה ומספר רישום — נבדוק ונחזור.",
        ]
    elif goal == "rapport":
        replies = [
            "מבין לגמרי — רוצה ליישר קו קצר לפני שממשיכים.",
            "תודה על הסבלנות; נסגור את זה בשקט ובמקצועיות.",
            "אפשר לסכם במשפט אחד מה הכי דחוף לך עכשיו?",
        ]
    else:
        replies = [
            "מציעים לסגור היום עם תנאים שקופים — מוכנים לחתום אם מתאים.",
            "נעלה הצעה סופית בכתב תוך שעה; תאשר ונתקדם.",
            "אם המספרים עובדים לשני הצדדים, נפתח שלב ביצוע מיד.",
        ]

    return {
        "suggested_replies": replies,
        "goal": goal,
        "echo_last": tail,
        "disclaimer": "Operational language only — use within legal/ethical bounds.",
    }


# ── REST: projects / visions ───────────────────────────────────────────────────


@router.get("/projects", summary="List project visions + metadata")
async def cc_list_projects() -> dict[str, Any]:
    projects = await list_projects()
    return {"projects": projects, "count": len(projects)}


@router.get("/projects/{slug}", summary="Single project vision")
async def cc_get_project(slug: str) -> dict[str, Any]:
    row = await get_project(slug)
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown project '{slug}'")
    return row


@router.patch("/projects/{slug}/metadata", summary="Merge metadata blob")
async def cc_patch_metadata(slug: str, body: PatchMetadataBody) -> dict[str, Any]:
    meta = await patch_project_metadata(slug, body.metadata)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"Unknown project '{slug}'")
    return {"slug": slug, "metadata": meta}


# ── REST: lead scoring (Management Ahu) ────────────────────────────────────────


@router.post("/intelligence/lead-score", response_model=LeadScoreResponse)
async def cc_lead_score(body: LeadScoreRequest) -> LeadScoreResponse:
    return _lead_score(body)


# ── REST: NUEL e-com snapshot ──────────────────────────────────────────────────


@router.get("/ecom/nuel", summary="Shopify + ads snapshot (Redis/env backed)")
async def cc_nuel_ecom(redis: RedisDep) -> dict[str, Any]:
    raw = await redis.get(NUEL_SNAPSHOT_KEY)
    base: dict[str, Any]
    if raw:
        try:
            base = json.loads(raw)
        except json.JSONDecodeError:
            base = {}
    else:
        base = {}
    base.setdefault(
        "shopify",
        {
            "store": os.getenv("SHOPIFY_STORE_DOMAIN", ""),
            "orders_today": 0,
            "revenue_today_usd": 0.0,
        },
    )
    base.setdefault(
        "ads",
        {
            "platform": os.getenv("NUEL_ADS_PLATFORM", "meta"),
            "spend_today_usd": float(os.getenv("NUEL_AD_SPEND_TODAY", "0") or 0),
            "roas_hint": float(os.getenv("NUEL_AD_ROAS_HINT", "0") or 0),
        },
    )
    base.setdefault(
        "creative_pipeline",
        {
            "queued_assets": int(os.getenv("NUEL_CREATIVE_QUEUED", "0") or 0),
            "last_generated_at": base.get("creative_pipeline", {}).get("last_generated_at"),
        },
    )
    return {"project": "nuel", "snapshot": base, "ts": datetime.now(timezone.utc).isoformat()}


# ── REST: Heshbonator (Telegram intelligence) ──────────────────────────────────


@router.get("/heshbonator/sentiment-heatmap", summary="Swarm / keyword heat surface")
async def cc_sentiment_heatmap(redis: RedisDep, limit_groups: int = 24) -> dict[str, Any]:
    lines = await redis.lrange(SWARM_SIGNAL_KEY, 0, min(limit_groups, 99) - 1)
    cells: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        if not isinstance(line, str):
            line = str(line)
        cells.append({"id": f"g{i}", "label": line[:80], "intensity": min(1.0, 0.15 + i * 0.02)})

    kw_raw = await redis.hgetall(SWARM_KEYWORD_HASH)
    keywords: dict[str, int] = {}
    for k, v in (kw_raw or {}).items():
        try:
            keywords[str(k)] = int(v)
        except (TypeError, ValueError):
            keywords[str(k)] = 0

    return {
        "telegram_groups": cells,
        "keyword_counts": keywords,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/heshbonator/predict-reply", summary="Suggested reply lines (goal-aware)")
async def cc_predict_reply(body: PredictReplyRequest) -> dict[str, Any]:
    return _predict_replies(body)


@router.post("/heshbonator/digital-shadow", summary="Heuristic profile from chat samples")
async def cc_digital_shadow(body: DigitalShadowRequest) -> dict[str, Any]:
    return {
        "target_id": body.target_id,
        "digital_shadow": _shadow_from_messages(body.messages),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# ── REST: infrastructure ───────────────────────────────────────────────────────


@router.get("/infra/cluster-heatmap", summary="CPU/RAM snapshot for 3D HUD")
async def cc_cluster_heatmap(redis: RedisDep) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    cursor = 0
    pattern = f"{HEARTBEAT_KEY_PREFIX}*".encode()
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            raw = await redis.get(key)
            if raw is None:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            ram_total = float(data.get("ram_total_mb") or 0)
            ram_used = float(data.get("ram_used_mb") or 0)
            ram_pct = round((ram_used / ram_total) * 100, 1) if ram_total > 0 else 0.0
            nodes.append(
                {
                    "node_id": data.get("node_id", "unknown"),
                    "role": data.get("role", "worker"),
                    "cpu_percent": float(data.get("cpu_percent") or 0),
                    "ram_percent": ram_pct,
                    "online": True,
                }
            )
        if cursor == 0:
            break

    nodes.sort(key=lambda n: (n["role"] != "master", n["node_id"]))
    return {"nodes": nodes, "ts": datetime.now(timezone.utc).isoformat()}


@router.post("/sentinel/recover-worker", summary="Signal worker process restart via Redis")
async def cc_recover_worker(body: RecoverWorkerRequest, redis: RedisDep) -> dict[str, Any]:
    target = body.node_id.strip() or "*"
    msg = f"RESTART_WORKER:{target}"
    await redis.publish(PANIC_CHANNEL, msg)
    await publish_cc_event(
        redis,
        "sentinel_recover",
        {"node_id": target, "mode": body.mode},
    )
    ssh_hint = os.getenv("NEXUS_SSH_RECOVERY_CMD", "").strip()
    if ssh_hint and body.mode != "signal_only":
        log.info("cc_recover_worker_ssh_stub", cmd_configured=bool(ssh_hint))

    return {
        "status": "published",
        "channel": PANIC_CHANNEL,
        "message": msg,
        "note": "Workers must subscribe to nexus:system:control and exit for supervisor restart.",
    }


# ── REST: finance / crypto ─────────────────────────────────────────────────────


@router.get("/finance/roi", summary="Per-project PnL blobs from Redis hash")
async def cc_finance_roi(redis: RedisDep) -> dict[str, Any]:
    raw_map = await redis.hgetall(ROI_REDIS_KEY)
    projects: dict[str, Any] = {}
    for k, v in (raw_map or {}).items():
        key = str(k)
        try:
            projects[key] = json.loads(v) if isinstance(v, str) else json.loads(str(v))
        except json.JSONDecodeError:
            projects[key] = {"raw": v}
    if not projects:
        projects = {
            "nuel": {"pnl_usd": 0.0, "note": "seed with HSET nexus:cc:roi <slug> '{...}'"},
            "management_ahu": {"pnl_usd": 0.0},
            "default": {"pnl_usd": 0.0},
        }
    return {"projects": projects, "ts": datetime.now(timezone.utc).isoformat()}


@router.get("/finance/crypto-snapshot", summary="Polymarket bot + wallet stub for HUD")
async def cc_crypto_snapshot(redis: RedisDep) -> dict[str, Any]:
    pnl_raw = await redis.get(POLY_BOT_PNL_KEY)
    st_raw = await redis.get(POLY_BOT_STATUS_KEY)
    wallet_raw = await redis.get(WALLET_BALANCE_KEY)
    pnl: Any = None
    status: Any = None
    if pnl_raw:
        try:
            pnl = json.loads(pnl_raw)
        except json.JSONDecodeError:
            pnl = {"raw": pnl_raw}
    if st_raw:
        try:
            status = json.loads(st_raw)
        except json.JSONDecodeError:
            status = {"raw": st_raw}
    wallet: Any = None
    if wallet_raw:
        try:
            wallet = json.loads(wallet_raw)
        except json.JSONDecodeError:
            wallet = {"raw": wallet_raw}
    else:
        wallet = {
            "address_hint": (os.getenv("NEXUS_DISPLAY_WALLET") or "")[:20],
            "usdc_polygon": float(os.getenv("NEXUS_WALLET_USDC_HINT", "0") or 0),
        }

    return {
        "polymarket_bot": {"pnl": pnl, "session": status},
        "wallet": wallet,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# ── WebSocket ──────────────────────────────────────────────────────────────────


@router.websocket("/ws")
async def cc_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    redis = websocket.app.state.redis
    await websocket.send_json(
        {
            "type": "hello",
            "channel": CC_EVENTS_CHANNEL,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
    )

    pubsub = redis.pubsub()
    try:
        await pubsub.subscribe(CC_EVENTS_CHANNEL)
    except Exception as exc:
        log.warning("cc_ws_subscribe_failed", error=str(exc))
        await websocket.close(code=1011)
        return

    async def _pump_out() -> None:
        try:
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8", errors="replace")
                await websocket.send_text(data)
        except WebSocketDisconnect:
            raise
        except Exception as exc:
            log.debug("cc_ws_pump_error", error=str(exc))

    pump = asyncio.create_task(_pump_out())
    try:
        while True:
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
    finally:
        pump.cancel()
        try:
            await pump
        except asyncio.CancelledError:
            pass
        try:
            await pubsub.unsubscribe(CC_EVENTS_CHANNEL)
            await pubsub.close()
        except Exception:
            pass
