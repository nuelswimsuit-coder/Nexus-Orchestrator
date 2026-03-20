"""
Evolution Engine — First-Birth Protocol & Autonomous Project Spawner.

Responsibilities
----------------
1. Scout   — Discovers profitable niches using AI analysis of market signals,
             Telegram group trends, and the existing telefix.db data.

2. Architect — Generates a complete project scaffold (Python bot, config,
               README, monetisation strategy) in C:\\Users\\Yarin\\Desktop\\Nexus-Projects\\.

3. Birth Gate — On the very first project, triggers a PROJECT_BIRTH_APPROVAL
                HITL event so the operator can review the proposal before
                deployment.  After approval, `first_project_approved` is set
                to True in Redis and any subsequent project with Scout
                confidence > 80 deploys automatically.

4. Incubator  — Writes project metadata to the Redis key
                `nexus:incubator:projects` (list of JSON objects) so the
                /incubator dashboard page can display live status.

Redis Keys
----------
nexus:incubator:projects   — JSON list of IncubatorProject objects (no TTL)
nexus:evolution:state      — Current engine state (TTL 10 min)
nexus:birth:approved       — Persistent flag: "true" once first birth approved

HITL callback data format
--------------------------
birth_approve:<project_id>
birth_reject:<project_id>

These are handled by scripts/start_telegram_bot.py and POST to
POST /api/evolution/birth-resolve.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import textwrap
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

BIRTH_APPROVED_KEY     = "nexus:birth:approved"
INCUBATOR_KEY          = "nexus:incubator:projects"
EVOLUTION_STATE_KEY    = "nexus:evolution:state"
EVOLUTION_STATE_TTL    = 600   # 10 min
AGENT_LOG_KEY          = "nexus:agent:log"
AGENT_LOG_MAX          = 200

# Auto-deploy threshold: Scout confidence above this → skip HITL after first approval
AUTO_DEPLOY_THRESHOLD  = 80

# ── Niche catalogue ────────────────────────────────────────────────────────────
# Each entry is a candidate niche the Scout can propose.
# In production this list would be dynamically extended from super_scrape data.

NICHE_CATALOGUE: list[dict[str, Any]] = [
    {
        "id": "crypto_signals",
        "name": "Crypto Signals & Alerts",
        "description": "Automated Telegram channel delivering AI-generated crypto trading signals",
        "monetisation": "Paid subscription tiers ($9/$29/$99/mo), affiliate exchange links",
        "target_audience": "Retail crypto traders seeking alpha",
        "estimated_roi_pct": 340,
        "confidence": 87,
        "keywords": ["crypto", "signals", "bitcoin", "altcoin", "trading"],
    },
    {
        "id": "forex_vip",
        "name": "Forex VIP Signals",
        "description": "Premium Forex signal channel with AI entry/exit points and risk management",
        "monetisation": "Monthly VIP membership ($19/$49/mo), broker referral CPA",
        "target_audience": "Forex retail traders, prop-firm students",
        "estimated_roi_pct": 280,
        "confidence": 82,
        "keywords": ["forex", "fx", "trading", "pips", "signals"],
    },
    {
        "id": "ai_tools_news",
        "name": "AI Tools & Prompts Daily",
        "description": "Curated daily digest of new AI tools, prompts, and productivity hacks",
        "monetisation": "Sponsored posts ($200–$800/post), affiliate SaaS links, digital products",
        "target_audience": "Entrepreneurs, developers, content creators",
        "estimated_roi_pct": 210,
        "confidence": 91,
        "keywords": ["ai", "chatgpt", "prompts", "tools", "productivity"],
    },
    {
        "id": "dropship_winning",
        "name": "Winning Dropship Products",
        "description": "Daily winning product alerts for Shopify/TikTok dropshippers",
        "monetisation": "Membership ($15/mo), course upsell, supplier affiliate",
        "target_audience": "E-commerce entrepreneurs, TikTok shop sellers",
        "estimated_roi_pct": 195,
        "confidence": 78,
        "keywords": ["dropshipping", "shopify", "tiktok", "ecommerce", "products"],
    },
    {
        "id": "onlyfans_growth",
        "name": "Creator Growth Automation",
        "description": "Telegram community + automation tools for content creator audience growth",
        "monetisation": "SaaS tool subscription ($29/mo), consulting, agency retainer",
        "target_audience": "Content creators on subscription platforms",
        "estimated_roi_pct": 420,
        "confidence": 85,
        "keywords": ["creator", "growth", "automation", "fans", "subscribers"],
    },
]


# ── Data models ────────────────────────────────────────────────────────────────

class ProjectStatus(str, Enum):
    SCOUTING    = "scouting"
    ARCHITECTING = "architecting"
    PENDING_BIRTH = "pending_birth"   # waiting for HITL approval
    DEPLOYING   = "deploying"
    LIVE        = "live"
    REJECTED    = "rejected"
    FAILED      = "failed"


@dataclass
class IncubatorProject:
    project_id: str
    name: str
    niche_id: str
    niche_description: str
    ai_logic: str
    file_path: str
    estimated_roi_pct: int
    confidence: int
    status: ProjectStatus
    created_at: str
    updated_at: str
    hitl_request_id: str = ""
    deployed_worker_id: str = ""
    rejection_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "IncubatorProject":
        d = dict(d)
        d["status"] = ProjectStatus(d["status"])
        return cls(**d)


# ── EvolutionEngine ────────────────────────────────────────────────────────────

class EvolutionEngine:
    """
    Autonomous project spawner — Scout → Architect → Birth Gate → Deploy.

    Parameters
    ----------
    redis       : redis.asyncio.Redis instance
    dispatcher  : nexus.master.dispatcher.Dispatcher (for task dispatch)
    notifier    : nexus.shared.notifications.service.NotificationService
    settings    : nexus.shared.config.Settings
    """

    def __init__(
        self,
        redis: Any,
        dispatcher: Any = None,
        notifier: Any = None,
    ) -> None:
        self._redis      = redis
        self._dispatcher = dispatcher
        self._notifier   = notifier
        self._running    = False

    # ── Public API ─────────────────────────────────────────────────────────────

    async def run_loop(self, interval_seconds: int = 1800) -> None:
        """Background loop — scouts for new niches every `interval_seconds`."""
        self._running = True
        log.info("evolution_engine_started", interval_s=interval_seconds)
        await self._log("info", "Evolution Engine started — First-Birth Protocol active", {})

        while self._running:
            try:
                await self._cycle()
            except Exception as exc:
                log.error("evolution_cycle_error", error=str(exc))
                await self._log("error", f"Evolution cycle error: {exc}", {})
            await asyncio.sleep(interval_seconds)

    async def run_once(self) -> IncubatorProject | None:
        """Run a single scout+architect cycle. Returns the spawned project or None."""
        return await self._cycle()

    def stop(self) -> None:
        self._running = False

    async def handle_birth_approval(
        self,
        project_id: str,
        approved: bool,
        reviewer_id: str = "operator",
        reason: str = "",
    ) -> None:
        """
        Called when the operator clicks APPROVE or REJECT on the birth proposal.

        On approval:
          - Sets nexus:birth:approved = "true" in Redis (persistent)
          - Updates project status → DEPLOYING
          - Dispatches a nexus.project.deploy task

        On rejection:
          - Updates project status → REJECTED
          - Triggers a new scout cycle after 30 s
        """
        projects = await self._load_projects()
        project  = next((p for p in projects if p.project_id == project_id), None)

        if project is None:
            log.warning("birth_approval_project_not_found", project_id=project_id)
            return

        now = datetime.now(timezone.utc).isoformat()

        if approved:
            log.info("birth_approved", project_id=project_id, reviewer=reviewer_id)
            await self._log("action", f"Birth APPROVED for '{project.name}' by {reviewer_id}", {
                "project_id": project_id,
            })

            # Persist the global approval flag
            await self._redis.set(BIRTH_APPROVED_KEY, "true")

            project.status     = ProjectStatus.DEPLOYING
            project.updated_at = now
            await self._save_projects(projects)

            # Dispatch the deploy task
            await self._deploy_project(project)

        else:
            log.info("birth_rejected", project_id=project_id, reviewer=reviewer_id, reason=reason)
            await self._log("warning", f"Birth REJECTED for '{project.name}': {reason}", {
                "project_id": project_id,
            })

            project.status           = ProjectStatus.REJECTED
            project.rejection_reason = reason or "Rejected by operator"
            project.updated_at       = now
            await self._save_projects(projects)

            # Re-scout after a short delay (pick a different niche)
            asyncio.create_task(self._delayed_regenerate(project_id, delay=30))

    # ── Internal cycle ─────────────────────────────────────────────────────────

    async def _cycle(self) -> IncubatorProject | None:
        await self._set_state("scouting")
        await self._log("decision", "── Evolution cycle: scouting for new niche ──", {})

        # Check if we already have a pending-birth project (don't spawn duplicates)
        projects = await self._load_projects()
        pending  = [p for p in projects if p.status == ProjectStatus.PENDING_BIRTH]
        if pending:
            await self._log("info", f"Skipping scout — {len(pending)} project(s) already pending birth", {})
            await self._set_state("idle")
            return None

        # Scout: pick the best niche
        niche = await self._scout()
        if niche is None:
            await self._log("info", "Scout found no viable niche this cycle", {})
            await self._set_state("idle")
            return None

        await self._log("info",
            f"Scout selected niche: '{niche['name']}' (confidence={niche['confidence']}%)",
            {"niche_id": niche["id"], "confidence": niche["confidence"]},
        )

        # Architect: generate project scaffold
        await self._set_state("architecting")
        project = await self._architect(niche)
        await self._log("action", f"Architect generated project: '{project.name}' at {project.file_path}", {
            "project_id": project.project_id,
        })

        # Persist to incubator
        projects.append(project)
        await self._save_projects(projects)

        # Birth gate
        first_approved = await self._is_first_approved()
        auto_deploy    = first_approved and niche["confidence"] > AUTO_DEPLOY_THRESHOLD

        if auto_deploy:
            await self._log("action",
                f"Auto-deploying '{project.name}' — confidence {niche['confidence']}% > {AUTO_DEPLOY_THRESHOLD}% threshold",
                {"project_id": project.project_id},
            )
            project.status     = ProjectStatus.DEPLOYING
            project.updated_at = datetime.now(timezone.utc).isoformat()
            await self._save_projects(projects)
            await self._deploy_project(project)
        else:
            # Trigger HITL — operator must approve the first birth
            await self._log("decision",
                f"First-Birth Gate: sending PROJECT_BIRTH_APPROVAL for '{project.name}'",
                {"project_id": project.project_id},
            )
            project.status     = ProjectStatus.PENDING_BIRTH
            project.updated_at = datetime.now(timezone.utc).isoformat()
            await self._save_projects(projects)
            await self._send_birth_proposal(project)

        await self._set_state("idle")
        return project

    # ── Scout ──────────────────────────────────────────────────────────────────

    async def _scout(self) -> dict[str, Any] | None:
        """
        Select the highest-confidence niche not already in the incubator.

        In production this would query super_scraper results, Google Trends,
        and the Gemini API for real-time market intelligence.  For now it
        ranks the static catalogue and avoids already-used niches.
        """
        projects    = await self._load_projects()
        used_niches = {p.niche_id for p in projects if p.status != ProjectStatus.REJECTED}

        candidates = [
            n for n in NICHE_CATALOGUE
            if n["id"] not in used_niches
        ]

        if not candidates:
            return None

        # Sort by confidence descending
        candidates.sort(key=lambda n: n["confidence"], reverse=True)
        return candidates[0]

    # ── Architect ──────────────────────────────────────────────────────────────

    async def _architect(self, niche: dict[str, Any]) -> IncubatorProject:
        """
        Generate a complete project scaffold on disk and return the metadata.
        """
        from nexus.shared.config import settings

        project_id   = str(uuid.uuid4())[:8]
        project_name = _slugify(niche["name"])
        project_dir  = Path(settings.nexus_projects_dir) / project_name

        project_dir.mkdir(parents=True, exist_ok=True)

        # Write the main bot file
        bot_code = _generate_bot_code(niche, project_name)
        (project_dir / "bot.py").write_text(bot_code, encoding="utf-8")

        # Write config
        config_code = _generate_config(niche, project_name)
        (project_dir / "config.py").write_text(config_code, encoding="utf-8")

        # Write requirements
        (project_dir / "requirements.txt").write_text(
            "aiogram>=3.20\nstructlog\nhttpx\npython-dotenv\n",
            encoding="utf-8",
        )

        # Write README / monetisation plan
        readme = _generate_readme(niche, project_name)
        (project_dir / "README.md").write_text(readme, encoding="utf-8")

        # Write .env template
        env_template = _generate_env_template(niche)
        (project_dir / ".env.example").write_text(env_template, encoding="utf-8")

        now = datetime.now(timezone.utc).isoformat()
        return IncubatorProject(
            project_id       = project_id,
            name             = niche["name"],
            niche_id         = niche["id"],
            niche_description= niche["description"],
            ai_logic         = niche["monetisation"],
            file_path        = str(project_dir),
            estimated_roi_pct= niche["estimated_roi_pct"],
            confidence       = niche["confidence"],
            status           = ProjectStatus.ARCHITECTING,
            created_at       = now,
            updated_at       = now,
        )

    # ── Birth proposal (HITL) ──────────────────────────────────────────────────

    async def _send_birth_proposal(self, project: IncubatorProject) -> None:
        """Send the PROJECT_BIRTH_APPROVAL HITL notification via Telegram."""
        if self._notifier is None:
            log.warning("evolution_no_notifier", project_id=project.project_id)
            return

        request_id = str(uuid.uuid4())
        project.hitl_request_id = request_id
        projects = await self._load_projects()
        for i, p in enumerate(projects):
            if p.project_id == project.project_id:
                projects[i] = project
                break
        await self._save_projects(projects)

        # Store the pending birth request in Redis so the API can surface it
        birth_request = {
            "request_id": request_id,
            "project_id": project.project_id,
            "project_name": project.name,
            "niche_description": project.niche_description,
            "ai_logic": project.ai_logic,
            "file_path": project.file_path,
            "estimated_roi_pct": project.estimated_roi_pct,
            "confidence": project.confidence,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "type": "PROJECT_BIRTH_APPROVAL",
        }
        await self._redis.set(
            f"nexus:birth:pending:{request_id}",
            json.dumps(birth_request),
            ex=86400,  # 24 h TTL
        )

        # Deliver via Telegram provider
        for provider in getattr(self._notifier, "_providers", []):
            if hasattr(provider, "send_birth_proposal"):
                await provider.send_birth_proposal(
                    request_id       = request_id,
                    project_id       = project.project_id,
                    project_name     = project.name,
                    niche_description= project.niche_description,
                    ai_logic         = project.ai_logic,
                    file_path        = project.file_path,
                    estimated_roi_pct= project.estimated_roi_pct,
                )
                break

        log.info("birth_proposal_sent",
            project_id=project.project_id,
            request_id=request_id,
        )

    # ── Deploy ─────────────────────────────────────────────────────────────────

    async def _deploy_project(self, project: IncubatorProject) -> None:
        """Dispatch a nexus.project.deploy task to an available worker."""
        if self._dispatcher is None:
            log.warning("evolution_no_dispatcher", project_id=project.project_id)
            await self._log("warning", f"No dispatcher — cannot deploy '{project.name}'", {})
            return

        from nexus.shared.schemas import TaskPayload

        task = TaskPayload(
            task_type  = "nexus.project.deploy",
            parameters = {
                "project_id":  project.project_id,
                "project_name": project.name,
                "project_path": project.file_path,
                "niche_id":    project.niche_id,
            },
            project_id = project.project_id,
            priority   = 3,
        )

        try:
            job_id = await self._dispatcher.dispatch(task)
            project.deployed_worker_id = job_id or ""
            project.status     = ProjectStatus.LIVE
            project.updated_at = datetime.now(timezone.utc).isoformat()

            projects = await self._load_projects()
            for i, p in enumerate(projects):
                if p.project_id == project.project_id:
                    projects[i] = project
                    break
            await self._save_projects(projects)

            await self._log("action",
                f"Project '{project.name}' deployed → job_id={job_id}",
                {"project_id": project.project_id, "job_id": job_id},
            )
        except Exception as exc:
            log.error("evolution_deploy_error", error=str(exc), project_id=project.project_id)
            await self._log("error", f"Deploy failed for '{project.name}': {exc}", {})

    # ── Delayed regenerate ─────────────────────────────────────────────────────

    async def _delayed_regenerate(self, rejected_project_id: str, delay: int = 30) -> None:
        """Wait `delay` seconds then run a new scout cycle to replace a rejected project."""
        await asyncio.sleep(delay)
        await self._log("info", "Re-scouting after rejection — picking alternative niche", {})
        await self._cycle()

    # ── Redis helpers ──────────────────────────────────────────────────────────

    async def _is_first_approved(self) -> bool:
        """Check if the first-birth flag is set (Redis or config)."""
        from nexus.shared.config import settings
        if settings.first_project_approved:
            return True
        val = await self._redis.get(BIRTH_APPROVED_KEY)
        return val == "true"

    async def _load_projects(self) -> list[IncubatorProject]:
        raw = await self._redis.get(INCUBATOR_KEY)
        if not raw:
            return []
        try:
            items = json.loads(raw)
            return [IncubatorProject.from_dict(d) for d in items]
        except Exception as exc:
            log.error("incubator_load_error", error=str(exc))
            return []

    async def _save_projects(self, projects: list[IncubatorProject]) -> None:
        payload = json.dumps([p.to_dict() for p in projects])
        await self._redis.set(INCUBATOR_KEY, payload)

    async def _set_state(self, state: str) -> None:
        payload = json.dumps({
            "state": state,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        await self._redis.set(EVOLUTION_STATE_KEY, payload, ex=EVOLUTION_STATE_TTL)

    async def _log(self, level: str, message: str, metadata: dict[str, Any]) -> None:
        entry = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": f"[Evolution] {message}",
            "metadata": metadata,
        })
        log.debug("evolution_log", level=level, message=message)
        if self._redis is None:
            return
        await self._redis.lpush(AGENT_LOG_KEY, entry)
        await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)


# ── Code generation helpers ────────────────────────────────────────────────────

def _slugify(name: str) -> str:
    """Convert a niche name to a filesystem-safe slug."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


def _generate_bot_code(niche: dict[str, Any], project_name: str) -> str:
    return textwrap.dedent(f"""\
        \"\"\"
        {niche['name']} — Nexus Auto-Generated Bot
        Niche: {niche['description']}
        Monetisation: {niche['monetisation']}
        \"\"\"

        from __future__ import annotations
        import asyncio
        import os
        from aiogram import Bot, Dispatcher
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        from aiogram.filters import CommandStart
        from aiogram.types import Message
        from config import BOT_TOKEN, CHANNEL_ID

        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp  = Dispatcher()


        @dp.message(CommandStart())
        async def start(message: Message) -> None:
            await message.answer(
                f"Welcome to <b>{niche['name']}</b>!\\n\\n"
                f"{niche['description']}\\n\\n"
                "Use /subscribe to join the VIP channel."
            )


        async def main() -> None:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot)


        if __name__ == "__main__":
            asyncio.run(main())
    """)


def _generate_config(niche: dict[str, Any], project_name: str) -> str:
    return textwrap.dedent(f"""\
        \"\"\"Configuration for {niche['name']} bot.\"\"\"
        import os
        from dotenv import load_dotenv

        load_dotenv()

        BOT_TOKEN  = os.environ["BOT_TOKEN"]
        CHANNEL_ID = os.environ.get("CHANNEL_ID", "")
        NICHE      = "{niche['id']}"
        PROJECT    = "{project_name}"
    """)


def _generate_readme(niche: dict[str, Any], project_name: str) -> str:
    keywords = ", ".join(f"`{k}`" for k in niche.get("keywords", []))
    return textwrap.dedent(f"""\
        # {niche['name']}

        > Auto-generated by Nexus Evolution Engine

        ## Overview
        {niche['description']}

        ## Target Audience
        {niche['target_audience']}

        ## Monetisation Strategy
        {niche['monetisation']}

        ## Financial Projections
        | Metric | Value |
        |--------|-------|
        | Estimated ROI | {niche['estimated_roi_pct']}% |
        | Scout Confidence | {niche['confidence']}% |

        ## Keywords
        {keywords}

        ## Setup
        1. Copy `.env.example` to `.env` and fill in your tokens.
        2. `pip install -r requirements.txt`
        3. `python bot.py`

        ## Architecture
        Generated by Nexus Orchestrator Phase 14 — First-Birth Protocol.
    """)


def _generate_env_template(niche: dict[str, Any]) -> str:
    return textwrap.dedent(f"""\
        # {niche['name']} — Environment Variables
        BOT_TOKEN=your_telegram_bot_token_here
        CHANNEL_ID=@your_channel_username
        ADMIN_CHAT_ID=your_telegram_user_id
    """)
