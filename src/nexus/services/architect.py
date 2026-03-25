"""
Architect Service — AI-Powered Project Generator.

The Architect is the "creative arm" of the Nexus Evolution Engine.  It
bridges to Gemini 1.5 Flash (Google Generative Language API v1) and generates
fully functional Nexus project scaffolds on demand.

What it does
------------
1. Receives a NicheCandidate (from the Scout) + optional custom brief.
2. Calls Gemini 1.5 Flash (v1 API) to generate:
     - main.py      — the project entry point (uses NexusWorker API)
     - config.yaml  — project configuration
     - requirements.txt — Python dependencies
3. Writes the files to:
       C:\\Users\\Yarin\\Desktop\\Nexus-Projects\\<project_slug>\\
4. Records the new project in Redis (nexus:incubator:projects list).
5. Logs the generation event to the agent log.

GOD MODE
--------
When god_mode=True (set via the Incubator dashboard toggle), the Architect
deploys projects without any human approval gate.  When god_mode=False,
it writes the project to disk but sets status="pending_review" and waits
for HITL approval before marking it "live".

Redis Keys
----------
nexus:incubator:projects  — JSON list of IncubatorProject dicts
nexus:incubator:god_mode  — "1" | "0"  (GOD MODE toggle)
nexus:incubator:state     — "idle" | "generating" | "deploying" | "error"
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
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
NEXUS_PROJECTS_ROOT = Path(r"C:\Users\Yarin\Desktop\Nexus-Projects")

# ── Redis keys ────────────────────────────────────────────────────────────────
INCUBATOR_PROJECTS_KEY = "nexus:incubator:projects"
INCUBATOR_GOD_MODE_KEY = "nexus:incubator:god_mode"
INCUBATOR_STATE_KEY    = "nexus:incubator:state"
INCUBATOR_STATE_TTL    = 600

# Backward-compat aliases used by feedback_loop.py and evolution.py
INCUBATOR_LIST_KEY         = INCUBATOR_PROJECTS_KEY
INCUBATOR_PROJECT_KEY      = "nexus:incubator:project"  # prefix for per-project keys
GRADUATION_USERS_THRESHOLD = 100   # users added before a project "graduates"
GRADUATION_DAYS            = 2     # days window for graduation check

# ── Gemini (Generative Language API v1 — not legacy google.generativeai v1beta) ─
GEMINI_API_VERSION = "v1"
GEMINI_MODEL = "gemini-1.5-flash"


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class IncubatorProject:
    """Metadata for a project born by the Architect."""
    project_id: str
    name: str
    slug: str
    niche: str
    niche_source: str
    generation: int                    # how many AI iterations this project has had
    status: str                        # "pending_review" | "live" | "paused" | "killed"
    path: str                          # absolute path on disk
    born_at: str
    last_updated: str
    confidence_at_birth: int           # niche confidence score when generated
    estimated_roi: str
    files_generated: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)   # filled by NexusWorker callbacks
    god_mode_deployed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def age_hours(self) -> float:
        try:
            born = datetime.fromisoformat(self.born_at)
            return (datetime.now(timezone.utc) - born).total_seconds() / 3600
        except Exception:
            return 0.0


# ── Gemini code generation ────────────────────────────────────────────────────

_MAIN_PY_PROMPT = """
You are an expert Python developer building a Telegram growth automation project.

Project details:
- Niche: {niche}
- Keywords: {keywords}
- Project slug: {slug}
- ROI estimate: {roi_estimate}

Generate a complete, production-ready main.py for a Nexus Worker project.
Requirements:
1. The script must import and use the NexusWorker API pattern:
   - Report stats back to master via HTTP POST to http://localhost:8000/api/business/agent-log
   - Use structlog for logging
   - Include a ResourceGuard check (CPU < 30%, RAM < 512MB) before heavy work
2. The main task should be relevant to the niche (e.g., content generation, user research, market monitoring)
3. Include a main() async function and if __name__ == "__main__": asyncio.run(main())
4. Keep it under 150 lines, clean, and well-commented
5. Use only standard library + httpx + structlog + asyncio

Return ONLY the Python code, no markdown fences.
"""

_CONFIG_YAML_PROMPT = """
Generate a config.yaml for a Nexus project with these details:
- Project name: {name}
- Niche: {niche}
- Keywords: {keywords}
- Slug: {slug}

Include sections: project, nexus_master, resource_limits, schedule, niche_config
Keep it concise and practical. Return ONLY the YAML, no markdown fences.
"""

_REQUIREMENTS_PROMPT = """
Generate a minimal requirements.txt for a Python Telegram automation project
focused on the "{niche}" niche.

Must include: httpx, structlog, pydantic
May include niche-relevant packages (e.g., for crypto: ccxt; for news: feedparser)
Keep it to 8 packages maximum. Return ONLY the requirements, one per line.
"""


def _sync_gemini_generate(prompt: str, api_key: str) -> str:
    """Blocking call to Generative Language API v1 (runs in thread pool)."""
    from google.ai.generativelanguage_v1 import GenerativeServiceClient  # type: ignore[import]
    from google.ai.generativelanguage_v1.types import (  # type: ignore[import]
        Content,
        GenerateContentRequest,
        GenerationConfig,
        Part,
    )
    from google.api_core import client_options as client_options_lib  # type: ignore[import]

    opts = client_options_lib.ClientOptions(api_key=api_key)
    client = GenerativeServiceClient(client_options=opts)
    model_id = GEMINI_MODEL if "/" in GEMINI_MODEL else f"models/{GEMINI_MODEL}"
    req = GenerateContentRequest(
        model=model_id,
        contents=[Content(role="user", parts=[Part(text=prompt)])],
        generation_config=GenerationConfig(temperature=0.7, max_output_tokens=2048),
    )
    resp = client.generate_content(request=req)
    if not resp.candidates:
        return ""
    parts_out: list[str] = []
    for c in resp.candidates:
        if not c.content or not c.content.parts:
            continue
        for p in c.content.parts:
            if p.text:
                parts_out.append(p.text)
    return "".join(parts_out).strip()


async def _call_gemini(prompt: str, api_key: str) -> str:
    """Call Gemini via Generative Language API v1 and return the text response."""
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: _sync_gemini_generate(prompt, api_key))
    except ImportError:
        log.warning(
            "architect_gemini_gapic_missing",
            hint="pip install google-ai-generativelanguage (or google-generativeai, which pulls it in)",
        )
        raise
    except Exception as exc:
        log.error("architect_gemini_error", api=GEMINI_API_VERSION, error=str(exc))
        raise


def _slug_from_name(name: str) -> str:
    """Convert a niche name to a filesystem-safe slug."""
    slug = re.sub(r"[^\w\s-]", "", name.lower())
    slug = re.sub(r"[\s_-]+", "-", slug).strip("-")
    return slug[:40]


def _fallback_main_py(slug: str, niche: str, keywords: list[str]) -> str:
    """Generate a safe fallback main.py if Gemini is unavailable."""
    kw_str = ", ".join(f'"{k}"' for k in keywords[:5])
    return textwrap.dedent(f'''\
        """
        Nexus Project: {slug}
        Niche: {niche}
        Auto-generated by the Nexus Architect.
        """
        from __future__ import annotations

        import asyncio
        import os
        from datetime import datetime, timezone

        import httpx
        import structlog

        log = structlog.get_logger(__name__)

        MASTER_API = os.environ.get("NEXUS_MASTER_API", "http://localhost:8000")
        PROJECT_ID = "{slug}"
        KEYWORDS   = [{kw_str}]


        async def report_stats(stats: dict) -> None:
            """Report project stats back to the Nexus master."""
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.post(
                        f"{{MASTER_API}}/api/business/agent-log",
                        json={{
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "level": "info",
                            "message": f"[{{PROJECT_ID}}] Stats update",
                            "metadata": {{"project_id": PROJECT_ID, **stats}},
                        }},
                    )
            except Exception as exc:
                log.warning("report_stats_failed", error=str(exc))


        async def run_niche_monitor() -> dict:
            """
            Core task: monitor the {niche} niche for opportunities.
            Replace this with niche-specific logic.
            """
            log.info("niche_monitor_running", project=PROJECT_ID, niche="{niche}")

            # TODO: implement niche-specific monitoring logic here
            # Example: fetch RSS feeds, monitor Telegram channels, track prices, etc.

            result = {{
                "project_id": PROJECT_ID,
                "niche": "{niche}",
                "keywords_tracked": KEYWORDS,
                "status": "monitoring",
                "cycle_at": datetime.now(timezone.utc).isoformat(),
            }}

            await report_stats(result)
            return result


        async def main() -> None:
            log.info("project_started", project=PROJECT_ID, niche="{niche}")
            while True:
                try:
                    result = await run_niche_monitor()
                    log.info("cycle_complete", result=result)
                except Exception as exc:
                    log.error("cycle_error", error=str(exc))
                await asyncio.sleep(3600)  # run every hour


        if __name__ == "__main__":
            asyncio.run(main())
    ''')


def _fallback_config_yaml(name: str, slug: str, niche: str, keywords: list[str]) -> str:
    kw_list = "\n".join(f"  - {k}" for k in keywords[:6])
    return textwrap.dedent(f"""\
        project:
          name: "{name}"
          slug: "{slug}"
          niche: "{niche}"
          version: "1.0.0"
          generated_by: "Nexus Architect"

        nexus_master:
          api_url: "http://localhost:8000"
          report_interval_seconds: 3600
          project_id: "{slug}"

        resource_limits:
          cpu_cap_percent: 20
          ram_cap_mb: 256
          max_concurrent_tasks: 2

        schedule:
          run_interval_hours: 1
          quiet_hours_start: 2
          quiet_hours_end: 7

        niche_config:
          target_niche: "{niche}"
          keywords:
{kw_list}
          confidence_threshold: 70
    """)


def _fallback_requirements(niche: str) -> str:
    base = ["httpx>=0.27.0", "structlog>=24.0.0", "pydantic>=2.0.0", "python-dotenv>=1.0.0"]
    niche_lower = niche.lower()
    if any(k in niche_lower for k in ["crypto", "bitcoin", "trading", "defi"]):
        base.append("ccxt>=4.0.0")
    if any(k in niche_lower for k in ["news", "rss", "feed"]):
        base.append("feedparser>=6.0.0")
    if any(k in niche_lower for k in ["ai", "chatgpt", "gpt"]):
        base.append("openai>=1.0.0")
    return "\n".join(base) + "\n"


# ── Project scaffolding ───────────────────────────────────────────────────────

async def _generate_project_files(
    project_dir: Path,
    name: str,
    slug: str,
    niche: str,
    keywords: list[str],
    roi_estimate: str,
    api_key: str,
) -> list[str]:
    """
    Generate main.py, config.yaml, requirements.txt using Gemini.
    Falls back to templates if Gemini fails.
    Returns list of generated file paths.
    """
    project_dir.mkdir(parents=True, exist_ok=True)
    generated: list[str] = []

    # ── main.py ───────────────────────────────────────────────────────────────
    main_py_path = project_dir / "main.py"
    try:
        if api_key:
            prompt = _MAIN_PY_PROMPT.format(
                niche=niche, keywords=", ".join(keywords),
                slug=slug, roi_estimate=roi_estimate,
            )
            code = await _call_gemini(prompt, api_key)
            # Strip markdown fences if Gemini added them
            code = re.sub(r"^```python\n?|^```\n?|```$", "", code, flags=re.MULTILINE).strip()
        else:
            code = _fallback_main_py(slug, niche, keywords)
    except Exception:
        code = _fallback_main_py(slug, niche, keywords)

    main_py_path.write_text(code, encoding="utf-8")
    generated.append(str(main_py_path))
    log.info("architect_file_written", file="main.py", project=slug)

    # ── config.yaml ───────────────────────────────────────────────────────────
    config_path = project_dir / "config.yaml"
    try:
        if api_key:
            prompt = _CONFIG_YAML_PROMPT.format(
                name=name, niche=niche, keywords=", ".join(keywords), slug=slug,
            )
            config_content = await _call_gemini(prompt, api_key)
            config_content = re.sub(r"^```yaml\n?|^```\n?|```$", "", config_content, flags=re.MULTILINE).strip()
        else:
            config_content = _fallback_config_yaml(name, slug, niche, keywords)
    except Exception:
        config_content = _fallback_config_yaml(name, slug, niche, keywords)

    config_path.write_text(config_content, encoding="utf-8")
    generated.append(str(config_path))
    log.info("architect_file_written", file="config.yaml", project=slug)

    # ── requirements.txt ──────────────────────────────────────────────────────
    req_path = project_dir / "requirements.txt"
    try:
        if api_key:
            prompt = _REQUIREMENTS_PROMPT.format(niche=niche)
            req_content = await _call_gemini(prompt, api_key)
            req_content = re.sub(r"^```.*\n?|```$", "", req_content, flags=re.MULTILINE).strip()
        else:
            req_content = _fallback_requirements(niche)
    except Exception:
        req_content = _fallback_requirements(niche)

    req_path.write_text(req_content, encoding="utf-8")
    generated.append(str(req_path))
    log.info("architect_file_written", file="requirements.txt", project=slug)

    # ── README.md ─────────────────────────────────────────────────────────────
    readme_path = project_dir / "README.md"
    readme_content = textwrap.dedent(f"""\
        # {name}

        > Auto-generated by the **Nexus Architect** on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

        ## Niche
        **{niche}**

        ## Keywords
        {', '.join(keywords)}

        ## ROI Estimate
        {roi_estimate}

        ## Structure
        - `main.py` — Entry point (runs the niche monitor loop)
        - `config.yaml` — Project configuration
        - `requirements.txt` — Python dependencies

        ## Running
        ```bash
        pip install -r requirements.txt
        python main.py
        ```

        ## Stats Reporting
        This project reports stats back to the Nexus Master API at:
        `http://localhost:8000/api/business/agent-log`
    """)
    readme_path.write_text(readme_content, encoding="utf-8")
    generated.append(str(readme_path))

    return generated


# ── ArchitectService ──────────────────────────────────────────────────────────

class ArchitectService:
    """
    AI-powered project generator.

    Usage
    -----
        architect = ArchitectService(redis)
        project = await architect.generate_project(niche_dict)
    """

    def __init__(
        self,
        redis: Any,
        dispatcher: Any = None,         # accepted for compat with master startup
        gemini_api_key: str = "",
    ) -> None:
        self._redis = redis
        self._dispatcher = dispatcher
        self._api_key = gemini_api_key or os.environ.get("GEMINI_API_KEY", "")

    async def is_god_mode(self) -> bool:
        """Check if GOD MODE is enabled."""
        val = await self._redis.get(INCUBATOR_GOD_MODE_KEY)
        return val == b"1" or val == "1"

    async def set_god_mode(self, enabled: bool) -> None:
        """Enable or disable GOD MODE."""
        await self._redis.set(INCUBATOR_GOD_MODE_KEY, "1" if enabled else "0")
        log.info("god_mode_changed", enabled=enabled)

    async def generate_project(
        self,
        niche: dict[str, Any],
        custom_brief: str = "",
        force_god_mode: bool = False,
    ) -> IncubatorProject:
        """
        Generate a new project from a niche candidate.

        Parameters
        ----------
        niche         — NicheCandidate dict (from Scout or manual input)
        custom_brief  — Optional extra instructions for the Architect
        force_god_mode — Override the Redis GOD MODE flag for this call

        Returns
        -------
        IncubatorProject with status="live" (god mode) or "pending_review"
        """
        await self._set_state("generating")

        niche_name   = niche.get("name", "Unknown Niche")
        keywords     = niche.get("keywords", [])
        roi_estimate = niche.get("roi_estimate", "Unknown")
        confidence   = niche.get("confidence", 50)
        niche_source = niche.get("source", "manual")

        project_id   = str(uuid.uuid4())[:8]
        slug         = f"{_slug_from_name(niche_name)}-{project_id}"
        name         = f"Nexus: {niche_name}"
        project_dir  = NEXUS_PROJECTS_ROOT / slug
        now_iso      = datetime.now(timezone.utc).isoformat()

        log.info("architect_generating", slug=slug, niche=niche_name)
        await self._log_agent(
            "action",
            f"[Architect] Generating project: {name}",
            {"slug": slug, "niche": niche_name, "confidence": confidence},
        )

        # Generate files
        try:
            files = await _generate_project_files(
                project_dir=project_dir,
                name=name,
                slug=slug,
                niche=niche_name,
                keywords=keywords,
                roi_estimate=roi_estimate,
                api_key=self._api_key,
            )
        except Exception as exc:
            log.error("architect_generation_failed", error=str(exc))
            await self._set_state("error")
            raise

        # Determine status based on GOD MODE
        god_mode_on = force_god_mode or await self.is_god_mode()
        status = "live" if god_mode_on else "pending_review"

        project = IncubatorProject(
            project_id=project_id,
            name=name,
            slug=slug,
            niche=niche_name,
            niche_source=niche_source,
            generation=1,
            status=status,
            path=str(project_dir),
            born_at=now_iso,
            last_updated=now_iso,
            confidence_at_birth=confidence,
            estimated_roi=roi_estimate,
            files_generated=files,
            god_mode_deployed=god_mode_on,
        )

        # Persist to Redis
        await self._save_project(project)
        await self._set_state("idle")

        await self._log_agent(
            "decision",
            f"[Architect] Project born: {name} → status={status} | path={project_dir}",
            {"project_id": project_id, "god_mode": god_mode_on, "files": len(files)},
        )

        log.info(
            "architect_project_born",
            slug=slug,
            status=status,
            god_mode=god_mode_on,
            path=str(project_dir),
        )
        return project

    async def get_all_projects(self) -> list[dict[str, Any]]:
        """Return all incubator projects from Redis."""
        raw = await self._redis.get(INCUBATOR_PROJECTS_KEY)
        if not raw:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    async def kill_project(self, project_id: str) -> bool:
        """Mark a project as killed (does not delete files)."""
        projects = await self.get_all_projects()
        updated = False
        for p in projects:
            if p.get("project_id") == project_id:
                p["status"] = "killed"
                p["last_updated"] = datetime.now(timezone.utc).isoformat()
                updated = True
        if updated:
            await self._redis.set(INCUBATOR_PROJECTS_KEY, json.dumps(projects))
        return updated

    async def build_project(self, opportunity: dict[str, Any]) -> str:
        """
        Build a project from a Scout opportunity dict.
        Alias for generate_project() — used by the master startup auto-start logic.
        Returns the project_id.
        """
        niche_dict = {
            "name": opportunity.get("niche", "Unknown"),
            "keywords": opportunity.get("keywords", []),
            "roi_estimate": opportunity.get("roi_estimate", "Unknown"),
            "confidence": opportunity.get("confidence", 50),
            "source": opportunity.get("source", "scout"),
        }
        project = await self.generate_project(niche_dict)
        return project.project_id

    async def approve_project(self, project_id: str) -> bool:
        """Approve a pending_review project, setting it to live."""
        projects = await self.get_all_projects()
        updated = False
        for p in projects:
            if p.get("project_id") == project_id and p.get("status") == "pending_review":
                p["status"] = "live"
                p["last_updated"] = datetime.now(timezone.utc).isoformat()
                updated = True
        if updated:
            await self._redis.set(INCUBATOR_PROJECTS_KEY, json.dumps(projects))
        return updated

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _save_project(self, project: IncubatorProject) -> None:
        """Append project to the Redis incubator list."""
        projects = await self.get_all_projects()
        # Update if exists, else append
        existing_ids = {p.get("project_id") for p in projects}
        if project.project_id in existing_ids:
            projects = [
                project.to_dict() if p.get("project_id") == project.project_id else p
                for p in projects
            ]
        else:
            projects.append(project.to_dict())
        await self._redis.set(INCUBATOR_PROJECTS_KEY, json.dumps(projects))

    async def _set_state(self, state: str) -> None:
        await self._redis.set(INCUBATOR_STATE_KEY, state, ex=INCUBATOR_STATE_TTL)

    async def _log_agent(self, level: str, message: str, metadata: dict) -> None:
        from nexus.services.decision_engine import AGENT_LOG_KEY, AGENT_LOG_MAX
        entry = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            "metadata": metadata,
        })
        await self._redis.lpush(AGENT_LOG_KEY, entry)
        await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)
