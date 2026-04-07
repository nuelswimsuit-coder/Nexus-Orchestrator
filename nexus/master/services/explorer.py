"""
Explorer Service — Desktop Project Catalog & Integration

Scans predefined project directories on the Master's desktop, extracts metadata
(language, status, configuration, live stats), and makes them available to
the Dashboard "Project Hub" and deployer services.

Monitored Projects
------------------
<Desktop>/OTP_Sessions_Creator
<Desktop>/1XPanel_API
<Desktop>/BudgetTracker
<Desktop>/CryptoSellsBot
<Desktop>/fix-express-labs-invoicing

The Desktop root is resolved dynamically via ``get_telefix_path()`` so no
username or OS-specific path is hardcoded here.  The same code runs on the
Windows Master and on Linux Workers.

For each project, the Explorer extracts:
  * Language/Stack: Python, Node.js, PHP, etc.
  * Status: Running/Stopped (by checking process names)
  * Config: .env file keys (passwords masked)
  * Live stats: project-specific metrics (e.g., budget balance, active sessions)

Redis Keys
----------
nexus:explorer:projects     -- JSON dict of all scanned projects
nexus:explorer:last_scan    -- ISO timestamp of last successful scan
nexus:explorer:scan_state   -- "idle" | "scanning" | "complete" | "error"
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import psutil
import structlog

from nexus.shared.memory_cache import TTLMemoryCache
from nexus.shared.paths import get_telefix_path

log = structlog.get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DESKTOP_BASE = get_telefix_path()

MONITORED_PROJECTS = [
    "OTP_Sessions_Creator",
    "1XPanel_API", 
    "BudgetTracker",
    "CryptoSellsBot",
    "fix-express-labs-invoicing",
]

# Redis keys
EXPLORER_PROJECTS_KEY = "nexus:explorer:projects"
EXPLORER_PROJECTS_MEMORY_KEY = "nexus:explorer:projects:mem"
EXPLORER_PROJECTS_MEMORY_TTL_S = 15.0
EXPLORER_LAST_SCAN_KEY = "nexus:explorer:last_scan"
EXPLORER_SCAN_STATE_KEY = "nexus:explorer:scan_state"
EXPLORER_TTL = 24 * 3600  # 24 hours

# Language detection patterns
LANGUAGE_PATTERNS = {
    "Python":     ["*.py", "requirements.txt", "pyproject.toml", "setup.py"],
    "Node.js":    ["package.json", "package-lock.json", "*.js", "*.ts", "yarn.lock"],
    "PHP":        ["*.php", "composer.json", "composer.lock"],
    "C#":         ["*.cs", "*.csproj", "*.sln"],
    "Java":       ["*.java", "pom.xml", "build.gradle"],
    "Go":         ["*.go", "go.mod", "go.sum"],
    "Rust":       ["*.rs", "Cargo.toml", "Cargo.lock"],
    "React":      ["src/", "public/", "package.json"],
    "Next.js":    ["next.config.js", "next.config.ts", "pages/", "app/"],
}

# Process name patterns for status detection
PROCESS_PATTERNS = {
    "python":     ["python.exe", "python3", "py.exe"],
    "node":       ["node.exe", "npm.exe", "yarn.exe", "pnpm.exe"],
    "php":        ["php.exe", "php"],
    "dotnet":     ["dotnet.exe", "dotnet"],
    "java":       ["java.exe", "javaw.exe"],
    "go":         ["go.exe"],
    "nginx":      ["nginx.exe"],
    "apache":     ["httpd.exe", "apache2"],
}


# ── Project data model ─────────────────────────────────────────────────────────

class ProjectInfo:
    """Metadata for a single desktop project."""

    def __init__(self, name: str, path: Path) -> None:
        self.name = name
        self.path = path
        self.exists = path.exists()
        self.language = "Unknown"
        self.stack: List[str] = []
        self.status = "Unknown"
        self.running_processes: List[str] = []
        self.config_keys: List[str] = []
        self.env_file = ""
        self.live_stats: Dict[str, Any] = {}
        self.last_modified = ""
        self.size_mb = 0.0

    def scan(self) -> None:
        """Perform a full metadata scan of this project."""
        if not self.exists:
            self.status = "Not Found"
            return

        try:
            # Basic filesystem info
            self.size_mb = self._calculate_size()
            self.last_modified = self._get_last_modified()

            # Language/stack detection
            self.language, self.stack = self._detect_language()

            # Process status
            self.status, self.running_processes = self._check_status()

            # Configuration
            self.config_keys, self.env_file = self._scan_config()

            # Project-specific live stats
            self.live_stats = self._get_live_stats()

        except Exception as exc:
            log.warning("project_scan_error", project=self.name, error=str(exc))
            self.status = "Scan Error"

    def _calculate_size(self) -> float:
        """Calculate total size in MB (excluding node_modules, .venv, .git)."""
        total_bytes = 0
        skip_dirs = {"node_modules", ".venv", ".git", "__pycache__", ".mypy_cache", "vendor"}
        
        try:
            for item in self.path.rglob("*"):
                if item.is_file() and not any(skip in item.parts for skip in skip_dirs):
                    try:
                        total_bytes += item.stat().st_size
                    except (OSError, PermissionError):
                        pass
        except Exception:
            pass
        
        return round(total_bytes / (1024 * 1024), 1)

    def _get_last_modified(self) -> str:
        """Get the most recent file modification time."""
        try:
            latest = max(
                (f.stat().st_mtime for f in self.path.rglob("*") 
                 if f.is_file() and ".git" not in f.parts),
                default=0
            )
            return datetime.fromtimestamp(latest).isoformat() if latest else ""
        except Exception:
            return ""

    def _detect_language(self) -> tuple[str, List[str]]:
        """Detect primary language and tech stack."""
        detected: List[str] = []
        
        for lang, patterns in LANGUAGE_PATTERNS.items():
            matches = 0
            for pattern in patterns:
                if "*" in pattern:
                    matches += len(list(self.path.glob(pattern)))
                else:
                    if (self.path / pattern).exists():
                        matches += 1
            
            if matches > 0:
                detected.append(f"{lang}({matches})")

        if not detected:
            return "Unknown", []

        # Primary language is the one with the most file matches
        primary = max(detected, key=lambda x: int(x.split("(")[1].rstrip(")")))
        primary_lang = primary.split("(")[0]
        
        return primary_lang, detected

    def _check_status(self) -> tuple[str, List[str]]:
        """Check if processes related to this project are running."""
        running: List[str] = []
        
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if str(self.path) in cmdline or self.name in cmdline:
                        running.append(f"{proc.info['name']}({proc.info['pid']})")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception:
            pass

        status = "Running" if running else "Stopped"
        return status, running

    def _scan_config(self) -> tuple[List[str], str]:
        """Extract configuration keys from .env files."""
        env_files = list(self.path.glob(".env*"))
        if not env_files:
            return [], ""

        env_file = env_files[0]
        keys: List[str] = []
        
        try:
            text = env_file.read_text(encoding="utf-8", errors="ignore")
            for line in text.splitlines():
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key = line.split("=")[0].strip()
                    if key:
                        # Mask sensitive keys
                        if any(word in key.lower() for word in ["password", "secret", "key", "token"]):
                            keys.append(f"{key}=***")
                        else:
                            value = line.split("=", 1)[1].strip()[:20]
                            keys.append(f"{key}={value}")
        except Exception:
            pass

        return keys, str(env_file.relative_to(self.path)) if env_file.exists() else ""

    def _get_live_stats(self) -> Dict[str, Any]:
        """Extract project-specific live metrics."""
        stats: Dict[str, Any] = {}

        # BudgetTracker: read daily P&L
        if self.name == "BudgetTracker":
            stats.update(self._get_budget_stats())
        
        # OTP_Sessions_Creator: count active session files
        elif self.name == "OTP_Sessions_Creator":
            stats.update(self._get_otp_stats())
            
        # CryptoSellsBot: check bot status and balance
        elif self.name == "CryptoSellsBot":
            stats.update(self._get_crypto_bot_stats())

        return stats

    def _get_budget_stats(self) -> Dict[str, Any]:
        """Extract BudgetTracker daily profit/loss."""
        try:
            # Look for a database or JSON file with financial data
            db_files = list(self.path.glob("**/*.db")) + list(self.path.glob("**/*budget*.json"))
            if not db_files:
                return {"budget_available": False}

            # Try to read basic stats (this would need to be customized based on actual schema)
            return {
                "budget_available": True,
                "daily_pnl": 0.0,  # Would extract from actual DB
                "currency": "USD",
                "last_transaction": "",
                "file_count": len(db_files),
            }
        except Exception:
            return {"budget_available": False}

    def _get_otp_stats(self) -> Dict[str, Any]:
        """Count OTP session files."""
        try:
            session_files = list(self.path.glob("**/*.session")) + list(self.path.glob("**/*session*.json"))
            return {
                "otp_available": True,
                "session_count": len(session_files),
                "sessions_modified_today": sum(
                    1 for f in session_files
                    if f.stat().st_mtime > (datetime.now().timestamp() - 86400)
                ),
            }
        except Exception:
            return {"otp_available": False}

    def _get_crypto_bot_stats(self) -> Dict[str, Any]:
        """Check CryptoSellsBot status."""
        try:
            # Look for bot token in config
            config_files = list(self.path.glob("**/*config*.py")) + list(self.path.glob("**/*.env"))
            has_config = len(config_files) > 0
            
            return {
                "crypto_available": has_config,
                "config_files": len(config_files),
                "bot_running": "python.exe" in str(self.running_processes),
            }
        except Exception:
            return {"crypto_available": False}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to API-friendly dict."""
        return {
            "name": self.name,
            "path": str(self.path),
            "exists": self.exists,
            "language": self.language,
            "stack": self.stack,
            "status": self.status,
            "running_processes": self.running_processes,
            "config_keys": self.config_keys,
            "env_file": self.env_file,
            "live_stats": self.live_stats,
            "last_modified": self.last_modified,
            "size_mb": self.size_mb,
            "scanned_at": datetime.now(timezone.utc).isoformat(),
        }


_EXPLORER_PROJECTS_MEM = TTLMemoryCache[Dict[str, ProjectInfo]](max_entries=4)


# ── ExplorerService ────────────────────────────────────────────────────────────

class ExplorerService:
    """
    Background service that scans desktop projects and maintains a live catalog.
    
    Usage:
        explorer = ExplorerService(redis)
        asyncio.create_task(explorer.run_loop(interval_hours=6))
    """

    def __init__(self, redis: Any, interval_hours: int = 6) -> None:
        self._redis = redis
        self._running = False
        self._interval_hours = interval_hours

    async def run_loop(self) -> None:
        """Background loop that scans projects every interval_hours."""
        self._running = True
        interval_s = self._interval_hours * 3600
        
        log.info("explorer_service_started", interval_hours=self._interval_hours)

        while self._running:
            try:
                projects = await self.scan_all_projects()
                await self._persist_results(projects)
                await self._log_scan_summary(projects)
            except Exception as exc:
                log.error("explorer_scan_error", error=str(exc))
                await self._redis.set(EXPLORER_SCAN_STATE_KEY, "error", ex=3600)

            await asyncio.sleep(interval_s)

    async def scan_all_projects(self) -> Dict[str, ProjectInfo]:
        """
        Scan all monitored project directories and return a dict of 
        {project_name: ProjectInfo}.
        """
        await self._redis.set(EXPLORER_SCAN_STATE_KEY, "scanning", ex=3600)
        
        projects: Dict[str, ProjectInfo] = {}
        
        def _scan_project(name: str) -> ProjectInfo:
            """Blocking scan — run in executor to avoid blocking the event loop."""
            project = ProjectInfo(name, DESKTOP_BASE / name)
            project.scan()
            return project

        # Scan projects in parallel via executor threads
        tasks = [
            asyncio.get_event_loop().run_in_executor(None, _scan_project, name)
            for name in MONITORED_PROJECTS
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for name, result in zip(MONITORED_PROJECTS, results):
            if isinstance(result, ProjectInfo):
                projects[name] = result
            else:
                log.warning("project_scan_failed", project=name, error=str(result))
                # Create a minimal failed entry
                failed_project = ProjectInfo(name, DESKTOP_BASE / name)
                failed_project.status = "Scan Failed"
                projects[name] = failed_project

        await self._redis.set(EXPLORER_SCAN_STATE_KEY, "complete", ex=3600)
        log.info("explorer_scan_complete", project_count=len(projects))
        
        return projects

    async def get_project(self, name: str) -> ProjectInfo | None:
        """Get metadata for a specific project (triggers fresh scan if needed)."""
        projects = await self.get_all_projects()
        return projects.get(name)

    async def get_all_projects(self) -> Dict[str, ProjectInfo]:
        """
        Return all project metadata from Redis cache.
        If cache is stale/empty, triggers a fresh scan.
        """
        mem_hit = _EXPLORER_PROJECTS_MEM.get(EXPLORER_PROJECTS_MEMORY_KEY)
        if mem_hit is not None:
            return mem_hit

        raw = await self._redis.get(EXPLORER_PROJECTS_KEY)
        if raw:
            try:
                data = json.loads(raw)
                # Convert back from JSON to ProjectInfo objects
                projects = {}
                for name, proj_dict in data.items():
                    project = ProjectInfo(name, Path(proj_dict["path"]))
                    project.exists = proj_dict["exists"]
                    project.language = proj_dict["language"]
                    project.stack = proj_dict["stack"]
                    project.status = proj_dict["status"]
                    project.running_processes = proj_dict["running_processes"]
                    project.config_keys = proj_dict["config_keys"]
                    project.env_file = proj_dict["env_file"]
                    project.live_stats = proj_dict["live_stats"]
                    project.last_modified = proj_dict["last_modified"]
                    project.size_mb = proj_dict["size_mb"]
                    projects[name] = project
                _EXPLORER_PROJECTS_MEM.set(
                    EXPLORER_PROJECTS_MEMORY_KEY,
                    projects,
                    EXPLORER_PROJECTS_MEMORY_TTL_S,
                )
                return projects
            except Exception as exc:
                log.warning("explorer_cache_decode_error", error=str(exc))

        # Cache miss — fresh scan
        projects = await self.scan_all_projects()
        _EXPLORER_PROJECTS_MEM.set(
            EXPLORER_PROJECTS_MEMORY_KEY,
            projects,
            EXPLORER_PROJECTS_MEMORY_TTL_S,
        )
        return projects

    async def get_budget_tracker_stats(self) -> Dict[str, Any]:
        """Extract live budget stats for the dashboard widget."""
        project = await self.get_project("BudgetTracker")
        if not project or not project.exists:
            return {"available": False}

        stats = project.live_stats.copy()
        stats["available"] = True
        stats["project_path"] = str(project.path)
        stats["status"] = project.status
        
        return stats

    def stop(self) -> None:
        """Stop the background scanning loop."""
        self._running = False

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _persist_results(self, projects: Dict[str, ProjectInfo]) -> None:
        """Write scan results to Redis."""
        data = {name: proj.to_dict() for name, proj in projects.items()}
        payload = json.dumps(data, indent=None)
        
        await self._redis.set(EXPLORER_PROJECTS_KEY, payload, ex=EXPLORER_TTL)
        _EXPLORER_PROJECTS_MEM.set(
            EXPLORER_PROJECTS_MEMORY_KEY,
            projects,
            EXPLORER_PROJECTS_MEMORY_TTL_S,
        )
        await self._redis.set(EXPLORER_LAST_SCAN_KEY, 
                             datetime.now(timezone.utc).isoformat())

    async def _log_scan_summary(self, projects: Dict[str, ProjectInfo]) -> None:
        """Write scan results to the agent log."""
        from nexus.master.services.decision_engine import AGENT_LOG_KEY, AGENT_LOG_MAX

        running_count = sum(1 for p in projects.values() if p.status == "Running")
        total_size = sum(p.size_mb for p in projects.values() if p.exists)
        
        entry = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "info",
            "message": (f"[Explorer] Desktop scan complete: {len(projects)} projects, "
                       f"{running_count} running, {total_size:.1f} MB total"),
            "metadata": {
                "projects": list(projects.keys()),
                "running": [p.name for p in projects.values() if p.status == "Running"],
                "total_size_mb": total_size,
            },
        })
        await self._redis.lpush(AGENT_LOG_KEY, entry)
        await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)


# ── Standalone functions (for API usage) ───────────────────────────────────────

async def scan_desktop_projects(redis: Any) -> Dict[str, Dict[str, Any]]:
    """
    One-shot scan of all desktop projects.
    Returns a dict suitable for the API response.
    """
    explorer = ExplorerService(redis)
    projects = await explorer.scan_all_projects()
    return {name: proj.to_dict() for name, proj in projects.items()}


async def get_cached_projects(redis: Any) -> Dict[str, Dict[str, Any]]:
    """
    Return cached project data from Redis.
    Triggers a fresh scan if cache is empty.
    """
    explorer = ExplorerService(redis)
    projects = await explorer.get_all_projects()
    return {name: proj.to_dict() for name, proj in projects.items()}


async def get_budget_widget_data(redis: Any) -> Dict[str, Any]:
    """
    Extract BudgetTracker data for the dashboard widget.
    Returns {"available": False} if BudgetTracker is not found/accessible.
    """
    explorer = ExplorerService(redis)
    return await explorer.get_budget_tracker_stats()