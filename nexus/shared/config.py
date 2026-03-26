"""
Centralised settings loaded from environment variables / .env file.

Both master and worker import from here.  pydantic-settings reads from env
vars; we also call ``load_dotenv`` once for the repository ``.env`` so
imports behave the same no matter which working directory launched the process.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.shared.redis_util import (
    apply_redis_url_to_environment,
    coerce_redis_url_for_platform,
    default_redis_url_string,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _REPO_ROOT / ".env"
if _ENV_FILE.is_file():
    load_dotenv(_ENV_FILE, override=False)
apply_redis_url_to_environment()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Broker ────────────────────────────────────────────────────────────────
    redis_url: str = Field(default_factory=default_redis_url_string)

    # ── Node identity ─────────────────────────────────────────────────────────
    node_id: str = Field(default="master")

    # ── Worker capabilities ───────────────────────────────────────────────────
    # Comma-separated list of WorkerCapability tags this node declares.
    # Example: "linux-only,high-ram"
    worker_capabilities: str = Field(default="any")

    # ── Master resource caps ──────────────────────────────────────────────────
    # 0 = no cap. CPU is a percentage (0–100); RAM is in megabytes.
    master_cpu_cap_percent: float = Field(default=80.0, ge=0, le=100)
    master_ram_cap_mb: float = Field(default=8192.0, ge=0)

    # ── Worker ────────────────────────────────────────────────────────────────
    worker_max_jobs: int = Field(default=4, ge=1)
    task_default_timeout: int = Field(default=300, ge=10)
    worker_max_tries: int = Field(default=3, ge=1)

    # ── API server ────────────────────────────────────────────────────────────
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8001, ge=1, le=65535)

    # ── Notifications — WhatsApp ──────────────────────────────────────────────
    # Set to "twilio" or "evolution" to activate live WhatsApp delivery.
    whatsapp_provider: str = Field(default="mock")
    whatsapp_to_number: str = Field(default="+0000000000")

    # ── Notifications — Telegram ──────────────────────────────────────────────
    # BotFather token for the Nexus bot.
    telegram_bot_token: str = Field(default="")
    # my.telegram.org — MTProto credentials for Telethon user sessions (session factory).
    telegram_api_id: int = Field(default=0)
    telegram_api_hash: str = Field(default="")
    # Your personal chat ID or a group/channel ID.
    # Find yours by messaging @userinfobot on Telegram.
    telegram_admin_chat_id: str = Field(default="")
    # Numeric Telegram *user* id for /terminate_nexus_now (not chat id). @userinfobot
    telegram_admin_user_id: str = Field(default="")
    # URL shown in HITL messages — set to your LAN IP for remote access.
    telegram_dashboard_url: str = Field(default="http://localhost:3000")

    # ── AI / Content Factory ──────────────────────────────────────────────────
    # Gemini API key for text generation (gemini-2.0-flash) and
    # image generation (imagen-4.0).  Get yours at:
    # https://aistudio.google.com/app/apikey
    gemini_api_key: str = Field(default="")

    # OpenAI — optional; required only for task types that inject OPENAI_API_KEY.
    openai_api_key: str = Field(default="")

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")
    environment: str = Field(default="PRODUCTION")

    # ── First-Birth Protocol ──────────────────────────────────────────────────
    # Set to true after the operator approves the first autonomous project birth.
    # While false, new projects wait for HITL approval before deployment.
    # After approval, projects with Scout confidence > 80 deploy automatically.
    first_project_approved: bool = Field(default=False)

    # Root directory where the Architect writes generated project code.
    # Defaults to ~/Desktop/Nexus-Projects — override with NEXUS_PROJECTS_DIR env var.
    nexus_projects_dir: str = Field(
        default_factory=lambda: str(Path.home() / "Desktop" / "Nexus-Projects")
    )

    # ── TeleFix integration paths ─────────────────────────────────────────────
    # Root of the Mangement Ahu / TeleFix project on this machine.
    # Override with TELEFIX_ROOT env var.
    telefix_root: str = Field(
        default_factory=lambda: str(Path.home() / "Desktop" / "Mangement Ahu")
    )
    # Telefix SQLite database path. Override with TELEFIX_DB env var.
    telefix_db: str = Field(default="")
    # Telefix sessions directory. Override with TELEFIX_SESSIONS_DIR env var.
    telefix_sessions_dir: str = Field(default="")

    # ── Auto-Deployer — SSH credentials & targets ────────────────────────────
    # Used by DeployerService to SSH into worker laptops and push updates.
    # Store the password in the Vault (NEXUS_SECRET_WORKER_SSH_PASSWORD) for
    # production; the plain .env value is fine for local/LAN deployments.
    worker_ssh_user: str = Field(default="yadmin")
    worker_ssh_password: str = Field(default="")
    worker_ssh_key_file: str = Field(default="")
    # Direct IP of the Linux worker laptop (used when no Redis heartbeat exists)
    worker_ip: str = Field(default="")
    # Canonical remote path — used by /api/deploy/sync (Phase 18)
    # Falls back to worker_deploy_root_linux if not set.
    worker_remote_path: str = Field(default="")
    # Destination path on the Linux worker — must match where the repo lives
    worker_deploy_root_linux: str = Field(
        default="/home/yadmin/Desktop/Nexus-Orchestrator"
    )
    # Remote project root on Windows workers
    # Defaults to ~/Desktop/Nexus-Orchestrator — override with WORKER_DEPLOY_ROOT_WIN env var.
    worker_deploy_root_win: str = Field(
        default_factory=lambda: str(Path.home() / "Desktop" / "Nexus-Orchestrator")
    )


# Module-level singleton — import `settings` everywhere.
settings = Settings()
settings.redis_url = coerce_redis_url_for_platform(settings.redis_url)
