"""
Centralised settings loaded from environment variables / .env file.

Both master and worker import from here.  pydantic-settings automatically
reads from a .env file in the working directory and from real env vars,
with env vars taking precedence.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Broker ────────────────────────────────────────────────────────────────
    redis_url: str = Field(default="redis://127.0.0.1:6379/0")

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
    # Your personal chat ID or a group/channel ID.
    # Find yours by messaging @userinfobot on Telegram.
    telegram_admin_chat_id: str = Field(default="")
    # URL shown in HITL messages — set to your LAN IP for remote access.
    telegram_dashboard_url: str = Field(default="http://localhost:3000")

    # ── AI / Content Factory ──────────────────────────────────────────────────
    # Gemini API key for text generation (gemini-2.0-flash) and
    # image generation (imagen-4.0).  Get yours at:
    # https://aistudio.google.com/app/apikey
    gemini_api_key: str = Field(default="")

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")
    environment: str = Field(default="PRODUCTION")

    # ── First-Birth Protocol ──────────────────────────────────────────────────
    # Set to true after the operator approves the first autonomous project birth.
    # While false, new projects wait for HITL approval before deployment.
    # After approval, projects with Scout confidence > 80 deploy automatically.
    first_project_approved: bool = Field(default=False)

    # Root directory where the Architect writes generated project code.
    nexus_projects_dir: str = Field(
        default=r"C:\Users\Yarin\Desktop\Nexus-Projects"
    )

    # ── Auto-Deployer — SSH credentials & targets ────────────────────────────
    # Used by DeployerService to SSH into worker laptops and push updates.
    # Store the password in the Vault (NEXUS_SECRET_WORKER_SSH_PASSWORD) for
    # production; the plain .env value is fine for local/LAN deployments.
    worker_ssh_user: str = Field(default="yadmin")
    worker_ssh_password: str = Field(default="")
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
    worker_deploy_root_win: str = Field(
        default=r"C:\Users\Yarin\Desktop\Nexus-Orchestrator"
    )


# Module-level singleton — import `settings` everywhere.
settings = Settings()
