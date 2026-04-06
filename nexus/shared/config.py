"""
Centralised settings loaded from environment variables / .env file.

Both master and worker import from here.  pydantic-settings reads from env
vars; we also call ``load_dotenv`` once for the repository ``.env`` so
imports behave the same no matter which working directory launched the process.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.shared.redis_util import (
    apply_redis_url_to_environment,
    apply_remote_worker_env_overrides,
    apply_worker_cli_redis_host_override,
    coerce_redis_url_for_platform,
    default_redis_url_string,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONFIGS_ENV = _REPO_ROOT / "configs" / ".env"
if _CONFIGS_ENV.is_file():
    load_dotenv(_CONFIGS_ENV, override=False)
_ENV_FILE = _REPO_ROOT / ".env"
if _ENV_FILE.is_file():
    # Repo `.env` must win over stale machine/user env (e.g. old POLYMARKET_* from
    # Windows System Environment), otherwise dashboard keeps querying the wrong address.
    load_dotenv(_ENV_FILE, override=True)
apply_remote_worker_env_overrides()
apply_worker_cli_redis_host_override()
apply_redis_url_to_environment()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("telegram_api_id", mode="before")
    @classmethod
    def _empty_env_int(cls, value: object) -> object:
        """`.env` often has `TELEGRAM_API_ID=` with no value — treat as unset (0)."""
        if value is None:
            return 0
        if isinstance(value, str) and not value.strip():
            return 0
        return value

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
    # Cap concurrent Telethon MTProto connections / network slots (swarm + vault probes).
    # Default is conservative to avoid mass FloodWait / account bans when many sessions run at once.
    telegram_network_concurrency: int = Field(
        default=5,
        ge=1,
        le=500,
        validation_alias=AliasChoices(
            "NEXUS_TELEGRAM_NETWORK_CONCURRENCY",
            "TELEGRAM_NETWORK_CONCURRENCY",
        ),
    )
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

    # When False, Nexus hides legacy TeleFix bot control (AHU start/stop, etc.).
    legacy_telefix_bot_enabled: bool = Field(default=True)

    # management.sentinel_seo — session stem (under staged_accounts) for search probes.
    nexus_seo_probe_session: str = Field(default="")
    nexus_seo_auto_rename: bool = Field(default=False)
    nexus_seo_target_title: str = Field(default="")
    nexus_seo_auto_rename_max: int = Field(default=3, ge=1, le=20)
    nexus_seo_rename_cooldown_s: float = Field(default=90.0, ge=5.0)

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
    # macOS workers (e.g. Mac mini). Empty = derive from user home on the remote.
    worker_deploy_root_darwin: str = Field(default="")
    # Remote project root on Windows workers
    # Defaults to ~/Desktop/Nexus-Orchestrator — override with WORKER_DEPLOY_ROOT_WIN env var.
    worker_deploy_root_win: str = Field(
        default_factory=lambda: str(Path.home() / "Desktop" / "Nexus-Orchestrator")
    )
    # macOS workers (Apple Silicon / Intel) — e.g. Mac Mini; ``uname`` returns Darwin.
    worker_deploy_root_darwin: str = Field(default="")
    # Optional path to ``workers.json`` (static node_id → IP list). Also: NEXUS_WORKERS_CONFIG, WORKERS_JSON.
    workers_config_path: str = Field(
        default="",
        validation_alias=AliasChoices(
            "NEXUS_WORKERS_CONFIG",
            "WORKERS_JSON",
            "WORKERS_CONFIG_PATH",
        ),
    )


def apply_polymarket_wallet_alignment() -> None:
    """
    Keep ``POLYMARKET_SIGNER_ADDRESS`` and ``POLYMARKET_PORTFOLIO_ADDRESS`` aligned with the
    EOA derived from the configured signing material so the UI and CLOB signer match.

    Uses the same key resolution as :func:`nexus.trading.wallet_manager.get_polymarket_private_key`
    (includes ``POLYMARKET_WALLET_PRIVATE_KEY``). Legacy code only read ``POLYMARKET_RELAYER_KEY``,
    so operators with wallet key only had an empty portfolio address and zero Data API rows.

    Disable with ``POLYMARKET_SYNC_WALLET_ENV=0`` (advanced: separate portfolio view address).
    """
    import os

    if (os.getenv("POLYMARKET_SYNC_WALLET_ENV", "1") or "").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return
    from nexus.trading.wallet_manager import get_polymarket_private_key

    key = get_polymarket_private_key()
    if not key:
        return
    try:
        from eth_account import Account

        derived = Account.from_key(key).address
    except Exception:
        return
    os.environ["POLYMARKET_SIGNER_ADDRESS"] = derived
    os.environ["POLYMARKET_PORTFOLIO_ADDRESS"] = derived


def log_polymarket_wallet_mismatch_at_startup() -> None:
    """
    If sync is disabled and portfolio / signer env disagree with the relayer key, log CRITICAL.
    When ``POLYMARKET_SYNC_WALLET_ENV`` is default (on), env was already aligned — skip.
    """
    import os

    import structlog

    log = structlog.get_logger("nexus.polymarket.wallet")
    if (os.getenv("POLYMARKET_SYNC_WALLET_ENV", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    ):
        return  # auto-sync on — portfolio/signer were forced to derived address
    from nexus.trading.wallet_manager import get_polymarket_private_key

    key = get_polymarket_private_key()
    if not key:
        return
    try:
        from eth_account import Account

        derived = Account.from_key(key).address.lower()
    except Exception:
        return
    port = (os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip().lower()
    signer = (os.getenv("POLYMARKET_SIGNER_ADDRESS") or "").strip().lower()
    if port and port != derived:
        log.critical(
            "FATAL: Wallet Mismatch! Master is looking at one wallet but signing with another.",
            derived_signing_address=derived,
            polymarket_portfolio_address=port,
            hint="Set POLYMARKET_SYNC_WALLET_ENV=1 or align POLYMARKET_PORTFOLIO_ADDRESS with the relayer key.",
        )
    if signer and signer != derived:
        allow = (os.getenv("POLYMARKET_ALLOW_FUNDER_ENV_MISMATCH") or "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        if not allow:
            log.critical(
                "FATAL: Wallet Mismatch! POLYMARKET_SIGNER_ADDRESS does not match RELAYER_KEY-derived EOA.",
                derived_signing_address=derived,
                polymarket_signer_address=signer,
            )


# Module-level singleton — import `settings` everywhere.
settings = Settings()
settings.redis_url = coerce_redis_url_for_platform(settings.redis_url)
apply_polymarket_wallet_alignment()
