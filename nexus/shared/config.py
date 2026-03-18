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
    redis_url: str = Field(default="redis://localhost:6379/0")

    # ── Node identity ─────────────────────────────────────────────────────────
    node_id: str = Field(default="master")

    # ── Master resource caps ──────────────────────────────────────────────────
    master_cpu_cap_percent: float = Field(default=25.0, ge=0, le=100)
    master_ram_cap_mb: float = Field(default=512.0, ge=0)

    # ── Worker ────────────────────────────────────────────────────────────────
    worker_max_jobs: int = Field(default=4, ge=1)

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")


# Module-level singleton — import `settings` everywhere.
settings = Settings()
