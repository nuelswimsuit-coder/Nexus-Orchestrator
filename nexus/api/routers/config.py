"""
Config router — live settings read & write.

GET  /api/config
    Return the current values of all editable settings fields.

PATCH /api/config
    Accept a partial update dict, validate it, write changed values back to
    the .env file, and hot-reload the in-process `settings` singleton so the
    new values take effect immediately without restarting the API.

Editable fields (the ones shown in the Performance section of the dashboard):
    master_cpu_cap_percent  float  0–100
    master_ram_cap_mb       float  ≥ 0
    worker_max_jobs         int    ≥ 1
    task_default_timeout    int    ≥ 10
    worker_max_tries        int    ≥ 1
    worker_ip               str    (deployer target IP)
    worker_ssh_user         str
    worker_deploy_root_linux str
    log_level               str    DEBUG|INFO|WARNING|ERROR

The .env file is rewritten atomically: we read all existing lines, update only
the matching KEY= lines (or append new ones), then write back.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from nexus.shared.config import settings
from nexus.shared.system_settings import read_system_settings, write_system_settings

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/config", tags=["config"])

# Path to the .env file — same directory the API is launched from (project root)
ENV_FILE = Path(".env")


# ── Schemas ────────────────────────────────────────────────────────────────────

class ConfigResponse(BaseModel):
    power_limit:              int
    max_workers:              int
    master_cpu_cap_percent:   float
    master_ram_cap_mb:        float
    worker_max_jobs:          int
    task_default_timeout:     int
    worker_max_tries:         int
    worker_ip:                str
    worker_ssh_user:          str
    worker_deploy_root_linux: str
    log_level:                str


class ConfigPatch(BaseModel):
    power_limit:              int   | None = Field(default=None, ge=0, le=100)
    max_workers:              int   | None = Field(default=None, ge=1, le=64)
    master_cpu_cap_percent:   float | None = Field(default=None, ge=0, le=100)
    master_ram_cap_mb:        float | None = Field(default=None, ge=0)
    worker_max_jobs:          int   | None = Field(default=None, ge=1)
    task_default_timeout:     int   | None = Field(default=None, ge=10)
    worker_max_tries:         int   | None = Field(default=None, ge=1)
    worker_ip:                str   | None = None
    worker_ssh_user:          str   | None = None
    worker_deploy_root_linux: str   | None = None
    log_level:                str   | None = None

    @field_validator("log_level")
    @classmethod
    def _valid_log_level(cls, v: str | None) -> str | None:
        if v is not None and v.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError("log_level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return v.upper() if v else v


# ── .env helpers ───────────────────────────────────────────────────────────────

def _read_env() -> list[str]:
    if not ENV_FILE.exists():
        return []
    return ENV_FILE.read_text(encoding="utf-8").splitlines()


def _write_env(updates: dict[str, str]) -> None:
    """
    Update specific KEY=value pairs in .env, preserving all comments and
    other lines.  Appends any keys that don't already exist.
    """
    lines = _read_env()
    remaining = dict(updates)  # keys still to be written

    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        # Skip blank / comment lines unchanged
        if not stripped or stripped.startswith("#"):
            new_lines.append(line)
            continue

        # Match KEY=... (with optional inline comment)
        m = re.match(r"^([A-Z0-9_]+)\s*=", stripped, re.IGNORECASE)
        if m:
            key = m.group(1).upper()
            if key in remaining:
                # Preserve any trailing inline comment
                comment_match = re.search(r"\s+#.*$", line)
                comment = comment_match.group(0) if comment_match else ""
                new_lines.append(f"{key}={remaining.pop(key)}{comment}")
                continue
        new_lines.append(line)

    # Append any keys that weren't found in the existing file
    if remaining:
        new_lines.append("")
        for key, val in remaining.items():
            new_lines.append(f"{key}={val}")

    ENV_FILE.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    log.info("config_env_updated", keys=list(updates.keys()))


def _hot_reload(updates: dict[str, Any]) -> None:
    """
    Apply validated updates directly to the in-process settings singleton
    so the new values are visible immediately without restarting.
    """
    for key, value in updates.items():
        if hasattr(settings, key):
            object.__setattr__(settings, key, value)
    log.info("config_hot_reloaded", keys=list(updates.keys()))


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("", response_model=ConfigResponse, summary="Read current editable settings")
async def get_config() -> ConfigResponse:
    dyn = read_system_settings()
    return ConfigResponse(
        power_limit               = int(dyn["power_limit"]),
        max_workers               = int(dyn["max_workers"]),
        master_cpu_cap_percent   = settings.master_cpu_cap_percent,
        master_ram_cap_mb        = settings.master_ram_cap_mb,
        worker_max_jobs          = settings.worker_max_jobs,
        task_default_timeout     = settings.task_default_timeout,
        worker_max_tries         = settings.worker_max_tries,
        worker_ip                = settings.worker_ip,
        worker_ssh_user          = settings.worker_ssh_user,
        worker_deploy_root_linux = settings.worker_deploy_root_linux,
        log_level                = settings.log_level,
    )


@router.patch("", response_model=ConfigResponse, summary="Update editable settings")
async def patch_config(body: ConfigPatch) -> ConfigResponse:
    """
    Apply a partial update.  Only fields that are not None are changed.
    Values are written to .env and hot-reloaded into the running process.
    """
    # Build dict of only the fields the caller actually provided
    changed: dict[str, Any] = {
        k: v for k, v in body.model_dump().items() if v is not None
    }

    if not changed:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Keep runtime JSON + legacy env fields in sync.
    if "power_limit" in changed:
        changed["master_cpu_cap_percent"] = float(changed["power_limit"])
    if "max_workers" in changed:
        changed["worker_max_jobs"] = int(changed["max_workers"])

    # Map Python field names → .env key names (upper-case)
    env_keys = {
        "master_cpu_cap_percent",
        "master_ram_cap_mb",
        "worker_max_jobs",
        "task_default_timeout",
        "worker_max_tries",
        "worker_ip",
        "worker_ssh_user",
        "worker_deploy_root_linux",
        "log_level",
    }
    env_updates = {
        k.upper(): str(v)
        for k, v in changed.items()
        if k in env_keys
    }
    json_updates = {
        "power_limit": int(changed["power_limit"]) if "power_limit" in changed else int(changed.get("master_cpu_cap_percent", settings.master_cpu_cap_percent)),
        "max_workers": int(changed["max_workers"]) if "max_workers" in changed else int(changed.get("worker_max_jobs", settings.worker_max_jobs)),
        "log_level": str(changed.get("log_level", settings.log_level)).upper(),
    }

    try:
        _write_env(env_updates)
        write_system_settings(json_updates)
        _hot_reload(changed)
    except Exception as exc:
        log.exception("config_update_error", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to save settings: {exc}") from exc

    log.info("config_updated", changed=changed)
    return await get_config()
