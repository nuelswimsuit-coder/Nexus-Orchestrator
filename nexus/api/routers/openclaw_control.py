"""
OpenClaw control plane — write runtime settings beside Moltbot and reload the process.

POST /api/openclaw/settings
    Merge ``scan_interval_seconds`` and/or ``target_websites`` into
    ``{NEXUS_OPENCLAW_CONTROL_DIR}/{OPENCLAW_SETTINGS_FILENAME}`` (default
    ``openclaw_settings.json``), then run the configured reload shell command.

Environment
-----------
NEXUS_OPENCLAW_CONTROL_DIR / OPENCLAW_CONTROL_DIR — required absolute directory.
NEXUS_OPENCLAW_SETTINGS_FILENAME — optional filename (default ``openclaw_settings.json``).
NEXUS_MOLTBOT_RELOAD_CMD — preferred shell command to restart Moltbot after a write.
NEXUS_OPENCLAW_RELOAD_CMD — fallback (same semantics as ``openclaw_auto_editor``).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator

from nexus.shared.config import settings

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/openclaw", tags=["openclaw-control"])

SETTINGS_SCHEMA = "nexus.openclaw.settings.v1"
DEFAULT_SCAN_INTERVAL_S = 300


def _reload_command() -> str:
    for key in ("NEXUS_MOLTBOT_RELOAD_CMD", "NEXUS_OPENCLAW_RELOAD_CMD"):
        raw = (os.environ.get(key) or "").strip()
        if raw:
            return raw
    return ""


def _control_dir() -> Path:
    raw = (settings.openclaw_control_dir or "").strip()
    if not raw:
        raise HTTPException(
            status_code=503,
            detail=(
                "OpenClaw control directory is not configured. Set "
                "NEXUS_OPENCLAW_CONTROL_DIR (or OPENCLAW_CONTROL_DIR) to the OpenClaw "
                "install directory where Moltbot reads its config."
            ),
        )
    p = Path(raw).expanduser().resolve()
    if not p.is_dir():
        raise HTTPException(
            status_code=503,
            detail=f"OpenClaw control path is not a directory: {p}",
        )
    return p


def _settings_path() -> Path:
    name = (settings.openclaw_settings_filename or "").strip() or "openclaw_settings.json"
    if name in (".", "..") or "/" in name or "\\" in name:
        raise HTTPException(
            status_code=500,
            detail="Invalid openclaw_settings_filename in server configuration.",
        )
    return _control_dir() / name


def _validate_urls(urls: list[str]) -> list[str]:
    out: list[str] = []
    for u in urls:
        s = (u or "").strip()
        if not s:
            continue
        parsed = urlparse(s)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise HTTPException(
                status_code=400,
                detail=f"Each target_website must be an http(s) URL with a host: {u!r}",
            )
        out.append(s)
    return out


def _load_existing(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


class OpenclawSettingsBody(BaseModel):
    """At least one field must be set so the endpoint always represents a deliberate change."""

    scan_interval_seconds: int | None = Field(
        default=None,
        ge=5,
        le=86400,
        description="Seconds between Moltbot/OpenClaw scan cycles.",
    )
    target_websites: list[str] | None = Field(
        default=None,
        description="Full http(s) URLs for Moltbot to monitor (replaces the list when sent).",
    )

    @model_validator(mode="after")
    def _require_patch(self) -> OpenclawSettingsBody:
        if self.scan_interval_seconds is None and self.target_websites is None:
            raise ValueError("Provide scan_interval_seconds and/or target_websites")
        return self


class OpenclawSettingsResponse(BaseModel):
    config_path: str
    scan_interval_seconds: int
    target_websites: list[str]
    reload_command_configured: bool
    reload_ok: bool
    reload_detail: str = ""


@router.post("/settings", response_model=OpenclawSettingsResponse)
async def post_openclaw_settings(body: OpenclawSettingsBody) -> OpenclawSettingsResponse:
    path = _settings_path()
    prior = _load_existing(path)

    scan_s = prior.get("scan_interval_seconds")
    if not isinstance(scan_s, int) or scan_s < 5:
        scan_s = DEFAULT_SCAN_INTERVAL_S
    sites = prior.get("target_websites")
    if not isinstance(sites, list):
        sites_list: list[str] = []
    else:
        sites_list = [str(x).strip() for x in sites if str(x).strip()]

    if body.scan_interval_seconds is not None:
        scan_s = body.scan_interval_seconds
    if body.target_websites is not None:
        sites_list = _validate_urls(body.target_websites)

    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "schema": SETTINGS_SCHEMA,
        "scan_interval_seconds": scan_s,
        "target_websites": sites_list,
        "updated_at": now.isoformat(),
        "updated_by": "nexus-api",
    }
    # Preserve unknown keys for forward compatibility
    for k, v in prior.items():
        if k not in payload:
            payload[k] = v

    try:
        _atomic_write_json(path, payload)
    except OSError as exc:
        log.exception("openclaw_settings_write_failed", path=str(path), error=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to write OpenClaw settings file: {exc}",
        ) from exc

    cmd = _reload_command()
    reload_ok = False
    reload_detail = ""
    if not cmd:
        reload_detail = (
            "Settings file written; reload skipped — set NEXUS_MOLTBOT_RELOAD_CMD or "
            "NEXUS_OPENCLAW_RELOAD_CMD to restart Moltbot automatically."
        )
        log.warning("openclaw_settings_reload_cmd_missing", path=str(path))
    else:
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=120.0)
            code = proc.returncode if proc.returncode is not None else -1
            err = (err_b or b"").decode(errors="replace").strip()
            out = (out_b or b"").decode(errors="replace").strip()
            if code == 0:
                reload_ok = True
                reload_detail = "Moltbot reload command completed successfully."
                log.info("openclaw_settings_reload_ok", path=str(path))
            else:
                reload_detail = (err or out or f"exit code {code}")[:2000]
                log.warning(
                    "openclaw_settings_reload_failed",
                    path=str(path),
                    code=code,
                    detail=reload_detail[:500],
                )
        except TimeoutError:
            reload_detail = "Reload command timed out after 120s."
            log.warning("openclaw_settings_reload_timeout", path=str(path))
        except OSError as exc:
            reload_detail = str(exc)
            log.warning("openclaw_settings_reload_os_error", path=str(path), error=str(exc))

    resp = OpenclawSettingsResponse(
        config_path=str(path),
        scan_interval_seconds=scan_s,
        target_websites=sites_list,
        reload_command_configured=bool(cmd),
        reload_ok=reload_ok,
        reload_detail=reload_detail,
    )
    if cmd and not reload_ok:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Settings were written but the Moltbot reload command did not succeed.",
                "result": resp.model_dump(),
            },
        )
    return resp
