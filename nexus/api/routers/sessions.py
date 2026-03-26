"""
POST /api/sessions/send-code        — start Telethon login (SMS / app OTP).
POST /api/sessions/verify-code      — submit OTP; optional second step with 2FA password.
GET  /api/sessions/list             — staged sessions with Online / Offline status.
GET  /api/sessions/vault/commander  — vault session health overview (lease + health status).
"""

from __future__ import annotations

import glob
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from nexus.master.services import session_factory as session_factory_svc

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/sessions", tags=["sessions"])


class SendCodeRequest(BaseModel):
    phone: str = Field(..., min_length=8, description="E.164-style, e.g. +15551234567")


class VerifyCodeRequest(BaseModel):
    auth_token: str = Field(..., min_length=8)
    code: str | None = Field(None, description="OTP from Telegram")
    password: str | None = Field(
        None,
        description="2FA cloud password (required after password_required response)",
    )


@router.post("/send-code", summary="Send Telegram login code")
async def send_code(body: SendCodeRequest) -> dict:
    try:
        return await session_factory_svc.send_login_code(body.phone)
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("sessions_send_code_failed", error=str(exc))
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send login code",
        ) from exc


@router.post("/verify-code", summary="Verify OTP and save session")
async def verify_code(body: VerifyCodeRequest) -> dict:
    try:
        return await session_factory_svc.verify_login(
            body.auth_token,
            code=body.code,
            password=body.password,
        )
    except KeyError as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail="Unknown or expired auth_token — start again with send-code",
        ) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("sessions_verify_failed", error=str(exc))
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Login verification failed",
        ) from exc


@router.get("/list", summary="List staged Telethon sessions")
async def list_sessions() -> dict:
    rows = await session_factory_svc.list_staged_sessions()
    return {"sessions": rows, "count": len(rows)}


@router.get("/vault/commander", summary="Session vault health overview")
async def vault_commander(request: Request) -> dict:
    """
    Return a health overview of all Telethon session files found in the vault.

    Reads session JSON files from the configured TELEFIX_SESSIONS_DIR and
    enriches them with Redis lease data (if available).  Falls back gracefully
    if the vault directory does not exist.
    """
    redis = request.app.state.redis

    sessions_dir = os.getenv(
        "TELEFIX_SESSIONS_DIR",
        str(Path.home() / "Desktop" / "Mangement Ahu" / "sessions"),
    )

    accounts: list[dict] = []

    for sub in ("adders", "managers", "frozen"):
        pattern = os.path.join(sessions_dir, sub, "*.json")
        for filepath in glob.glob(pattern):
            stem = Path(filepath).stem
            health = "green" if sub == "adders" else ("yellow" if sub == "managers" else "red")
            phone: str | None = None
            proxy_ip: str | None = None

            try:
                with open(filepath, encoding="utf-8") as f:
                    data = json.load(f)
                phone    = data.get("phone") or data.get("phone_number")
                proxy_ip = data.get("proxy_ip") or data.get("proxy")
            except Exception:
                pass

            # Check Redis lease data
            lease_worker_id: str | None = None
            lease_task_id:   str | None = None
            lease_ttl:       int | None = None
            try:
                lease_key = f"nexus:session:lease:{stem}"
                raw_lease = await redis.get(lease_key)
                if raw_lease:
                    lease_data     = json.loads(raw_lease)
                    lease_worker_id = lease_data.get("worker_id")
                    lease_task_id   = lease_data.get("task_id")
                    lease_ttl_raw   = await redis.ttl(lease_key)
                    lease_ttl       = int(lease_ttl_raw) if lease_ttl_raw and lease_ttl_raw > 0 else None
            except Exception:
                pass

            accounts.append({
                "session_stem":      stem,
                "phone":             phone,
                "proxy_ip":          proxy_ip,
                "status":            sub,
                "health":            health,
                "lease_worker_id":   lease_worker_id,
                "lease_task_id":     lease_task_id,
                "lease_ttl_seconds": lease_ttl,
            })

    return {
        "accounts":   accounts,
        "total":      len(accounts),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
