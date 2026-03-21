"""
POST /api/sessions/send-code   — start Telethon login (SMS / app OTP).
POST /api/sessions/verify-code — submit OTP; optional second step with 2FA password.
GET  /api/sessions/list        — staged sessions with Online / Offline status.
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, HTTPException, status
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
