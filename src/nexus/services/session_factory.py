"""
Telethon user-session creation — phone → OTP → optional 2FA cloud password.

Persists ``.session`` + companion ``.json`` (with ``api_id`` / ``api_hash``) under
``data/session_vault/`` (see :mod:`nexus.services.session_vault`).

Pending logins keep an in-memory ``TelegramClient`` (single API worker). For
multi-replica deployments, pin session routes to one instance or add shared state.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.config import settings

from nexus.services.session_vault import vault_root

log = structlog.get_logger(__name__)

_PENDING_TTL_S = 600.0
_lock = asyncio.Lock()

# token -> pending login
_pending: dict[str, _PendingLogin] = {}


@dataclass
class _PendingLogin:
    client: Any
    phone: str
    session_path_base: str
    created_monotonic: float
    awaiting_password: bool = False


def _normalize_phone(raw: str) -> str:
    p = (raw or "").strip().replace(" ", "")
    if not p.startswith("+"):
        p = "+" + p.lstrip("+")
    return p


def _ensure_api_credentials() -> tuple[int, str]:
    api_id = int(settings.telegram_api_id)
    api_hash = (settings.telegram_api_hash or "").strip()
    if api_id <= 0 or not api_hash:
        raise RuntimeError(
            "Telegram API credentials missing — set TELEGRAM_API_ID and TELEGRAM_API_HASH "
            "(from https://my.telegram.org) in the environment or .env"
        )
    return api_id, api_hash


async def _sweep_stale_unlocked() -> None:
    now = time.monotonic()
    dead: list[str] = []
    for token, pend in _pending.items():
        if now - pend.created_monotonic > _PENDING_TTL_S:
            dead.append(token)
    for token in dead:
        pend = _pending.pop(token, None)
        if pend is not None:
            try:
                await pend.client.disconnect()
            except Exception as exc:
                log.debug("session_factory_pending_disconnect_failed", error=str(exc))


async def send_login_code(phone: str) -> dict[str, Any]:
    """
    Start login: send Telegram OTP. Returns ``auth_token`` for ``verify_login``.
    """
    from telethon import TelegramClient  # type: ignore[import-untyped]
    from telethon.errors import (  # type: ignore[import-untyped]
        ApiIdInvalidError,
        FloodWaitError,
        PhoneNumberInvalidError,
    )

    api_id, api_hash = _ensure_api_credentials()
    phone_n = _normalize_phone(phone)
    staged = vault_root()
    staged.mkdir(parents=True, exist_ok=True)

    token = secrets.token_urlsafe(24)
    stem = f"nexus_{int(time.time())}_{token[:8]}"
    session_path_base = str(staged / stem)

    client = TelegramClient(session_path_base, api_id, api_hash)

    async with _lock:
        await _sweep_stale_unlocked()
        await client.connect()
        try:
            await client.send_code_request(phone_n)
        except PhoneNumberInvalidError as exc:
            await client.disconnect()
            raise ValueError("Invalid phone number") from exc
        except ApiIdInvalidError as exc:
            await client.disconnect()
            raise RuntimeError("Invalid TELEGRAM_API_ID / TELEGRAM_API_HASH") from exc
        except FloodWaitError as exc:
            await client.disconnect()
            raise RuntimeError(f"Telegram rate limit — try again in {exc.seconds}s") from exc
        except Exception:
            await client.disconnect()
            raise

        _pending[token] = _PendingLogin(
            client=client,
            phone=phone_n,
            session_path_base=session_path_base,
            created_monotonic=time.monotonic(),
        )

    log.info("session_factory_code_sent", phone_tail=phone_n[-4:])
    return {
        "auth_token": token,
        "phone": phone_n,
        "expires_in_seconds": int(_PENDING_TTL_S),
        "session_stem": stem,
    }


async def verify_login(
    auth_token: str,
    code: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    """
    Complete login with OTP and optionally 2FA cloud password.

    If 2FA is enabled, first call with ``code`` returns
    ``{"status": "password_required", ...}``; call again with ``password`` only
    (same ``auth_token``).
    """
    from telethon import TelegramClient  # type: ignore[import-untyped]
    from telethon.errors import (  # type: ignore[import-untyped]
        PasswordHashInvalidError,
        PhoneCodeInvalidError,
        SessionPasswordNeededError,
    )

    api_id, api_hash = _ensure_api_credentials()
    code_t = (code or "").strip()
    password_t = (password or "").strip()

    async with _lock:
        await _sweep_stale_unlocked()
        pend = _pending.get(auth_token)
        if pend is None:
            raise KeyError("Unknown or expired auth_token")

        client: TelegramClient = pend.client
        phone = pend.phone

        if pend.awaiting_password:
            if not password_t:
                raise ValueError("Two-factor password required")
            try:
                await client.sign_in(password=password_t)
            except PasswordHashInvalidError as exc:
                raise ValueError("Invalid two-factor password") from exc
        else:
            if not code_t:
                raise ValueError("OTP code is required")
            try:
                await client.sign_in(phone=phone, code=code_t)
            except SessionPasswordNeededError:
                pend.awaiting_password = True
                pend.created_monotonic = time.monotonic()
                log.info("session_factory_2fa_required", phone_tail=phone[-4:])
                return {
                    "status": "password_required",
                    "auth_token": auth_token,
                    "detail": "This account has two-factor authentication enabled.",
                }
            except PhoneCodeInvalidError as exc:
                raise ValueError("Invalid or expired OTP code") from exc

        me = await client.get_me()
        meta_path = Path(pend.session_path_base).with_suffix(".json")
        meta = {
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "user_id": me.id,
            "username": getattr(me, "username", None),
            "first_name": getattr(me, "first_name", None) or "",
            "last_name": getattr(me, "last_name", None) or "",
            "created_via": "nexus_session_factory",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        meta_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        await client.disconnect()
        _pending.pop(auth_token, None)

        log.info(
            "session_factory_session_saved",
            stem=meta_path.stem,
            user_id=me.id,
        )
        return {
            "status": "authorized",
            "session_stem": meta_path.stem,
            "meta_path": str(meta_path),
            "session_path": pend.session_path_base + ".session",
            "user_id": me.id,
            "username": getattr(me, "username", None),
            "phone": phone,
        }


def session_online_status_sync(meta_json: Path) -> dict[str, Any]:
    """Blocking: return Online/Offline for one staged session (used from thread pool)."""
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]

    try:
        meta = json.loads(meta_json.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "session_stem": meta_json.stem,
            "phone": None,
            "status": "Offline",
            "detail": f"invalid meta: {exc}",
        }

    if not isinstance(meta, dict):
        return {
            "session_stem": meta_json.stem,
            "phone": None,
            "status": "Offline",
            "detail": "meta is not a JSON object",
        }

    phone = meta.get("phone")
    try:
        api_id = int(meta["api_id"])
        api_hash = str(meta["api_hash"])
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "session_stem": meta_json.stem,
            "phone": phone,
            "status": "Offline",
            "detail": f"invalid api_id/api_hash: {exc}",
        }
    session_file = str(meta_json.with_suffix(""))

    client = TelegramClient(session_file, api_id, api_hash)
    try:
        client.connect()
        ok = bool(client.is_user_authorized())
        return {
            "session_stem": meta_json.stem,
            "phone": phone,
            "status": "Online" if ok else "Offline",
            "user_id": (client.get_me().id if ok else None),
        }
    except Exception as exc:
        return {
            "session_stem": meta_json.stem,
            "phone": phone,
            "status": "Offline",
            "detail": str(exc),
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


async def list_staged_sessions(staged_root: Path | None = None) -> list[dict[str, Any]]:
    from nexus.services.session_vault import discover_all_meta_json_files

    if staged_root is not None:
        from nexus.shared.staged_accounts import discover_session_meta_json_files

        metas = discover_session_meta_json_files(staged_root)
    else:
        metas = discover_all_meta_json_files()
    loop = asyncio.get_event_loop()
    out: list[dict[str, Any]] = []
    for path in metas:
        row = await loop.run_in_executor(None, session_online_status_sync, path)
        row["meta_path"] = str(path)
        out.append(row)
    return out
