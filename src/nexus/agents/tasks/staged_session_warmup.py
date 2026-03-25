"""
telegram.run_warmup — staged session DC handshake

Connects Telethon user sessions under ``data/staged_accounts/`` (same discovery
as ``account_mapper.map``) and runs a lightweight ``get_me`` so accounts stay
“warm” for MTProto. The autonomous decision engine dispatches this task when
session health is critical.

Task type
---------
telegram.run_warmup

Parameters (optional)
---------------------
staged_dir   : str — override path (default: ``<repo>/data/staged_accounts``)
max_sessions : int — cap (default: 10)
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.staged_accounts import (
    discover_session_meta_json_files,
    staged_accounts_root,
)
from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)


def _warm_one_session_string(cred: dict[str, Any]) -> dict[str, Any]:
    from telethon.sessions import StringSession  # type: ignore[import-untyped]
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]

    string_session = str(cred.get("string_session") or "")
    api_id = int(cred["api_id"])
    api_hash = str(cred["api_hash"])
    stem = str(cred.get("session_stem") or "vault")
    client = TelegramClient(StringSession(string_session), api_id, api_hash)
    try:
        client.connect()
        if not client.is_user_authorized():
            return {
                "session_stem": stem,
                "phone": None,
                "status": "Offline",
                "ok": False,
                "error": "not authorized",
            }
        me = client.get_me()
        return {
            "session_stem": stem,
            "phone": getattr(me, "phone", None),
            "status": "Online",
            "ok": True,
            "user_id": me.id,
        }
    except Exception as exc:
        log.warning(
            "staged_session_warmup_failed", session=stem, error=str(exc)
        )
        return {
            "session_stem": stem,
            "phone": None,
            "status": "Offline",
            "ok": False,
            "error": str(exc),
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _warm_one_session(meta_json: Path) -> dict[str, Any]:
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]

    try:
        with open(meta_json, encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as exc:
        return {
            "session_stem": meta_json.stem,
            "status": "error",
            "ok": False,
            "error": f"meta read failed: {exc}",
        }

    api_id = int(meta["api_id"])
    api_hash = str(meta["api_hash"])
    session_file = str(meta_json.with_suffix(""))
    phone = meta.get("phone")

    client = TelegramClient(session_file, api_id, api_hash)
    try:
        client.connect()
        if not client.is_user_authorized():
            return {
                "session_stem": meta_json.stem,
                "phone": phone,
                "status": "Offline",
                "ok": False,
                "error": "not authorized",
            }
        me = client.get_me()
        return {
            "session_stem": meta_json.stem,
            "phone": phone,
            "status": "Online",
            "ok": True,
            "user_id": me.id,
        }
    except Exception as exc:
        log.warning(
            "staged_session_warmup_failed", session=meta_json.stem, error=str(exc)
        )
        return {
            "session_stem": meta_json.stem,
            "phone": phone,
            "status": "Offline",
            "ok": False,
            "error": str(exc),
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


@registry.register("telegram.run_warmup")
async def run_staged_session_warmup(parameters: dict[str, Any]) -> dict[str, Any]:
    t0 = time.monotonic()
    staged_dir = Path(parameters.get("staged_dir", str(staged_accounts_root())))
    max_sessions = int(parameters.get("max_sessions", 10))
    if max_sessions < 1:
        max_sessions = 1

    vault_raw = parameters.get("__vault_string_sessions__")
    use_vault = "__vault_string_sessions__" in parameters
    vault_creds: list[dict[str, Any]] = (
        list(vault_raw) if use_vault and isinstance(vault_raw, list) else []
    )

    if use_vault:
        metas: list[Path] = []
        creds = vault_creds[:max_sessions]
    else:
        creds = []
        metas = discover_session_meta_json_files(staged_dir)[:max_sessions]

    if use_vault and not creds:
        return {
            "status": "no_sessions",
            "staged_dir": str(staged_dir),
            "session_source": "vault",
            "sessions": [],
            "duration_s": round(time.monotonic() - t0, 2),
        }
    if not use_vault and not metas:
        return {
            "status": "no_sessions",
            "staged_dir": str(staged_dir),
            "sessions": [],
            "duration_s": round(time.monotonic() - t0, 2),
        }

    loop = asyncio.get_event_loop()
    results: list[dict[str, Any]] = []
    if use_vault:
        for cred in creds:
            one = await loop.run_in_executor(None, _warm_one_session_string, cred)
            results.append(one)
    else:
        for path in metas:
            one = await loop.run_in_executor(None, _warm_one_session, path)
            results.append(one)

    ok_n = sum(1 for r in results if r.get("ok"))
    log.info(
        "staged_session_warmup_done",
        staged_dir=str(staged_dir),
        attempted=len(results),
        ok=ok_n,
    )
    return {
        "status": "completed",
        "staged_dir": str(staged_dir),
        "session_source": "vault_string_session" if use_vault else "local_meta",
        "sessions": results,
        "warmed_ok": ok_n,
        "duration_s": round(time.monotonic() - t0, 2),
    }
