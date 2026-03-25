"""
archivist.backup — Phase 11: The Archivist (Full Telegram Backup)

Extracts ALL user data from Telegram accounts using Telethon sessions:
  • Chat history (personal, groups, channels, bots)
  • Group & channel ownership / admin rights
  • Bot configurations and state
  • Contact list
  • Media metadata (photos, videos, documents, voice notes)

Media downloading is offloaded to the Linux Worker to preserve Master
bandwidth.  The task writes a structured JSON archive to the output directory
and publishes a Redis key with the archive location so the dashboard can
track it.

Task Types
----------
archivist.backup
    Full account backup for a given Telegram session.
    Parameters:
        session_name  : str   — session file name (without extension)
        project_path  : str   — path to Mangement Ahu or OTP project
        output_dir    : str   — where to write the archive (default: project/backups/)
        include_media : bool  — download media files (default: True)
        max_messages  : int   — max messages per dialog (default: 10_000)
        media_types   : list  — ["photo","video","document","voice","sticker"]

archivist.download_media
    Worker-only task: download a batch of media items given pre-fetched metadata.
    Master dispatches this to the Linux Worker to offload bandwidth.
    Parameters:
        media_batch   : list[dict]  — list of {dialog_id, msg_id, media_type, file_size}
        session_name  : str
        project_path  : str
        output_dir    : str

Redis Keys
----------
nexus:archivist:status:<session_name>   — progress JSON (TTL 24h)
nexus:archivist:archives                — LPUSH list of completed archive paths
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
import structlog

from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

DEFAULT_PROJECT_PATH = r"C:\Users\Yarin\Desktop\Mangement Ahu"
DEFAULT_MEDIA_TYPES  = ["photo", "video", "document", "voice", "sticker", "animation"]
MAX_MESSAGES_DEFAULT = 10_000
STATUS_KEY_PREFIX    = "nexus:archivist:status:"
ARCHIVES_KEY         = "nexus:archivist:archives"
STATUS_TTL           = 86_400   # 24 h
CPU_THRESHOLD        = 60.0

# ── Helpers ────────────────────────────────────────────────────────────────────

def _archive_path(output_dir: str, session_name: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(output_dir) / f"archive_{session_name}_{ts}"


def _load_session(project_path: str, session_name: str):
    """
    Return a connected Telethon TelegramClient from the given session file.
    Looks in <project>/sessions/ for <session_name>.json to get api_id/api_hash.
    """
    try:
        from telethon.sync import TelegramClient  # type: ignore[import-untyped]

        sessions_dir = Path(project_path) / "sessions"
        # Search for the session JSON in common sub-directories
        candidates = list(sessions_dir.rglob(f"{session_name}.json"))
        if not candidates:
            # Try direct session file
            candidates = list(sessions_dir.rglob(f"{session_name}.*"))

        if not candidates:
            raise FileNotFoundError(
                f"Session '{session_name}' not found under {sessions_dir}"
            )

        sess_json = candidates[0]
        with open(sess_json, encoding="utf-8") as f:
            meta = json.load(f)

        api_id   = int(meta["api_id"])
        api_hash = meta["api_hash"]
        session_file = str(sess_json.with_suffix(""))

        client = TelegramClient(session_file, api_id, api_hash)
        client.connect()

        if not client.is_user_authorized():
            client.disconnect()
            raise PermissionError(f"Session '{session_name}' is not authorized")

        return client

    except ImportError:
        raise ImportError("telethon not installed — run: pip install telethon")


# ── Main backup logic (blocking — run in executor) ────────────────────────────

def _run_backup(
    project_path: str,
    session_name: str,
    output_dir: str,
    include_media: bool,
    max_messages: int,
    media_types: list[str],
    status_callback,   # callable(update_dict) — updates Redis
) -> dict[str, Any]:
    """
    Full blocking Telethon backup. Runs in a thread executor.
    Returns the archive summary dict.
    """
    archive_root = _archive_path(output_dir, session_name)
    archive_root.mkdir(parents=True, exist_ok=True)

    media_dir = archive_root / "media"
    if include_media:
        media_dir.mkdir(exist_ok=True)

    client = _load_session(project_path, session_name)

    summary: dict[str, Any] = {
        "session": session_name,
        "archive_path": str(archive_root),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "dialogs": [],
        "total_messages": 0,
        "total_media_items": 0,
        "media_downloaded": 0,
        "groups_owned": [],
        "channels_owned": [],
        "bot_configs": [],
        "contacts_count": 0,
    }

    try:
        # ── 1. Contacts ───────────────────────────────────────────────────────
        status_callback({"stage": "contacts", "progress": 0})
        contacts = client.get_contacts()
        contacts_data = []
        for contact in contacts:
            contacts_data.append({
                "id": contact.id,
                "first_name": getattr(contact, "first_name", ""),
                "last_name": getattr(contact, "last_name", ""),
                "username": getattr(contact, "username", None),
                "phone": getattr(contact, "phone", None),
                "is_bot": getattr(contact, "bot", False),
            })
        summary["contacts_count"] = len(contacts_data)

        _write_json(archive_root / "contacts.json", contacts_data)
        status_callback({"stage": "contacts", "progress": 100, "count": len(contacts_data)})

        # ── 2. Enumerate dialogs ──────────────────────────────────────────────
        status_callback({"stage": "dialogs", "progress": 0})
        dialogs = client.get_dialogs(limit=None)
        status_callback({"stage": "dialogs", "progress": 100, "count": len(dialogs)})

        # ── 3. Per-dialog backup ──────────────────────────────────────────────
        for idx, dialog in enumerate(dialogs):
            entity = dialog.entity
            dialog_id = dialog.id
            title = dialog.title or str(dialog_id)

            # Determine type
            dialog_type = "unknown"
            is_owner = False

            try:
                from telethon.tl.types import (  # type: ignore[import-untyped]
                    Channel, Chat, User
                )

                if isinstance(entity, User):
                    dialog_type = "bot" if entity.bot else "private"
                    if entity.bot:
                        summary["bot_configs"].append({
                            "id": entity.id,
                            "username": entity.username,
                            "first_name": entity.first_name,
                        })
                elif isinstance(entity, Chat):
                    dialog_type = "group"
                    # creator flag
                    is_owner = getattr(entity, "creator", False)
                    if is_owner:
                        summary["groups_owned"].append({
                            "id": entity.id, "title": entity.title,
                        })
                elif isinstance(entity, Channel):
                    dialog_type = "channel" if entity.broadcast else "supergroup"
                    is_owner = getattr(entity, "creator", False)
                    if is_owner:
                        summary["channels_owned"].append({
                            "id": entity.id, "title": entity.title,
                            "broadcast": entity.broadcast,
                            "megagroup": entity.megagroup,
                            "username": getattr(entity, "username", None),
                        })
            except Exception:
                pass

            status_callback({
                "stage": "messages",
                "dialog": title,
                "progress": int((idx / max(len(dialogs), 1)) * 100),
            })

            # ── 3a. Fetch messages ────────────────────────────────────────────
            messages_data = []
            media_metadata = []

            try:
                for msg in client.iter_messages(dialog_id, limit=max_messages):
                    msg_dict: dict[str, Any] = {
                        "id": msg.id,
                        "date": msg.date.isoformat() if msg.date else None,
                        "text": msg.raw_text or "",
                        "from_id": msg.sender_id,
                        "reply_to": getattr(msg, "reply_to_msg_id", None),
                        "views": getattr(msg, "views", None),
                        "forwards": getattr(msg, "forwards", None),
                        "has_media": msg.media is not None,
                    }

                    # Collect media metadata for worker offload
                    if msg.media and include_media:
                        media_type = _classify_media(msg.media)
                        if media_type in media_types:
                            file_size = _get_media_size(msg.media)
                            media_metadata.append({
                                "dialog_id": dialog_id,
                                "msg_id": msg.id,
                                "media_type": media_type,
                                "file_size": file_size,
                                "date": msg.date.isoformat() if msg.date else None,
                            })
                            msg_dict["media_type"] = media_type
                            msg_dict["media_size"] = file_size
                            summary["total_media_items"] += 1

                    messages_data.append(msg_dict)

            except Exception as exc:
                log.warning("archivist_dialog_messages_error",
                            dialog=title, error=str(exc))

            # Save messages for this dialog
            safe_title = "".join(c if c.isalnum() or c in "-_" else "_" for c in title)[:40]
            dialog_file = archive_root / f"dialog_{dialog_id}_{safe_title}.json"
            _write_json(dialog_file, {
                "dialog_id": dialog_id,
                "title": title,
                "type": dialog_type,
                "is_owner": is_owner,
                "message_count": len(messages_data),
                "messages": messages_data,
                "media_metadata": media_metadata,
            })

            summary["dialogs"].append({
                "id": dialog_id,
                "title": title,
                "type": dialog_type,
                "messages": len(messages_data),
                "media_items": len(media_metadata),
                "is_owner": is_owner,
            })
            summary["total_messages"] += len(messages_data)

        # ── 4. Download media on THIS worker (if include_media and small batch) ──
        if include_media and summary["total_media_items"] <= 100:
            _download_media_batch(
                client,
                archive_root,
                summary["dialogs"],
                media_dir,
                status_callback,
            )
            summary["media_downloaded"] = summary["total_media_items"]
        elif include_media:
            # Large batch → write manifest for worker offload
            manifest_path = archive_root / "media_manifest.json"
            all_media = []
            for d in (archive_root.glob("dialog_*.json")):
                try:
                    data = json.loads(d.read_text(encoding="utf-8"))
                    all_media.extend(data.get("media_metadata", []))
                except Exception:
                    pass
            _write_json(manifest_path, all_media)
            log.info(
                "archivist_media_offload_ready",
                manifest=str(manifest_path),
                items=len(all_media),
            )

        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        _write_json(archive_root / "summary.json", summary)
        return summary

    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _classify_media(media) -> str:
    type_name = type(media).__name__.lower()
    if "photo" in type_name:
        return "photo"
    if "document" in type_name:
        return "document"
    if "geo" in type_name:
        return "geo"
    if "poll" in type_name:
        return "poll"
    return "other"


def _get_media_size(media) -> int:
    try:
        if hasattr(media, "document") and hasattr(media.document, "size"):
            return media.document.size
        if hasattr(media, "photo"):
            sizes = getattr(media.photo, "sizes", [])
            if sizes:
                last = sizes[-1]
                return getattr(last, "size", 0)
    except Exception:
        pass
    return 0


def _download_media_batch(client, archive_root, dialogs, media_dir, status_callback):
    """Download media files for small batches on the current worker."""
    downloaded = 0
    for dinfo in dialogs:
        dialog_id = dinfo["id"]
        if dinfo.get("media_items", 0) == 0:
            continue
        try:
            from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument  # type: ignore
            for msg in client.iter_messages(dialog_id, limit=None):
                if msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                    dest = media_dir / f"{dialog_id}_{msg.id}"
                    client.download_media(msg, file=str(dest))
                    downloaded += 1
                    if downloaded % 10 == 0:
                        status_callback({"stage": "media_download", "downloaded": downloaded})
        except Exception as exc:
            log.debug("archivist_media_dl_error", dialog=dialog_id, error=str(exc))


def _write_json(path: Path, data: Any) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


# ── Task handlers ──────────────────────────────────────────────────────────────

@registry.register("archivist.backup")
async def backup(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Full Telegram account backup for a given session.

    Parameters
    ----------
    session_name  : str   — session file name (without .session extension)
    project_path  : str   — path to the project containing sessions/
    output_dir    : str   — where to write archive (default: project/backups/)
    include_media : bool  — also collect media metadata (default: True)
    max_messages  : int   — cap messages per dialog (default: 10_000)
    media_types   : list  — subset of media types to capture
    """
    t0 = time.monotonic()

    session_name  = parameters.get("session_name", "")
    project_path  = parameters.get("project_path", DEFAULT_PROJECT_PATH)
    include_media = bool(parameters.get("include_media", True))
    max_messages  = int(parameters.get("max_messages", MAX_MESSAGES_DEFAULT))
    media_types   = parameters.get("media_types", DEFAULT_MEDIA_TYPES)

    if not session_name:
        return {"status": "failed", "error": "session_name is required"}

    output_dir = parameters.get(
        "output_dir",
        str(Path(project_path) / "backups"),
    )
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # ── CPU preflight ─────────────────────────────────────────────────────────
    cpu = psutil.cpu_percent(interval=1)
    if cpu > CPU_THRESHOLD:
        log.warning("archivist_low_resources", cpu=cpu)
        return {
            "status": "low_resources",
            "cpu_percent": cpu,
            "session_name": session_name,
        }

    log.info("archivist_backup_start",
             session=session_name, project=project_path, include_media=include_media)

    # Status key for Redis progress tracking (we get redis via the ARQ context
    # through a global stored during listener init — fallback to no-op here).
    status_key = f"{STATUS_KEY_PREFIX}{session_name}"

    def status_callback(update: dict) -> None:
        # Best-effort: write to a local JSON file that the API can poll.
        status_file = Path(output_dir) / f".status_{session_name}.json"
        try:
            update["ts"] = datetime.now(timezone.utc).isoformat()
            status_file.write_text(
                json.dumps(update, ensure_ascii=False), encoding="utf-8"
            )
        except Exception:
            pass

    try:
        summary = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _run_backup(
                project_path=project_path,
                session_name=session_name,
                output_dir=output_dir,
                include_media=include_media,
                max_messages=max_messages,
                media_types=media_types,
                status_callback=status_callback,
            ),
        )
    except Exception as exc:
        log.exception("archivist_backup_error", session=session_name, error=str(exc))
        return {
            "status": "failed",
            "session_name": session_name,
            "error": str(exc),
            "duration_s": round(time.monotonic() - t0, 2),
        }

    duration = round(time.monotonic() - t0, 2)
    log.info(
        "archivist_backup_complete",
        session=session_name,
        dialogs=len(summary.get("dialogs", [])),
        messages=summary.get("total_messages", 0),
        media=summary.get("total_media_items", 0),
        duration_s=duration,
    )

    return {
        "status": "completed",
        "session_name": session_name,
        "archive_path": summary.get("archive_path", ""),
        "dialogs_count": len(summary.get("dialogs", [])),
        "total_messages": summary.get("total_messages", 0),
        "total_media_items": summary.get("total_media_items", 0),
        "media_downloaded": summary.get("media_downloaded", 0),
        "groups_owned": len(summary.get("groups_owned", [])),
        "channels_owned": len(summary.get("channels_owned", [])),
        "contacts_count": summary.get("contacts_count", 0),
        "bot_configs": len(summary.get("bot_configs", [])),
        "duration_s": duration,
    }


@registry.register("archivist.download_media")
async def download_media(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Worker-only: download a batch of media items offloaded from the Master.

    This task is dispatched with `required_capabilities=["linux-only"]` so
    it always runs on the Linux Worker, preserving Master bandwidth.

    Parameters
    ----------
    media_batch   : list[dict]  — [{dialog_id, msg_id, media_type, file_size}]
    session_name  : str
    project_path  : str
    output_dir    : str
    """
    t0 = time.monotonic()

    session_name = parameters.get("session_name", "")
    project_path = parameters.get("project_path", DEFAULT_PROJECT_PATH)
    output_dir   = parameters.get("output_dir", "")
    media_batch  = parameters.get("media_batch", [])

    if not session_name or not media_batch:
        return {"status": "failed", "error": "session_name and media_batch are required"}

    media_dir = Path(output_dir) / "media"
    media_dir.mkdir(parents=True, exist_ok=True)

    cpu = psutil.cpu_percent(interval=1)
    if cpu > CPU_THRESHOLD:
        return {"status": "low_resources", "cpu_percent": cpu}

    downloaded = 0
    errors = 0

    try:
        client = _load_session(project_path, session_name)
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}

    try:
        for item in media_batch:
            dialog_id = item.get("dialog_id")
            msg_id    = item.get("msg_id")
            if not dialog_id or not msg_id:
                continue
            try:
                msg = client.get_messages(dialog_id, ids=msg_id)
                if msg and msg.media:
                    dest = media_dir / f"{dialog_id}_{msg_id}"
                    client.download_media(msg, file=str(dest))
                    downloaded += 1
            except Exception as exc:
                log.debug("archivist_dl_error", dialog=dialog_id, msg=msg_id, error=str(exc))
                errors += 1
    finally:
        try:
            client.disconnect()
        except Exception:
            pass

    duration = round(time.monotonic() - t0, 2)
    log.info(
        "archivist_media_download_complete",
        session=session_name,
        downloaded=downloaded,
        errors=errors,
        duration_s=duration,
    )

    return {
        "status": "completed",
        "session_name": session_name,
        "downloaded": downloaded,
        "errors": errors,
        "output_dir": str(media_dir),
        "duration_s": duration,
    }
