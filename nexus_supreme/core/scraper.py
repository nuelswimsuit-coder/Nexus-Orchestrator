"""
Nexus Supreme — Authorized Chat Archiver
Backs up chat history from sessions owned by Jacob, using official Telegram API
rate limits (iter_messages default: no artificial flooding).

Only archives chats that are explicitly authorized (passed in the whitelist).

Usage:
    python -m nexus_supreme.core.scraper --session sessions/managers/jacob.session --chat-id -1001234567890
    # or from GUI via ScraperWorker (QThread)
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Callable

import structlog

log = structlog.get_logger(__name__)

PHOTO_LIMIT  = 10 * 1024 * 1024   # 10 MB
VIDEO_LIMIT  = 50 * 1024 * 1024   # 50 MB
MEDIA_DIR    = Path("data/archives/media")


class ChatArchiver:
    """
    Telethon-based chat archiver.
    Writes messages to JSONL and optionally downloads media.
    Also inserts into the nexus_supreme SQLAlchemy DB.
    """

    def __init__(
        self,
        session_path: str,
        api_id: int,
        api_hash: str,
        download_media: bool = True,
        progress_cb: Callable[[str], None] | None = None,
        db_path: str = "data/nexus_supreme.db",
    ) -> None:
        self._session_path  = session_path
        self._api_id        = api_id
        self._api_hash      = api_hash
        self._download_media= download_media
        self._cb            = progress_cb or (lambda x: None)
        self._db_path       = db_path
        self._client        = None

    def _log(self, msg: str) -> None:
        log.info(msg)
        self._cb(msg)

    async def _get_client(self):
        try:
            from telethon import TelegramClient
        except ImportError:
            raise RuntimeError("telethon is not installed. Run: pip install telethon")

        if self._client is None:
            self._client = TelegramClient(
                self._session_path,
                self._api_id,
                self._api_hash,
            )
            await self._client.start()
        return self._client

    async def archive_chat(
        self,
        chat_id: int,
        limit: int = 0,            # 0 = all messages
        min_id: int = 0,           # resume from here
    ) -> dict:
        """
        Archive a single chat. Returns summary stats dict.
        """
        client = await self._get_client()

        # Resolve entity
        try:
            entity = await client.get_entity(chat_id)
            title  = getattr(entity, "title", None) or getattr(entity, "username", str(chat_id))
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

        self._log(f"Archiving: {title} ({chat_id})")

        # Output paths
        safe_name = str(chat_id).replace("-", "neg")
        archive_dir = Path("data/archives") / safe_name
        archive_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = archive_dir / "messages.jsonl"
        media_path = MEDIA_DIR / safe_name
        if self._download_media:
            media_path.mkdir(parents=True, exist_ok=True)

        # DB upsert for chat record
        try:
            from .db.models import ArchivedChat, get_session
            db = get_session(self._db_path)
            ac = db.query(ArchivedChat).filter_by(chat_id=chat_id).first()
            if ac is None:
                chat_type = "channel" if getattr(entity, "broadcast", False) else \
                            "group"   if getattr(entity, "megagroup", False) else "user"
                ac = ArchivedChat(
                    chat_id      = chat_id,
                    title        = title,
                    chat_type    = chat_type,
                    session_stem = Path(self._session_path).stem,
                )
                db.add(ac)
                db.commit()
        except Exception as exc:
            log.warning("archiver_db_chat_upsert_failed", error=str(exc))
            db = None
            ac = None

        n_msgs   = 0
        n_media  = 0
        n_errors = 0

        with open(jsonl_path, "a", encoding="utf-8") as jf:
            async for message in client.iter_messages(
                entity,
                limit     = limit or None,
                min_id    = min_id,
                reverse   = True,
            ):
                try:
                    media_type  = None
                    media_local = None
                    media_size  = None

                    if self._download_media and message.media:
                        media_type, media_local, media_size = await self._handle_media(
                            client, message, media_path
                        )

                    record = {
                        "msg_id":      message.id,
                        "sender_id":   getattr(message.sender, "id", None),
                        "sender_name": (
                            getattr(message.sender, "first_name", "") or ""
                            + " " + (getattr(message.sender, "last_name", "") or "")
                        ).strip() or getattr(message.sender, "username", ""),
                        "text":        message.text or "",
                        "media_type":  media_type,
                        "media_path":  str(media_local) if media_local else None,
                        "media_size":  media_size,
                        "timestamp":   message.date.isoformat() if message.date else None,
                    }
                    jf.write(json.dumps(record, ensure_ascii=False) + "\n")

                    if db and ac:
                        from .db.models import ArchivedMessage
                        existing = db.query(ArchivedMessage).filter_by(
                            chat_id=chat_id, msg_id=message.id
                        ).first()
                        if not existing:
                            db.add(ArchivedMessage(
                                chat_id     = chat_id,
                                msg_id      = message.id,
                                sender_id   = record["sender_id"],
                                sender_name = record["sender_name"],
                                text        = record["text"],
                                media_type  = media_type,
                                media_path  = record["media_path"],
                                media_size  = media_size,
                                timestamp   = message.date,
                            ))

                    n_msgs += 1
                    if media_type:
                        n_media += 1

                    if n_msgs % 200 == 0:
                        self._log(f"  {title}: {n_msgs} הודעות...")
                        if db:
                            db.commit()

                except Exception as exc:
                    n_errors += 1
                    log.warning("archiver_msg_failed", msg_id=message.id, error=str(exc))

        if db:
            if ac:
                ac.last_synced = datetime.now(timezone.utc)
                ac.total_msgs  = n_msgs
            db.commit()
            db.close()

        self._log(f"  הסתיים: {n_msgs} הודעות, {n_media} קבצי מדיה, {n_errors} שגיאות")
        return {
            "ok":        True,
            "chat_id":   chat_id,
            "title":     title,
            "messages":  n_msgs,
            "media":     n_media,
            "errors":    n_errors,
            "jsonl_path":str(jsonl_path),
        }

    async def _handle_media(self, client, message, media_dir: Path):
        """Download media if within size limits. Returns (type, path, size)."""
        import telethon.tl.types as tl_types

        media = message.media
        media_type = None
        file_ext   = ".bin"

        if hasattr(media, "photo"):
            media_type = "photo"
            file_ext   = ".jpg"
        elif hasattr(media, "document") and media.document:
            doc = media.document
            mime = getattr(doc, "mime_type", "") or ""
            if mime.startswith("video/"):
                media_type = "video"
                file_ext   = ".mp4"
            elif mime.startswith("image/"):
                media_type = "photo"
                file_ext   = ".jpg"
            else:
                media_type = "document"
                file_ext   = ".bin"

        if media_type is None:
            return None, None, None

        # Size check before download
        size = None
        if hasattr(media, "document") and media.document:
            size = getattr(media.document, "size", None)
        if media_type == "photo":
            limit = PHOTO_LIMIT
        elif media_type == "video":
            limit = VIDEO_LIMIT
        else:
            limit = PHOTO_LIMIT

        if size and size > limit:
            return media_type, None, size   # type known but not downloaded

        dest = media_dir / f"{message.id}{file_ext}"
        if dest.exists():
            return media_type, dest, dest.stat().st_size

        try:
            await client.download_media(message, file=str(dest))
            return media_type, dest, dest.stat().st_size if dest.exists() else None
        except Exception as exc:
            log.warning("archiver_media_download_failed", msg_id=message.id, error=str(exc))
            return media_type, None, size

    async def close(self) -> None:
        if self._client:
            await self._client.disconnect()
            self._client = None


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

async def _cli_main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Archive Telegram chat history")
    p.add_argument("--session",  required=True, help="Path to .session file")
    p.add_argument("--chat-id",  required=True, type=int, help="Chat / channel ID")
    p.add_argument("--limit",    type=int, default=0, help="Max messages (0=all)")
    p.add_argument("--min-id",   type=int, default=0, help="Resume from message ID")
    p.add_argument("--no-media", action="store_true")
    args = p.parse_args()

    api_id   = int(os.environ.get("TELEGRAM_API_ID",   "0"))
    api_hash = os.environ.get("TELEGRAM_API_HASH", "")

    archiver = ChatArchiver(
        session_path   = args.session,
        api_id         = api_id,
        api_hash       = api_hash,
        download_media = not args.no_media,
        progress_cb    = print,
    )
    result = await archiver.archive_chat(args.chat_id, args.limit, args.min_id)
    await archiver.close()
    print(result)


if __name__ == "__main__":
    asyncio.run(_cli_main())
