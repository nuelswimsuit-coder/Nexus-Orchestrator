"""
Telethon-backed creation of Telegram broadcast channels with a starter “menu HUD”.

Uses vault sessions (same layout as :mod:`nexus.services.session_vault`).
Environment (optional):

* ``NEXUS_MENU_FACTORY_SESSION`` — session path base (no ``.session`` suffix); default: first vault session.
* ``NEXUS_MENU_FACTORY_PUBLIC`` — ``1``/``true`` to publish a public ``t.me/<username>`` link.
* ``NEXUS_MENU_FACTORY_USERNAME`` — username without ``@`` (required when public).
* ``NEXUS_MENU_FACTORY_PHOTO`` — image path for channel photo.
* ``TELEGRAM_DASHBOARD_URL`` / ``NEXUS_MENU_FACTORY_DASHBOARD_URL`` — URL for default HUD buttons.

Redis (for :mod:`scripts.node_monitor`): ``nexus:menu_factory:deploy_seq`` + ``nexus:menu_factory:last_deploy``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import structlog

from nexus.services.session_vault import discover_all_meta_json_files

log = structlog.get_logger(__name__)

_REPO = Path(__file__).resolve().parents[3]

REDIS_DEPLOY_SEQ_KEY = "nexus:menu_factory:deploy_seq"
REDIS_LAST_DEPLOY_KEY = "nexus:menu_factory:last_deploy"


def _read_meta(meta_json: Path) -> tuple[int, str]:
    data = json.loads(meta_json.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("invalid session json")
    return int(data["api_id"]), str(data["api_hash"])


def _resolve_session() -> tuple[str, int, str]:
    override = (os.getenv("NEXUS_MENU_FACTORY_SESSION") or "").strip()
    if override:
        base = override
        meta = Path(base + ".json")
        if not meta.is_file():
            raise FileNotFoundError(f"missing meta json for NEXUS_MENU_FACTORY_SESSION={base}")
        api_id, api_hash = _read_meta(meta)
        if not Path(base + ".session").is_file():
            raise FileNotFoundError(f"missing .session for {base}")
        return base, api_id, api_hash

    for meta_path in discover_all_meta_json_files():
        base = str(meta_path.with_suffix(""))
        if Path(base + ".session").is_file():
            api_id, api_hash = _read_meta(meta_path)
            return base, api_id, api_hash

    raise RuntimeError("no Telethon user session found for menu factory (vault empty or unpaired)")


def _truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _dashboard_url() -> str:
    return (
        (os.getenv("NEXUS_MENU_FACTORY_DASHBOARD_URL") or "").strip()
        or (os.getenv("TELEGRAM_DASHBOARD_URL") or "").strip()
        or "http://localhost:8001/nexus-os"
    )


def _photo_path() -> Path | None:
    raw = (os.getenv("NEXUS_MENU_FACTORY_PHOTO") or "").strip()
    if not raw:
        cfg = _REPO / "vault" / "config" / "menu_factory.json"
        if cfg.is_file():
            try:
                data = json.loads(cfg.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    p = str(data.get("photo_path") or "").strip()
                    if p:
                        raw = p
            except Exception:
                pass
    if not raw:
        return None
    p = Path(raw).expanduser()
    return p if p.is_file() else None


def _default_hud_rows(name: str, dash: str) -> list[list[dict[str, str]]]:
    slug = "".join(c if c.isalnum() else "_" for c in name)[:48] or "menu"
    return [
        [
            {"type": "url", "text": "Dashboard", "url": dash},
            {"type": "url", "text": "Docs", "url": "https://core.telegram.org/bots"},
        ],
        [
            {"type": "callback", "text": "Ping", "data": f"mf:{slug}:ping"},
        ],
    ]


def _load_hud_spec(name: str, dash: str) -> list[dict[str, Any]]:
    raw_json = (os.getenv("NEXUS_MENU_FACTORY_HUD_JSON") or "").strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except Exception:
            log.warning("menu_factory_hud_json_invalid")

    cfg = _REPO / "vault" / "config" / "menu_factory.json"
    if cfg.is_file():
        try:
            data = json.loads(cfg.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("hud_messages"), list):
                return [x for x in data["hud_messages"] if isinstance(x, dict)]
        except Exception:
            log.warning("menu_factory_config_invalid", path=str(cfg))

    return [
        {
            "text": f"{name} — Nexus menu HUD\n\nTap a link below.",
            "rows": _default_hud_rows(name, dash),
        },
        {
            "text": "Assets & tools",
            "rows": [
                [{"type": "url", "text": "API (local)", "url": "http://127.0.0.1:8000/docs"}],
            ],
        },
    ]


def _buttons_from_rows(
    rows: list[list[dict[str, Any]]],
    *,
    name: str,
    dash: str,
) -> list[list[Any]]:
    from telethon.tl.custom import Button  # type: ignore[import-untyped]

    out: list[list[Any]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        btn_row: list[Any] = []
        for cell in row:
            if not isinstance(cell, dict):
                continue
            kind = str(cell.get("type") or "url").lower()
            label = str(cell.get("text") or "Link")[:64]
            if kind == "url":
                url_t = str(cell.get("url") or dash).format(name=name, dashboard_url=dash)
                btn_row.append(Button.url(label, url_t))
            elif kind == "callback":
                data_s = str(cell.get("data") or "mf:noop")[:64]
                btn_row.append(Button.inline(label, data_s))
        if btn_row:
            out.append(btn_row)
    return out


async def _redis_publish_deploy(link: str, name: str) -> int | None:
    try:
        from nexus.shared.config import settings  # local import: pulls redis_url

        import redis.asyncio as aioredis  # type: ignore[import]

        parsed = urlparse(settings.redis_url)
        host = parsed.hostname or "127.0.0.1"
        if host == "localhost":
            host = "127.0.0.1"
        port = parsed.port or 6379
        db = 0
        if parsed.path and parsed.path != "/":
            try:
                db = int(parsed.path.lstrip("/"))
            except ValueError:
                db = 0
        password = unquote(parsed.password) if parsed.password else None
        username = unquote(parsed.username) if parsed.username else None
        use_ssl = parsed.scheme in {"rediss", "redis+ssl"}
        r = aioredis.Redis(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            ssl=use_ssl,
            socket_connect_timeout=2,
            decode_responses=True,
        )
        try:
            seq = int(await r.incr(REDIS_DEPLOY_SEQ_KEY))
            payload = json.dumps(
                {
                    "seq": seq,
                    "link": link,
                    "name": name,
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
                ensure_ascii=False,
            )
            await r.set(REDIS_LAST_DEPLOY_KEY, payload)
            return seq
        finally:
            await r.aclose()
    except Exception as exc:
        log.debug("menu_factory_redis_publish_skipped", error=str(exc))
        return None


class MenuChannelFactory:
    """
    Async Telethon workflow: create a broadcast channel, optional photo, optional public
    username, then post HUD messages with inline keyboards (URL + callback rows).
    """

    def __init__(self, *, session_base: str | None = None) -> None:
        self._session_override = session_base

    @classmethod
    async def create_full_setup(cls, name: str) -> dict[str, Any]:
        return await cls()._create_full_setup_impl(name)

    async def _create_full_setup_impl(self, raw_name: str) -> dict[str, Any]:
        name = (raw_name or "").strip()
        if len(name) < 2:
            raise ValueError("name must be at least 2 characters")

        if self._session_override:
            base = self._session_override
            meta = Path(base + ".json")
            api_id, api_hash = _read_meta(meta)
            if not Path(base + ".session").is_file():
                raise FileNotFoundError(f"missing .session for {base}")
        else:
            base, api_id, api_hash = _resolve_session()

        title = name[:128]
        about = (
            (os.getenv("NEXUS_MENU_FACTORY_ABOUT") or "").strip()
            or f"Nexus menu hub · {name}"
        )[:255]

        public = _truthy(os.getenv("NEXUS_MENU_FACTORY_PUBLIC"))
        username = (os.getenv("NEXUS_MENU_FACTORY_USERNAME") or "").strip().lstrip("@")
        if public:
            if not username or len(username) < 5:
                raise ValueError("NEXUS_MENU_FACTORY_USERNAME required when NEXUS_MENU_FACTORY_PUBLIC is set")

        from telethon import TelegramClient  # type: ignore[import-untyped]
        from telethon.tl.functions.channels import (  # type: ignore[import-untyped]
            CreateChannelRequest,
            EditPhotoRequest,
            UpdateUsernameRequest,
        )
        from telethon.tl.types import Channel, InputChatUploadedPhoto  # type: ignore[import-untyped]

        dash = _dashboard_url()
        hud_messages = _load_hud_spec(name, dash)
        photo = _photo_path()

        async with TelegramClient(base, api_id, api_hash) as client:
            if not await client.is_user_authorized():
                raise RuntimeError(f"session not authorized: {base}")

            created = await client(
                CreateChannelRequest(title=title, about=about, megagroup=False, broadcast=True)
            )
            chats = list(getattr(created, "chats", None) or [])
            ch = next((c for c in chats if isinstance(c, Channel)), None)
            if ch is None and chats:
                ch = chats[0]
            if ch is None:
                raise RuntimeError("CreateChannelRequest returned no channel")

            if photo is not None:
                uploaded = await client.upload_file(str(photo))
                inp = await client.get_input_entity(ch)
                await client(
                    EditPhotoRequest(channel=inp, photo=InputChatUploadedPhoto(file=uploaded))
                )

            invite_link: str
            if public:
                inp = await client.get_input_entity(ch)
                await client(UpdateUsernameRequest(channel=inp, username=username))
                invite_link = f"https://t.me/{username}"
            else:
                invite_link = await client.export_chat_invite_link(ch)

            for spec in hud_messages:
                text = str(spec.get("text") or "Menu").format(name=name, dashboard_url=dash)
                rows_raw = spec.get("rows")
                buttons: list[list[Any]] = []
                if isinstance(rows_raw, list):
                    buttons = _buttons_from_rows(rows_raw, name=name, dash=dash)
                await client.send_message(ch, text, buttons=buttons or None, link_preview=False)

        await _redis_publish_deploy(invite_link, name)

        log.info("menu_factory_channel_ready", title=title, public=public, link=invite_link)
        return {
            "invite_link": invite_link,
            "title": title,
            "about": about,
            "broadcast": True,
            "public": public,
            "username": username if public else None,
        }
