"""
Mass bot creation via @BotFather using rotating Telethon user sessions.

Every 5 successful (or attempted) creations switches to the next session file
to spread load across IPs. Tokens append to ``vault/data/bots.json``.

Requires Telethon and authorized ``.session`` + meta ``.json`` pairs (same
layout as :mod:`nexus.services.session_vault`).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import discover_all_meta_json_files, vault_root

log = structlog.get_logger(__name__)

_REPO = Path(__file__).resolve().parents[3]
_BOTS_JSON = _REPO / "vault" / "data" / "bots.json"
_ROTATE_EVERY = 5
_BF = "botfather"
_TOKEN_RE = re.compile(r"(\d{8,}:[A-Za-z0-9_-]{30,})")


def _ensure_vault_data() -> None:
    _BOTS_JSON.parent.mkdir(parents=True, exist_ok=True)


def _load_bots() -> list[dict[str, Any]]:
    _ensure_vault_data()
    if not _BOTS_JSON.is_file():
        return []
    try:
        data = json.loads(_BOTS_JSON.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_bots(rows: list[dict[str, Any]]) -> None:
    _ensure_vault_data()
    _BOTS_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def _session_stems() -> list[str]:
    stems: list[str] = []
    for meta in discover_all_meta_json_files():
        base = str(meta.with_suffix(""))
        if Path(base + ".session").is_file():
            stems.append(base)
    if not stems:
        vr = vault_root()
        if vr.is_dir():
            for p in sorted(vr.glob("*.session")):
                stems.append(str(p.with_suffix("")))
    return sorted(set(stems))


def _read_meta(api_json: Path) -> tuple[int, str]:
    data = json.loads(api_json.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("invalid session json")
    return int(data["api_id"]), str(data["api_hash"])


def _botfather_create_sync(
    session_base: str,
    api_id: int,
    api_hash: str,
    display_name: str,
    username: str,
) -> str:
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]

    meta_path = Path(session_base + ".json")
    if not meta_path.is_file():
        raise FileNotFoundError(f"missing meta json for {session_base}")
    user = username.strip()
    if not user.endswith("bot"):
        user = (user + "bot") if not user.endswith("Bot") else user
    client = TelegramClient(session_base, api_id, api_hash)
    token: str | None = None
    with client:
        if not client.is_user_authorized():
            raise RuntimeError(f"session not authorized: {session_base}")
        bf = client.get_entity(_BF)
        client.send_message(bf, "/cancel")
        time.sleep(0.4)
        client.send_message(bf, "/newbot")
        deadline = time.monotonic() + 120.0
        last_sent = 0.0
        while time.monotonic() < deadline:
            time.sleep(1.1)
            msgs = list(client.iter_messages(bf, limit=6))
            texts = [(m.id, (m.message or "").strip()) for m in reversed(msgs)]
            if token is None:
                for _mid, t in texts:
                    m = _TOKEN_RE.search(t.replace("`", ""))
                    if m:
                        token = m.group(1)
                        break
            if token:
                break
            for _mid, t in texts:
                tl = t.lower()
                if "choose a name" in tl or "how are we going to call" in tl:
                    if time.monotonic() - last_sent > 2.0:
                        client.send_message(bf, display_name)
                        last_sent = time.monotonic()
                    break
                username_prompt = (
                    "choose a username" in tl
                    or "must end in `bot`" in tl
                    or "must end in 'bot'" in tl
                )
                if username_prompt:
                    if time.monotonic() - last_sent > 2.0:
                        client.send_message(bf, user)
                        last_sent = time.monotonic()
                    break
        if not token:
            raise RuntimeError("BotFather did not return a token in time")
    return token


class BotFactoryService:
    def __init__(self, redis: Any | None = None) -> None:
        self._redis = redis
        self._redis_queue_key = "nexus:bot_factory:queue"
        self._env_specs: list[dict[str, Any]] = []
        raw = os.getenv("NEXUS_BOT_FACTORY_QUEUE_JSON", "").strip()
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, list):
                    self._env_specs = [x for x in data if isinstance(x, dict)]
            except Exception:
                pass

    def _pop_env_spec(self) -> dict[str, Any] | None:
        if not self._env_specs:
            return None
        return self._env_specs.pop(0)

    async def _redis_pop_batch(self, max_n: int = 3) -> list[dict[str, Any]]:
        if self._redis is None:
            return []
        out: list[dict[str, Any]] = []
        for _ in range(max_n):
            raw = await self._redis.lpop(self._redis_queue_key)
            if not raw:
                break
            try:
                item = json.loads(raw)
                if isinstance(item, dict):
                    out.append(item)
            except Exception:
                continue
        return out

    async def process_one(
        self,
        display_name: str,
        username: str,
        session_index: int,
    ) -> dict[str, Any]:
        stems = _session_stems()
        if not stems:
            raise RuntimeError("no Telethon session files found for BotFather")
        idx = session_index % len(stems)
        base = stems[idx]
        meta_json = Path(base + ".json")
        api_id, api_hash = _read_meta(meta_json)

        loop = asyncio.get_running_loop()
        token = await loop.run_in_executor(
            None,
            lambda: _botfather_create_sync(base, api_id, api_hash, display_name, username),
        )
        row = {
            "display_name": display_name,
            "username": username.strip(),
            "token": token,
            "session_stem": base,
            "session_index": idx,
        }
        bots = _load_bots()
        bots.append(row)
        _save_bots(bots)
        log.info("bot_factory_token_saved", username=row["username"], session_index=idx)
        return row

    async def run_loop(self, interval_s: float = 45.0) -> None:
        log.info("bot_factory_service_started", rotate_every=_ROTATE_EVERY)
        created = 0
        session_cursor = 0

        while True:
            try:
                batch = await self._redis_pop_batch(2)
                if not batch:
                    one = self._pop_env_spec()
                    batch = [one] if one else []
                for spec in batch:
                    name = str(spec.get("display_name") or spec.get("name") or "").strip()
                    user = str(spec.get("username") or "").strip()
                    if not name or not user:
                        continue
                    try:
                        await self.process_one(name, user, session_cursor)
                        created += 1
                        if created % _ROTATE_EVERY == 0:
                            session_cursor += 1
                    except Exception as exc:
                        log.warning(
                            "bot_factory_item_failed",
                            display_name=name,
                            error=str(exc),
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("bot_factory_tick_failed", error=str(exc))
            await asyncio.sleep(interval_s)
