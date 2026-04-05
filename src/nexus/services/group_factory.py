"""
Private-group warm-up → public indexing loop (master-side state machine).

Uses Hebrew naming hints from ``vault/config/group_names.json``, merges with
live ``nexus:swarm:warmer:groups`` entries plus **telefix.db** ``managed_groups``
rows (public ``t.me`` targets), and keeps durable state in
``vault/data/group_factory_state.json``. If ``group_factory_settings.json``
exists, ticks are skipped while ``automation_armed`` is false; if that file is
missing, the loop keeps the historical always-on behaviour.

Phases
------
* Days 0–13 from first seen: **warmup** (stay private).
* Day 14+: **public_trial** — target public visibility; probe ``https://t.me/{username}``.
* If not visible: **private_cooldown** for 24 hours, then retry public_trial.

Telegram visibility toggles are not executed here (no MTProto from this module);
the worker / operator applies changes; this service tracks schedule and probes
public indexing.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog

from nexus.services.swarm_social_scheduler import SWARM_GROUPS_KEY
from nexus.shared.db_util import TELEFIX_DB_PATH

log = structlog.get_logger(__name__)

_REPO = Path(__file__).resolve().parents[3]
_GROUP_NAMES_PATH = _REPO / "vault" / "config" / "group_names.json"
_STATE_PATH = _REPO / "vault" / "data" / "group_factory_state.json"
_FACTORY_SETTINGS_PATH = _REPO / "vault" / "data" / "group_factory_settings.json"
_UI_KEY = "nexus:ui:group_factory"
_WARMUP_DAYS = 14
_COOLDOWN_HOURS = 24


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_group_names_config() -> dict[str, Any]:
    if not _GROUP_NAMES_PATH.is_file():
        return {
            "convention": "he_IL",
            "private_title_template": "{name} — פרטי",
            "public_title_template": "{name} — ציבורי",
            "name_pool": [],
        }
    try:
        data = json.loads(_GROUP_NAMES_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        log.warning("group_names_json_invalid", error=str(exc))
        return {}


def hebrew_title_for_phase(base_name: str, public: bool, cfg: dict[str, Any]) -> str:
    key = "public_title_template" if public else "private_title_template"
    tpl = str(cfg.get(key) or ("{name} — ציבורי" if public else "{name} — פרטי"))
    return tpl.replace("{name}", base_name).strip()


def _load_state() -> dict[str, Any]:
    if not _STATE_PATH.is_file():
        return {"groups": {}}
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"groups": {}}
    except Exception:
        return {"groups": {}}


def _save_state(state: dict[str, Any]) -> None:
    _ensure_parent(_STATE_PATH)
    _STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def _factory_automation_armed() -> bool:
    # No settings file yet → keep legacy behaviour (tick always ran before this flag existed).
    if not _FACTORY_SETTINGS_PATH.is_file():
        return True
    try:
        data = json.loads(_FACTORY_SETTINGS_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return True
        armed = data.get("automation_armed")
        if armed is None:
            return True
        return bool(armed)
    except Exception:
        return True


def _sqlite_table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def _invite_for_managed_row(joined_invite: Any, username: Any) -> str | None:
    if joined_invite:
        s = str(joined_invite).strip()
        if s:
            return s
    if username:
        un = str(username).strip().lstrip("@")
        if un:
            return f"https://t.me/{un}"
    return None


def _usable_tme_public_link(invite: str | None) -> bool:
    if not invite:
        return False
    low = invite.lower()
    return "t.me/" in low or "telegram.me/" in low


def _username_from_invite(invite: str) -> str:
    low = invite.lower()
    for marker in ("t.me/", "telegram.me/"):
        if marker in low:
            rest = low.split(marker, 1)[1].split("?", 1)[0].strip().strip("/")
            if "/" not in rest:
                return rest
    return ""


def _load_telefix_managed_factory_seed() -> dict[str, dict[str, Any]]:
    """
    Mirror telefix ``managed_groups`` (public t.me targets) into the same shape as
    ``nexus:swarm:warmer:groups`` so the warm-up / probe loop runs without Redis prep.
    """
    db_path = TELEFIX_DB_PATH
    if not db_path.is_file():
        return {}
    out: dict[str, dict[str, Any]] = {}
    try:
        conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            if not _sqlite_table_exists(cur, "managed_groups"):
                return {}
            if _sqlite_table_exists(cur, "groups"):
                sql = """
                    SELECT mg.group_id, mg.title, mg.username,
                           g.invite_link AS joined_invite
                    FROM managed_groups mg
                    LEFT JOIN groups g ON CAST(mg.group_id AS TEXT) = CAST(g.id AS TEXT)
                """
            else:
                sql = """
                    SELECT mg.group_id, mg.title, mg.username, NULL AS joined_invite
                    FROM managed_groups mg
                """
            cur.execute(sql)
            for row in cur.fetchall():
                invite = _invite_for_managed_row(row["joined_invite"], row["username"])
                if not _usable_tme_public_link(invite):
                    continue
                un = str(row["username"] or "").strip().lstrip("@")
                if not un:
                    un = _username_from_invite(invite or "")
                if not un:
                    continue
                gid = row["group_id"]
                key = f"mg:{gid}"
                title = str(row["title"] or "").strip() or key
                out[key] = {
                    "group_title": title,
                    "public_username": un,
                    "username": un,
                    "enabled": True,
                }
        finally:
            conn.close()
    except Exception as exc:
        log.warning("group_factory_telefix_seed_failed", error=str(exc))
    return out


async def _probe_tme_public(username: str) -> bool:
    u = (username or "").strip().lstrip("@")
    if not u:
        return False
    url = f"https://t.me/{u}"
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": "NexusGroupFactory/1.0"})
            if r.status_code != 200:
                return False
            text = (r.text or "").lower()
            if "tgme_page_extra" in text or "telegram-channel" in text:
                return True
            return "peer" in text and "telegram" in text
    except Exception as exc:
        log.debug("group_factory_tme_probe_failed", username=u, error=str(exc))
        return False


class GroupFactoryService:
    def __init__(self, redis: Any) -> None:
        self._redis = redis

    async def tick(self) -> None:
        if not _factory_automation_armed():
            log.debug("group_factory_tick_skipped_disarmed")
            return

        cfg = load_group_names_config()
        raw = await self._redis.get(SWARM_GROUPS_KEY)
        groups_cfg: dict[str, Any] = {}
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    groups_cfg = parsed
            except Exception:
                pass

        telefix_seed = _load_telefix_managed_factory_seed()
        for k, v in telefix_seed.items():
            groups_cfg.setdefault(k, v)

        state = _load_state()
        gmap: dict[str, Any] = state.setdefault("groups", {})
        for stale in list(gmap.keys()):
            sk = str(stale)
            if sk.startswith("mg:") and sk not in telefix_seed:
                gmap.pop(stale, None)

        now = datetime.now(timezone.utc)
        ui_rows: list[dict[str, Any]] = []

        for gkey, wc in groups_cfg.items():
            if not isinstance(wc, dict):
                continue
            base_title = str(wc.get("group_title") or gkey)
            username = str(wc.get("public_username") or wc.get("username") or "").strip()

            st = gmap.get(gkey)
            if not isinstance(st, dict):
                st = {}
                gmap[gkey] = st
            if not st.get("birth_ts"):
                st["birth_ts"] = now.isoformat()

            birth = _parse_iso(st.get("birth_ts")) or now
            days = max(0, (now - birth).days)
            phase = str(st.get("phase") or "warmup")

            cooldown_until = _parse_iso(st.get("cooldown_until"))
            if cooldown_until and now < cooldown_until:
                phase = "private_cooldown"
            elif phase == "private_cooldown" and cooldown_until and now >= cooldown_until:
                phase = "public_trial"
                st.pop("cooldown_until", None)

            if days < _WARMUP_DAYS:
                phase = "warmup"
                st["visibility_target"] = "private"
                st["display_title_hint"] = hebrew_title_for_phase(base_title, False, cfg)
            else:
                if phase == "warmup":
                    phase = "public_trial"
                st["visibility_target"] = "public" if phase == "public_trial" else "private"
                st["display_title_hint"] = hebrew_title_for_phase(
                    base_title, phase == "public_trial", cfg
                )

            indexed: bool | None = None
            if phase == "public_trial" and username:
                indexed = await _probe_tme_public(username)
                st["last_index_probe_at"] = now.isoformat()
                st["search_indexed"] = indexed
                if indexed is False:
                    st["phase"] = "private_cooldown"
                    st["cooldown_until"] = (now + timedelta(hours=_COOLDOWN_HOURS)).isoformat()
                    st["visibility_target"] = "private"
                else:
                    st["phase"] = "public_trial"
            else:
                st["phase"] = phase
                if phase != "public_trial":
                    st.pop("search_indexed", None)

            ui_rows.append(
                {
                    "group_key": gkey,
                    "days_since_birth": days,
                    "phase": st.get("phase"),
                    "visibility_target": st.get("visibility_target"),
                    "username": username or None,
                    "search_indexed": st.get("search_indexed"),
                    "title_hint": st.get("display_title_hint"),
                }
            )

        state["updated_at"] = now.isoformat()
        state["name_pool"] = cfg.get("name_pool", [])
        _save_state(state)

        payload = {
            "updated_at": now.isoformat(),
            "warmup_days": _WARMUP_DAYS,
            "cooldown_hours": _COOLDOWN_HOURS,
            "groups": ui_rows,
        }
        await self._redis.set(_UI_KEY, json.dumps(payload, ensure_ascii=False), ex=600)

    async def run_loop(self, interval_s: float = 300.0) -> None:
        import asyncio

        log.info("group_factory_service_started", interval_s=interval_s)
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("group_factory_tick_failed", error=str(exc))
            await asyncio.sleep(interval_s)
