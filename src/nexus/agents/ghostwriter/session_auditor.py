"""
Session Auditor — connects to each Telegram account one by one (with proxy rotation
and human-like delays), and collects:
  - Account info (phone, username, name, premium status)
  - Groups / channels / bots the account OWNS or ADMINS
  - Member count per entity
  - Real premium member count (from member list scan)
  - Boost-panel premium count (from getChatBoosts)
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from telethon import TelegramClient
from telethon.errors import (
    AuthKeyUnregisteredError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
    FloodWaitError,
    RPCError,
)
from telethon.tl.functions.channels import (
    GetFullChannelRequest,
    GetParticipantsRequest,
)
try:
    from telethon.tl.functions.premium import GetBoostsStatusRequest as _GetBoostsStatusRequest
    _BOOSTS_AVAILABLE = True
except ImportError:
    try:
        from telethon.tl.functions.messages import GetBoostsStatusRequest as _GetBoostsStatusRequest
        _BOOSTS_AVAILABLE = True
    except ImportError:
        _BOOSTS_AVAILABLE = False
        _GetBoostsStatusRequest = None
from telethon.tl.types import (
    Channel,
    ChannelParticipantsSearch,
    Chat,
    InputChannel,
    User,
)

from .proxy_rotator import ProxyRotator


# ── Israeli classification (substring match on title + username, case-insensitive for ASCII) ─
ISRAELI_CLASSIFIER_KEYWORDS: tuple[str, ...] = (
    "ישראל",
    "ישראלי",
    "ישראלים",
    "ישראלית",
    "israel",
    "israeli",
    "jerusalem",
    "tel aviv",
    "tel-aviv",
    "telaviv",
    "haifa",
    "beer sheva",
    "beer-sheva",
    "beersheva",
    "חדשות ישראל",
    "צה״ל",
    "צהל",
    "idf",
    "knesset",
    "כנסת",
    "herzliya",
    "netanya",
    "ashdod",
    "eilat",
    "galilee",
    "נתניה",
    "אשדוד",
    "חיפה",
    "ירושלים",
)


def _parse_filter_keywords(filter_str: str | None) -> list[str]:
    if not filter_str or not str(filter_str).strip():
        return []
    return [p.strip() for p in str(filter_str).split(",") if p.strip()]


def _haystack_matches_any_keyword(haystack: str, keywords: list[str] | tuple[str, ...]) -> bool:
    if not haystack or not keywords:
        return False
    h = haystack.casefold()
    for kw in keywords:
        if not kw:
            continue
        if kw.casefold() in h:
            return True
    return False


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class EntityAudit:
    entity_id: int
    title: str
    entity_type: str          # "group" | "channel" | "bot_chat"
    username: str
    role: str                 # "owner" | "admin"
    member_count: int
    premium_real: int         # counted from member list
    premium_boosts: int       # from GetBoostsStatus
    invite_link: str
    is_israeli: bool = False


@dataclass
class AccountAudit:
    session_path: str
    phone: str
    username: str
    first_name: str
    last_name: str
    is_premium: bool
    is_banned: bool
    is_unregistered: bool
    error: str
    entities: list[EntityAudit] = field(default_factory=list)


# ── Human-like delay helpers ──────────────────────────────────────────────────

async def _human_pause(min_s: float = 0.3, max_s: float = 0.8, scale: float = 1.0) -> None:
    """Scaled sleep used to tune scan speed without touching call-sites."""
    low = max(0.0, min_s * scale)
    high = max(low, max_s * scale)
    await asyncio.sleep(random.uniform(low, high))


async def _scroll_pause(scale: float = 1.0) -> None:
    low = max(0.0, 0.1 * scale)
    high = max(low, 0.3 * scale)
    await asyncio.sleep(random.uniform(low, high))


# ── Premium counter from member list ─────────────────────────────────────────

async def _count_premium_members(
    client: TelegramClient,
    entity,
    cap: int = 2_000,
    pause_scale: float = 1.0,
) -> int:
    """
    Counts premium members via paginated GetParticipants.
    Cap is 2,000 by default (was 10,000) to keep things fast.
    Pass cap=0 to disable the scan entirely (returns 0 instantly).
    """
    if cap == 0:
        return 0
    premium_count = 0
    offset = 0
    limit = 200

    try:
        while offset < cap:
            result = await client(GetParticipantsRequest(
                channel=entity,
                filter=ChannelParticipantsSearch(""),
                offset=offset,
                limit=limit,
                hash=0,
            ))
            if not result.users:
                break
            for user in result.users:
                if getattr(user, "premium", False):
                    premium_count += 1
            offset += len(result.users)
            if len(result.users) < limit:
                break
            await _scroll_pause(pause_scale)
    except Exception:
        pass

    return premium_count


# ── Boost panel premium count ─────────────────────────────────────────────────

async def _get_boost_premium_count(client: TelegramClient, entity) -> int:
    if not _BOOSTS_AVAILABLE:
        return 0
    try:
        result = await client(_GetBoostsStatusRequest(peer=entity))
        boost_value = getattr(result, "premium_audience", None) or 0
        try:
            return int(boost_value)
        except (TypeError, ValueError):
            try:
                return int(float(str(boost_value)))
            except (TypeError, ValueError):
                return 0
    except Exception:
        return 0


def _json_fallback_serializer(obj) -> str:
    """Safely serialize non-JSON-native values (including Telethon custom types)."""
    try:
        return str(obj)
    except Exception:
        return repr(obj)


# ── Single account audit ──────────────────────────────────────────────────────

# Per-account hard timeout (seconds). Prevents a single stuck account from
# blocking a worker slot indefinitely.
ACCOUNT_TIMEOUT_S = int(os.environ.get("AUDIT_ACCOUNT_TIMEOUT", "60"))


async def audit_account(
    session_path: Path,
    api_id: int,
    api_hash: str,
    proxy: dict | None,
    log: Callable[[str, str], None],
    premium_scan_cap: int = 2_000,
    pause_scale: float = 1.0,
    filter_keywords: list[str] | None = None,
) -> AccountAudit:
    phone = session_path.stem
    audit = AccountAudit(
        session_path=str(session_path),
        phone=phone,
        username="", first_name="", last_name="",
        is_premium=False, is_banned=False,
        is_unregistered=False, error="",
    )

    try:
        audit = await asyncio.wait_for(
            _audit_account_inner(
                session_path=session_path,
                api_id=api_id,
                api_hash=api_hash,
                proxy=proxy,
                log=log,
                premium_scan_cap=premium_scan_cap,
                pause_scale=pause_scale,
                audit=audit,
                filter_keywords=filter_keywords,
            ),
            timeout=ACCOUNT_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        audit.error = f"timeout_{ACCOUNT_TIMEOUT_S}s"
        log(f"[{phone}] TIMEOUT after {ACCOUNT_TIMEOUT_S}s — skipping", "warning")

    return audit


async def _audit_account_inner(
    session_path: Path,
    api_id: int,
    api_hash: str,
    proxy: dict | None,
    log: Callable[[str, str], None],
    premium_scan_cap: int,
    pause_scale: float,
    audit: AccountAudit,
    filter_keywords: list[str] | None = None,
) -> AccountAudit:
    phone = session_path.stem

    client_kwargs: dict = {"api_id": api_id, "api_hash": api_hash}
    if proxy:
        client_kwargs["proxy"] = proxy

    client: TelegramClient | None = None
    try:
        client = TelegramClient(str(session_path), **client_kwargs)
        await client.connect()

        if not await client.is_user_authorized():
            audit.error = "not_authorized"
            return audit

        # ── Get self ──────────────────────────────────────────────────────
        await _human_pause(1.0, 2.5, pause_scale)
        me: User = await client.get_me()
        audit.username   = me.username or ""
        audit.first_name = me.first_name or ""
        audit.last_name  = me.last_name or ""
        audit.is_premium = bool(getattr(me, "premium", False))

        log(
            f"[{phone}] Connected: {audit.first_name} (@{audit.username}) "
            f"{'[PREMIUM]' if audit.is_premium else ''}",
            "info",
        )

        # ── Iterate dialogs to find owned/admin entities ──────────────────
        await _human_pause(1.5, 3.0, pause_scale)

        async for dialog in client.iter_dialogs():
            entity = dialog.entity

            if not isinstance(entity, (Channel, Chat)):
                continue

            role = None
            if isinstance(entity, Channel):
                if getattr(entity, "creator", False):
                    role = "owner"
                elif getattr(entity, "admin_rights", None):
                    role = "admin"
            elif isinstance(entity, Chat):
                if getattr(entity, "creator", False):
                    role = "owner"
                elif getattr(entity, "admin_rights", None):
                    role = "admin"

            if role is None:
                continue

            if isinstance(entity, Channel):
                etype = "channel" if entity.broadcast else "group"
            else:
                etype = "group"

            title = dialog.name or ""
            username = getattr(entity, "username", "") or ""
            combined = f"{title} {username}".strip()
            is_israeli = _haystack_matches_any_keyword(combined, ISRAELI_CLASSIFIER_KEYWORDS)

            fk = filter_keywords or []
            if fk and not _haystack_matches_any_keyword(combined, fk):
                continue

            await _human_pause(0.8, 2.0, pause_scale)

            member_count = 0
            try:
                full = await client(GetFullChannelRequest(entity))
                member_count = getattr(full.full_chat, "participants_count", 0) or 0
            except Exception:
                member_count = getattr(entity, "participants_count", 0) or 0
            try:
                member_count = int(member_count)
            except (TypeError, ValueError):
                try:
                    member_count = int(float(str(member_count)))
                except (TypeError, ValueError):
                    member_count = 0

            invite_link = f"https://t.me/{username}" if username else ""

            await _human_pause(1.0, 2.5, pause_scale)

            premium_real   = await _count_premium_members(
                client,
                entity,
                cap=premium_scan_cap,
                pause_scale=pause_scale,
            )
            await _human_pause(0.2, 0.5, pause_scale)
            premium_boosts = await _get_boost_premium_count(client, entity)
            premium_real = int(premium_real)
            try:
                premium_boosts = int(premium_boosts)
            except (TypeError, ValueError):
                try:
                    premium_boosts = int(float(str(premium_boosts)))
                except (TypeError, ValueError):
                    premium_boosts = 0

            audit.entities.append(EntityAudit(
                entity_id=entity.id,
                title=title,
                entity_type=etype,
                username=username,
                role=role,
                member_count=member_count,
                premium_real=premium_real,
                premium_boosts=premium_boosts,
                invite_link=invite_link,
                is_israeli=is_israeli,
            ))

            log(
                f"  [{phone}] {etype.upper()} '{title}' | "
                f"members={member_count} | premium_real={premium_real} | "
                f"boosts={premium_boosts} | role={role}",
                "info",
            )

            await _scroll_pause(pause_scale)

    except UserDeactivatedBanError:
        audit.is_banned = True
        audit.error = "banned"
        log(f"[{phone}] BANNED", "warning")

    except AuthKeyUnregisteredError:
        audit.is_unregistered = True
        audit.error = "unregistered"
        log(f"[{phone}] Session unregistered (key revoked)", "warning")

    except FloodWaitError as e:
        audit.error = f"flood_wait_{e.seconds}s"
        log(f"[{phone}] FloodWait {e.seconds}s — skipping", "warning")

    except SessionPasswordNeededError:
        audit.error = "2fa_required"
        log(f"[{phone}] 2FA required — skipping", "warning")

    except RPCError as e:
        audit.error = str(e)
        log(f"[{phone}] RPC error: {e}", "error")

    except Exception as e:
        audit.error = str(e)
        log(f"[{phone}] Unexpected error: {e}", "error")

    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass

    return audit


# ── Batch auditor ─────────────────────────────────────────────────────────────

class SessionAuditor:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        proxy_rotator: ProxyRotator,
        log: Callable[[str, str], None],
        delay_between_accounts: tuple[float, float] = (1.0, 3.0),
        concurrency: int = 5,
        premium_scan_cap: int = 2_000,
        pause_scale: float = 1.0,
        filter_keywords: list[str] | None = None,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.proxy_rotator = proxy_rotator
        self.log = log
        self.delay_range = delay_between_accounts
        self.concurrency = concurrency
        self.premium_scan_cap = premium_scan_cap
        self.pause_scale = max(0.0, pause_scale)
        self.filter_keywords = filter_keywords or []

    async def audit_all(
        self,
        session_paths: list[Path],
        progress_callback: Callable[[int, int, AccountAudit], None] | None = None,
        checkpoint_path: Path | None = None,
        existing_results: list[AccountAudit] | None = None,
    ) -> list[AccountAudit]:
        """
        Audit all sessions with crash-safe per-account checkpointing.

        Parameters
        ----------
        session_paths      : Sessions to audit in this run (already filtered for resume).
        progress_callback  : Called after each account completes.
        checkpoint_path    : If set, saves a JSON checkpoint after every account.
        existing_results   : Pre-loaded results from a previous checkpoint (for resume).
        """
        total_original = len(session_paths) + len(existing_results or [])
        total = len(session_paths)
        results: list[AccountAudit | None] = [None] * total
        completed = 0
        ok_count = sum(
            1 for r in (existing_results or [])
            if not r.error and not r.is_banned and not r.is_unregistered
        )
        banned_count  = sum(1 for r in (existing_results or []) if r.is_banned)
        dead_count    = sum(1 for r in (existing_results or []) if r.is_unregistered)
        err_count     = sum(
            1 for r in (existing_results or [])
            if r.error and not r.is_banned and not r.is_unregistered
        )
        sem = asyncio.Semaphore(self.concurrency)
        _cp_lock = asyncio.Lock()
        _stats_lock = asyncio.Lock()

        already_done = len(existing_results or [])
        self.log(
            f"Starting audit of {total} sessions "
            f"({'with' if self.proxy_rotator.available else 'WITHOUT'} proxy rotation) "
            f"| concurrency={self.concurrency} | timeout={ACCOUNT_TIMEOUT_S}s/account"
            + (f" | resuming from {already_done}/{total_original}" if already_done else ""),
            "info",
        )

        async def _save_checkpoint(current_results: list[AccountAudit]) -> None:
            if checkpoint_path is None:
                return
            all_so_far = list(existing_results or []) + current_results
            data = []
            for a in all_so_far:
                data.append({
                    "session": a.session_path, "phone": a.phone,
                    "username": a.username, "name": f"{a.first_name} {a.last_name}".strip(),
                    "premium": a.is_premium, "banned": a.is_banned,
                    "unregistered": a.is_unregistered, "error": a.error,
                    "entities": [
                        {"id": e.entity_id, "title": e.title, "type": e.entity_type,
                         "role": e.role, "members": int(e.member_count),
                         "premium_real": int(e.premium_real), "premium_boosts": int(e.premium_boosts),
                         "link": e.invite_link, "is_israeli": bool(e.is_israeli)}
                        for e in a.entities
                    ],
                })
            tmp = checkpoint_path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    data,
                    ensure_ascii=False,
                    indent=2,
                    default=_json_fallback_serializer,
                ),
                encoding="utf-8",
            )
            try:
                tmp.replace(checkpoint_path)
            except PermissionError:
                # Windows may deny atomic replace if the target is locked;
                # fall back to delete-then-rename.
                if checkpoint_path.exists():
                    checkpoint_path.unlink(missing_ok=True)
                tmp.rename(checkpoint_path)

        async def _worker(idx: int, session_path: Path) -> None:
            nonlocal completed, ok_count, banned_count, dead_count, err_count
            async with sem:
                # Get proxy once — avoid calling next() twice (would advance the rotator twice)
                proxy = self.proxy_rotator.next_telethon() if self.proxy_rotator.available else None
                proxy_label = proxy.get("addr", "proxy") if isinstance(proxy, dict) else (str(proxy) if proxy else "no proxy")
                self.log(
                    f"[{already_done + idx+1}/{total_original}] Auditing {session_path.stem} via {proxy_label}",
                    "info",
                )
                # Stagger start to avoid simultaneous connection bursts
                await asyncio.sleep(random.uniform(*self.delay_range) * (idx % self.concurrency) / self.concurrency)
                audit = await audit_account(
                    session_path=session_path,
                    api_id=self.api_id,
                    api_hash=self.api_hash,
                    proxy=proxy,
                    log=self.log,
                    premium_scan_cap=self.premium_scan_cap,
                    pause_scale=self.pause_scale,
                    filter_keywords=self.filter_keywords,
                )
                results[idx] = audit

                async with _stats_lock:
                    completed += 1
                    if audit.is_banned:
                        banned_count += 1
                    elif audit.is_unregistered:
                        dead_count += 1
                    elif audit.error:
                        err_count += 1
                    else:
                        ok_count += 1

                if progress_callback:
                    progress_callback(already_done + completed, total_original, audit)

                # ── Save checkpoint after every completed account ──────────
                if checkpoint_path is not None:
                    async with _cp_lock:
                        done_so_far = [r for r in results if r is not None]
                        await _save_checkpoint(done_so_far)

        await asyncio.gather(*[_worker(i, p) for i, p in enumerate(session_paths)])

        final: list[AccountAudit] = list(existing_results or []) + [r for r in results if r is not None]

        self.log(
            f"\nAudit complete: {total_original} total | "
            f"{ok_count} OK | {banned_count} banned | {dead_count} dead | {err_count} err",
            "info",
        )
        return final
