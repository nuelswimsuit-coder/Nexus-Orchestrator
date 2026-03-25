"""
Ghostwriter Engine — Telethon-based listener that monitors Israeli AI groups
and fires context-aware Hebrew replies when trigger keywords are detected.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from telethon import TelegramClient, events
from telethon.tl.types import Message, User

from .ai_reply import generate_reply
from .session_scanner import SessionScanner, ScanResult, run_scan

logger = logging.getLogger("ghostwriter")


class RateLimiter:
    """Tracks reply counts per (session, group) pair within a rolling hour."""

    def __init__(self, max_per_hour: int) -> None:
        self.max_per_hour = max_per_hour
        self._timestamps: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        now = time.monotonic()
        window = self._timestamps[key]
        # Drop timestamps older than 1 hour
        self._timestamps[key] = [t for t in window if now - t < 3600]
        if len(self._timestamps[key]) >= self.max_per_hour:
            return False
        self._timestamps[key].append(now)
        return True


class GhostwriterSession:
    """Wraps a single Telethon client (one Telegram account)."""

    def __init__(
        self,
        session_path: Path,
        cfg: dict[str, Any],
        rate_limiter: RateLimiter,
        live_log_callback,
    ) -> None:
        self.session_path = session_path
        self.cfg = cfg
        self.rate_limiter = rate_limiter
        self.live_log = live_log_callback
        self.name = session_path.stem

        tg = cfg["telegram"]
        self.client = TelegramClient(
            str(session_path),
            tg["api_id"],
            tg["api_hash"],
        )

    async def start(self) -> None:
        await self.client.start()
        me: User = await self.client.get_me()
        display = f"{me.first_name or ''} {me.last_name or ''}".strip() or self.name
        self.live_log(f"[{self.name}] Logged in as: {display}", level="info")
        self._register_handlers()

    def _register_handlers(self) -> None:
        groups = self.cfg["groups"]
        triggers: list[str] = [t.lower() for t in self.cfg["triggers"]]
        behavior = self.cfg["behavior"]
        stealth = behavior.get("stealth_mode", False)

        @self.client.on(events.NewMessage(chats=groups))
        async def handler(event: events.NewMessage.Event) -> None:
            msg: Message = event.message
            text: str = msg.message or ""

            # Skip empty, own messages, and bot messages
            if not text:
                return
            if behavior.get("skip_own_messages") and msg.out:
                return
            sender = await msg.get_sender()
            if behavior.get("skip_bots") and getattr(sender, "bot", False):
                return

            # Stealth mode: only react to direct questions
            if stealth and not text.strip().endswith("?"):
                return

            # Trigger detection (case-insensitive)
            text_lower = text.lower()
            matched_trigger = next((t for t in triggers if t in text_lower), None)
            if not matched_trigger:
                return

            group_id = str(event.chat_id)
            rate_key = f"{self.name}:{group_id}"
            if not self.rate_limiter.is_allowed(rate_key):
                self.live_log(
                    f"[{self.name}] Rate limit hit for group {group_id} — skipping",
                    level="warning",
                )
                return

            # Fetch context: last N messages before this one
            context: list[str] = []
            async for past_msg in self.client.iter_messages(
                event.chat_id,
                limit=behavior.get("context_messages", 10),
                offset_id=msg.id,
            ):
                if past_msg.message:
                    context.insert(0, past_msg.message)

            group_name = getattr(event.chat, "title", group_id)
            self.live_log(
                f"[{self.name}] Trigger «{matched_trigger}» in [{group_name}] — generating reply…",
                level="info",
            )

            try:
                ai_cfg = self.cfg["ai"]
                reply = await generate_reply(
                    trigger_word=matched_trigger,
                    context_messages=context,
                    personality=self.cfg.get("personality", "Expert"),
                    provider=ai_cfg.get("provider", "gemini"),
                    gemini_api_key=ai_cfg.get("gemini_api_key", ""),
                    openai_api_key=ai_cfg.get("openai_api_key", ""),
                    anthropic_api_key=ai_cfg.get("anthropic_api_key", ""),
                    model_gemini=ai_cfg.get("model_gemini", "gemini-1.5-flash"),
                    model_openai=ai_cfg.get("model_openai", "gpt-4o-mini"),
                    model_anthropic=ai_cfg.get("model_anthropic", "claude-3-haiku-20240307"),
                    max_tokens=ai_cfg.get("max_tokens", 200),
                    temperature=ai_cfg.get("temperature", 0.85),
                )
            except Exception as exc:
                self.live_log(f"[{self.name}] AI error: {exc}", level="error")
                return

            # Anti-flood delay
            delay = random.uniform(
                behavior.get("min_delay_seconds", 5),
                behavior.get("max_delay_seconds", 15),
            )
            self.live_log(
                f"[{self.name}] Waiting {delay:.1f}s before sending to [{group_name}]…",
                level="debug",
            )
            await asyncio.sleep(delay)

            await event.reply(reply)
            self.live_log(
                f"[{self.name}] ✓ Replied in [{group_name}]: {reply[:60]}…",
                level="info",
            )

    async def run_until_disconnected(self) -> None:
        await self.client.run_until_disconnected()

    async def stop(self) -> None:
        await self.client.disconnect()


class GhostwriterEngine:
    """Manages multiple GhostwriterSession instances (one per .session file)."""

    def __init__(self, cfg: dict[str, Any], live_log_callback) -> None:
        self.cfg = cfg
        self.live_log = live_log_callback
        self.sessions: list[GhostwriterSession] = []
        self.rate_limiter = RateLimiter(
            max_per_hour=cfg["behavior"].get("max_replies_per_hour", 8)
        )

    def _discover_sessions(self) -> list[Path]:
        scan_cfg = self.cfg.get("scanner", {})
        use_scanner = scan_cfg.get("enabled", False)
        max_sessions = self.cfg["telegram"].get("max_sessions", 20)

        if use_scanner:
            # ── Full-disk scan mode ────────────────────────────────────────
            scan_roots = scan_cfg.get("roots") or None
            staging = Path(scan_cfg.get("staging_dir", "")) if scan_cfg.get("staging_dir") else None

            self.live_log("Full-disk session scan starting...", level="info")
            scanner = SessionScanner(
                scan_roots=scan_roots,
                staging_dir=staging,
                log=self.live_log,
            )
            result: ScanResult = scanner.scan()
            scanner.print_report(result)

            # Prioritise Israeli +972 sessions
            israeli = [s for s in result.sessions if s.phone.startswith("+972")]
            others  = [s for s in result.sessions if not s.phone.startswith("+972")]
            ordered = israeli + others
            capped  = ordered[:max_sessions]

            self.live_log(
                f"Scanner found {result.total} total sessions "
                f"({len(israeli)} Israeli +972). "
                f"Loading top {len(capped)}.",
                level="info",
            )
            return [s.session_path for s in capped]

        else:
            # ── Fixed directory mode (original behaviour) ──────────────────
            sessions_dir = Path(self.cfg["telegram"].get("sessions_dir", "vault/sessions"))
            if not sessions_dir.is_absolute():
                sessions_dir = Path(__file__).resolve().parents[4] / sessions_dir

            all_files = list(sessions_dir.glob("*.session"))
            israeli = [f for f in all_files if f.stem.startswith("+972") or f.stem.startswith("972")]
            others  = [f for f in all_files if f not in israeli]
            files   = (israeli + others)[:max_sessions]

            if not files:
                self.live_log(
                    f"No .session files found in {sessions_dir}.",
                    level="warning",
                )
            else:
                self.live_log(
                    f"Loading {len(files)} session(s) from {sessions_dir} "
                    f"({len(israeli)} Israeli +972 prioritised)",
                    level="info",
                )
            return files

    async def start_all(self) -> None:
        session_files = self._discover_sessions()
        self.live_log(f"Found {len(session_files)} session(s) to load.", level="info")

        for sf in session_files:
            sess = GhostwriterSession(sf, self.cfg, self.rate_limiter, self.live_log)
            try:
                await sess.start()
                self.sessions.append(sess)
            except Exception as exc:
                self.live_log(f"Failed to start session {sf.name}: {exc}", level="error")

    async def run(self) -> None:
        await self.start_all()
        if not self.sessions:
            self.live_log("No active sessions — exiting.", level="error")
            return
        self.live_log(
            f"Ghostwriter active on {len(self.sessions)} account(s). Monitoring groups…",
            level="info",
        )
        await asyncio.gather(*(s.run_until_disconnected() for s in self.sessions))

    async def stop_all(self) -> None:
        for s in self.sessions:
            await s.stop()
