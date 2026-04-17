"""
Nexus Supreme — Neural Link (Remote DevOps Bridge)

Receives:   /claude <instruction>   from the authorized owner Telegram account ONLY
Executes:   claude --print <instruction>  via the host Claude Code CLI
Streams:    live output back to Telegram in 2-second edit-window chunks
Finalizes:  sends full output as a .txt document if it exceeds 3800 chars

Security:
  - Hard owner-ID guard (TELEGRAM_ADMIN_CHAT_ID env var)
  - One command at a time; /claude_stop kills the running process
  - Full SQLAlchemy audit log in cli_audit_log table
  - Execution root is always the Nexus-Orchestrator directory

Commands registered by register(dp):
  /claude <instruction>   — run via Claude Code CLI
  /claude_stop            — kill running process
  /claude_logs            — last 30 audit lines
  /claude_prompt          — insert a structured prompt template
"""
from __future__ import annotations

import asyncio
import io
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    pass

log = structlog.get_logger(__name__)

ROOT      = Path(__file__).resolve().parents[2]   # Nexus-Orchestrator root
TIMEOUT_S = 600                                    # 10-min hard kill
LIVE_CHUNK = 3800                                  # max chars per live-edit message
FILE_THRESH= 3800                                  # send as file above this length


def _owner_id() -> int:
    raw = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "0").strip()
    try:
        return int(raw)
    except ValueError:
        return 0


# ── Prompt templates ──────────────────────────────────────────────────────────

PROMPT_TEMPLATES = [
    "review nexus/api/routers/{module}.py for security issues and suggest fixes",
    "find all TODO and FIXME comments in the codebase and summarize them",
    "add structured logging to every function in {file} that currently has print()",
    "write pytest tests for {module} covering the 5 most critical code paths",
    "optimize Redis connection handling in the master dispatcher for high concurrency",
    "explain the architecture of the Nexus-Orchestrator in a concise technical brief",
    "generate a CHANGELOG.md entry for the last 5 git commits in professional Hebrew",
    "audit the .env file for exposed secrets and suggest which to move to a vault",
]


def _escape_md(text: str) -> str:
    """Escape special chars for MarkdownV2 inside code blocks."""
    return text.replace("\\", "\\\\").replace("`", "\\`")


def _esc(text: str) -> str:
    """Full MarkdownV2 escape for inline text (outside code blocks)."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


# ── DevLink class ─────────────────────────────────────────────────────────────

class DevLink:
    """
    Wire into an aiogram Dispatcher with:
        bridge = DevLink(bot)
        bridge.register(dp)
    """

    def __init__(self, bot, owner_id: int | None = None) -> None:
        self._bot      = bot
        self._owner_id = owner_id or _owner_id()
        self._proc: asyncio.subprocess.Process | None = None
        self._running  = False
        self._lock     = asyncio.Lock()

    def register(self, dp) -> None:
        from aiogram.filters import Command
        dp.message.register(self._cmd_claude,        Command("claude"))
        dp.message.register(self._cmd_claude_stop,   Command("claude_stop"))
        dp.message.register(self._cmd_claude_logs,   Command("claude_logs"))
        dp.message.register(self._cmd_claude_prompt, Command("claude_prompt"))

    # ── Auth ──────────────────────────────────────────────────────────────────

    async def _guard(self, msg) -> bool:
        uid = getattr(msg.from_user, "id", None)
        if uid != self._owner_id:
            await msg.answer("⛔ Unauthorized\\.")
            log.warning("devlink_unauthorized", user_id=uid)
            return False
        return True

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _cmd_claude(self, msg) -> None:
        if not await self._guard(msg):
            return

        async with self._lock:
            if self._running:
                await msg.answer(
                    "⏳ פקודה אחרת רצה כרגע\\.\n"
                    "שלח `/claude_stop` לביטולה\\.",
                    parse_mode="MarkdownV2",
                )
                return

        text  = msg.text or ""
        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await msg.answer(
                "📟 *Neural Link — Claude CLI Bridge*\n\n"
                "שימוש: `/claude \\<הוראה\\>`\n"
                "דוגמה: `/claude fix the Redis timeout in deployer\\.py`\n\n"
                "פקודות נוספות:\n"
                "`/claude_stop` — בטל הרצה\n"
                "`/claude_logs` — הצג audit log\n"
                "`/claude_prompt` — תבנית פרומפט מובנה",
                parse_mode="MarkdownV2",
            )
            return

        await self._execute(msg, parts[1].strip())

    async def _cmd_claude_stop(self, msg) -> None:
        if not await self._guard(msg):
            return
        if self._proc and self._running:
            try:
                self._proc.kill()
            except Exception:
                pass
            self._running = False
            await msg.answer("🛑 פקודה בוטלה\\.", parse_mode="MarkdownV2")
        else:
            await msg.answer("ℹ️ אין פקודה פעילה כרגע\\.", parse_mode="MarkdownV2")

    async def _cmd_claude_logs(self, msg) -> None:
        if not await self._guard(msg):
            return
        log_path = ROOT / "logs" / "claude_bridge.log"
        if not log_path.exists():
            await msg.answer("_אין לוג זמין עדיין_", parse_mode="MarkdownV2")
            return
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-30:]
        body  = _escape_md("\n".join(lines) or "ריק")
        await msg.answer(f"```\n{body}\n```", parse_mode="MarkdownV2")

    async def _cmd_claude_prompt(self, msg) -> None:
        if not await self._guard(msg):
            return
        import random
        tmpl = random.choice(PROMPT_TEMPLATES)
        lines = ["🧬 *Prompt Architect — תבנית מובנית*\n"]
        lines.append("העתק והשלם את הפרמטרים בסוגריים המסולסלים:\n")
        lines.append(f"`/claude {_escape_md(tmpl)}`\n")
        lines.append("\nכל התבניות:")
        for i, t in enumerate(PROMPT_TEMPLATES, 1):
            lines.append(f"`{i}\\. {_escape_md(t)}`")
        await msg.answer("\n".join(lines), parse_mode="MarkdownV2")

    # ── Core execution ────────────────────────────────────────────────────────

    async def _execute(self, msg, instruction: str) -> None:
        self._running = True
        chat_id       = msg.chat.id

        # ── Status message ────────────────────────────────────────────────────
        preview = _esc(instruction[:120])
        status_msg = await msg.answer(
            f"⚡ *מריץ פקודה:*\n`{_escape_md(instruction[:200])}`",
            parse_mode="MarkdownV2",
        )
        mid = status_msg.message_id

        cmd = self._build_cmd(instruction)
        all_chunks: list[str] = []
        exit_code             = -1

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd    = str(ROOT),
                stdout = asyncio.subprocess.PIPE,
                stderr = asyncio.subprocess.STDOUT,
                env    = {**os.environ, "PYTHONUNBUFFERED": "1"},
            )

            # ── Concurrent: stream reader + live-edit updater ─────────────────
            buf       = ""
            last_edit = ""

            async def _reader():
                nonlocal buf
                assert self._proc and self._proc.stdout
                async for raw in self._proc.stdout:
                    chunk = raw.decode("utf-8", errors="replace")
                    buf  += chunk
                    all_chunks.append(chunk)

            async def _updater():
                nonlocal last_edit
                while self._running:
                    await asyncio.sleep(2)
                    if buf == last_edit:
                        continue
                    snippet = buf[-LIVE_CHUNK:]
                    try:
                        await self._bot.edit_message_text(
                            chat_id    = chat_id,
                            message_id = mid,
                            text=(
                                f"⚡ *פלט חי:*\n```\n{_escape_md(snippet)}\n```"
                            ),
                            parse_mode = "MarkdownV2",
                        )
                        last_edit = buf
                    except Exception:
                        pass   # edit throttled or message unchanged

            reader_task  = asyncio.create_task(_reader())
            updater_task = asyncio.create_task(_updater())

            try:
                await asyncio.wait_for(self._proc.wait(), timeout=TIMEOUT_S)
                exit_code = self._proc.returncode or 0
            except asyncio.TimeoutError:
                self._proc.kill()
                exit_code = -1
                buf += "\n\n[⏱ TIMEOUT — process killed after 10 minutes]"

            reader_task.cancel()
            updater_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        except FileNotFoundError:
            buf = (
                "❌ claude CLI לא נמצא ב\\-PATH\\.\n"
                "הרץ: `npm install \\-g @anthropic\\-ai/claude\\-code`"
            )
            exit_code = 127
        except Exception as exc:
            buf = f"❌ שגיאה: {_esc(str(exc))}"
            exit_code = -1
        finally:
            self._running = False
            self._proc    = None

        # ── Final delivery ────────────────────────────────────────────────────
        full_output = "".join(all_chunks) or buf
        await self._deliver_result(chat_id, mid, instruction, full_output, exit_code)
        self._write_audit(instruction, exit_code, full_output)

    def _build_cmd(self, instruction: str) -> list[str]:
        """
        Try `claude --print` first.
        Falls back gracefully — the FileNotFoundError is caught in _execute.
        """
        return ["claude", "--print", instruction]

    async def _deliver_result(
        self,
        chat_id: int,
        status_mid: int,
        instruction: str,
        output: str,
        exit_code: int,
    ) -> None:
        icon    = "✅" if exit_code == 0 else "❌"
        summary = f"{icon} *סיום \\(exit {exit_code}\\)*"

        if len(output) <= FILE_THRESH:
            # ── Short output: inline code block ──────────────────────────────
            body = _escape_md(output[-LIVE_CHUNK:]) if output else "_ללא פלט_"
            try:
                await self._bot.edit_message_text(
                    chat_id    = chat_id,
                    message_id = status_mid,
                    text       = f"{summary}\n```\n{body}\n```",
                    parse_mode = "MarkdownV2",
                )
            except Exception:
                pass
        else:
            # ── Long output: send as .txt document ───────────────────────────
            short_preview = _escape_md(output[:400])
            try:
                await self._bot.edit_message_text(
                    chat_id    = chat_id,
                    message_id = status_mid,
                    text=(
                        f"{summary}\n"
                        f"_פלט ארוך \\({len(output):,} תווים\\) — שולח כקובץ\\._\n\n"
                        f"```\n{short_preview}\\.\\.\\.\n```"
                    ),
                    parse_mode = "MarkdownV2",
                )
            except Exception:
                pass

            # Write to temp file and send
            ts_str   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"claude_output_{ts_str}.txt"
            header   = (
                f"# Nexus Neural Link — Claude CLI Output\n"
                f"# Timestamp : {datetime.now(timezone.utc).isoformat()}\n"
                f"# Exit code : {exit_code}\n"
                f"# Command   : {instruction[:200]}\n"
                f"# Length    : {len(output)} chars\n"
                f"{'─' * 60}\n\n"
            )
            full_doc = (header + output).encode("utf-8", errors="replace")

            try:
                await self._bot.send_document(
                    chat_id     = chat_id,
                    document    = (filename, io.BytesIO(full_doc)),
                    caption     = f"{icon} פלט מלא · {len(output):,} תווים · exit {exit_code}",
                )
            except Exception as exc:
                log.warning("devlink_send_document_failed", error=str(exc))
                # Last resort: split into 3800-char chunks
                for i in range(0, min(len(output), 20_000), LIVE_CHUNK):
                    piece = _escape_md(output[i: i + LIVE_CHUNK])
                    try:
                        await self._bot.send_message(
                            chat_id    = chat_id,
                            text       = f"```\n{piece}\n```",
                            parse_mode = "MarkdownV2",
                        )
                    except Exception:
                        break

    # ── Audit log ─────────────────────────────────────────────────────────────

    def _write_audit(self, instruction: str, exit_code: int, output: str) -> None:
        log_path = ROOT / "logs" / "claude_bridge.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(
                    f"[{ts}] exit={exit_code} cmd={instruction[:200]}\n"
                    f"{output[:500]}\n---\n"
                )
        except Exception:
            pass

        # DB audit
        try:
            from .db.models import CliAuditLog, get_session
            s = get_session()
            s.add(CliAuditLog(
                telegram_id = self._owner_id,
                command     = instruction,
                exit_code   = exit_code,
                output_head = output[:500],
            ))
            s.commit()
            s.close()
        except Exception:
            pass
