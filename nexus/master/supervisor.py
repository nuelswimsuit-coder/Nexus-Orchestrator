"""
Supervisor — Self-Healing Orchestrator for Nexus Master.

Monitors Redis connectivity and Worker heartbeats every CHECK_INTERVAL_S
seconds.  On failure it applies the *3-Strikes Auto-Recovery* protocol:

  Strike 1 → wait 10 s → SSH-restart → log "[RECOVERY] ניסיון שחזור 1/3 מופעל..."
  Strike 2 → wait 30 s → SSH-restart → log "[RECOVERY] ניסיון שחזור 2/3 מופעל..."
  Strike 3 → wait 60 s → SSH-restart (final)
             If still dead → CRITICAL status + Telegram alert

The 3-strike window resets if 5 minutes elapse without a new failure.

Non-blocking design: each worker's recovery runs in its own asyncio.Task so
other workers are never blocked by one failing node.

Every automated action is logged with structured prefixes that the frontend
GlobalErrorOverlay surfaces in the live HUD:
  [SUCCESS]   — service healthy / restart succeeded
  [RECOVERY]  — recovery attempt in progress
  [CRITICAL]  — all retries exhausted / manual intervention required

Redis keys
----------
nexus:supervisor:status   JSON snapshot polled by /api/business/supervisor-status
nexus:agent:log           Shared agent-thinking log

Usage
-----
    supervisor = Supervisor(redis=arq_pool, settings=settings, telegram_provider=tg)
    await supervisor.start()
    # Register additional local processes to watch:
    supervisor.register_local("telefix-bot", restart_cmd=["python", "scripts/start_telegram_bot.py"])
    ...
    supervisor.stop()
    # Manual reset from dashboard:
    await supervisor.manual_reset("worker-ssh")
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.shared.config import Settings

log = structlog.get_logger(__name__)

# ── Timing ────────────────────────────────────────────────────────────────────
CHECK_INTERVAL_S      = int(os.getenv("SUPERVISOR_INTERVAL_S",      "10"))
REDIS_RETRY_ATTEMPTS  = int(os.getenv("SUPERVISOR_REDIS_RETRIES",    "3"))
SSH_COMMAND_TIMEOUT_S = int(os.getenv("SUPERVISOR_SSH_TIMEOUT",      "30"))
STRIKES_WINDOW_S      = int(os.getenv("SUPERVISOR_STRIKES_WINDOW",   "300"))

# Exponential backoff: Strike 1 → 10 s, Strike 2 → 30 s, Strike 3 → 60 s
BACKOFF_DELAYS: list[int] = [10, 30, 60]

# ── Redis keys ────────────────────────────────────────────────────────────────
WORKER_HEARTBEAT_KEY  = "nexus:heartbeat:worker"
SUPERVISOR_STATUS_KEY = "nexus:supervisor:status"
SUPERVISOR_STATUS_TTL = 300
AGENT_LOG_KEY         = "nexus:agent:log"
AGENT_LOG_MAX         = 200


# ── Markdown escaping (aiogram MarkdownV2) ────────────────────────────────────

_MD_ESCAPE_RE = re.compile(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])")


def _esc(text: str) -> str:
    return _MD_ESCAPE_RE.sub(r"\\\1", str(text))


def _esc_code(text: str) -> str:
    return text.replace("\\", "\\\\").replace("`", "\\`")


# ── Per-worker state ──────────────────────────────────────────────────────────

@dataclass
class WorkerRecord:
    name:            str
    node_id:         str
    restart_cmd:     list[str] | None = None
    strike_count:    int              = 0
    first_strike_ts: float            = 0.0
    last_restart_ts: float            = 0.0
    # "healthy" | "recovering" | "critical"
    status:          str              = "healthy"
    pid:             int | None       = None
    _proc: Any = field(default=None, repr=False)


# ── Main Supervisor ───────────────────────────────────────────────────────────

class Supervisor:
    """
    Self-healing supervisor with 3-strikes auto-recovery.

    Watches:
      1. Redis connectivity (ping check).
      2. Remote worker heartbeat key in Redis.
      3. Any additional local processes registered via register_local().

    Recovery protocol (per worker):
      Strike 1 → 10 s back-off → restart
      Strike 2 → 30 s back-off → restart
      Strike 3 → 60 s back-off → final restart
                 If still dead → CRITICAL + Telegram escalation
    """

    def __init__(
        self,
        redis: Any,
        settings: Settings,
        telegram_provider: Any = None,
    ) -> None:
        self._redis    = redis
        self._settings = settings
        self._telegram = telegram_provider
        self._running  = False

        # Pre-register the SSH-managed remote worker
        self._workers: dict[str, WorkerRecord] = {}
        self._recovery_tasks: dict[str, asyncio.Task] = {}
        self._redis_last_warn_ts: float = 0.0
        self._redis_warn_cooldown_s: float = 20.0

        # The main SSH-based worker is always registered
        self._workers["worker-ssh"] = WorkerRecord(
            name    = "worker-ssh",
            node_id = settings.worker_ip or "worker-remote",
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Launch the supervisor as a background asyncio task."""
        self._running = True
        asyncio.create_task(self._loop(), name="nexus-supervisor")
        log.info(
            "supervisor_started",
            interval_s=CHECK_INTERVAL_S,
            worker_ip=self._settings.worker_ip or "not-configured",
            status="[SUCCESS] Self-healing supervisor started (3-Strikes active)",
        )

    def stop(self) -> None:
        self._running = False
        log.info("supervisor_stopped", status="[SUCCESS] Supervisor shut down cleanly")

    def register_local(
        self,
        name: str,
        restart_cmd: list[str],
        node_id: str | None = None,
        pid: int | None = None,
    ) -> None:
        """Register a local OS process to supervise alongside the SSH worker."""
        self._workers[name] = WorkerRecord(
            name        = name,
            node_id     = node_id or name,
            restart_cmd = restart_cmd,
            pid         = pid,
        )
        log.info("supervisor_registered_local", name=name, restart_cmd=restart_cmd)

    def update_pid(self, name: str, pid: int) -> None:
        if name in self._workers:
            self._workers[name].pid = pid

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(CHECK_INTERVAL_S)
            await self._check_redis()
            await self._check_workers()

    # ── Redis health check ────────────────────────────────────────────────────

    async def _check_redis(self) -> None:
        for attempt in range(1, REDIS_RETRY_ATTEMPTS + 1):
            try:
                await self._redis.ping()
                log.debug("supervisor_redis_healthy", status="[SUCCESS] Redis connection OK")
                return
            except Exception as exc:
                now = time.time()
                should_warn = (
                    attempt == REDIS_RETRY_ATTEMPTS
                    or attempt == 1
                    or (now - self._redis_last_warn_ts) >= self._redis_warn_cooldown_s
                )
                if should_warn:
                    self._redis_last_warn_ts = now
                    log.warning(
                        "supervisor_redis_unreachable",
                        attempt=attempt,
                        max_attempts=REDIS_RETRY_ATTEMPTS,
                        error=str(exc),
                        status=(
                            f"[REPAIRING] Redis ping failed (attempt {attempt}/"
                            f"{REDIS_RETRY_ATTEMPTS}) — retrying in 2s..."
                        ),
                    )
                await asyncio.sleep(2)

        log.error(
            "supervisor_redis_failed",
            status=(
                f"[CRITICAL] Redis unreachable after {REDIS_RETRY_ATTEMPTS} attempts. "
                "ACTION: Check Redis process and network. Manual intervention required."
            ),
        )
        log.error(
            "supervisor_critical_redis_down",
            reason=(
                f"Redis unreachable after {REDIS_RETRY_ATTEMPTS} consecutive ping "
                "failures. Supervisor cannot reach the message broker."
            ),
        )

    # ── Worker health check (Redis heartbeat + local PIDs) ────────────────────

    async def _check_workers(self) -> None:
        """
        Check each registered worker.  On failure, spawn an isolated recovery
        task so other workers are not blocked.
        """
        # ── SSH-managed remote worker (heartbeat-based check) ─────────────────
        ssh_worker = self._workers["worker-ssh"]
        if ssh_worker.status != "critical":
            task = self._recovery_tasks.get("worker-ssh")
            if task is None or task.done():
                worker_ip = self._settings.worker_ip
                if worker_ip:
                    try:
                        raw = await self._redis.get(WORKER_HEARTBEAT_KEY)
                    except Exception as exc:
                        log.error(
                            "supervisor_heartbeat_check_error",
                            error=str(exc),
                            status=(
                                "[CRITICAL] Cannot read worker heartbeat key. "
                                "ACTION: Verify Redis connectivity."
                            ),
                        )
                        raw = None

                    if raw is None:
                        log.warning(
                            "supervisor_worker_offline",
                            worker_ip=worker_ip,
                            status="[RECOVERY] Worker offline — triggering recovery...",
                        )
                        self._recovery_tasks["worker-ssh"] = asyncio.create_task(
                            self._handle_failure(ssh_worker, restart_fn=self._restart_via_ssh),
                            name="recovery-worker-ssh",
                        )
                    else:
                        log.debug(
                            "supervisor_worker_healthy",
                            worker_ip=worker_ip,
                            status="[SUCCESS] Worker heartbeat OK",
                        )

        # ── Local processes ───────────────────────────────────────────────────
        for name, worker in list(self._workers.items()):
            if name == "worker-ssh":
                continue
            if worker.status == "critical":
                continue
            task = self._recovery_tasks.get(name)
            if task is not None and not task.done():
                continue
            if worker.pid is not None and not _pid_alive(worker.pid):
                log.warning(
                    "supervisor_local_process_down",
                    name=name,
                    pid=worker.pid,
                    status=f"[RECOVERY] Local process '{name}' (PID {worker.pid}) is down.",
                )
                self._recovery_tasks[name] = asyncio.create_task(
                    self._handle_failure(
                        worker,
                        restart_fn=lambda w=worker: self._restart_local(w),
                    ),
                    name=f"recovery-{name}",
                )

    # ── 3-Strikes recovery handler ────────────────────────────────────────────

    async def _handle_failure(
        self,
        worker: WorkerRecord,
        restart_fn: Any,
    ) -> None:
        """
        Apply the 3-strikes protocol for a single worker.

        Each call increments the strike counter.  If 3 strikes occur within
        STRIKES_WINDOW_S, the worker is marked CRITICAL and Telegram is alerted.
        """
        now = time.time()

        # Reset counter if last failure is outside the 5-minute window
        if (
            worker.strike_count > 0
            and (now - worker.first_strike_ts) > STRIKES_WINDOW_S
        ):
            log.info(
                "supervisor_strike_window_expired",
                name=worker.name,
                previous_strikes=worker.strike_count,
            )
            worker.strike_count    = 0
            worker.first_strike_ts = 0.0
            worker.status          = "healthy"

        if worker.strike_count == 0:
            worker.first_strike_ts = now

        worker.strike_count += 1
        strike_n = worker.strike_count

        if strike_n > len(BACKOFF_DELAYS):
            await self._enter_critical(worker)
            return

        delay = BACKOFF_DELAYS[strike_n - 1]

        # ── Announce recovery attempt ─────────────────────────────────────────
        recovery_msg = (
            f"[RECOVERY] ניסיון שחזור {strike_n}/3 מופעל — "
            f"המתנה {delay} שניות לפני הפעלה מחדש של המעבד (Worker)"
        )
        log.warning(
            "supervisor_recovery_attempt",
            name=worker.name,
            strike=strike_n,
            delay_s=delay,
            status=recovery_msg,
        )
        worker.status = "recovering"
        await self._write_agent_log("warning", recovery_msg, {"worker": worker.name, "strike": strike_n})
        await self._write_status()

        await asyncio.sleep(delay)
        restarted = await restart_fn()

        # ── On final strike, verify it came back ──────────────────────────────
        if strike_n == 3:
            await asyncio.sleep(5)
            # For SSH worker, re-check heartbeat; for local, re-check PID
            still_dead = not restarted
            if not still_dead and worker.pid is not None:
                still_dead = not _pid_alive(worker.pid)

            if still_dead:
                await self._enter_critical(worker)
            else:
                worker.strike_count    = 0
                worker.first_strike_ts = 0.0
                worker.status          = "healthy"
                await self._write_status()

    async def _enter_critical(self, worker: WorkerRecord) -> None:
        worker.status = "critical"
        critical_msg  = (
            f"[CRITICAL] מעבד (Worker) '{worker.name}' קרס 3 פעמים רצופות. "
            f"המערכת עצרה ניסיונות אוטומטיים. נדרשת התערבות ידנית."
        )
        log.error(
            "supervisor_critical_failure",
            name=worker.name,
            node_id=worker.node_id,
            strikes=worker.strike_count,
            status=critical_msg,
        )
        await self._write_agent_log(
            "error",
            critical_msg,
            {"worker": worker.name, "node_id": worker.node_id, "status": "critical"},
        )
        await self._write_status()
        await self._escalate_to_telegram(worker)

        log.error(
            "supervisor_worker_quarantined",
            worker=worker.name,
            node_id=worker.node_id,
            strikes=worker.strike_count,
            reason=(
                f"Worker '{worker.name}' (node: {worker.node_id}) exhausted all "
                f"{worker.strike_count} recovery attempts within {STRIKES_WINDOW_S}s. "
                "Supervisor entering critical state — manual intervention required."
            ),
        )

    # ── Restart backends ──────────────────────────────────────────────────────

    async def _restart_via_ssh(self) -> bool:
        """SSH-restart the remote worker. Returns True if the command succeeded."""
        worker  = self._workers["worker-ssh"]
        s       = self._settings

        if not s.worker_ssh_user or not s.worker_ip:
            log.error(
                "supervisor_ssh_not_configured",
                status=(
                    "[CRITICAL] SSH credentials missing. "
                    "ACTION: Set WORKER_SSH_USER and WORKER_IP in .env"
                ),
            )
            return False

        deploy_root = s.worker_deploy_root_linux or "/home/yadmin/Desktop/Nexus-Orchestrator"
        remote_cmd  = (
            f"cd {deploy_root} && "
            "nohup python scripts/start_worker.py > /tmp/nexus_worker.log 2>&1 &"
        )
        log.info(
            "supervisor_ssh_restart_attempt",
            worker_ip=s.worker_ip,
            ssh_user=s.worker_ssh_user,
            strike=worker.strike_count,
            status=(
                f"[REPAIRING] Connecting to {s.worker_ip} — launching worker via SSH..."
            ),
        )

        if s.worker_ssh_password:
            cmd = [
                "sshpass", "-p", s.worker_ssh_password,
                "ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                f"{s.worker_ssh_user}@{s.worker_ip}",
                remote_cmd,
            ]
        else:
            cmd = [
                "ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                f"{s.worker_ssh_user}@{s.worker_ip}",
                remote_cmd,
            ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=SSH_COMMAND_TIMEOUT_S
            )
            worker.last_restart_ts = time.time()

            if proc.returncode == 0:
                log.info(
                    "supervisor_ssh_restart_success",
                    worker_ip=s.worker_ip,
                    strike=worker.strike_count,
                    status=f"[SUCCESS] Worker on {s.worker_ip} restarted via SSH.",
                    stdout=stdout.decode(errors="replace")[:300],
                )
                await self._write_agent_log(
                    "action",
                    f"[RECOVERY] מעבד (Worker) '{worker.name}' הופעל מחדש בהצלחה דרך SSH "
                    f"(ניסיון {worker.strike_count}/3)",
                    {"worker": worker.name, "strike": worker.strike_count},
                )
                return True
            else:
                log.error(
                    "supervisor_ssh_restart_failed",
                    worker_ip=s.worker_ip,
                    return_code=proc.returncode,
                    stderr=stderr.decode(errors="replace")[:500],
                    status=(
                        f"[CRITICAL] SSH restart FAILED (exit code {proc.returncode})."
                    ),
                )
                return False

        except asyncio.TimeoutError:
            log.error(
                "supervisor_ssh_timeout",
                worker_ip=s.worker_ip,
                timeout_s=SSH_COMMAND_TIMEOUT_S,
                status=f"[CRITICAL] SSH connection timed out after {SSH_COMMAND_TIMEOUT_S}s.",
            )
            return False
        except FileNotFoundError as exc:
            log.error(
                "supervisor_ssh_command_not_found",
                error=str(exc),
                status="[CRITICAL] 'ssh'/'sshpass' not found. Install openssh-client.",
            )
            return False
        except Exception as exc:
            log.error(
                "supervisor_ssh_error",
                error=str(exc),
                status=f"[CRITICAL] Unexpected SSH error: {exc}",
            )
            return False

    async def _restart_local(self, worker: WorkerRecord) -> bool:
        """Restart a locally-registered process via subprocess.Popen."""
        if worker.restart_cmd is None:
            log.warning("supervisor_no_restart_cmd", name=worker.name)
            return False
        try:
            new_proc = subprocess.Popen(
                worker.restart_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            worker.pid             = new_proc.pid
            worker.last_restart_ts = time.time()
            log.info(
                "supervisor_local_restarted",
                name=worker.name,
                new_pid=new_proc.pid,
                strike=worker.strike_count,
                status=f"[SUCCESS] '{worker.name}' restarted (PID {new_proc.pid}).",
            )
            await self._write_agent_log(
                "action",
                f"[RECOVERY] מעבד (Worker) '{worker.name}' הופעל מחדש (PID: {new_proc.pid}, "
                f"ניסיון {worker.strike_count}/3)",
                {"worker": worker.name, "new_pid": new_proc.pid, "strike": worker.strike_count},
            )
            return True
        except Exception as exc:
            log.error("supervisor_local_restart_failed", name=worker.name, error=str(exc))
            return False

    # ── Telegram escalation ───────────────────────────────────────────────────

    async def _escalate_to_telegram(self, worker: WorkerRecord) -> None:
        if self._telegram is None:
            log.warning("supervisor_telegram_not_configured", name=worker.name)
            return

        first_fail_str = (
            datetime.fromtimestamp(worker.first_strike_ts, tz=timezone.utc).strftime("%H:%M:%S UTC")
            if worker.first_strike_ts
            else "לא ידוע"
        )

        lines = [
            "⚠️ *התראה קריטית*",
            "",
            f"המעבד \\(Worker\\) `{_esc(worker.name)}` קרס 3 פעמים רצופות\\.",
            "המערכת נעצרה למניעת נזק\\.",
            "*נדרשת התערבות ידנית\\.*",
            "",
            f"🔴 *Node:* `{_esc(worker.node_id)}`",
            f"⏱ *כישלון ראשון:* `{_esc(first_fail_str)}`",
            f"🔢 *קריסות:* `{worker.strike_count}`",
        ]

        log_tail = await self._get_log_tail(20)
        if log_tail:
            lines += [
                "",
                "📋 *לוג אחרון \\(20 שורות\\):*",
                f"```\n{_esc_code(chr(10).join(log_tail))}\n```",
            ]

        try:
            await self._telegram.send_message("\n".join(lines))
            log.info("supervisor_telegram_alert_sent", name=worker.name)
        except Exception as exc:
            log.error("supervisor_telegram_alert_failed", name=worker.name, error=str(exc))

    # ── Manual reset ──────────────────────────────────────────────────────────

    async def manual_reset(self, name: str) -> bool:
        """
        Reset a CRITICAL worker and attempt one clean restart.
        Called from POST /api/business/supervisor-reset/{name}.
        """
        if name not in self._workers:
            log.warning("supervisor_manual_reset_unknown", name=name)
            return False

        worker = self._workers[name]
        log.info("supervisor_manual_reset", name=name, previous_status=worker.status)

        worker.strike_count    = 0
        worker.first_strike_ts = 0.0
        worker.status          = "healthy"

        await self._write_agent_log(
            "action",
            f"[MANUAL RESET] מעבד (Worker) '{name}' אופס ידנית על-ידי המפעיל ומופעל מחדש.",
            {"worker": name, "action": "manual_reset"},
        )
        await self._write_status()

        if name == "worker-ssh":
            return await self._restart_via_ssh()
        elif worker.restart_cmd:
            return await self._restart_local(worker)
        return True

    def get_all_statuses(self) -> dict[str, dict]:
        """Snapshot for the API endpoint."""
        return {
            name: {
                "name":            w.name,
                "node_id":         w.node_id,
                "status":          w.status,
                "strike_count":    w.strike_count,
                "pid":             w.pid,
                "last_restart_ts": w.last_restart_ts,
                "first_strike_ts": w.first_strike_ts,
            }
            for name, w in self._workers.items()
        }

    # ── Redis helpers ─────────────────────────────────────────────────────────

    async def _write_agent_log(self, level: str, message: str, metadata: dict) -> None:
        try:
            entry = json.dumps({
                "ts":       datetime.now(timezone.utc).isoformat(),
                "level":    level,
                "message":  message,
                "metadata": metadata,
            })
            await self._redis.lpush(AGENT_LOG_KEY, entry)
            await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)
        except Exception:
            pass

    async def _write_status(self) -> None:
        try:
            payload = json.dumps({
                "workers":    self.get_all_statuses(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            await self._redis.set(SUPERVISOR_STATUS_KEY, payload, ex=SUPERVISOR_STATUS_TTL)
        except Exception:
            pass

    async def _get_log_tail(self, n: int = 20) -> list[str]:
        try:
            raws = await self._redis.lrange(AGENT_LOG_KEY, 0, n - 1)
            lines: list[str] = []
            for raw in raws:
                try:
                    d   = json.loads(raw)
                    ts  = str(d.get("ts", ""))[:19]
                    lvl = str(d.get("level", "info")).upper()
                    msg = str(d.get("message", ""))
                    lines.append(f"[{ts}] [{lvl}] {msg}")
                except Exception:
                    pass
            return lines
        except Exception:
            return []


# ── Helper ────────────────────────────────────────────────────────────────────

def _pid_alive(pid: int) -> bool:
    try:
        import psutil
        p = psutil.Process(pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except Exception:
        return False


# ── ProcessSupervisor ─────────────────────────────────────────────────────────

import signal as _signal  # noqa: E402


class ProcessSupervisor:
    """
    OS-level process lifecycle helper.

    Sends SIGTERM to a PID, waits `term_timeout_s` seconds, then escalates
    to SIGKILL if the process is still alive.  Prevents ConflictError zombies
    from stale Telegram bot instances.

    Usage
    -----
        sup = ProcessSupervisor(term_timeout_s=3.0)
        await sup.terminate(pid)
    """

    def __init__(self, term_timeout_s: float = 3.0) -> None:
        self.term_timeout_s = term_timeout_s

    async def terminate(self, pid: int) -> bool:
        """
        Gracefully terminate process `pid`.

        Returns True if the process exited cleanly after SIGTERM,
        False if a SIGKILL was required.
        """
        import time as _time  # noqa: PLC0415

        try:
            os.kill(pid, _signal.SIGTERM)
        except (OSError, ProcessLookupError):
            return True  # already gone

        deadline = _time.monotonic() + self.term_timeout_s
        while _time.monotonic() < deadline:
            await asyncio.sleep(0.2)
            try:
                result = os.waitpid(pid, os.WNOHANG)
                if result[0] != 0:
                    return True
            except ChildProcessError:
                return True
            except OSError:
                return True

        try:
            os.kill(pid, _signal.SIGKILL)
            log.warning("process_force_killed", pid=pid, reason="SIGTERM_timeout")
        except (OSError, ProcessLookupError):
            pass

        return False

    # also expose register/run as no-ops so it can be used interchangeably
    def register(self, *args: object, **kwargs: object) -> None:  # noqa: ARG002
        pass

    async def run(self) -> None:
        while True:
            await asyncio.sleep(3600)
