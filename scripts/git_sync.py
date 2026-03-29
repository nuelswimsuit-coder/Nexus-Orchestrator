"""
Nexus Git Overdrive — Dual-mode auto-sync service.

MASTER mode (Jacob-PC / GIT_SYNC_MODE=master):
  Every GIT_SYNC_PUSH_INTERVAL_S seconds (default: 600 / 10 min):
    git add .
    git commit -m "Auto-sync [<timestamp>]"
    git push origin <branch>

WORKER mode (Linux laptops / GIT_SYNC_MODE=worker):
  Every GIT_SYNC_PULL_INTERVAL_S seconds (default: 1800 / 30 min):
    git fetch origin
    git reset --hard origin/<branch>

Both modes:
  - Log every action silently to the unified terminal (stdout).
  - On conflict / non-zero exit: push a Redis alert and send a Telegram
    notification.
  - On a successful push/pull that changed commits: send a Telegram
    notification.

Environment variables (all optional)
--------------------------------------
GIT_SYNC_MODE              — "master" or "worker" (default: auto-detect via hostname)
GIT_SYNC_BRANCH            — branch name (default: main)
GIT_SYNC_PUSH_INTERVAL_S   — master push interval in seconds (default: 600)
GIT_SYNC_PULL_INTERVAL_S   — worker pull interval in seconds (default: 1800)
GIT_SYNC_COMMIT_MSG        — commit message prefix (default: "Auto-sync")
TELEGRAM_NEXUS_BOT_TOKEN   — project bot (preferred for git notifications)
TELEGRAM_NEXUS_ADMIN_CHAT_ID — optional; defaults to TELEGRAM_ADMIN_CHAT_ID
TELEGRAM_BOT_TOKEN         — fallback if NEXUS token unset
TELEGRAM_ADMIN_CHAT_ID     — chat-id to receive notifications
REDIS_URL                  — Redis connection string (default: redis://127.0.0.1:6379/0)
NEXUS_MASTER_HOSTNAME      — hostname of the master node (default: Jacob-PC)
"""

from __future__ import annotations

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.getcwd())

import json
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


# ── Config ────────────────────────────────────────────────────────────────────

BRANCH: str = os.getenv("GIT_SYNC_BRANCH", "main")
PUSH_INTERVAL_S: int = int(os.getenv("GIT_SYNC_PUSH_INTERVAL_S", "600"))
PULL_INTERVAL_S: int = int(os.getenv("GIT_SYNC_PULL_INTERVAL_S", "1800"))
COMMIT_MSG_PREFIX: str = os.getenv("GIT_SYNC_COMMIT_MSG", "Auto-sync")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
MASTER_HOSTNAME: str = os.getenv("NEXUS_MASTER_HOSTNAME", "Jacob-PC")

ALERTS_KEY = "nexus:system:alerts"
MAX_ALERTS = 50

_HERE = Path(os.path.dirname(os.path.abspath(__file__)))
ROOT: Path = _HERE.parent if _HERE.name == "scripts" else _HERE


# ── Mode detection ────────────────────────────────────────────────────────────

def _detect_mode() -> str:
    """Return 'master' or 'worker' based on env var or hostname."""
    env_mode = os.getenv("GIT_SYNC_MODE", "").strip().lower()
    if env_mode in ("master", "worker"):
        return env_mode
    hostname = socket.gethostname()
    return "master" if hostname == MASTER_HOSTNAME else "worker"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _is_git_repo(path: Path) -> bool:
    return (path / ".git").exists() or _run_git(["rev-parse", "--git-dir"], path) is not None


def _run_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _current_commit(cwd: Path) -> str:
    result = _run_git(["rev-parse", "HEAD"], cwd)
    if result and result.returncode == 0:
        return result.stdout.strip()
    return ""


def _push_redis_alert(payload: dict) -> None:
    try:
        import redis as _redis  # type: ignore[import]

        client = _redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=3)
        client.rpush(ALERTS_KEY, json.dumps(payload, ensure_ascii=False))
        client.ltrim(ALERTS_KEY, -MAX_ALERTS, -1)
        client.close()
    except Exception:
        pass


def _git_notify_telegram_credentials() -> tuple[str, str]:
    tok = (os.getenv("TELEGRAM_NEXUS_BOT_TOKEN") or "").strip()
    if not tok:
        tok = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat = (os.getenv("TELEGRAM_NEXUS_ADMIN_CHAT_ID") or "").strip()
    if not chat:
        chat = (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "").strip()
    return tok, chat


def _send_telegram(message: str) -> None:
    bot_token, chat_id = _git_notify_telegram_credentials()
    if not bot_token or not chat_id:
        return
    try:
        import urllib.request

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = json.dumps({"chat_id": chat_id, "text": message}).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception:
        pass


def _log(msg: str) -> None:
    """Write a timestamped line to stdout (captured by the unified multiplexer)."""
    print(f"[GIT-SYNC] {_ts()} {msg}", flush=True)


# ── Master: add / commit / push ───────────────────────────────────────────────

def _do_master_push() -> None:
    """
    Stage all changes, commit with a timestamped message, and push.
    Skips the commit step if there is nothing to commit.
    """
    commit_before = _current_commit(ROOT)

    # git add .
    add_result = _run_git(["add", "."], ROOT)
    if add_result is None or add_result.returncode != 0:
        _log(f"WARNING: git add failed — {(add_result.stderr if add_result else 'timeout').strip()}")
        return

    # Check if there is anything to commit
    status_result = _run_git(["status", "--porcelain"], ROOT)
    if status_result and status_result.returncode == 0 and not status_result.stdout.strip():
        _log("Nothing to commit — working tree clean.")
        return

    # git commit
    commit_msg = f"{COMMIT_MSG_PREFIX} [{_ts()}]"
    commit_result = _run_git(["commit", "-m", commit_msg], ROOT)
    if commit_result is None:
        _log("WARNING: git commit timed out or git not found.")
        return
    if commit_result.returncode != 0:
        stderr = commit_result.stderr.strip()
        # "nothing to commit" is not a real error
        if "nothing to commit" in (commit_result.stdout + stderr).lower():
            _log("Nothing to commit — working tree clean.")
            return
        _log(f"ERROR: git commit failed (exit {commit_result.returncode}): {stderr}")
        _push_redis_alert({
            "ts": _ts(),
            "source": "git_sync_master",
            "level": "warning",
            "message": f"[GIT-SYNC MASTER] commit failed: {stderr[:300]}",
        })
        return

    # git push
    push_result = _run_git(["push", "origin", BRANCH], ROOT)
    if push_result is None:
        _log("WARNING: git push timed out or git not found.")
        return

    stdout = push_result.stdout.strip()
    stderr = push_result.stderr.strip()
    combined = f"{stdout}\n{stderr}".strip()

    if push_result.returncode != 0:
        _log(f"ERROR: git push failed (exit {push_result.returncode}):\n{combined}")
        _push_redis_alert({
            "ts": _ts(),
            "source": "git_sync_master",
            "level": "critical",
            "message": f"[GIT-SYNC MASTER] push failed: {combined[:400]}",
        })
        _send_telegram(f"⚠️ [GIT-SYNC MASTER] Push failed on {socket.gethostname()}\n{combined[:300]}")
        return

    commit_after = _current_commit(ROOT)
    _log(f"Push successful. Commit: {commit_after[:8] if commit_after else '?'}")

    if commit_after and commit_before != commit_after:
        _send_telegram(
            f"🚀 [GIT-SYNC MASTER] Auto-pushed on {socket.gethostname()}\n"
            f"Branch: {BRANCH} | Commit: {commit_after[:8]}"
        )


# ── Worker: fetch / hard-reset ────────────────────────────────────────────────

def _do_worker_reset() -> None:
    """
    Fetch the latest refs from origin and hard-reset to origin/<branch>.
    This guarantees the worker always mirrors the master exactly.
    """
    commit_before = _current_commit(ROOT)

    # git fetch origin
    fetch_result = _run_git(["fetch", "origin"], ROOT)
    if fetch_result is None:
        _log("WARNING: git fetch timed out or git not found.")
        return
    if fetch_result.returncode != 0:
        stderr = fetch_result.stderr.strip()
        _log(f"ERROR: git fetch failed (exit {fetch_result.returncode}): {stderr}")
        _push_redis_alert({
            "ts": _ts(),
            "source": "git_sync_worker",
            "level": "warning",
            "message": f"[GIT-SYNC WORKER] fetch failed: {stderr[:300]}",
        })
        return

    # git reset --hard origin/<branch>
    reset_result = _run_git(["reset", "--hard", f"origin/{BRANCH}"], ROOT)
    if reset_result is None:
        _log("WARNING: git reset timed out or git not found.")
        return
    if reset_result.returncode != 0:
        stderr = reset_result.stderr.strip()
        _log(f"ERROR: git reset --hard failed (exit {reset_result.returncode}): {stderr}")
        _push_redis_alert({
            "ts": _ts(),
            "source": "git_sync_worker",
            "level": "critical",
            "message": f"[GIT-SYNC WORKER] reset --hard failed: {stderr[:300]}",
        })
        _send_telegram(
            f"⚠️ [GIT-SYNC WORKER] Reset failed on {socket.gethostname()}\n{stderr[:300]}"
        )
        return

    commit_after = _current_commit(ROOT)
    _log(f"Reset successful. HEAD: {commit_after[:8] if commit_after else '?'}")

    if commit_after and commit_before and commit_after != commit_before:
        _send_telegram(
            f"🔄 [GIT-SYNC WORKER] New code pulled on {socket.gethostname()}\n"
            f"Branch: {BRANCH} | Commit: {commit_after[:8]}"
        )


# ── FORCE_GIT_PULL Redis subscriber ──────────────────────────────────────────

NEXUS_COMMANDS_CHANNEL = "nexus:commands"
FORCE_GIT_PULL_KEY = "nexus:commands:force_git_pull"

# Event set by the subscriber thread to trigger an immediate pull/reset
_force_pull_event = threading.Event()


def _redis_key_poller() -> None:
    """
    Background thread that polls the ``nexus:commands:force_git_pull`` Redis key
    every 5 seconds. When the key is set (by the dashboard button or any API call),
    it is consumed and ``_force_pull_event`` is set for immediate execution.
    """
    try:
        import redis as _redis  # type: ignore[import]

        client = _redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
        _log(f"Polling Redis key '{FORCE_GIT_PULL_KEY}' every 5s for force-pull triggers.")
        while True:
            try:
                val = client.get(FORCE_GIT_PULL_KEY)
                if val:
                    client.delete(FORCE_GIT_PULL_KEY)
                    _log(f"FORCE_GIT_PULL key consumed (value={val!r}) — triggering immediate reset.")
                    _force_pull_event.set()
            except Exception:
                pass
            time.sleep(5.0)
    except Exception as exc:
        _log(f"Redis key poller error: {exc} — key-based force-pull will be unavailable.")


def _redis_command_listener() -> None:
    """
    Subscribe to the ``nexus:commands`` Redis pub/sub channel.
    When a ``FORCE_GIT_PULL`` message arrives, set ``_force_pull_event``
    so the main loop executes an immediate worker reset.
    """
    try:
        import redis as _redis  # type: ignore[import]

        client = _redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
        pubsub = client.pubsub()
        pubsub.subscribe(NEXUS_COMMANDS_CHANNEL)
        _log(f"Subscribed to Redis channel '{NEXUS_COMMANDS_CHANNEL}' for FORCE_GIT_PULL commands.")

        for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            try:
                data = json.loads(message.get("data", "{}"))
            except (json.JSONDecodeError, TypeError):
                # Also handle plain string "FORCE_GIT_PULL"
                if str(message.get("data", "")).strip() == "FORCE_GIT_PULL":
                    _log("FORCE_GIT_PULL plain string received — triggering immediate reset.")
                    _force_pull_event.set()
                continue
            # Support both 'command' and 'action' field names
            cmd = data.get("command") or data.get("action") or ""
            if cmd == "FORCE_GIT_PULL":
                _log(f"FORCE_GIT_PULL received from '{data.get('issued_by', data.get('source', 'unknown'))}' — triggering immediate reset.")
                _force_pull_event.set()
    except Exception as exc:
        _log(f"Redis command listener error: {exc} — force-pull via dashboard will be unavailable.")


# ── Task failure tracker ──────────────────────────────────────────────────────

_consecutive_failures: int = 0
_FAILURE_REPORT_THRESHOLD: int = 3


def _handle_action_failure(exc: Exception, mode: str) -> None:
    """Track consecutive failures; send Telegram ERROR_REPORT after 3 in a row."""
    global _consecutive_failures  # noqa: PLW0603
    _consecutive_failures += 1
    _log(f"Action failed ({_consecutive_failures}/{_FAILURE_REPORT_THRESHOLD}): {exc}")

    if _consecutive_failures >= _FAILURE_REPORT_THRESHOLD:
        hostname = socket.gethostname()
        report_body = (
            f"Host: {hostname}\n"
            f"Mode: {mode}\n"
            f"Branch: {BRANCH}\n"
            f"Consecutive failures: {_consecutive_failures}\n\n"
            f"Last error:\n{exc}"
        )
        _push_redis_alert({
            "ts": _ts(),
            "source": f"git_sync_{mode}",
            "level": "critical",
            "message": f"[GIT-SYNC] ERROR_REPORT — {_consecutive_failures} consecutive failures on {hostname}",
        })
        _send_telegram(
            f"🚨 [GIT-SYNC ERROR_REPORT]\n\n"
            f"{report_body[:3500]}"
        )
        _log(f"ERROR_REPORT dispatched to Telegram after {_consecutive_failures} failures.")
        _consecutive_failures = 0  # reset after reporting


# ── Main ──────────────────────────────────────────────────────────────────────

def _disable_linux_sleep() -> None:
    """
    Prevent the display and system from sleeping on Linux worker nodes.
    Runs ``xset s off && xset -dpms`` via os.system on startup.
    Safe no-op on Windows/macOS.
    """
    if sys.platform.startswith("linux"):
        try:
            ret = os.system("xset s off && xset -dpms")  # noqa: S605
            if ret == 0:
                _log("Linux sleep/DPMS disabled via xset.")
            else:
                _log(f"xset sleep-disable returned non-zero ({ret}) — DISPLAY may not be set.")
        except Exception as exc:
            _log(f"Could not disable Linux sleep: {exc}")


def main() -> None:
    global _consecutive_failures  # noqa: PLW0603

    _disable_linux_sleep()

    if not _is_git_repo(ROOT):
        _log(f"{ROOT} is not a Git repository — service exiting.")
        return

    mode = _detect_mode()
    hostname = socket.gethostname()

    if mode == "master":
        _log(
            f"MASTER mode active on {hostname} — "
            f"pushing '{BRANCH}' every {PUSH_INTERVAL_S}s"
        )
        interval = PUSH_INTERVAL_S
        action = _do_master_push
    else:
        _log(
            f"WORKER mode active on {hostname} — "
            f"hard-resetting to origin/{BRANCH} every {PULL_INTERVAL_S}s"
        )
        interval = PULL_INTERVAL_S
        action = _do_worker_reset

    # Start the Redis pub/sub command listener
    listener_thread = threading.Thread(
        target=_redis_command_listener,
        name="git-sync-redis-listener",
        daemon=True,
    )
    listener_thread.start()

    # Start the Redis key poller (catches dashboard button / API key-set triggers)
    poller_thread = threading.Thread(
        target=_redis_key_poller,
        name="git-sync-redis-key-poller",
        daemon=True,
    )
    poller_thread.start()

    last_run = time.monotonic() - interval  # run immediately on first tick

    while True:
        now = time.monotonic()
        force_triggered = _force_pull_event.is_set()

        if force_triggered or (now - last_run) >= interval:
            if force_triggered:
                _log("Executing FORCED git pull/reset (triggered via dashboard).")
                _force_pull_event.clear()
            try:
                action()
                _consecutive_failures = 0  # reset on success
            except Exception as exc:
                _handle_action_failure(exc, mode)
            last_run = time.monotonic()

        time.sleep(1.0)


if __name__ == "__main__":
    main()
