"""
Nexus Git Auto-Sync Manager — NexusGitSync background service.

NexusGitSync is a role-aware background service that keeps all nodes in sync
with the Master (Jacob-PC) at all times.

MASTER ROLE (Jacob-PC / NODE_ROLE=master):
  - Every 10 minutes: 'git add .', 'git commit -m "Auto-sync Master"', 'git push'.
  - Ensures the remote always has the latest master state.
  - Merge conflicts are resolved by always keeping the Master's local version
    (git reset --hard HEAD before push if push fails due to divergence).

WORKER / LAPTOP ROLE (Linux nodes / NODE_ROLE=worker):
  - Every 30 minutes: 'git fetch --all', 'git reset --hard origin/main'.
  - Guarantees perfect alignment with master; local changes are ALWAYS discarded.
  - Merge conflicts are impossible because reset --hard overwrites everything.

Role detection order:
  1. NODE_ROLE env var: "master" → master, anything else → worker.
  2. NODE_ID env var: contains "master" → master role.
  3. Fallback: worker role.

Environment variables (all optional)
--------------------------------------
NODE_ROLE               — "master" | "worker" (overrides auto-detect)
NODE_ID                 — node identifier (used for role auto-detect)
GIT_SYNC_BRANCH         — branch name (default: main)
REDIS_URL               — Redis connection string (default: redis://127.0.0.1:6379/0)
TELEGRAM_NEXUS_BOT_TOKEN — project / Nexus bot (preferred for git push notices)
TELEGRAM_NEXUS_ADMIN_CHAT_ID — optional; defaults to TELEGRAM_ADMIN_CHAT_ID
TELEGRAM_BOT_TOKEN      — fallback bot token if NEXUS token unset
TELEGRAM_ADMIN_CHAT_ID  — chat-id to receive git sync messages

Usage
-----
    # Run as a standalone background service:
    python scripts/git_manager.py

    # Or import and run in-process:
    from scripts.git_manager import NexusGitSync
    svc = NexusGitSync()
    svc.start()   # non-blocking daemon thread
    svc.stop()    # graceful shutdown
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
REDIS_URL: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

MASTER_INTERVAL_S: int = 600    # 10 minutes
WORKER_INTERVAL_S: int = 1800   # 30 minutes

ALERTS_KEY = "nexus:system:alerts"
MAX_ALERTS = 50

_HERE = Path(os.path.dirname(os.path.abspath(__file__)))
ROOT: Path = _HERE.parent if _HERE.name == "scripts" else _HERE


# ── Role detection ─────────────────────────────────────────────────────────────

_MASTER_HOSTNAMES = {"jacob-pc", "admindesktop"}


def _detect_role() -> str:
    """Return 'master' or 'worker' based on env vars or hostname."""
    explicit = os.getenv("NODE_ROLE", "").strip().lower()
    if explicit == "master":
        return "master"
    if explicit and explicit != "master":
        return "worker"

    node_id = os.getenv("NODE_ID", "").strip().lower()
    if "master" in node_id:
        return "master"

    import socket
    hostname = socket.gethostname().strip().lower()
    if hostname in _MASTER_HOSTNAMES:
        return "master"

    return "worker"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _is_git_repo(path: Path) -> bool:
    return (path / ".git").exists()


def _run_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        print(f"⚠️  [GIT-MANAGER] git command failed: {exc}", flush=True)
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
    """Nexus project bot first (same as Polymarket / wallet alerts), then channel bot."""
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
            url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception:
        pass


# ── Master sync logic ─────────────────────────────────────────────────────────

def _do_master_push() -> None:
    """
    git add . → git commit → git push.
    Skips commit if there is nothing to commit.
    On push rejection (diverged remote): force-push via --force-with-lease so
    Master's version always wins. On other push failures: logs alert, continues.
    """
    hostname = socket.gethostname()

    # Stage all changes
    add_result = _run_git(["add", "."], ROOT)
    if add_result is None or add_result.returncode != 0:
        print(f"⚠️  [GIT-MASTER] 'git add .' failed at {_ts()}", flush=True)
        return

    # Commit (skip if nothing staged)
    commit_result = _run_git(
        ["commit", "-m", "Nexus Pulse"],
        ROOT,
    )
    if commit_result is None:
        print(f"⚠️  [GIT-MASTER] 'git commit' timed out at {_ts()}", flush=True)
        return

    stdout = commit_result.stdout.strip()
    if "nothing to commit" in stdout or "nothing added to commit" in stdout:
        print(f"✅ [GIT-MASTER] Nothing to commit at {_ts()}", flush=True)
        return

    if commit_result.returncode != 0:
        stderr = commit_result.stderr.strip()
        print(f"⚠️  [GIT-MASTER] Commit failed at {_ts()}: {stderr}", flush=True)
        return

    print(f"📝 [GIT-MASTER] Committed changes at {_ts()}", flush=True)

    # Push to remote
    push_result = _run_git(["push", "origin", BRANCH], ROOT)
    if push_result is None:
        print(f"⚠️  [GIT-MASTER] 'git push' timed out at {_ts()}", flush=True)
        return

    if push_result.returncode != 0:
        combined = f"{push_result.stdout}\n{push_result.stderr}".strip()
        # Diverged remote — Master always wins: force-push with lease
        if "rejected" in combined or "non-fast-forward" in combined or "diverged" in combined:
            print(
                f"⚠️  [GIT-MASTER] Push rejected (diverged) — forcing Master version at {_ts()}",
                flush=True,
            )
            force_result = _run_git(["push", "--force-with-lease", "origin", BRANCH], ROOT)
            if force_result and force_result.returncode == 0:
                msg = (
                    f"🚀 [GIT-MASTER] Force-pushed to origin/{BRANCH} from {hostname} "
                    f"(conflict resolved — Master wins) at {_ts()}"
                )
                print(msg, flush=True)
                _send_telegram(msg)
                return
            # Last resort: hard force
            _run_git(["push", "--force", "origin", BRANCH], ROOT)
            msg = (
                f"🚀 [GIT-MASTER] Hard-forced push to origin/{BRANCH} from {hostname} at {_ts()}"
            )
            print(msg, flush=True)
            _send_telegram(msg)
            return

        alert = {
            "ts": _ts(),
            "source": "git_manager_master",
            "level": "warning",
            "message": f"[GIT-MASTER] Push failed on {hostname}: {combined[:300]}",
        }
        print(f"⚠️  [GIT-MASTER] Push failed at {_ts()}:\n{combined}", flush=True)
        _push_redis_alert(alert)
        return

    msg = f"🚀 [GIT-MASTER] Code pushed to origin/{BRANCH} from {hostname} at {_ts()}"
    print(msg, flush=True)
    _send_telegram(msg)


# ── Worker sync logic ─────────────────────────────────────────────────────────

def _do_worker_pull() -> None:
    """
    git fetch --all → git reset --hard origin/<BRANCH>.
    Forces perfect alignment with master; local changes are discarded.
    On conflict or error: logs alert, continues (does not halt).
    """
    hostname = socket.gethostname()
    commit_before = _current_commit(ROOT)

    # Fetch all remotes
    fetch_result = _run_git(["fetch", "--all"], ROOT)
    if fetch_result is None:
        print(f"⚠️  [GIT-WORKER] 'git fetch --all' timed out at {_ts()}", flush=True)
        return

    if fetch_result.returncode != 0:
        stderr = fetch_result.stderr.strip()
        print(f"⚠️  [GIT-WORKER] Fetch failed at {_ts()}: {stderr}", flush=True)
        _push_redis_alert({
            "ts": _ts(), "source": "git_manager_worker", "level": "warning",
            "message": f"[GIT-WORKER] Fetch failed on {hostname}: {stderr[:300]}",
        })
        return

    # Hard reset to origin/BRANCH — guarantees perfect alignment
    reset_result = _run_git(["reset", "--hard", f"origin/{BRANCH}"], ROOT)
    if reset_result is None:
        print(f"⚠️  [GIT-WORKER] 'git reset --hard' timed out at {_ts()}", flush=True)
        return

    if reset_result.returncode != 0:
        stderr = reset_result.stderr.strip()
        print(f"⚠️  [GIT-WORKER] Reset failed at {_ts()}: {stderr}", flush=True)
        _push_redis_alert({
            "ts": _ts(), "source": "git_manager_worker", "level": "warning",
            "message": f"[GIT-WORKER] Reset failed on {hostname}: {stderr[:300]}",
        })
        return

    commit_after = _current_commit(ROOT)
    if commit_after and commit_before and commit_after != commit_before:
        msg = f"🔄 [GIT-WORKER] Synced to master on {hostname} — new commit: {commit_after[:8]}"
        print(msg, flush=True)
        _send_telegram(msg)
    else:
        print(f"✅ [GIT-WORKER] Already up-to-date at {_ts()}", flush=True)


# ── NexusGitSync — named background service class ─────────────────────────────

class NexusGitSync:
    """
    Drop-in background service wrapper around the git sync logic.

    Usage
    -----
        svc = NexusGitSync()
        svc.start()   # starts a daemon thread — non-blocking
        svc.stop()    # signals the thread to exit gracefully

    The service auto-detects the node role (master / worker) and runs the
    appropriate sync cycle at the configured interval.
    """

    SERVICE_NAME = "NexusGitSync"

    def __init__(
        self,
        branch: str = BRANCH,
        master_interval: int = MASTER_INTERVAL_S,
        worker_interval: int = WORKER_INTERVAL_S,
    ) -> None:
        self.branch = branch
        self.master_interval = master_interval
        self.worker_interval = worker_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the sync loop in a background daemon thread."""
        if self._thread and self._thread.is_alive():
            print(f"[{self.SERVICE_NAME}] Already running.", flush=True)
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=self.SERVICE_NAME,
            daemon=True,
        )
        self._thread.start()
        role = _detect_role()
        interval = self.master_interval if role == "master" else self.worker_interval
        print(
            f"[{self.SERVICE_NAME}] Started — role={role.upper()} "
            f"branch={self.branch} interval={interval}s host={socket.gethostname()}",
            flush=True,
        )

    def stop(self) -> None:
        """Signal the background thread to stop and wait for it to exit."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        print(f"[{self.SERVICE_NAME}] Stopped.", flush=True)

    def _loop(self) -> None:
        if not _is_git_repo(ROOT):
            print(
                f"[{self.SERVICE_NAME}] {ROOT} is not a Git repository — exiting.",
                flush=True,
            )
            return

        role = _detect_role()
        interval = self.master_interval if role == "master" else self.worker_interval

        while not self._stop_event.is_set():
            try:
                if role == "master":
                    _do_master_push()
                else:
                    _do_worker_pull()
            except Exception as exc:
                print(
                    f"⚠️  [{self.SERVICE_NAME}] Unexpected error at {_ts()}: {exc}",
                    flush=True,
                )
            # Sleep in small increments so stop() is responsive
            for _ in range(interval):
                if self._stop_event.is_set():
                    return
                time.sleep(1)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_once() -> None:
    """Execute a single sync cycle based on detected role. Used by nexus_core integration."""
    if not _is_git_repo(ROOT):
        return
    role = _detect_role()
    if role == "master":
        _do_master_push()
    else:
        _do_worker_pull()


def _disable_linux_sleep() -> None:
    """
    Prevent display/system sleep on Linux worker nodes.
    Executes ``xset s off && xset -dpms`` on startup.
    Safe no-op on non-Linux platforms.
    """
    if sys.platform.startswith("linux"):
        try:
            ret = os.system("xset s off && xset -dpms")  # noqa: S605
            if ret == 0:
                print("[GIT-MANAGER] Linux sleep/DPMS disabled via xset.", flush=True)
            else:
                print(
                    f"[GIT-MANAGER] xset returned {ret} — DISPLAY may not be set.",
                    flush=True,
                )
        except Exception as exc:
            print(f"[GIT-MANAGER] Could not disable Linux sleep: {exc}", flush=True)


def main() -> None:
    """
    Run NexusGitSync in the foreground (blocking).
    Intended for use as a standalone process / systemd service.
    """
    _disable_linux_sleep()

    if not _is_git_repo(ROOT):
        print(f"[GIT-MANAGER] {ROOT} is not a Git repository — skipping.", flush=True)
        return

    svc = NexusGitSync()
    svc.start()

    # Block the main thread so the daemon thread keeps running
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        svc.stop()


# ── NexusGitOrchestrator / NexusGitDaemon — canonical aliases ────────────────
# Exposed so other modules can import by any preferred name:
#   from scripts.git_manager import NexusGitOrchestrator
#   from scripts.git_manager import NexusGitDaemon
NexusGitOrchestrator = NexusGitSync
NexusGitDaemon = NexusGitSync


if __name__ == "__main__":
    main()
