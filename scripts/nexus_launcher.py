"""
Nexus master stack: Redis, core worker, node monitor TUI, deployer API, Telegram bot(s),
Polymarket helper, frontend, and optional git-sync — all in one process tree.

All child stdout/stderr is captured and displayed in a single Rich.Live dashboard
(unified terminal multiplexer). No separate CMD windows are opened.
Run: ``python -m scripts.nexus_launcher`` from the repo root (``.env`` is loaded here).

Ctrl+C tears down the full process tree.

Self-healing: any service that exits with a non-zero code is restarted up to 3 times.
After 3 failed restarts a Telegram critical alert is dispatched.
"""

from __future__ import annotations

import sys, os

# Prefer the canonical nexus/ package (root-level) over the legacy src/nexus/ layout.
# The root package is installed via pyproject.toml; add the repo root so imports work
# when running this script directly (not via pip install -e).
_repo_root = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _repo_root)
sys.path.insert(0, os.getcwd())

import atexit
import collections
import ctypes
import json
import shutil
import signal
import socket
import subprocess
import threading
import time
import traceback
import urllib.request
import zipfile
from pathlib import Path


def _base_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(os.path.dirname(sys.executable)).resolve()
    return Path(os.path.dirname(os.path.abspath(__file__))).resolve()


BASE_PATH = _base_path()


def _project_root() -> Path:
    if getattr(sys, "frozen", False):
        return BASE_PATH
    return BASE_PATH.parent


ROOT = _project_root()


def _load_repo_dotenv() -> None:
    """Apply repo ``.env`` into ``os.environ`` so project config wins over inherited OS env."""
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().split("#")[0].strip()
            if key:
                os.environ[key] = val
    except OSError:
        pass


# ── Debug log (file only — stdout stays for Rich) ────────────────────────────
_DEBUG_LOG = ROOT / "launcher_debug.txt"
try:
    _DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
    _debug_file = open(_DEBUG_LOG, "a", encoding="utf-8", errors="replace", buffering=1)
    _debug_file.write(
        f"\n{'=' * 72}\n"
        f"[launcher] session start {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"[launcher] EXE={sys.executable}  frozen={getattr(sys, 'frozen', False)}\n"
        f"{'=' * 72}\n"
    )
    _debug_file.flush()
except Exception:
    _debug_file = None  # type: ignore[assignment]


def _dbg(msg: str) -> None:
    if _debug_file:
        try:
            _debug_file.write(msg if msg.endswith("\n") else msg + "\n")
            _debug_file.flush()
        except Exception:
            pass


def _show_error_popup(title: str, message: str) -> None:
    if sys.platform == "win32":
        try:
            ctypes.windll.user32.MessageBoxW(0, message, title, 0x10)
        except Exception:
            pass


def _subprocess_resource_path(*parts: str) -> str:
    rel = os.path.join(*parts)
    if getattr(sys, "frozen", False):
        install_root = os.path.abspath(os.path.dirname(sys.executable))
        primary = os.path.abspath(os.path.join(install_root, rel))
        if os.path.exists(primary):
            return primary
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            bundled = os.path.abspath(os.path.join(meipass, rel))
            if os.path.exists(bundled):
                return bundled
        return primary
    return os.path.abspath(os.path.join(str(ROOT), rel))


LOG_DIR = ROOT / "logs"
LOG_FILE = LOG_DIR / "combined_launcher.log"

# ── Per-service ring buffers (last N lines) ───────────────────────────────────
_RING_SIZE = 200
_log_rings: dict[str, collections.deque[str]] = {}
_ring_lock = threading.Lock()

_processes: list[subprocess.Popen[str]] = []
_reader_threads: list[threading.Thread] = []
_log_lock = threading.Lock()
_cleanup_lock = threading.Lock()
_cleaned = False

# ── Self-healing: per-service restart tracking ────────────────────────────────
_MAX_RESTARTS = 3
_RESTART_COUNTS: dict[str, int] = {}
_RESTART_LOCK = threading.Lock()

# ── Service argv registry for restart ────────────────────────────────────────
_SERVICE_REGISTRY: dict[str, tuple[list[str], str, dict[str, str] | None]] = {}
_PROC_TO_LABEL: dict[int, str] = {}

# ── Service color map for Rich ────────────────────────────────────────────────
_SERVICE_COLORS: dict[str, str] = {
    "redis":          "bright_red",
    "core":           "bright_cyan",
    "monitor":        "bright_yellow",
    "deployer":       "bright_magenta",
    "api":            "bright_green",
    "frontend":       "bright_blue",
    "git-sync":       "bright_white",
    "polymarket":     "bright_yellow",
    "telegram-bot":   "bright_green",
    "israeli-swarm":  "bright_cyan",
}

_SERVICES_ORDER = [
    "redis",
    "core",
    "monitor",
    "deployer",
    "telegram-bot",
    "api",
    "frontend",
    "israeli-swarm",
    "git-sync",
    "polymarket",
]


def _atexit_kill_children() -> None:
    kill_all()


atexit.register(_atexit_kill_children)


def _redis_server_path() -> Path:
    return Path(_subprocess_resource_path("redis-local", "redis-server.exe"))


def _redis_listen_lan() -> bool:
    """When True, bundled Redis binds 0.0.0.0 so LAN workers can use the Master IPv4 (see redis.windows-lan.conf)."""
    return os.environ.get("NEXUS_REDIS_LISTEN_LAN", "").strip().lower() in ("1", "true", "yes", "on")


def _redis_argv() -> list[str]:
    exe = _subprocess_resource_path("redis-local", "redis-server.exe")
    if not _redis_listen_lan():
        return [exe]
    lan_conf = ROOT / "redis-local" / "redis.windows-lan.conf"
    if lan_conf.is_file():
        return [exe, str(lan_conf)]
    return [exe, "--bind", "0.0.0.0"]


def _ensure_redis_extracted(logf=None) -> bool:
    redis_exe = _redis_server_path()
    if redis_exe.is_file():
        return True

    candidates = [ROOT / "redis-win.zip", BASE_PATH / "redis-win.zip"]
    zip_path: Path | None = None
    for c in candidates:
        if c.is_file():
            zip_path = c
            break

    if zip_path is None:
        msg = "[launcher] redis-win.zip not found — cannot auto-extract Redis."
        _dbg(msg)
        if logf:
            logf.write(msg + "\n"); logf.flush()
        return False

    dest_dir = redis_exe.parent
    dest_dir.mkdir(parents=True, exist_ok=True)
    msg = f"[launcher] Extracting {zip_path} → {dest_dir} ..."
    _dbg(msg)
    if logf:
        logf.write(msg + "\n"); logf.flush()

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                filename = Path(member).name
                if not filename:
                    continue
                target = dest_dir / filename
                with zf.open(member) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
        ok = redis_exe.is_file()
        result_msg = f"[launcher] Extraction complete — redis-server.exe {'found' if ok else 'STILL MISSING'}."
        _dbg(result_msg)
        if logf:
            logf.write(result_msg + "\n"); logf.flush()
        return ok
    except Exception as exc:
        err_msg = f"[launcher] Failed to extract redis-win.zip: {exc}"
        _dbg(err_msg)
        if logf:
            logf.write(err_msg + "\n"); logf.flush()
        return False


def _is_redis_running(host: str = "127.0.0.1", port: int = 6379) -> bool:
    """Check Redis is actually responding to PING (not just TCP-open by another process)."""
    for check_host in [host, "::1"]:
        try:
            with socket.create_connection((check_host, port), timeout=1.0) as sock:
                sock.sendall(b"PING\r\n")
                data = sock.recv(16)
                if data and data.startswith(b"+PONG"):
                    return True
        except OSError:
            pass
    return False


def _kill_port(port: int) -> None:
    if sys.platform != "win32":
        try:
            subprocess.run(["fuser", "-k", f"{port}/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
        return
    try:
        run_kw: dict[str, object] = {"capture_output": True, "text": True}
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            run_kw["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
        result = subprocess.run(["netstat", "-ano", "-p", "TCP"], **run_kw)  # type: ignore[arg-type]
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) < 5:
                continue
            local, state, pid_str = parts[1], parts[3].upper(), parts[4]
            if f":{port}" in local and state == "LISTENING":
                try:
                    pid = int(pid_str)
                except ValueError:
                    continue
                kill_kw: dict[str, object] = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
                if hasattr(subprocess, "CREATE_NO_WINDOW"):
                    kill_kw["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], **kill_kw)
                time.sleep(0.5)
    except Exception as exc:
        _dbg(f"[launcher] _kill_port({port}) error: {exc}")


def _python_for_children() -> str | None:
    if not getattr(sys, "frozen", False):
        return sys.executable
    override = (os.environ.get("NEXUS_LAUNCHER_PYTHON") or "").strip()
    if override:
        return override
    for name in ("python", "py"):
        found = shutil.which(name)
        if found:
            return found
    return None


def _child_env(base: dict[str, str]) -> dict[str, str]:
    out = dict(base)
    pr = str(ROOT.resolve())
    if pr:
        prev = (out.get("PYTHONPATH") or "").strip()
        out["PYTHONPATH"] = pr if not prev else f"{pr}{os.pathsep}{prev}"
    out["PYTHONIOENCODING"] = "utf-8"
    out["PYTHONUTF8"] = "1"
    return out


def _kill_process_tree(pid: int) -> None:
    if pid <= 0:
        return
    if sys.platform == "win32":
        run_kw: dict[str, object] = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            run_kw["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], **run_kw)
    else:
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except PermissionError:
            pass


def kill_all() -> None:
    global _cleaned
    with _cleanup_lock:
        if _cleaned:
            return
        _cleaned = True

    for proc in list(_processes):
        if proc.poll() is not None:
            continue
        try:
            _kill_process_tree(proc.pid)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

    deadline = time.monotonic() + 10.0
    for proc in list(_processes):
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.05)
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
    _processes.clear()

    for t in _reader_threads:
        t.join(timeout=2.0)
    _reader_threads.clear()


def _popen_session_kw() -> dict[str, object]:
    if sys.platform == "win32":
        return {}
    return {"start_new_session": True}


def _stream_reader(proc: subprocess.Popen[str], label: str, logf) -> None:
    """Read stdout from a child process, write to file log and ring buffer."""
    if proc.stdout is None:
        return
    try:
        for line in iter(proc.stdout.readline, ""):
            if line == "":
                if proc.poll() is not None:
                    break
                continue
            stripped = line.rstrip("\n")
            with _ring_lock:
                ring = _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE))
                ring.append(stripped)
            with _log_lock:
                logf.write(f"[{label}] {line}")
                logf.flush()
    except Exception:
        pass


def _spawn(label: str, argv: list[str], *, cwd: str, logf, env: dict[str, str] | None) -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        **_popen_session_kw(),
    )
    _processes.append(proc)
    with _ring_lock:
        _PROC_TO_LABEL[proc.pid] = label
    t = threading.Thread(
        target=_stream_reader,
        args=(proc, label, logf),
        name=f"nexus-log-{label}",
        daemon=True,
    )
    _reader_threads.append(t)
    t.start()
    # Seed ring buffer
    with _ring_lock:
        _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE))
    return proc


def _send_telegram_critical(message: str) -> None:
    """Best-effort Telegram critical alert (no external deps). Uses Nexus project bot when set."""
    bot_token = (os.environ.get("TELEGRAM_NEXUS_BOT_TOKEN") or "").strip()
    if not bot_token:
        bot_token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_NEXUS_ADMIN_CHAT_ID") or "").strip()
    if not chat_id:
        chat_id = (os.environ.get("TELEGRAM_ADMIN_CHAT_ID") or "").strip()
    if not bot_token or not chat_id:
        return
    try:
        text = f"🚨 *NEXUS LAUNCHER — CRITICAL*\n\n{message[:3000]}"
        payload = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as exc:
        _dbg(f"[launcher] Telegram alert failed: {exc}")


def _register_service(label: str, argv: list[str], cwd: str, env: dict[str, str] | None) -> None:
    """Register a service so the watchdog can restart it."""
    _SERVICE_REGISTRY[label] = (argv, cwd, env)
    with _RESTART_LOCK:
        _RESTART_COUNTS.setdefault(label, 0)


def _watchdog_restart(label: str, logf, stop_event: threading.Event) -> None:
    """Called by the watchdog when a service exits unexpectedly. Restarts up to _MAX_RESTARTS times."""
    with _RESTART_LOCK:
        count = _RESTART_COUNTS.get(label, 0) + 1
        _RESTART_COUNTS[label] = count

    if label not in _SERVICE_REGISTRY:
        _dbg(f"[watchdog] {label} not in registry — cannot restart.")
        return

    if count > _MAX_RESTARTS:
        msg = (
            f"[WATCHDOG] Service *{label}* has crashed {count - 1} times and exceeded "
            f"the max restart limit ({_MAX_RESTARTS}). Manual intervention required."
        )
        _dbg(msg)
        with _ring_lock:
            _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE)).append(
                f"[DEAD] Exceeded {_MAX_RESTARTS} restarts — NOT restarting. Check logs."
            )
        _send_telegram_critical(msg)
        return

    argv, cwd, env = _SERVICE_REGISTRY[label]
    delay = min(5 * count, 30)
    _dbg(f"[watchdog] Restarting [{label}] in {delay}s (attempt {count}/{_MAX_RESTARTS})…")
    with _ring_lock:
        _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE)).append(
            f"[WATCHDOG] Crashed — restarting in {delay}s (attempt {count}/{_MAX_RESTARTS})"
        )

    if stop_event.wait(timeout=delay):
        return

    # Free the port before restarting port-bound services so the new process
    # doesn't immediately crash with EADDRINUSE.
    if label == "deployer":
        _kill_port(8002)
    elif label == "api":
        _kill_port(8001)

    try:
        new_proc = _spawn(label, argv, cwd=cwd, logf=logf, env=env)
        _dbg(f"[watchdog] [{label}] restarted — pid={new_proc.pid}")
        with _ring_lock:
            _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE)).append(
                f"[WATCHDOG] Restarted successfully (pid={new_proc.pid})"
            )
    except Exception as exc:
        _dbg(f"[watchdog] Failed to restart [{label}]: {exc}")
        with _ring_lock:
            _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE)).append(
                f"[WATCHDOG] Restart FAILED: {exc}"
            )


# ── Rich live dashboard ───────────────────────────────────────────────────────

def _build_rich_layout():
    """Build and return the Rich Layout used for the live dashboard."""
    try:
        from rich.layout import Layout
        from rich.panel import Panel
        from rich.text import Text

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=5),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )
        layout["body"].split_row(
            Layout(name="left"),
            Layout(name="right"),
        )
        layout["left"].split_column(
            Layout(name="redis"),
            Layout(name="core"),
            Layout(name="monitor"),
            Layout(name="deployer"),
            Layout(name="telegram-bot"),
        )
        layout["right"].split_column(
            Layout(name="api"),
            Layout(name="frontend"),
            Layout(name="git-sync"),
            Layout(name="polymarket"),
            Layout(name="israeli-swarm"),
        )
        return layout
    except ImportError:
        return None


def _make_service_panel(label: str, lines: list[str], color: str):
    """Render a Rich Panel for a single service."""
    from rich.panel import Panel
    from rich.text import Text

    content = Text()
    for line in lines[-20:]:
        # Colorize error/warn/success lines
        if any(k in line.lower() for k in ("error", "exception", "traceback", "critical", "fatal")):
            content.append(line + "\n", style="bold red")
        elif any(k in line.lower() for k in ("warn", "warning")):
            content.append(line + "\n", style="yellow")
        elif any(k in line.lower() for k in ("success", "started", "ready", "ok")):
            content.append(line + "\n", style="bright_green")
        else:
            content.append(line + "\n", style=color)

    if not lines:
        content.append("Waiting for output…", style="dim")

    return Panel(
        content,
        title=f"[bold {color}]{label.upper()}[/]",
        border_style=color,
        padding=(0, 1),
    )


def _run_rich_dashboard(logf, stop_event: threading.Event) -> None:
    """Run the Rich.Live unified dashboard in the main thread."""
    try:
        from rich.live import Live
        from rich.layout import Layout
        from rich.panel import Panel
        from rich.text import Text
        from rich.console import Console
        from rich import box
    except ImportError:
        _dbg("[launcher] Rich not installed — falling back to plain log mode.")
        stop_event.wait()
        return

    console = Console()
    layout = _build_rich_layout()
    if layout is None:
        stop_event.wait()
        return

    _HATAN_HEADER = (
        "[bold cyan]╔══ NEXUS OS — HATAN INDUSTRIES ══╗[/]\n"
        "[bold cyan]║[/]  [bright_white]Frontend[/] → [cyan]http://localhost:3000[/]   "
        "[bright_white]API[/] → [cyan]http://localhost:8001[/]   "
        "[bright_white]Deployer[/] → [cyan]http://localhost:8002[/]  [bold cyan]║[/]"
    )

    with Live(layout, console=console, refresh_per_second=4, screen=True) as live:
        while not stop_event.is_set():
            # Header
            from rich.align import Align
            layout["header"].update(
                Panel(
                    Align.center(_HATAN_HEADER),
                    border_style="cyan",
                    padding=(0, 2),
                )
            )

            # Footer
            layout["footer"].update(
                Panel(
                    f"[dim]{time.strftime('%Y-%m-%d %H:%M:%S')}[/]  "
                    "[dim]Press Ctrl+C to stop all services[/]",
                    border_style="dim",
                )
            )

            # Service panels
            with _ring_lock:
                rings_snapshot = {k: list(v) for k, v in _log_rings.items()}

            for svc in _SERVICES_ORDER:
                lines = rings_snapshot.get(svc, [])
                color = _SERVICE_COLORS.get(svc, "white")
                panel = _make_service_panel(svc, lines, color)
                try:
                    layout[svc].update(panel)
                except Exception:
                    pass

            time.sleep(0.25)


# ── Banner (written to debug log only) ───────────────────────────────────────

_HATAN_BANNER = """
╔══════════════════════════════════════════════════════════════════════╗
║   NEXUS OS — HATAN INDUSTRIES — UNIFIED TERMINAL MULTIPLEXER        ║
║   Frontend  → http://localhost:3000                                  ║
║   Deployer  → http://localhost:8002                                  ║
║   API       → http://localhost:8001                                  ║
╚══════════════════════════════════════════════════════════════════════╝
✅ [HATAN INDUSTRIES] ISRAELI SWARM ACTIVE - 5,000 MEMBER SIMULATION RUNNING
"""


def main() -> int:
    global _cleaned
    _load_repo_dotenv()
    _cleaned = False
    _processes.clear()
    _reader_threads.clear()
    _log_rings.clear()

    _dbg(_HATAN_BANNER)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logf = LOG_FILE.open("a", encoding="utf-8", errors="replace", buffering=1)

    try:
        with _log_lock:
            logf.write(
                f"\n{'=' * 72}\n"
                f"[launcher] session start {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"[launcher] ROOT={ROOT} BASE_PATH={BASE_PATH}\n"
                f"[launcher] UNIFIED TERMINAL MULTIPLEXER ACTIVE\n"
                f"{'=' * 72}\n"
            )
            logf.flush()

        if not _ensure_redis_extracted(logf=logf):
            redis_exe = _redis_server_path()
            _dbg(f"[launcher] ERROR: missing {redis_exe}")
            with _log_lock:
                logf.write(f"[launcher] ERROR: missing {redis_exe}\n")
                logf.flush()
            return 1

        py = _python_for_children()
        if py is None:
            msg = (
                "Frozen launcher needs a Python on PATH to spawn workers, or set "
                "NEXUS_LAUNCHER_PYTHON to python.exe."
            )
            _dbg(f"[launcher] ERROR: {msg}")
            with _log_lock:
                logf.write(f"[launcher] ERROR: {msg}\n")
                logf.flush()
            return 1

        child_env = _child_env(os.environ.copy())
        stop_event = threading.Event()

        def _on_signal(signum: int, _frame) -> None:
            _dbg(f"[launcher] received signal {signum}; kill_all()")
            with _log_lock:
                logf.write(f"[launcher] received signal {signum}; kill_all()\n")
                logf.flush()
            stop_event.set()
            kill_all()
            code = 128 + signum if signum > 0 else 0
            sys.exit(code)

        signal.signal(signal.SIGINT, _on_signal)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _on_signal)

        # ── Redis ─────────────────────────────────────────────────────────────
        redis_already_up = _is_redis_running()
        if redis_already_up:
            with _ring_lock:
                _log_rings.setdefault("redis", collections.deque(maxlen=_RING_SIZE)).append(
                    "Redis already running — skipped spawn."
                )
            with _log_lock:
                logf.write("[launcher] Redis already running — skipping spawn.\n")
                logf.flush()
        else:
            # Try default argv first; if 127.0.0.1:6379 is occupied by another
            # process (e.g. Windows svchost/iphlpsvc), fall back to ::1-only.
            redis_argv = _redis_argv()
            _dbg(f"[launcher] spawn redis: {redis_argv!r}")
            _spawn("redis", redis_argv, cwd=str(ROOT), logf=logf, env=child_env)
            with _log_lock:
                logf.write("[launcher] waiting 5 s for Redis to become ready...\n")
                logf.flush()
            time.sleep(3)
            # If Redis failed to bind on 127.0.0.1, retry with ::1-only bind (not when LAN listen is requested).
            if not _is_redis_running() and not _redis_listen_lan():
                _dbg("[launcher] Redis failed on 127.0.0.1 — retrying with --bind ::1")
                with _log_lock:
                    logf.write("[launcher] Redis bind failed on 127.0.0.1 — retrying with --bind ::1\n")
                    logf.flush()
                with _ring_lock:
                    _log_rings.setdefault("redis", collections.deque(maxlen=_RING_SIZE)).append(
                        "127.0.0.1:6379 occupied — retrying on ::1:6379"
                    )
                _spawn("redis", redis_argv + ["--bind", "::1"], cwd=str(ROOT), logf=logf, env=child_env)
                time.sleep(2)
            else:
                time.sleep(2)

        # ── Free port 8002 ────────────────────────────────────────────────────
        _kill_port(8002)

        # ── Core services (all captured inside the multiplexer) ───────────────
        _frontend_dir = str(ROOT / "frontend")
        _frontend_env = os.environ.copy()
        _frontend_env["PORT"] = "3000"
        _frontend_env["PYTHONIOENCODING"] = "utf-8"

        services: list[tuple[str, list[str], str, dict[str, str] | None]] = [
            (
                "core",
                [py, "-m", "scripts.nexus_core",
                 "--master-ip", "127.0.0.1",
                 "--worker",
                 "--turbo-boost",
                 "--skip-sync-check"],
                str(ROOT),
                child_env,
            ),
            (
                "monitor",
                [py, "-m", "scripts.node_monitor"],
                str(ROOT),
                child_env,
            ),
            (
                "deployer",
                [py, _subprocess_resource_path("scripts", "start_deployer.py")],
                str(ROOT),
                child_env,
            ),
            (
                "api",
                [py, _subprocess_resource_path("scripts", "start_api.py")],
                str(ROOT),
                child_env,
            ),
            (
                "polymarket",
                [py, _subprocess_resource_path("scripts", "start_polymarket_bot.py")],
                str(ROOT),
                child_env,
            ),
            (
                "telegram-bot",
                [py, _subprocess_resource_path("scripts", "start_telegram_bot.py")],
                str(ROOT),
                child_env,
            ),
        ]

        # Frontend via npm
        npm_exe = shutil.which("npm") or "npm"
        services.append((
            "frontend",
            [npm_exe, "run", "dev"],
            _frontend_dir,
            _frontend_env,
        ))

        # Israeli Swarm Engine — session harvester + community engine
        services.append((
            "israeli-swarm",
            [py, _subprocess_resource_path("src", "nexus", "services", "israeli_swarm.py")],
            str(ROOT),
            child_env,
        ))

        # Git-sync daemon (NexusGitDaemon — master pushes every 10 min, workers pull every 30 min)
        if (ROOT / ".git").is_dir():
            services.append((
                "git-sync",
                [py, _subprocess_resource_path("scripts", "git_manager.py")],
                str(ROOT),
                child_env,
            ))
            with _log_lock:
                logf.write("[launcher] Git repository detected — NexusGitDaemon (git_manager.py) added.\n")
                logf.flush()

        for label, argv, cwd, env in services:
            _dbg(f"[launcher] spawn [{label}]: {argv[0]} ... (cwd={cwd})")
            with _log_lock:
                logf.write(f"[launcher] spawn [{label}]: {argv!r}\n")
                logf.flush()
            _register_service(label, argv, cwd, env)
            try:
                _spawn(label, argv, cwd=cwd, logf=logf, env=env)
            except Exception as exc:
                _dbg(f"[launcher] WARNING: failed to spawn {label}: {exc}")
                with _log_lock:
                    logf.write(f"[launcher] WARNING: failed to spawn {label}: {exc}\n")
                    logf.flush()
                with _ring_lock:
                    _log_rings.setdefault(label, collections.deque(maxlen=_RING_SIZE)).append(
                        f"[SPAWN FAILED] {exc}"
                    )

        with _log_lock:
            logf.write(
                "[launcher] All services spawned inside unified multiplexer.\n"
                "[launcher] NEXUS OS: http://localhost:8002/nexus-os\n"
            )
            logf.flush()

        # ── Start Rich dashboard in a background thread ───────────────────────
        dashboard_thread = threading.Thread(
            target=_run_rich_dashboard,
            args=(logf, stop_event),
            name="nexus-rich-dashboard",
            daemon=True,
        )
        dashboard_thread.start()

        # ── Auto-spawn local worker if no heartbeats after 10 s ──────────────
        def _auto_spawn_worker_if_needed() -> None:
            """Wait 10 s then check Redis heartbeat:count; spawn worker if 0."""
            time.sleep(10)
            if stop_event.is_set():
                return
            try:
                import socket as _socket
                # Quick TCP probe — if Redis is not up yet, skip silently.
                with _socket.create_connection(("127.0.0.1", 6379), timeout=2.0):
                    pass
            except OSError:
                _dbg("[auto-worker] Redis not reachable after 10 s — skipping auto-spawn.")
                return

            heartbeat_count = 0
            try:
                import subprocess as _sp
                # Use redis-cli to check heartbeat count key
                redis_cli = _subprocess_resource_path("redis-local", "redis-cli.exe")
                result = _sp.run(
                    [redis_cli, "-h", "127.0.0.1", "-p", "6379", "GET", "heartbeat:count"],
                    capture_output=True, text=True, timeout=5,
                )
                val = (result.stdout or "").strip()
                heartbeat_count = int(val) if val.lstrip("-").isdigit() else 0
            except Exception as exc:
                _dbg(f"[auto-worker] heartbeat:count check failed: {exc}")
                # Fall back: scan for nexus:heartbeat:* keys
                try:
                    import subprocess as _sp2
                    redis_cli = _subprocess_resource_path("redis-local", "redis-cli.exe")
                    result2 = _sp2.run(
                        [redis_cli, "-h", "127.0.0.1", "-p", "6379",
                         "KEYS", "nexus:heartbeat:*"],
                        capture_output=True, text=True, timeout=5,
                    )
                    keys = [k for k in (result2.stdout or "").splitlines() if k.strip()]
                    heartbeat_count = len(keys)
                except Exception as exc2:
                    _dbg(f"[auto-worker] heartbeat KEYS fallback failed: {exc2}")

            if heartbeat_count > 0:
                _dbg(f"[auto-worker] {heartbeat_count} worker heartbeat(s) found — no auto-spawn needed.")
                with _ring_lock:
                    _log_rings.setdefault("core", collections.deque(maxlen=_RING_SIZE)).append(
                        f"[AUTO-WORKER] {heartbeat_count} worker(s) online — queue active."
                    )
                return

            _dbg("[auto-worker] No worker heartbeats — auto-spawning local worker…")
            with _ring_lock:
                _log_rings.setdefault("core", collections.deque(maxlen=_RING_SIZE)).append(
                    "[AUTO-WORKER] heartbeat:count=0 — spawning local worker to drain queue…"
                )
            with _log_lock:
                logf.write("[auto-worker] No heartbeats after 10 s — spawning start_worker.py\n")
                logf.flush()

            _py = _python_for_children()
            if _py is None:
                _dbg("[auto-worker] No Python found — cannot auto-spawn worker.")
                return

            worker_argv = [_py, _subprocess_resource_path("scripts", "start_worker.py")]
            _register_service("auto-worker", worker_argv, str(ROOT), child_env)
            try:
                proc = _spawn("auto-worker", worker_argv, cwd=str(ROOT), logf=logf, env=child_env)
                _dbg(f"[auto-worker] Worker spawned — pid={proc.pid}")
                with _ring_lock:
                    _log_rings.setdefault("core", collections.deque(maxlen=_RING_SIZE)).append(
                        f"[AUTO-WORKER] Worker spawned (pid={proc.pid}) — tasks moving to Active."
                    )
            except Exception as exc:
                _dbg(f"[auto-worker] Failed to spawn worker: {exc}")

        _auto_worker_thread = threading.Thread(
            target=_auto_spawn_worker_if_needed,
            name="nexus-auto-worker-check",
            daemon=True,
        )
        _auto_worker_thread.start()

        # ── Main watchdog loop (self-healing) ────────────────────────────────
        try:
            while not stop_event.is_set():
                for proc in list(_processes):
                    code = proc.poll()
                    if code is not None:
                        # Resolve label from pid registry
                        with _ring_lock:
                            label = _PROC_TO_LABEL.get(proc.pid, "unknown")
                        msg = (
                            f"[launcher] child process exited pid={proc.pid} "
                            f"label={label} returncode={code}."
                        )
                        _dbg(msg)
                        with _log_lock:
                            logf.write(msg + "\n")
                            logf.flush()
                        # Remove dead process from tracking list
                        try:
                            _processes.remove(proc)
                        except ValueError:
                            pass
                        # Trigger self-healing restart for non-zero exits
                        if code != 0 and label != "unknown" and not stop_event.is_set():
                            restart_thread = threading.Thread(
                                target=_watchdog_restart,
                                args=(label, logf, stop_event),
                                name=f"nexus-watchdog-{label}",
                                daemon=True,
                            )
                            restart_thread.start()
                time.sleep(1.0)
        except KeyboardInterrupt:
            _dbg("[launcher] KeyboardInterrupt; kill_all()")
            with _log_lock:
                logf.write("[launcher] KeyboardInterrupt; kill_all()\n")
                logf.flush()
            stop_event.set()
            kill_all()
            return 0

        stop_event.set()
        kill_all()
        return 0

    finally:
        try:
            stop_event.set()
        except NameError:
            pass
        kill_all()
        logf.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as _exc:
        _tb = traceback.format_exc()
        _msg = f"Nexus Launcher crashed:\n\n{_exc}\n\n{_tb}"
        try:
            _DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
            with open(_DEBUG_LOG, "a", encoding="utf-8", errors="replace") as _f:
                _f.write(_msg + "\n")
        except Exception:
            pass
        _show_error_popup("Nexus Launcher — Fatal Error", _msg[:2000])
        raise SystemExit(1)
