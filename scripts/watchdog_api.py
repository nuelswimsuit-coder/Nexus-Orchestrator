"""
Nexus API Watchdog — keeps the FastAPI server alive on Windows.

Runs as a background process (via Task Scheduler at boot).
Restarts the API automatically if it crashes, with exponential backoff.

Usage:
    python scripts/watchdog_api.py
    python scripts/watchdog_api.py --port 8001 --host 0.0.0.0
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG_FILE = ROOT / "watchdog_api.log"
MAX_LOG_BYTES = 2 * 1024 * 1024  # 2 MB — rotate when exceeded


def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    print(line, end="", flush=True)
    try:
        if LOG_FILE.exists() and LOG_FILE.stat().st_size > MAX_LOG_BYTES:
            LOG_FILE.write_text("", encoding="utf-8")
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nexus API watchdog")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", default="8001")
    p.add_argument("--log-level", default="warning")
    return p.parse_args()


def _port_in_use(port: int) -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def main() -> None:
    args = _parse_args()
    python = sys.executable
    port = int(args.port)
    cmd = [
        python, "-m", "uvicorn",
        "nexus.api.main:create_app",
        "--factory",
        "--host", args.host,
        "--port", args.port,
        "--log-level", args.log_level,
    ]

    _log(f"Nexus API Watchdog started — python={python} port={args.port}")
    _log(f"Command: {' '.join(cmd)}")

    # If another instance is already running, wait for it to die before taking over
    if _port_in_use(port):
        _log(f"Port {port} already in use — waiting for it to free up before starting watchdog loop…")
        while _port_in_use(port):
            time.sleep(5)
        _log(f"Port {port} is now free. Starting managed API process.")

    backoff = 3
    restarts = 0

    while True:
        _log(f"Starting API (restart #{restarts})…")
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            # Stream output to watchdog log
            assert proc.stdout is not None
            for line in proc.stdout:
                try:
                    with LOG_FILE.open("a", encoding="utf-8") as f:
                        f.write(line)
                except Exception:
                    pass
            proc.wait()
            exit_code = proc.returncode
        except Exception as exc:
            _log(f"Failed to start process: {exc}")
            exit_code = -1

        restarts += 1
        _log(f"API exited with code {exit_code}. Restarting in {backoff}s… (total restarts: {restarts})")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)  # cap at 60s


if __name__ == "__main__":
    main()
