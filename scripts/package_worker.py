"""
One-Click Worker Packager.

Usage
-----
    python scripts/package_worker.py [--build] [--scale N] [--stop]

Options
-------
--build         Build the Docker image (nexus-worker:latest).
--scale N       Scale to N worker containers (default: 1).
--stop          Stop and remove all worker containers.
--status        Show status of all running worker containers.
--logs NAME     Tail logs for a specific worker container.

Requirements
------------
Docker must be installed and running.  The script uses the Docker CLI
via subprocess — no Docker SDK dependency needed.

Examples
--------
    # Build image and start 1 worker
    python scripts/package_worker.py --build --scale 1

    # Scale to 3 workers (adds 2 more)
    python scripts/package_worker.py --scale 3

    # Check status
    python scripts/package_worker.py --status

    # Stop all workers
    python scripts/package_worker.py --stop
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
IMAGE_NAME   = "nexus-worker:latest"
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.workers.yml"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, cwd=PROJECT_ROOT)


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def cmd_build() -> None:
    print("\n[BUILD] Building nexus-worker Docker image...")
    _run(["docker", "build", "-f", "Dockerfile.worker", "-t", IMAGE_NAME, "."])
    print(f"  ✓  Image built: {IMAGE_NAME}")


def cmd_scale(n: int) -> None:
    print(f"\n[SCALE] Scaling to {n} worker(s)...")
    _run([
        "docker", "compose",
        "-f", str(COMPOSE_FILE),
        "up", "-d",
        "--scale", f"worker={n}",
        "--no-recreate",
    ])
    print(f"  ✓  {n} worker container(s) running")


def cmd_stop() -> None:
    print("\n[STOP] Stopping all worker containers...")
    _run(["docker", "compose", "-f", str(COMPOSE_FILE), "down"], check=False)
    print("  ✓  Workers stopped")


def cmd_status() -> None:
    print("\n[STATUS] Worker containers:")
    _run(["docker", "compose", "-f", str(COMPOSE_FILE), "ps"], check=False)


def cmd_logs(name: str) -> None:
    print(f"\n[LOGS] Tailing logs for {name}...")
    _run(["docker", "logs", "--tail", "50", "-f", name], check=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Nexus Worker Packager")
    parser.add_argument("--build",  action="store_true", help="Build Docker image")
    parser.add_argument("--scale",  type=int, default=0, help="Scale to N workers")
    parser.add_argument("--stop",   action="store_true", help="Stop all workers")
    parser.add_argument("--status", action="store_true", help="Show worker status")
    parser.add_argument("--logs",   type=str, default="", help="Tail container logs")
    args = parser.parse_args()

    if not _docker_available():
        print("✗  Docker is not available. Install Docker Desktop and try again.")
        sys.exit(1)

    if not any([args.build, args.scale, args.stop, args.status, args.logs]):
        parser.print_help()
        return

    if args.build:
        cmd_build()
    if args.scale > 0:
        cmd_scale(args.scale)
    if args.stop:
        cmd_stop()
    if args.status:
        cmd_status()
    if args.logs:
        cmd_logs(args.logs)


if __name__ == "__main__":
    main()
