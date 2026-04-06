"""Pre-flight checks before Paramiko SSH deploy (TCP :22, optional debug line)."""

from __future__ import annotations

import socket
import subprocess
import sys

import structlog

log = structlog.get_logger(__name__)

_RED = "\033[91m"
_RESET = "\033[0m"


def _print_red(msg: str) -> None:
    if sys.stderr and sys.stderr.isatty():
        print(f"{_RED}{msg}{_RESET}", file=sys.stderr, flush=True)
    else:
        print(msg, file=sys.stderr, flush=True)


def print_ssh_debug_command(ssh_user: str, host: str, port: int = 22) -> None:
    if port == 22:
        line = f"ssh {ssh_user}@{host}"
    else:
        line = f"ssh -p {port} {ssh_user}@{host}"
    print(f"[DEBUG] Running: {line}", file=sys.stderr, flush=True)


def icmp_ping_host(host: str) -> bool:
    if sys.platform == "win32":
        cmd = ["ping", "-n", "1", "-w", "5000", host]
    elif sys.platform == "darwin":
        cmd = ["ping", "-c", "1", "-W", "5000", host]
    else:
        cmd = ["ping", "-c", "1", "-W", "5", host]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=12, check=False)
        return r.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def tcp_port_open(host: str, port: int = 22, *, timeout: float = 15.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def skipped_host_banner(host: str, reason: str) -> str:
    tail = reason.strip() if reason else "unreachable"
    return f"[SKIPPED] Host {host} unreachable — {tail}"


def print_skipped_unreachable(host: str, reason: str) -> None:
    _print_red(skipped_host_banner(host, reason))


def preflight_remote_ssh(host: str, *, port: int = 22) -> str | None:
    """
    Gate deploy on TCP reachability to the SSH port (not ICMP — many hosts block ping).

    Return None if the port accepts a connection, else a user-facing skip reason.
    """
    if tcp_port_open(host, port, timeout=15.0):
        return None
    msg = f"SSH port {port} closed or connection timed out (host down or sshd off)"
    print_skipped_unreachable(host, msg)
    log.warning(
        "deploy_preflight_failed",
        host=host,
        reason="ssh_port_unreachable",
        detail=msg,
    )
    return msg
