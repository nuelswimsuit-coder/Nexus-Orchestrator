"""Pre-flight checks before Paramiko SSH deploy (ping, TCP :22, debug line)."""

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


<<<<<<< Current (Your changes)
def preflight_remote_ssh(host: str, *, port: int = 22) -> str | None:
    """
    Gate deploy on TCP reachability to the SSH port (not ICMP — many hosts block ping).

    Return None if the port accepts a connection, else a user-facing skip reason.
    """
    if tcp_port_open(host, port, timeout=15.0):
        return None
    msg = f"Host {host} unreachable — SSH port {port} closed, timed out, or host down"
    _print_red(f"[SKIPPED] {msg}")
    return msg
=======
def preflight_remote_ssh(host: str, port: int = 22) -> str | None:
    """
    TCP connect to the SSH port (authoritative for “host up / sshd listening”).
    Ping is not required: many LAN workers block ICMP while SSH still works.

    Returns None if OK, else a short machine-readable reason (callers print
    :func:`print_skipped_unreachable`).
    """
<<<<<<< Current (Your changes)
    if tcp_port_open(host, port, timeout=15.0):
        return None
    return f"SSH port {port} closed or connection timed out (host down or sshd off)"


def skipped_host_banner(host: str, reason: str) -> str:
    """Single-line message for logs and ``skipped:`` results."""
    tail = reason.strip() if reason else "unreachable"
    return f"[SKIPPED] Host {host} unreachable — {tail}"


def print_skipped_unreachable(host: str, reason: str) -> None:
    _print_red(skipped_host_banner(host, reason))
>>>>>>> Incoming (Background Agent changes)
=======
    if not icmp_ping_host(host):
        msg = (
            f"❌ NETWORK ERROR: Cannot ping {host}. The worker is either offline, asleep, "
            "or the IP has changed."
        )
        _print_red(msg)
        log.warning("deploy_preflight_failed", host=host, reason="icmp_unreachable", detail=msg)
        return msg
    if not tcp_port_open(host, 22, timeout=15.0):
        msg = (
            "❌ SSH ERROR: IP is reachable, but port 22 is closed. Ensure 'sshd' is "
            "running on the Linux worker."
        )
        _print_red(msg)
        log.warning("deploy_preflight_failed", host=host, reason="ssh_port_closed", detail=msg)
        return msg
    return None
>>>>>>> Incoming (Background Agent changes)
