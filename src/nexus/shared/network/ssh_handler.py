"""
SSH helpers — clear stale known_hosts entries before connecting.

Paramiko / OpenSSH reject or crash when the remote host key changed; running
``ssh-keygen -R <host>`` drops the old line so the next handshake can proceed.

Localhost bypass
----------------
When the deploy/sync target is this machine (``127.0.0.1``, ``localhost``, or
other loopback addresses), callers must not open SSH — use
:func:`local_sync_project_tree` with ``shutil`` / ``os`` instead. This avoids
Paramiko hangs (e.g. ``Errno None``) during dashboard-driven sync.
"""

from __future__ import annotations

import ipaddress
import os
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Final

try:
    import paramiko as _paramiko  # type: ignore[import]

    # Disable rsa-sha2-* pubkey variants that cause KEX negotiation failures on
    # older OpenSSH servers — equivalent to forcing ssh-rsa acceptance.
    # Also disable sntrup761x25519-sha512@openssh.com which many worker SSH
    # daemons reject, producing a hard KEX mismatch on connect.
    _DISABLED_ALGORITHMS: dict = {
        "kex": ["sntrup761x25519-sha512@openssh.com"],
        "pubkeys": ["rsa-sha2-256", "rsa-sha2-512"],
    }

    def _get_ssh_client() -> "_paramiko.SSHClient":
        """Return a Paramiko SSHClient with AutoAddPolicy so unknown hosts are accepted."""
        client = _paramiko.SSHClient()
        client.set_missing_host_key_policy(_paramiko.AutoAddPolicy())
        # Clear all known-hosts state — equivalent to -o UserKnownHostsFile=/dev/null
        try:
            client._host_keys_filename = None  # type: ignore[union-attr]
            client._system_host_keys = _paramiko.HostKeys()
            client._host_keys = _paramiko.HostKeys()
            client.load_system_host_keys = lambda *_a, **_kw: None  # type: ignore[method-assign]
            client.load_host_keys = lambda *_a, **_kw: None  # type: ignore[method-assign]
        except Exception:
            pass
        return client

except ImportError:
    _paramiko = None  # type: ignore[assignment]
    _DISABLED_ALGORITHMS: dict = {}  # type: ignore[no-redef]

    def _get_ssh_client():  # type: ignore[misc]
        raise ImportError("paramiko is not installed — cannot create SSH client")

# Primary worker IP — LAN SSH/SFTP target (avoids loopback Paramiko hangs / Errno None).
DEFAULT_WORKER_LAN_HOST: Final[str] = "10.100.102.20"
_DEFAULT_WORKER_KNOWN_HOSTS_HOST: Final[str] = DEFAULT_WORKER_LAN_HOST

_DEFAULT_SKIP_PARTS: Final[tuple[str, ...]] = (
    "node_modules",
    ".venv",
    ".git",
    "__pycache__",
    ".mypy_cache",
    "venv",
    "vendor",
    ".next",
    "dist",
    "build",
)


def is_local_host(hostname: str | None) -> bool:
    """
    True when ``hostname`` refers to this host — SSH/SFTP should be skipped.

    Accepts ``127.0.0.1``, ``localhost``, ``::1``, and resolves hostnames that
    only map to loopback addresses.
    """
    h = (hostname or "").strip().lower()
    if not h:
        return False
    if h in ("127.0.0.1", "localhost", "::1", "[::1]"):
        return True
    try:
        for res in socket.getaddrinfo(h, None, type=socket.SOCK_STREAM):
            addr = res[4][0]
            try:
                if ipaddress.ip_address(addr).is_loopback:
                    return True
            except ValueError:
                continue
    except OSError:
        pass
    return False


def clear_known_host(hostname: str | None = None) -> None:
    """
    Remove ``hostname`` from the user's ``known_hosts`` (non-fatal if missing).

    When ``hostname`` is omitted, clears the default staging worker IP.
    Skips loopback targets — ``ssh-keygen -R`` is unnecessary and can misbehave.
    """
    host = (hostname or DEFAULT_WORKER_LAN_HOST).strip()
    if not host or is_local_host(host):
        return
    try:
        subprocess.run(
            ["ssh-keygen", "-R", host],
            capture_output=True,
            timeout=15,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass


def local_sync_project_tree(
    local_src: str | Path,
    local_dest: str | Path,
    *,
    skip_parts: tuple[str, ...] | None = None,
) -> int:
    """
    Copy a project directory tree locally (no SSH).

    Returns the number of files copied. Skips common vendor/artifact dirs.
    """
    src = Path(local_src).resolve()
    dst_root = Path(local_dest).resolve()
    skip = frozenset(skip_parts if skip_parts is not None else _DEFAULT_SKIP_PARTS)

    if not src.is_dir():
        raise FileNotFoundError(f"local sync source is not a directory: {src}")

    dst_root.mkdir(parents=True, exist_ok=True)
    n = 0
    for item in src.rglob("*"):
        if not item.is_file():
            continue
        if skip.intersection(item.parts):
            continue
        if item.suffix in (".pyc", ".pyo"):
            continue
        rel = item.relative_to(src)
        out = dst_root / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".nexus_tmp")
        shutil.copy2(item, tmp)
        os.replace(tmp, out)
        n += 1
    return n
