"""
Nexus Auto-Deployer Service  (Phase 17 — Zero-Touch Cluster Update)
====================================================================

All workers (Linux **and** Windows) use a **universal zip-based deployment
protocol**: the local ``src/nexus/`` tree is packed with ``zipfile.ZipFile``
into ``nexus_payload.zip``, a **single SFTP upload** is sent to the worker,
then the archive is extracted in-place and deleted.

* **Linux** — ``unzip -o`` via ``bash -lc`` (``apt-get install unzip`` if
  missing).
* **Windows** — ``Expand-Archive -Force`` via PowerShell (no extra tooling).

This replaces the legacy per-file SFTP tree walk that stalled at [99/163]
due to Windows file-locking.  Uploading one zip is ~10× faster and immune
to mid-transfer lock contention.

Deployment sequence per node
-----------------------------
1. Resolve the target IP (``WORKER_IP`` or Redis heartbeat).
2. SSH (paramiko) with ``WORKER_SSH_USER`` / ``WORKER_SSH_PASSWORD``.
3. Stop the remote worker (graceful SIGTERM / pkill).
4. *(Linux only)* Deep-purge ``src/nexus/*`` on the worker.
5. Build ``nexus_payload.zip`` locally, SFTP the single file, verify the
   remote size matches, extract with the OS-appropriate command, delete the
   zip, then verify the extracted file count under ``src/nexus``.
6. ``pip install -r requirements.txt`` in the remote ``.venv``.
7. Restart via ``start_worker.py`` / ``start_nexus.sh`` (detached).

Progress events
---------------
Each step emits a JSON event to  nexus:deploy:progress:<node_id>  ::

    {
        "node_id": "worker_laptop_01",
        "step":    "installing_deps",     # see DeployStep
        "status":  "running",             # "running" | "done" | "error"
        "detail":  "pip install -r …",
        "label":   "…",
        "ts":      "2025-01-01T00:00:00Z",
    }

Configuration (.env)
---------------------
    WORKER_SSH_USER=yadmin
    WORKER_SSH_PASSWORD=<password>
    WORKER_IP=192.168.1.42            # direct IP — no Redis heartbeat needed
    WORKER_DEPLOY_ROOT_LINUX=/home/yadmin/Desktop/Nexus-Orchestrator
    WORKER_DEPLOY_ROOT_WIN=C:\\Users\\Yarin\\Desktop\\Nexus-Orchestrator

    Optional: WORKER_IP_WINDOWS_FALLBACK overrides the default when the Windows IP
    is unset. You may also set ``worker_ip_windows`` in ``nexus/shared/config.json``.
    Linux ``WORKER_IP`` has no silent LAN default: set it to the worker address or
    ``127.0.0.1`` for same-machine deploy (skips SSH). Windows fallback still uses
    the LAN helper constant when nothing else is configured.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import ipaddress
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

from nexus.shared.deploy_preflight import (
    preflight_remote_ssh,
    print_ssh_debug_command,
)
from nexus.shared.network.ssh_handler import (
    DEFAULT_WORKER_LAN_HOST,
    clear_known_host,
    is_local_host,
    local_sync_project_tree,
)
from nexus.shared.paths import repository_root

log = structlog.get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

NEXUS_ROOT = repository_root()
_SHARED_CONFIG_JSON = NEXUS_ROOT / "src" / "nexus" / "shared" / "config.json"


def _load_nexus_shared_config_json() -> dict[str, Any]:
    """Optional JSON overrides (e.g. ``worker_ip_windows``) beside pydantic ``Settings``."""
    if not _SHARED_CONFIG_JSON.is_file():
        return {}
    try:
        with _SHARED_CONFIG_JSON.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        log.warning(
            "shared_config_json_read_failed",
            path=str(_SHARED_CONFIG_JSON),
            error=str(exc),
        )
        return {}


# Directories to sync (relative to project root)
SYNC_DIRS = ["src/nexus", "nexus", "scripts", "tools"]

# Individual root-level files to sync alongside the directories
SYNC_FILES = [
    "requirements.txt",
    "start_nexus.sh",
    "run_worker.sh",
    "pyproject.toml",
]

# Redis key prefix for progress events
PROGRESS_KEY_PREFIX = "nexus:deploy:progress:"
PROGRESS_MAX_LEN = 100

# Standalone deploy / Moltbot dispatch HTTP server (``scripts/start_deployer.py``).
# Host is fixed to all interfaces so LAN laptops can reach POST /api/deploy/* .
DEPLOYER_API_BIND_HOST = "0.0.0.0"
DEPLOYER_API_BIND_PORT = 8001


def deployer_api_bind() -> tuple[str, int]:
    """
    Return ``(host, port)`` for uvicorn when serving the deploy router standalone.

    Host defaults to ``0.0.0.0`` (external dispatch); override with
    ``NEXUS_DEPLOYER_BIND_HOST``. Port defaults to 8001; override with
    ``NEXUS_DEPLOYER_PORT``.
    """
    host_raw = os.environ.get("NEXUS_DEPLOYER_BIND_HOST", DEPLOYER_API_BIND_HOST).strip()
    host = host_raw or DEPLOYER_API_BIND_HOST
    port_raw = os.environ.get("NEXUS_DEPLOYER_PORT", str(DEPLOYER_API_BIND_PORT)).strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = DEPLOYER_API_BIND_PORT
    return host, max(1, min(port, 65535))

# SSH connect + banner (per attempt). Retries absorb transient failures.
CONNECT_PHASE_TIMEOUT_SEC = 15.0
SSH_CONNECT_ATTEMPTS = 3
SSH_CONNECT_RETRY_DELAY_SEC = 2.0

# Dashboard / routing: workers that failed deploy connect or sync handshake
DEPLOY_DEGRADED_KEY = "nexus:deploy:degraded_nodes"
# Pub/sub hint for master-side consumers (optional)
DEPLOY_FAILOVER_CHANNEL = "nexus:dispatcher:deploy_signal"

# Single archive uploaded to the worker (basename only; path is remote_root/NAME)
NEXUS_PAYLOAD_ZIP_NAME = "nexus_payload.zip"

# SFTP ``put`` wall-clock cap — stall → close SSH, reconnect, skip file (tree sync) or retry zip once.
SFTP_PUT_TIMEOUT_S = 5.0
# Large ``nexus_payload.zip`` uploads need a longer cap so the transfer stays on the LAN path.
SFTP_ZIP_PUT_TIMEOUT_S = 300.0

# Fire Management Ahu OpenClaw enqueue when this fraction of files have uploaded (multi-file syncs).
EARLY_MANAGEMENT_AHU_SYNC_FRACTION = 0.9

DeployStep = Literal[
    "connecting",
    "stopping_worker",
    "purging",
    "uploading",
    "installing_deps",
    "restarting",
    "done",
    "skipped",
    "error",
]

DeployStatus = Literal["running", "done", "error"]

# Human-readable labels shown in the dashboard status ticker
STEP_LABELS: dict[str, str] = {
    "connecting":      "Connecting…",
    "stopping_worker": "Stopping worker…",
    "purging":         "Purging remote nexus/…",
    "uploading":       "Syncing files…",
    "installing_deps": "Installing deps…",
    "restarting":      "Restarting worker…",
    "done":            "Worker Live ✓",
    "skipped":         "Skipped (unreachable)",
    "error":           "Error ✗",
}

class SFTPStallError(Exception):
    """Raised when a single SFTP ``put`` blocks longer than ``SFTP_PUT_TIMEOUT_S``."""


def _sftp_put_with_timeout(
    sftp: object,
    local: str,
    remote: str,
    timeout_s: float = SFTP_PUT_TIMEOUT_S,
) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(lambda: sftp.put(local, remote))  # type: ignore[union-attr]
        try:
            fut.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError as e:
            raise SFTPStallError(f"{local} -> {remote}") from e


def _ssh_reconnect_and_new_sftp(ssh_holder: list, connect_params: dict[str, Any]) -> object:
    """Close the current SSH session, connect again, return a new SFTP client."""
    import paramiko  # type: ignore[import-untyped]

    try:
        ssh_holder[0].close()
    except Exception:
        pass
    ssh = paramiko.SSHClient()
    _configure_ssh_security(ssh)
    clear_known_host(str(connect_params.get("hostname") or ""))
    merged = {**connect_params, **_KEX_CONNECT_KWARGS}
    ssh.connect(**merged)
    # Re-arm keepalive on the reconnected session (ServerAliveInterval=30).
    transport = ssh.get_transport()
    if transport is not None:
        transport.set_keepalive(30)
    ssh_holder[0] = ssh
    return ssh.open_sftp()


def _configure_ssh_security(ssh: object) -> None:
    """Equivalent of -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null.

    Forces AutoAddPolicy so unknown host keys are silently accepted and never
    block connections to worker nodes (same as passing the flags above to the
    OpenSSH CLI).  All known-hosts state is also cleared so stale fingerprints
    from previous deploys cannot cause a RejectPolicy fallback.
    """
    import paramiko  # type: ignore[import-untyped]

    # Forcibly set AutoAddPolicy — equivalent to -o StrictHostKeyChecking=no
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # type: ignore[union-attr]

    # Clear all host-key stores — equivalent to -o UserKnownHostsFile=/dev/null
    try:
        ssh._host_keys_filename = None  # type: ignore[union-attr]
        ssh._system_host_keys = paramiko.HostKeys()  # type: ignore[union-attr]
        ssh._host_keys = paramiko.HostKeys()  # type: ignore[union-attr]
    except Exception:
        pass

    # Prevent paramiko from loading the system known_hosts file on connect.
    # Patch both the instance method and the underlying _system_host_keys loader
    # so that even internal paramiko calls cannot re-populate the key store.
    try:
        ssh.load_system_host_keys = lambda *_a, **_kw: None  # type: ignore[union-attr]
    except Exception:
        pass
    try:
        ssh.load_host_keys = lambda *_a, **_kw: None  # type: ignore[union-attr]
    except Exception:
        pass
    # Re-apply AutoAddPolicy after clearing keys to guarantee it is the active policy
    try:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # type: ignore[union-attr]
    except Exception:
        pass


# KEX algorithms to enable for legacy/restricted SSH servers.
# Mirrors: -o KexAlgorithms=+curve25519-sha256,diffie-hellman-group14-sha256
_PREFERRED_KEX: list[str] = [
    "curve25519-sha256",
    "curve25519-sha256@libssh.org",
    "diffie-hellman-group14-sha256",
    "diffie-hellman-group14-sha1",
    "diffie-hellman-group-exchange-sha256",
    "diffie-hellman-group-exchange-sha1",
    "diffie-hellman-group1-sha1",
]

# sntrup761x25519-sha512@openssh.com causes KEX negotiation failures against
# many Linux workers — explicitly disable it so Paramiko never proposes it.
# Mirrors: -o KexAlgorithms=-sntrup761x25519-sha512@openssh.com
# Also mirrors: -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
#   (enforced via _configure_ssh_security on every SSHClient instance)
_DISABLED_KEX: list[str] = ["sntrup761x25519-sha512@openssh.com"]

# Disable the newer RSA pubkey variants that cause KEX negotiation failures
# on older OpenSSH servers — equivalent to forcing ssh-rsa acceptance.
_DISABLED_PUBKEYS: list[str] = ["rsa-sha2-256", "rsa-sha2-512"]

# Merged dict passed to every paramiko connect() call to fix KEX mismatches.
# Explicitly sets preferred_kex so Paramiko never proposes the sntrup variant
# that many worker SSH daemons reject, causing a hard KEX mismatch error.
# allow_agent=True / look_for_keys=True enable automatic key-based login so
# that workers provisioned via ssh-copy-id connect without a password.
_KEX_CONNECT_KWARGS: dict = {
    "disabled_algorithms": {
        "kex": _DISABLED_KEX,
        "pubkeys": _DISABLED_PUBKEYS,
    },
    "preferred_kex": _PREFERRED_KEX,
    "allow_agent": True,
    "look_for_keys": True,
}


def _make_nexus_payload_zip() -> tuple[Path, str, int]:
    """
    Archive ``src/`` and ``scripts/`` into ``nexus_payload.zip``.

    Layout inside the zip mirrors the repository root so the remote
    ``unzip -d <deploy_root>/`` call drops files at the correct paths:
      src/nexus/...
      scripts/...

    The zip is written to a .tmp file first, then atomically moved to the
    final name only after ZipFile has fully flushed and closed — this
    prevents 'unzip failed (exit 9)' from a partial/open zip being
    transferred before the OS has flushed all buffers.

    Returns (final_zip_path, tmpdir, file_count).
    """
    import zipfile

    skip_parts = frozenset({"__pycache__", ".mypy_cache", ".git"})
    skip_suffixes = frozenset({".pyc", ".pyo"})

    # Directories to pack (relative to NEXUS_ROOT)
    pack_dirs: list[tuple[Path, str]] = []
    for rel_dir in ("src", "scripts"):
        d = NEXUS_ROOT / rel_dir
        if d.is_dir():
            pack_dirs.append((d, rel_dir))

    if not pack_dirs:
        raise FileNotFoundError(
            f"Neither src/ nor scripts/ found under {NEXUS_ROOT}"
        )

    tmpdir = tempfile.mkdtemp(prefix="nexus_payload_")
    tmp_zip = Path(tmpdir) / (NEXUS_PAYLOAD_ZIP_NAME + ".tmp")
    final_zip = Path(tmpdir) / NEXUS_PAYLOAD_ZIP_NAME
    file_count = 0

    with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for base_dir, arc_prefix in pack_dirs:
            for path in base_dir.rglob("*"):
                if not path.is_file():
                    continue
                if path.suffix in skip_suffixes:
                    continue
                if skip_parts.intersection(path.parts):
                    continue
                rel = path.relative_to(NEXUS_ROOT)
                zf.write(path, arcname=rel.as_posix())
                file_count += 1

    # Atomic move — guarantees the file is fully flushed before SFTP opens it.
    shutil.move(str(tmp_zip), str(final_zip))

    # Brief pause so the filesystem has time to sync the inode metadata before
    # SFTP opens the file for transfer (prevents exit-9 on slow/NFS mounts).
    time.sleep(1.5)

    zip_bytes = final_zip.stat().st_size
    log.info(
        "nexus_payload_zip_built",
        path=str(final_zip),
        bytes=zip_bytes,
        files=file_count,
    )
    return final_zip, tmpdir, file_count


def _upload_nexus_zip_sftp(
    ssh_holder: list,
    connect_params: dict[str, Any],
    local_zip: Path,
    remote_root: str,
) -> None:
    """Upload ``nexus_payload.zip`` to ``remote_root`` (one stall → reconnect + single retry)."""
    root = remote_root.rstrip("/").replace("\\", "/")
    remote_zip = f"{root}/{NEXUS_PAYLOAD_ZIP_NAME}"
    last_err: Exception | None = None
    for _attempt in range(2):
        sftp = None
        try:
            sftp = ssh_holder[0].open_sftp()  # type: ignore[union-attr]
            _sftp_mkdir_p(sftp, root)
            _sftp_put_with_timeout(
                sftp,
                str(local_zip),
                remote_zip,
                timeout_s=SFTP_ZIP_PUT_TIMEOUT_S,
            )
            return
        except SFTPStallError as e:
            last_err = e
            _ssh_reconnect_and_new_sftp(ssh_holder, connect_params)
        finally:
            if sftp is not None:
                try:
                    sftp.close()
                except Exception:
                    pass
    if last_err:
        raise last_err
    raise RuntimeError("nexus zip upload failed")


def _remote_unzip_bash_lc(remote_root: str) -> str:
    """
    Remote command (Linux): install unzip if missing, extract, remove the zip.

    Equivalent to:
      unzip -o ~/Desktop/Nexus-Orchestrator/nexus_payload.zip -d .../ && rm .../zip
    with paths taken from ``remote_root``.
    """
    root = remote_root.rstrip("/")
    zip_path = f"{root}/{NEXUS_PAYLOAD_ZIP_NAME}"
    inner = (
        "if ! command -v unzip >/dev/null 2>&1; then "
        "sudo apt-get install -y unzip; fi; "
        f"unzip -o {shlex.quote(zip_path)} -d {shlex.quote(root + '/')} && "
        f"rm -f {shlex.quote(zip_path)}"
    )
    return "bash -lc " + shlex.quote(inner)


def _remote_unzip_windows_cmd(remote_root: str) -> str:
    """
    Remote command (Windows): extract ``nexus_payload.zip`` via PowerShell
    ``Expand-Archive`` and delete the zip afterwards.

    Uses ``-Force`` to overwrite existing files (equivalent to ``unzip -o``).
    The ``src\\nexus`` sub-tree is preserved because the zip already contains
    the ``nexus/`` prefix that maps to ``src/nexus`` on the remote.
    """
    root = remote_root.rstrip("\\").rstrip("/")
    zip_path = f"{root}\\{NEXUS_PAYLOAD_ZIP_NAME}"
    ps_inner = (
        f"Expand-Archive -Path '{zip_path}' -DestinationPath '{root}\\' -Force; "
        f"Remove-Item -Path '{zip_path}' -Force -ErrorAction SilentlyContinue"
    )
    return f"powershell -NoProfile -NonInteractive -Command \"{ps_inner}\""


def _remote_verify_file_count_cmd(remote_root: str, remote_os: str) -> str:
    """
    Return a shell command that prints the number of regular files under
    ``<remote_root>/src/nexus`` (or the ``nexus/`` subtree on Windows).

    Used after extraction to confirm the payload landed intact.
    """
    if remote_os == "Linux":
        nexus_dir = f"{remote_root.rstrip('/')}/src/nexus"
        inner = f"find {shlex.quote(nexus_dir)} -type f 2>/dev/null | wc -l"
        return "bash -lc " + shlex.quote(inner)
    else:
        nexus_dir = f"{remote_root.rstrip(chr(92))}\\src\\nexus"
        ps_inner = (
            f"(Get-ChildItem -Path '{nexus_dir}' -Recurse -File "
            f"-ErrorAction SilentlyContinue | Measure-Object).Count"
        )
        return f"powershell -NoProfile -NonInteractive -Command \"{ps_inner}\""


def _is_loopback_deploy_host(host: str) -> bool:
    """True when deploy target is this machine — SSH should be skipped."""
    return is_local_host(host)


def _effective_worker_linux_ssh_host(configured_ip: str) -> str:
    """
    Normalized ``WORKER_IP`` only — no substitution.

    Historically, empty or loopback values were replaced with a hardcoded LAN host,
    which forced SSH to an unreachable address and blocked local loopback deploy.
    Callers treat empty as unset (error) and non-empty loopback as local deploy.
    """
    return (configured_ip or "").strip()


def _close_own_tcp_connections_to_host(host: str, port: int = 22) -> None:
    """
    Tear down ESTABLISHED TCP connections owned by this process to host:port
    (e.g. stale Paramiko sessions to the worker) so a new sync starts clean.
    """
    host = host.strip()
    if not host:
        return
    closed = 0
    try:
        import psutil
    except ImportError:
        log.debug("pre_sync_close_skipped", reason="psutil not installed")
        psutil = None  # type: ignore[assignment]

    if psutil is not None:
        try:
            for c in psutil.Process().connections(kind="tcp"):
                if c.status != psutil.CONN_ESTABLISHED or not c.raddr:
                    continue
                rh, rp = c.raddr
                if str(rh) != host or int(rp) != port:
                    continue
                fd = getattr(c, "fd", -1) or -1
                if fd >= 0:
                    try:
                        os.close(fd)
                        closed += 1
                    except OSError as e:
                        log.debug("pre_sync_close_fd_failed", fd=fd, error=str(e))
        except (psutil.AccessDenied, psutil.ZombieProcess) as e:
            log.warning("pre_sync_close_psutil_denied", error=str(e))

    if sys.platform == "win32" and closed == 0:
        try:
            ps_script = (
                f"$c = Get-NetTCPConnection -OwningProcess $PID "
                f"-RemoteAddress '{host}' -RemotePort {port} "
                f"-State Established -ErrorAction SilentlyContinue; "
                f"if ($c) {{ $c | Close-NetTCPConnection -Confirm:$false -ErrorAction SilentlyContinue }}"
            )
            subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
                capture_output=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as e:
            log.debug("pre_sync_close_powershell_failed", error=str(e))

    if closed:
        log.info("pre_sync_closed_own_tcp", host=host, port=port, count=closed)


# ── Progress event helper ──────────────────────────────────────────────────────

def _event(
    node_id: str,
    step: DeployStep,
    status: DeployStatus,
    detail: str = "",
    extra: dict[str, object] | None = None,
) -> dict:
    ev: dict[str, object] = {
        "node_id": node_id,
        "step": step,
        "status": status,
        "detail": detail,
        "label": STEP_LABELS.get(step, step),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        ev.update(extra)
    return ev


# ── Deployer ──────────────────────────────────────────────────────────────────

class DeployerService:
    """
    Orchestrates rolling deployments to all active worker nodes.

    Parameters
    ----------
    redis   : Async Redis client (already connected).
    vault   : Vault instance for reading SSH credentials.
    settings: The shared Settings object (for WORKER_IP, paths, etc.).
    """

    def __init__(self, redis, vault, settings=None) -> None:  # type: ignore[type-arg]
        self._redis = redis
        self._vault = vault
        self._settings = settings
        self._early_ahu_dispatched = False
        self._early_ahu_lock = threading.Lock()

    # ── Public API ─────────────────────────────────────────────────────────────

    async def deploy_all(self, node_ids: list[str] | None = None) -> dict[str, str]:
        """
        Deploy to all active workers (or a specific subset).

        ``worker_windows`` is scheduled first (or in parallel from t=0) so the
        gaming laptop can sync while Linux SSH is still connecting.

        All targets run concurrently via ``asyncio.gather``; one node's failure
        does not cancel siblings.

        Returns {node_id: "ok" | "error: <reason>"}.
        """
        targets = node_ids or await self._build_target_list()
        if not targets:
            log.warning("deployer_no_targets")
            return {}

        ordered = self._prioritize_windows_first(targets)
        log.info("deployer_start", targets=ordered)
        self._early_ahu_dispatched = False

        async def _deploy_isolated(nid: str) -> tuple[str, str]:
            try:
                return nid, await self._deploy_node(nid)
            except Exception as exc:
                log.exception("deploy_node_unhandled", node_id=nid, error=str(exc))
                return nid, f"error: {exc}"

        pairs = await asyncio.gather(*[_deploy_isolated(nid) for nid in ordered])
        results = dict(pairs)
        log.info("deployer_done", results=results)

        # Immediately dispatch scraping tasks to all LIVE nodes after deploy
        live_count = sum(1 for v in results.values() if v == "ok")
        if live_count > 0:
            try:
                dispatch_results = await self.dispatch_scraping_to_live_nodes()
                log.info("post_deploy_scraping_dispatched", dispatch=dispatch_results)
            except Exception as _disp_exc:
                log.warning("post_deploy_scraping_dispatch_failed", error=str(_disp_exc))

        return results

    def _prioritize_windows_first(self, node_ids: list[str]) -> list[str]:
        """Gaming laptop (``worker_windows``) first so its sync starts immediately."""

        def _key(nid: str) -> tuple[int, str]:
            if nid == "worker_windows":
                return (0, nid)
            if nid == "worker_linux":
                return (1, nid)
            return (2, nid)

        return sorted(node_ids, key=_key)

    def _worker_ip_from_shared_config_json(self) -> str:
        data = _load_nexus_shared_config_json()
        for key in ("worker_ip_windows", "WORKER_IP_WINDOWS", "workerIpWindows"):
            raw = data.get(key)
            if raw is not None and str(raw).strip():
                return str(raw).strip()
        return ""

    def _get_worker_ip_windows(self) -> str:
        """Static IP for the Windows worker — settings, env, then ``nexus/shared/config.json``."""
        cfg = (self._get_setting("worker_ip_windows") or "").strip()
        if cfg:
            return cfg
        env_v = os.environ.get("WORKER_IP_WINDOWS", "").strip()
        if env_v:
            return env_v
        return self._worker_ip_from_shared_config_json()

    def _local_master_uses_loopback_api(self) -> bool:
        base = (
            self._get_setting("nexus_api_base_url")
            or os.environ.get("NEXUS_API_BASE_URL")
            or os.environ.get("NEXUS_API_BASE", "")
            or ""
        ).lower()
        return "127.0.0.1" in base or "localhost" in base

    def _fallback_worker_windows_ip(self) -> str:
        """
        When ``WORKER_IP_WINDOWS`` / settings are unset, still attempt deploy/sync.

        Order: ``WORKER_IP_WINDOWS_FALLBACK`` env → ``nexus/shared/config.json`` →
        ``127.0.0.1`` on Windows or when the master API base is loopback (local) →
        default LAN gaming-laptop segment.
        """
        fb = (os.environ.get("WORKER_IP_WINDOWS_FALLBACK") or "").strip()
        if fb:
            return fb
        json_ip = self._worker_ip_from_shared_config_json()
        if json_ip:
            return json_ip
        if sys.platform == "win32":
            # Single-box dev: WORKER_IP=127.0.0.1 but WORKER_IP_WINDOWS unset → avoid SSH to .20.
            lip = (
                self._get_setting("worker_ip") or os.environ.get("WORKER_IP") or ""
            ).strip()
            if lip and _is_loopback_deploy_host(lip):
                return "127.0.0.1"
            return DEFAULT_WORKER_LAN_HOST
        if self._local_master_uses_loopback_api():
            return DEFAULT_WORKER_LAN_HOST
        return DEFAULT_WORKER_LAN_HOST

    def _resolve_ssh_key_files(self) -> list[str]:
        """Return a list of explicit SSH private key paths to try.

        Reads WORKER_SSH_KEY_FILE from vault → env → settings.  Falls back to
        common default paths so that a standard ssh-keygen setup works without
        any extra configuration.
        """
        explicit = (
            self._vault._backend.get("WORKER_SSH_KEY_FILE")
            or os.environ.get("WORKER_SSH_KEY_FILE")
            or self._get_setting("worker_ssh_key_file")
            or ""
        )
        candidates: list[str] = []
        if explicit:
            expanded = os.path.expanduser(explicit.strip())
            if os.path.isfile(expanded):
                candidates.append(expanded)
            else:
                log.warning(
                    "deployer_ssh_key_file_not_found",
                    path=expanded,
                    msg="WORKER_SSH_KEY_FILE set but file not found — falling back to default keys",
                )
        # Always append common default key paths so Paramiko has explicit
        # candidates even when look_for_keys=True fails on Windows.
        home = os.path.expanduser("~")
        for name in ("id_ed25519", "id_rsa", "id_ecdsa", "id_dsa"):
            p = os.path.join(home, ".ssh", name)
            if os.path.isfile(p) and p not in candidates:
                candidates.append(p)
        return candidates

    async def _preflight_remote_ssh_executor(self, ip: str) -> str | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, preflight_remote_ssh, ip)

    async def _connect_ssh_with_deadline(
        self,
        ssh: object,
        *,
        hostname: str,
        username: str,
        password: str,
        node_id: str,
        key_files: list[str] | None = None,
    ) -> None:
        """SSH connect with per-attempt timeout and retries (slow / flaky links)."""
        loop = asyncio.get_event_loop()
        executor_budget = CONNECT_PHASE_TIMEOUT_SEC + 8.0
        last_exc: BaseException | None = None

        for attempt in range(SSH_CONNECT_ATTEMPTS):
            try:
                if attempt == 0:
                    log.debug(
                        "ssh_connect_deadline_start",
                        node_id=node_id,
                        hostname=hostname,
                        timeout_s=CONNECT_PHASE_TIMEOUT_SEC,
                        attempts=SSH_CONNECT_ATTEMPTS,
                    )
                    await loop.run_in_executor(
                        None, lambda: _close_own_tcp_connections_to_host(hostname)
                    )
                    await loop.run_in_executor(
                        None,
                        lambda h=hostname: clear_known_host(h),
                    )

                def _connect() -> None:
                    try:
                        ssh.close()  # type: ignore[union-attr]
                    except Exception:
                        pass
                    _configure_ssh_security(ssh)
                    ssh.connect(  # type: ignore[union-attr]
                        hostname=hostname,
                        username=username,
                        password=password or None,
                        key_filename=key_files or None,
                        timeout=int(CONNECT_PHASE_TIMEOUT_SEC),
                        banner_timeout=int(CONNECT_PHASE_TIMEOUT_SEC),
                        **_KEX_CONNECT_KWARGS,
                    )
                    # Keep TCP alive during long zip uploads / remote extractions.
                    # Equivalent to ServerAliveInterval=30 / ServerAliveCountMax=3.
                    transport = ssh.get_transport()  # type: ignore[union-attr]
                    if transport is not None:
                        transport.set_keepalive(30)

                await asyncio.wait_for(
                    loop.run_in_executor(None, _connect),
                    timeout=executor_budget,
                )
                return
            except asyncio.TimeoutError as exc:
                last_exc = exc
                log.warning(
                    "ssh_connect_deadline_timeout",
                    node_id=node_id,
                    hostname=hostname,
                    attempt=attempt + 1,
                    max_attempts=SSH_CONNECT_ATTEMPTS,
                    timeout_s=CONNECT_PHASE_TIMEOUT_SEC,
                )
            except Exception as exc:
                last_exc = exc
                en = getattr(exc, "errno", None)
                log.warning(
                    "ssh_connect_attempt_failed",
                    node_id=node_id,
                    hostname=hostname,
                    attempt=attempt + 1,
                    max_attempts=SSH_CONNECT_ATTEMPTS,
                    error=str(exc),
                    errno=en,
                )

            if attempt < SSH_CONNECT_ATTEMPTS - 1:
                await asyncio.sleep(SSH_CONNECT_RETRY_DELAY_SEC)

        log.error(
            "ssh_connect_deadline_failed",
            node_id=node_id,
            hostname=hostname,
            error=str(last_exc) if last_exc else "unknown",
            exc_info=last_exc is not None and not isinstance(last_exc, TimeoutError),
        )
        print(
            f"[Deployer] SSH connect error node_id={node_id!r} host={hostname!r}: {last_exc!r}",
            file=sys.stderr,
        )
        if isinstance(last_exc, asyncio.TimeoutError):
            raise TimeoutError(
                f"SSH connection to {hostname} ({node_id}) exceeded "
                f"{CONNECT_PHASE_TIMEOUT_SEC:.0f}s after {SSH_CONNECT_ATTEMPTS} attempts"
            ) from last_exc
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("SSH connect failed with no exception captured")

    async def _record_deploy_degraded(self, node_id: str) -> None:
        raw = await self._redis.get(DEPLOY_DEGRADED_KEY)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        nodes: list[str] = []
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    nodes = [str(x) for x in parsed]
            except Exception:
                nodes = []
        if node_id not in nodes:
            nodes.append(node_id)
        await self._redis.set(DEPLOY_DEGRADED_KEY, json.dumps(nodes), ex=86400)

    async def _clear_deploy_degraded_for_node(self, node_id: str) -> None:
        raw = await self._redis.get(DEPLOY_DEGRADED_KEY)
        if not raw:
            return
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                return
            nxt = [x for x in parsed if str(x) != node_id]
            if nxt:
                await self._redis.set(DEPLOY_DEGRADED_KEY, json.dumps(nxt), ex=86400)
            else:
                await self._redis.delete(DEPLOY_DEGRADED_KEY)
        except Exception:
            pass

    async def _publish_deploy_failover_signal(self, failed_node: str) -> None:
        payload = json.dumps(
            {
                "event": "worker_sync_degraded",
                "degraded": [failed_node],
                "requeue_project_ids": ["management_ahu"],
                "task_types": ["scraper.openclaw", "openclaw.browser_scrape"],
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        try:
            await self._redis.publish(DEPLOY_FAILOVER_CHANNEL, payload)
        except Exception as exc:
            log.debug("deploy_failover_publish_skipped", error=str(exc))

    async def _enqueue_management_ahu_openclaw(self, required_caps: list[str]) -> None:
        """
        Enqueue a Management Ahu OpenClaw job onto ``nexus:tasks`` with the given
        capability route (mirrors Scout routing: Windows by default).
        """
        from arq import create_pool
        from arq.connections import RedisSettings

        from nexus.shared.config import settings as nexus_settings
        from nexus.shared.constants import TASK_DEFAULT_TIMEOUT
        from nexus.shared.schemas import TaskPayload

        task = TaskPayload(
            task_type="scraper.openclaw",
            parameters={
                "mode": "social_forums",
                "query": "management ahu telegram bot problem",
                "project_id": "management_ahu",
                "max_leads": 60,
            },
            project_id="management_ahu",
            priority=3,
            required_capabilities=required_caps,
            approval_context=(
                "Deploy failover: route Management Ahu OpenClaw to healthy workers."
            ),
        )
        task = self._vault.inject(task)
        pool = await create_pool(
            RedisSettings.from_dsn(nexus_settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        try:
            job_ttl = task.job_expires_seconds or TASK_DEFAULT_TIMEOUT
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=task.model_dump_for_wire(),
                _queue_name="nexus:tasks",
                _expires=job_ttl,
            )
            log.info(
                "deploy_failover_openclaw_enqueued",
                job_id=getattr(job, "job_id", None),
                required_capabilities=required_caps,
            )
        finally:
            await pool.aclose()

    def _spawn_failover_management_ahu(self, failed_node: str) -> None:
        """Fire-and-forget: push Management Ahu work toward remaining healthy workers."""

        async def _run() -> None:
            try:
                if failed_node == "worker_windows":
                    caps = ["linux-only"]
                elif failed_node == "worker_linux":
                    caps = ["windows-only"]
                else:
                    return
                await self._enqueue_management_ahu_openclaw(caps)
            except Exception as exc:
                log.warning(
                    "management_ahu_failover_enqueue_failed",
                    failed_node=failed_node,
                    error=str(exc),
                )

        asyncio.create_task(_run(), name=f"deploy-failover-{failed_node}")

    async def _handle_deploy_connect_failure(
        self, node_id: str, exc: BaseException
    ) -> str:
        detail = str(exc)
        exc_type = type(exc).__name__

        # Classify the error so the master console shows a precise diagnosis
        # instead of a generic "Server not found" message.
        detail_lower = detail.lower()
        if "kex" in detail_lower or "key exchange" in detail_lower or "no matching" in detail_lower:
            error_class = "KEX_MISMATCH"
            console_msg = (
                f"[Deployer] KEX negotiation failed for node={node_id!r}: {detail}\n"
                f"  → Hint: worker SSH daemon rejected all proposed key-exchange algorithms.\n"
                f"  → Disabled: sntrup761x25519-sha512@openssh.com  |  Preferred: curve25519-sha256, dh-group14-sha256"
            )
        elif "authentication" in detail_lower or "auth" in detail_lower or "permission denied" in detail_lower:
            error_class = "AUTH_FAILURE"
            _node_ip = getattr(exc, "hostname", None) or node_id
            console_msg = (
                f"[Deployer] SSH authentication failed for node={node_id!r}: {detail}\n"
                f"  → Fix options (choose one):\n"
                f"  1. Set WORKER_SSH_PASSWORD=<password> in .env\n"
                f"  2. Set WORKER_SSH_KEY_FILE=~/.ssh/id_rsa (or id_ed25519) in .env\n"
                f"  3. Run: ssh-copy-id {_node_ip} to install your public key on the worker"
            )
        elif "timed out" in detail_lower or "timeout" in detail_lower:
            error_class = "TIMEOUT"
            console_msg = f"[Deployer] SSH connect timed out for node={node_id!r}: {detail}"
        elif "connection refused" in detail_lower or "no route" in detail_lower or "network" in detail_lower:
            error_class = "NETWORK"
            console_msg = f"[Deployer] SSH network error for node={node_id!r}: {detail}"
        else:
            error_class = exc_type
            console_msg = f"[Deployer] SSH connect error ({exc_type}) for node={node_id!r}: {detail}"

        print(console_msg, file=sys.stderr, flush=True)
        log.warning(
            "deploy_connect_failed",
            node_id=node_id,
            error_class=error_class,
            error=detail,
        )

        # Best-effort only: Redis/SSE must not abort the deploy/sync path so a later
        # Sentinel purge / retry can run a full cycle.
        try:
            _emit_detail = f"[DEGRADED] [{error_class}] {detail}"
            if error_class == "AUTH_FAILURE":
                _node_ip_emit = getattr(exc, "hostname", None) or node_id
                _emit_detail += (
                    f" | Fix: set WORKER_SSH_PASSWORD or WORKER_SSH_KEY_FILE in .env,"
                    f" or run: ssh-copy-id {_node_ip_emit}"
                )
            await self._emit(
                node_id,
                "error",
                "error",
                _emit_detail,
                extra={"deploy_health": "degraded", "degraded": True},
            )
            await self._record_deploy_degraded(node_id)
            await self._publish_deploy_failover_signal(node_id)
            self._spawn_failover_management_ahu(node_id)
        except Exception as side:
            log.debug(
                "deploy_degraded_side_effects_failed",
                node_id=node_id,
                error=str(side),
            )
        return f"error: [{error_class}] {detail}"

    async def sync_project_to_worker(
        self,
        project_name: str,
        project_path: str,
        remote_path: str | None = None,
    ) -> str:
        """
        Phase 20 — Multi-Project Sync.

        Sync a specific desktop project (OTP_Sessions_Creator, BudgetTracker,
        etc.) to the Linux worker at a custom remote path.

        Parameters
        ----------
        project_name : Name of the project (for logging and progress events)
        project_path : Local absolute path to the project directory
        remote_path  : Remote destination (defaults to /home/yadmin/Desktop/{project_name})

        Returns "ok" or "error: <reason>".
        """
        node_id = "worker_linux"
        raw_ip = (self._get_setting("worker_ip") or "").strip()
        ip = _effective_worker_linux_ssh_host(raw_ip)
        if not ip:
            detail = (
                "WORKER_IP is not set — set WORKER_IP in .env to your Linux worker "
                "IP, or 127.0.0.1 for same-machine deploy (no SSH)."
            )
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        if not os.path.exists(project_path):
            detail = f"Project path does not exist: {project_path}"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        remote_dest = remote_path or f"/home/yadmin/Desktop/{project_name}"

        await self.clear_progress(node_id)
        loop = asyncio.get_running_loop()

        if is_local_host(ip):
            local_dest = self._loopback_project_destination(
                node_id, project_name, remote_path
            )
            await self._emit(
                node_id,
                "connecting",
                "running",
                f"Local sync — skipping SSH ({ip} → {local_dest})",
            )
            await self._emit(node_id, "connecting", "done", "Same machine — no SSH")
            await self._emit(
                node_id,
                "uploading",
                "running",
                f"[{node_id}: ACTIVE] Copying {project_name} → {local_dest}",
            )
            try:

                def _copy() -> int:
                    return local_sync_project_tree(project_path, local_dest)

                file_count = await loop.run_in_executor(None, _copy)
            except Exception as exc:
                detail = str(exc)
                log.exception(
                    "deployer_loopback_project_sync_error",
                    project=project_name,
                    error=detail,
                )
                await self._emit(node_id, "error", "error", detail)
                return f"error: {detail}"
            await self._emit(
                node_id,
                "uploading",
                "done",
                f"{file_count} files copied locally to {local_dest}",
            )
            await self._emit(
                node_id,
                "done",
                "done",
                f"Project '{project_name}' synced locally — ready",
            )
            await self._clear_deploy_degraded_for_node(node_id)
            return "ok"

        ssh_user = self._get_setting("worker_ssh_user") or "yadmin"
        ssh_pass = (
            self._vault._backend.get("WORKER_SSH_PASSWORD")
            or os.environ.get("WORKER_SSH_PASSWORD")
            or self._get_setting("worker_ssh_password")
            or ""
        )
        ssh_key_files = self._resolve_ssh_key_files()

        if not ssh_pass:
            log.info(
                "deployer_no_password_key_auth_fallback",
                node_id=node_id,
                key_files=ssh_key_files,
                msg="WORKER_SSH_PASSWORD not set — attempting key-based auth",
            )
            if ssh_key_files:
                await self._emit(
                    node_id,
                    "connecting",
                    "running",
                    f"No password set — using key auth ({', '.join(ssh_key_files)})",
                )
            else:
                await self._emit(
                    node_id,
                    "connecting",
                    "running",
                    "No password set and no SSH key found — set WORKER_SSH_PASSWORD or WORKER_SSH_KEY_FILE in .env",
                )

        try:
            import paramiko  # type: ignore[import-untyped]
        except ImportError:
            detail = "paramiko not installed — run: pip install paramiko"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        ssh = paramiko.SSHClient()
        _configure_ssh_security(ssh)

        connect_params = {
            "hostname": ip,
            "username": ssh_user,
            "password": ssh_pass or None,
            "key_filename": ssh_key_files or None,
            "timeout": int(CONNECT_PHASE_TIMEOUT_SEC),
            "banner_timeout": int(CONNECT_PHASE_TIMEOUT_SEC),
            **_KEX_CONNECT_KWARGS,
        }
        ssh_h = [ssh]

        try:
            # ── 1. Connect (timeout + retries) ───────────────────────────────
            await self._emit(node_id, "connecting", "running",
                             f"SSH → {ssh_user}@{ip}")
            pf_err = await self._preflight_remote_ssh_executor(ip)
            if pf_err:
                await self._emit(
                    node_id,
                    "skipped",
                    "done",
                    f"[SKIPPED] {pf_err}",
                )
                log.warning(
                    "deployer_project_sync_skipped_preflight",
                    node_id=node_id,
                    detail=(pf_err or "")[:500],
                )
                return f"skipped: {pf_err}"
            print_ssh_debug_command(ssh_user, ip)
            try:
                await self._connect_ssh_with_deadline(
                    ssh_h[0],
                    hostname=ip,
                    username=ssh_user,
                    password=ssh_pass,
                    node_id=node_id,
                    key_files=ssh_key_files or None,
                )
            except Exception as connect_exc:
                return await self._handle_deploy_connect_failure(
                    node_id, connect_exc
                )
            await self._emit(node_id, "connecting", "done",
                             f"Connected to {ip}")

            # ── 2. Sync project files ─────────────────────────────────────────
            await self._emit(node_id, "uploading", "running",
                             f"[{node_id}: ACTIVE] Syncing {project_name} → {remote_dest}")

            file_count = await loop.run_in_executor(
                None,
                lambda: self._sync_single_project(
                    ssh_h,
                    connect_params,
                    project_path,
                    remote_dest,
                    project_name,
                    node_id,
                    loop,
                ),
            )

            await self._emit(node_id, "uploading", "done",
                             f"{file_count} files synced to {remote_dest}")

            await self._emit(node_id, "done", "done",
                             f"Project '{project_name}' deployed — ready on worker")
            await self._clear_deploy_degraded_for_node(node_id)
            return "ok"

        except Exception as exc:
            detail = str(exc)
            log.exception("deployer_project_sync_error", 
                         project=project_name, node_id=node_id, error=detail)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"
        finally:
            try:
                ssh_h[0].close()
            except Exception:
                pass

    def _sync_single_project(
        self,
        ssh_holder: list,
        connect_params: dict[str, Any],
        local_project_path: str,
        remote_dest: str,
        project_name: str,
        node_id: str,
        loop: asyncio.AbstractEventLoop | None,
    ) -> int:
        """
        SFTP-upload an entire desktop project to the worker (timed puts + reconnect on stall).
        Returns the number of files transferred successfully.
        """
        from pathlib import Path

        local_path = Path(local_project_path)
        skip_parts = (
            "node_modules", ".venv", ".git", "__pycache__",
            ".mypy_cache", "venv", "vendor", ".next", "dist", "build",
        )
        total = 0
        for item in local_path.rglob("*"):
            if not item.is_file():
                continue
            if any(skip in item.parts for skip in skip_parts):
                continue
            total += 1
        total = max(total, 1)

        sftp0 = ssh_holder[0].open_sftp()
        ctx: dict[str, Any] = {
            "ssh_holder": ssh_holder,
            "connect_params": connect_params,
            "sftp_holder": [sftp0],
            "node_id": node_id,
            "loop": loop,
            "done": [0],
            "total": total,
        }

        try:
            _sftp_mkdir_p(ctx["sftp_holder"][0], remote_dest)

            for item in local_path.rglob("*"):
                if any(skip in item.parts for skip in skip_parts):
                    continue
                if item.is_file():
                    try:
                        rel_path = item.relative_to(local_path)
                        remote_file_path = f"{remote_dest}/{str(rel_path).replace(os.sep, '/')}"
                        remote_parent = "/".join(remote_file_path.split("/")[:-1])
                        _sftp_mkdir_p(ctx["sftp_holder"][0], remote_parent)
                        self._upload_tree_file_ctx(ctx, str(item), remote_file_path)
                        if ctx["done"][0] and ctx["done"][0] % 50 == 0:
                            log.debug(
                                "project_sync_progress",
                                project=project_name,
                                files_done=ctx["done"][0],
                            )
                    except Exception as exc:
                        log.debug("project_sync_file_error",
                                  file=str(item), error=str(exc))

        finally:
            try:
                ctx["sftp_holder"][0].close()
            except Exception:
                pass

        n_done = ctx["done"][0]
        log.info("project_sync_complete",
                 project=project_name, files_transferred=n_done,
                 remote_dest=remote_dest)
        return n_done

    def _schedule_early_ahu_from_executor(
        self,
        loop: asyncio.AbstractEventLoop | None,
        file_fraction: float,
        progress_node_id: str = "worker_linux",
    ) -> None:
        if loop is None or not loop.is_running():
            return
        asyncio.run_coroutine_threadsafe(
            self._maybe_early_management_ahu(file_fraction, progress_node_id),
            loop,
        )

    async def _maybe_early_management_ahu(
        self, file_fraction: float, progress_node_id: str = "worker_linux"
    ) -> None:
        if file_fraction < EARLY_MANAGEMENT_AHU_SYNC_FRACTION:
            return
        with self._early_ahu_lock:
            if self._early_ahu_dispatched:
                return
            self._early_ahu_dispatched = True
        try:
            await self._emit(
                progress_node_id,
                "uploading",
                "running",
                "[dispatch] Management Ahu — OpenClaw task queued (≥90% sync)",
            )
        except Exception:
            pass
        try:
            from arq import create_pool
            from arq.connections import RedisSettings

            from nexus.services.scout import OPENCLAW_PROJECT_CONFIG
            from nexus.shared.constants import TASK_DEFAULT_TIMEOUT
            from nexus.shared.schemas import TaskPayload, WorkerCapability

            redis_url = ""
            if self._settings is not None:
                redis_url = (getattr(self._settings, "redis_url", None) or "").strip()
            if not redis_url:
                redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

            cfg = next(
                x for x in OPENCLAW_PROJECT_CONFIG if x.get("project_id") == "management_ahu"
            )
            task = TaskPayload(
                task_type="scraper.openclaw",
                parameters={
                    "mode": cfg["mode"],
                    "query": cfg["query"],
                    "project_id": cfg["project_id"],
                    "max_leads": cfg.get("max_leads", 60),
                },
                project_id="management_ahu",
                priority=3,
                required_capabilities=[WorkerCapability.WINDOWS.value],
                requires_approval=False,
                approval_context="Auto-start during deploy sync (≥90% files) — Management Ahu",
            )
            task = self._vault.inject(task)
            pool = await create_pool(
                RedisSettings.from_dsn(redis_url),
                default_queue_name="nexus:tasks",
            )
            try:
                await pool.enqueue_job(
                    "execute_task",
                    task_payload=task.model_dump_for_wire(),
                    _job_id=task.task_id,
                    _queue_name="nexus:tasks",
                    _expires=TASK_DEFAULT_TIMEOUT,
                )
                log.info("early_management_ahu_enqueued", task_id=task.task_id)
            finally:
                await pool.aclose()
        except Exception as exc:
            log.warning("early_management_ahu_failed", error=str(exc))

    def _count_tree_upload_files(self) -> int:
        skip_parts = frozenset(
            {"__pycache__", ".venv", ".git", "node_modules", ".mypy_cache"}
        )
        n = 0
        for dir_name in SYNC_DIRS:
            ld = NEXUS_ROOT / dir_name
            if not ld.is_dir():
                continue
            for p in ld.rglob("*"):
                if not p.is_file() or p.suffix in (".pyc", ".pyo"):
                    continue
                if skip_parts.intersection(p.parts):
                    continue
                n += 1
        for fn in SYNC_FILES:
            if (NEXUS_ROOT / fn).is_file():
                n += 1
        return max(n, 1)

    def _upload_tree_file_ctx(self, ctx: dict[str, Any], local: str, remote: str) -> None:
        ssh_holder: list[Any] = ctx["ssh_holder"]
        connect_params: dict[str, Any] = ctx["connect_params"]
        sftp_holder: list[Any] = ctx["sftp_holder"]
        node_id: str = ctx["node_id"]
        loop = ctx.get("loop")
        done_ref: list[int] = ctx["done"]
        total_files: int = ctx["total"]

        sftp = sftp_holder[0]
        try:
            _sftp_put_with_timeout(sftp, local, remote)
        except SFTPStallError:
            detail = (
                f"[{node_id}: STALLED] SFTP hang on {remote!r} — reconnecting, skipping file "
                f"(other workers may still show ACTIVE)"
            )
            if loop is not None and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._emit(node_id, "uploading", "running", detail),
                    loop,
                )
            try:
                sftp.close()
            except Exception:
                pass
            sftp_holder[0] = _ssh_reconnect_and_new_sftp(ssh_holder, connect_params)
            return

        done_ref[0] += 1
        d = done_ref[0]
        pct = int(100 * d / total_files) if total_files else 0
        detail = f"[{node_id}: ACTIVE] [{d}/{total_files}] {remote} ({pct}%)"
        extra: dict[str, object] = {"upload_done": d, "upload_total": total_files}
        if loop is not None and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._emit(node_id, "uploading", "running", detail, extra=extra),
                loop,
            )
        self._schedule_early_ahu_from_executor(
            loop,
            (d / float(total_files)) if total_files else 1.0,
            node_id,
        )

    def _sftp_put_dir_resilient(
        self, ctx: dict[str, Any], local_dir: Path, remote_dir: str, remote_os: str
    ) -> None:
        sep = "/" if remote_os == "Linux" else "\\"
        _sftp_mkdir_p(ctx["sftp_holder"][0], remote_dir)
        skip_dirs = frozenset(
            {"__pycache__", ".venv", ".git", "node_modules", ".mypy_cache"}
        )
        for item in local_dir.iterdir():
            if item.name in skip_dirs:
                continue
            if item.suffix in (".pyc", ".pyo"):
                continue
            remote_path = f"{remote_dir}{sep}{item.name}"
            if item.is_dir():
                self._sftp_put_dir_resilient(ctx, item, remote_path, remote_os)
            else:
                self._upload_tree_file_ctx(ctx, str(item), remote_path)

    async def _sync_linux_nexus_zip_push(self) -> str:
        """
        Linux Nexus-Push: deep purge ``nexus/``, zip upload, pip + worker restart
        (``worker_linux``).
        """
        node_id = "worker_linux"
        raw_ip = (self._get_setting("worker_ip") or "").strip()
        ip = _effective_worker_linux_ssh_host(raw_ip)
        if not ip:
            detail = (
                "WORKER_IP is not set — set WORKER_IP in .env to your Linux worker "
                "IP, or 127.0.0.1 for same-machine deploy (no SSH)."
            )
            await self.clear_progress(node_id)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        if _is_loopback_deploy_host(ip):
            await self.clear_progress(node_id)
            return await self._deploy_node_loopback(node_id, ip)

        remote_root = (
            self._get_setting("worker_remote_path")
            or self._get_setting("worker_deploy_root_linux")
            or "/home/yadmin/Desktop/Nexus-Orchestrator"
        )
        ssh_user = self._get_setting("worker_ssh_user") or "yadmin"
        ssh_pass = (
            self._vault._backend.get("WORKER_SSH_PASSWORD")
            or os.environ.get("WORKER_SSH_PASSWORD")
            or self._get_setting("worker_ssh_password")
            or ""
        )
        ssh_key_files = self._resolve_ssh_key_files()

        if not ssh_pass:
            log.info(
                "deployer_no_password_key_auth_fallback",
                node_id=node_id,
                key_files=ssh_key_files,
                msg="WORKER_SSH_PASSWORD not set — attempting key-based auth",
            )
            if ssh_key_files:
                await self._emit(
                    node_id,
                    "connecting",
                    "running",
                    f"No password set — using key auth ({', '.join(ssh_key_files)})",
                )
            else:
                await self._emit(
                    node_id,
                    "connecting",
                    "running",
                    "No password set and no SSH key found — set WORKER_SSH_PASSWORD or WORKER_SSH_KEY_FILE in .env",
                )

        import paramiko  # type: ignore[import-untyped]

        await self.clear_progress(node_id)
        ssh = paramiko.SSHClient()
        _configure_ssh_security(ssh)
        loop = asyncio.get_event_loop()
        connect_params = {
            "hostname": ip,
            "username": ssh_user,
            "password": ssh_pass or None,
            "key_filename": ssh_key_files or None,
            "timeout": int(CONNECT_PHASE_TIMEOUT_SEC),
            "banner_timeout": int(CONNECT_PHASE_TIMEOUT_SEC),
            **_KEX_CONNECT_KWARGS,
        }
        ssh_h = [ssh]

        try:
            await self._emit(
                node_id, "connecting", "running", f"SSH → {ssh_user}@{ip}"
            )
            pf_err = await self._preflight_remote_ssh_executor(ip)
            if pf_err:
                await self._emit(
                    node_id,
                    "skipped",
                    "done",
                    f"[SKIPPED] {pf_err}",
                )
                log.warning(
                    "deployer_linux_zip_push_skipped_preflight",
                    node_id=node_id,
                    detail=(pf_err or "")[:500],
                )
                return "skipped: worker_linux unreachable"
            print_ssh_debug_command(ssh_user, ip)
            try:
                await self._connect_ssh_with_deadline(
                    ssh_h[0],
                    hostname=ip,
                    username=ssh_user,
                    password=ssh_pass,
                    node_id=node_id,
                    key_files=ssh_key_files or None,
                )
            except Exception as connect_exc:
                return await self._handle_deploy_connect_failure(
                    node_id, connect_exc
                )
            await self._emit(node_id, "connecting", "done", f"Connected to {ip}")

            await self._emit(
                node_id,
                "stopping_worker",
                "running",
                "Stopping worker before payload deploy…",
            )
            await loop.run_in_executor(
                None, lambda: self._stop_worker(ssh_h[0], "Linux", remote_root)
            )
            await self._emit(node_id, "stopping_worker", "done", "Worker stopped")

            await self._emit(
                node_id,
                "purging",
                "running",
                f"Deep purge {remote_root}/src/nexus before transfer…",
            )
            await loop.run_in_executor(
                None,
                lambda: self._pre_sync_purge_linux_remote(
                    ssh_h[0], remote_root, ssh_user
                ),
            )
            await self._emit(
                node_id,
                "purging",
                "done",
                "Purge finished (warnings ignored if paths were missing)",
            )

            # Build a thread-safe progress callback that queues emit calls back
            # onto the event loop so the executor thread can report sub-phases.
            _emit_queue: "asyncio.Queue[tuple[str, str, str, str]]" = asyncio.Queue()

            def _zip_progress_cb(step: str, detail: str) -> None:
                try:
                    loop.call_soon_threadsafe(
                        lambda s=step, d=detail: asyncio.ensure_future(
                            self._emit(node_id, s, "running", d)
                        )
                    )
                except Exception:
                    pass

            await loop.run_in_executor(
                None,
                lambda: self._compressed_nexus_transfer_and_extract(
                    ssh_h, connect_params, remote_root,
                    remote_os="Linux",
                    progress_cb=_zip_progress_cb,
                ),
            )
            await self._emit(
                node_id,
                "uploading",
                "done",
                f"{NEXUS_PAYLOAD_ZIP_NAME} deployed and extracted",
            )

            await self._maybe_early_management_ahu(1.0, node_id)

            # ── Inject Master IP into remote .env so the worker knows where Redis is ──
            master_ip = self._resolve_master_ip()
            if master_ip:
                inject_env_cmd = (
                    f"bash -lc "
                    + shlex.quote(
                        f"cd {remote_root} && "
                        f"sed -i '/^REDIS_URL=/d' .env 2>/dev/null; "
                        f"sed -i '/^REDIS_HOST=/d' .env 2>/dev/null; "
                        f"echo 'REDIS_URL=redis://{master_ip}:6379/0' >> .env; "
                        f"echo 'REDIS_HOST={master_ip}' >> .env; "
                        f"echo '[nexus-push] Redis broker set to {master_ip}'"
                    )
                )
                await self._emit(
                    node_id, "uploading", "running",
                    f"[nexus-push] Injecting REDIS_HOST={master_ip} into remote .env"
                )
                await loop.run_in_executor(
                    None, lambda: self._run_heal(ssh_h[0], inject_env_cmd)
                )
                await self._emit(
                    node_id, "uploading", "done",
                    f"Remote .env updated — REDIS_HOST={master_ip}"
                )

            heal_cmd = (
                f"bash -c 'cd {remote_root}/tools"
                f" && source ../.venv/bin/activate"
                f" && pip install -r ../requirements.txt"
                f" && pkill -f start_worker.py || true"
                f" && nohup python3 start_worker.py > worker.log 2>&1 &'"
            )

            await self._emit(
                node_id, "installing_deps", "running", "pip install -r requirements.txt"
            )
            await self._emit(node_id, "restarting", "running", "Restarting worker…")
            exit_code, stdout_tail = await loop.run_in_executor(
                None, lambda: self._run_heal(ssh_h[0], heal_cmd),
            )
            if exit_code != 0:
                await self._emit(
                    node_id,
                    "installing_deps",
                    "error",
                    f"heal exited {exit_code} — {stdout_tail[-200:]}",
                )
                return f"error: heal failed ({exit_code})"
            await self._emit(
                node_id, "installing_deps", "done", "Dependencies installed"
            )
            await self._emit(
                node_id, "restarting", "done", "start_worker.py launched in background"
            )

            # ── Verify worker actually started (pgrep -f start_worker.py) ──────────
            await asyncio.sleep(3)
            verify_cmd = "bash -lc " + shlex.quote("pgrep -f start_worker.py || true")
            verify_exit, verify_out = await loop.run_in_executor(
                None, lambda: self._run_heal(ssh_h[0], verify_cmd)
            )
            worker_pid = verify_out.strip()
            if not worker_pid:
                # Pull last 10 lines of remote worker.log for diagnostics
                log_tail_cmd = "bash -lc " + shlex.quote(
                    f"tail -n 10 {remote_root}/worker.log 2>/dev/null || echo '(log not found)'"
                )
                _, log_tail = await loop.run_in_executor(
                    None, lambda: self._run_heal(ssh_h[0], log_tail_cmd)
                )
                fail_detail = (
                    f"[FAILED] start_worker.py not running after restart. "
                    f"Last 10 lines of worker.log:\n{log_tail.strip()}"
                )
                await self._emit(node_id, "error", "error", fail_detail)
                log.error(
                    "deployer_worker_verify_failed",
                    node_id=node_id,
                    log_tail=log_tail.strip(),
                )
                return f"error: worker did not start — {log_tail.strip()[-200:]}"

            await self._emit(
                node_id, "done", "done",
                f"Deployment complete — Worker Live ✓ (PID {worker_pid.splitlines()[0]})"
            )
            await self._clear_deploy_degraded_for_node(node_id)
            return "ok"

        except Exception as exc:
            detail = str(exc)
            log.exception("deployer_sync_error", node_id=node_id, error=detail)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"
        finally:
            try:
                ssh_h[0].close()
            except Exception:
                pass

    async def _sync_windows_worker_for_sync(self) -> str:
        """Rolling deploy to ``worker_windows`` in parallel with Linux Nexus-Push."""
        nid = "worker_windows"
        ip = await self._resolve_ip(nid)
        if not ip or ip in ("unknown", ""):
            log.info(
                "worker_windows_ip_unresolved",
                hint="using fallback — set WORKER_IP_WINDOWS or WORKER_IP_WINDOWS_FALLBACK",
            )
            ip = self._fallback_worker_windows_ip()
        if not self._get_worker_ip_windows():
            log.info(
                "worker_windows_using_fallback_ip",
                ip=ip,
                platform=sys.platform,
            )
        return await self._deploy_node(nid)

    async def sync_to_worker(self) -> str:
        """
        Nexus-Push — Linux zip sync and Windows tree deploy run in parallel via
        ``asyncio.gather`` (``sync_worker`` targets: ``worker_linux``,
        ``worker_windows``).

        Returns "ok" or "error: <reason>". Linux may return ``skipped:`` when the
        host is unreachable (ping/TCP :22 preflight); that does not fail the job if
        the other target succeeds.
        """
        self._early_ahu_dispatched = False

        lip_raw = (self._get_setting("worker_ip") or "").strip()
        lip_eff = _effective_worker_linux_ssh_host(lip_raw)
        wip_raw = await self._resolve_ip("worker_windows")
        wip = str(wip_raw).strip() if wip_raw else ""
        if not wip or wip in ("unknown",):
            wip = str(self._fallback_worker_windows_ip()).strip()
        both_loopback = bool(lip_eff and wip) and _is_loopback_deploy_host(
            lip_eff
        ) and _is_loopback_deploy_host(wip)

        if not both_loopback:
            try:
                import paramiko  # type: ignore[import-untyped]  # noqa: F401
            except ImportError:
                detail = "paramiko not installed — run: pip install paramiko"
                await self._emit("worker_linux", "error", "error", detail)
                return f"error: {detail}"

        linux_res, win_res = await asyncio.gather(
            self._sync_linux_nexus_zip_push(),
            self._sync_windows_worker_for_sync(),
        )

        def _hard_fail(res: object) -> bool:
            return isinstance(res, str) and res.startswith("error")

        if _hard_fail(linux_res):
            return linux_res
        if _hard_fail(win_res):
            return f"error: worker_windows: {win_res}"
        return "ok"

    def _pre_sync_purge_linux_remote(
        self, ssh: object, remote_root: str, ssh_user: str
    ) -> None:
        """
        Before SFTP/zip sync: kill stray Pythons on the worker, remove ``src/nexus/*``,
        recreate ``src/nexus``, and chown to ``ssh_user``. Matches the layout
        ``~/Desktop/Nexus-Orchestrator/src/nexus`` when ``remote_root`` is the
        project root. Failures are logged as warnings only; upload still runs.
        """
        root = remote_root.rstrip("/").replace("\\", "/")
        nexus_dir = f"{root}/src/nexus"
        # Target only the Nexus worker — never ``killall python3`` (that nukes every
        # Python on the box and prevents heartbeats / status until a full restart).
        inner = (
            "pkill -SIGTERM -f 'start_worker.py' 2>/dev/null || true; "
            "sleep 2; "
            "pkill -SIGKILL -f 'start_worker.py' 2>/dev/null || true; "
            f"sudo rm -rf {shlex.quote(nexus_dir)}/* 2>/dev/null; "
            f"mkdir -p {shlex.quote(nexus_dir)}; "
            f"sudo chown -R {shlex.quote(ssh_user)}:{shlex.quote(ssh_user)} "
            f"{shlex.quote(nexus_dir)}"
        )
        cmd = "bash -lc " + shlex.quote(inner)
        try:
            exit_code, tail = self._run_heal(ssh, cmd)
            if exit_code != 0:
                log.warning(
                    "pre_sync_purge_nonzero_exit",
                    exit_code=exit_code,
                    nexus_dir=nexus_dir,
                    tail=tail[-500:],
                )
            else:
                log.info("pre_sync_purge_ok", nexus_dir=nexus_dir)
        except Exception as exc:
            log.warning("pre_sync_purge_failed", error=str(exc), nexus_dir=nexus_dir)

    def _compressed_nexus_transfer_and_extract(
        self,
        ssh_holder: list,
        connect_params: dict[str, Any],
        remote_root: str,
        remote_os: str = "Linux",
        progress_cb: "Callable[[str, str], None] | None" = None,
    ) -> None:
        """
        Universal zip-based deploy (Linux + Windows): build ``nexus_payload.zip``
        locally, SFTP the single archive, verify the remote size, extract
        in-place, delete the zip, then confirm the extracted file count.

        ``progress_cb(step, detail)`` is called at each phase so the async
        caller can emit SSE events without blocking the executor thread.
        Phases: ``zipping``, ``uploading``, ``extracting``, ``verifying``.

        Linux  — ``unzip -o`` via ``bash -lc`` (installs unzip if missing).
        Windows — ``Expand-Archive -Force`` via PowerShell (no extra tools needed).

        A remote size mismatch triggers one automatic re-upload.
        Linux exit-code 3 (corrupt zip) triggers a full local rebuild + re-upload.
        After extraction, the remote file count under ``src/nexus`` is checked
        and logged; zero files raises an error.
        """
        from collections.abc import Callable  # local import avoids circular

        def _cb(step: str, detail: str) -> None:
            if progress_cb is not None:
                try:
                    progress_cb(step, detail)
                except Exception:
                    pass

        tmpdir: str | None = None
        try:
            _cb("zipping", "📦 [ZIPPING] Packing files…")
            zip_path, tmpdir, file_count = _make_nexus_payload_zip()
            local_size = zip_path.stat().st_size
            size_mb = local_size / (1024 * 1024)
            _cb("zipping", f"📦 [ZIPPING] Packing {file_count} files…")

            # ── Upload + remote size verification (retry once on mismatch) ──
            for attempt in range(2):
                _cb(
                    "uploading",
                    f"🚀 [UPLOADING] Payload.zip ({size_mb:.2f} MB) → {remote_root}",
                )
                _upload_nexus_zip_sftp(ssh_holder, connect_params, zip_path, remote_root)

                root_fwd = remote_root.rstrip("/").replace("\\", "/")
                remote_zip = f"{root_fwd}/{NEXUS_PAYLOAD_ZIP_NAME}"
                if remote_os == "Linux":
                    stat_cmd = f"stat -c%s {shlex.quote(remote_zip)} 2>/dev/null || echo 0"
                else:
                    remote_zip_win = remote_root.rstrip("\\") + "\\" + NEXUS_PAYLOAD_ZIP_NAME
                    stat_cmd = (
                        f"powershell -NoProfile -NonInteractive -Command "
                        f"\"(Get-Item '{remote_zip_win}' -ErrorAction SilentlyContinue).Length\""
                    )
                _, stat_out = self._run_heal(ssh_holder[0], stat_cmd)
                try:
                    remote_size = int(stat_out.strip().splitlines()[-1])
                except (ValueError, IndexError):
                    remote_size = 0

                if remote_size == local_size:
                    log.info(
                        "nexus_zip_size_verified",
                        local_size=local_size,
                        remote_size=remote_size,
                        remote_os=remote_os,
                        attempt=attempt + 1,
                    )
                    break
                else:
                    log.warning(
                        "nexus_zip_size_mismatch",
                        local_size=local_size,
                        remote_size=remote_size,
                        remote_os=remote_os,
                        attempt=attempt + 1,
                    )
                    if attempt == 0:
                        log.info("nexus_zip_retrying_upload", reason="size_mismatch")
                        continue
                    raise RuntimeError(
                        f"nexus_payload.zip size mismatch after retry: "
                        f"local={local_size} remote={remote_size}"
                    )

            # ── Remote extraction ────────────────────────────────────────────
            _cb("extracting", "⚡ [EXTRACTING] Unpacking on Worker…")
            if remote_os == "Linux":
                unzip_cmd = _remote_unzip_bash_lc(remote_root)
            else:
                unzip_cmd = _remote_unzip_windows_cmd(remote_root)
            exit_code, tail = self._run_heal(ssh_holder[0], unzip_cmd)

            if exit_code == 3 and remote_os == "Linux":
                # Exit code 3 = corrupt zip — delete remote, rebuild, re-upload, retry once
                log.warning("nexus_zip_corrupt_exit3_rebuilding", tail=tail[-400:])
                remote_zip_path = f"{remote_root.rstrip('/')}/{NEXUS_PAYLOAD_ZIP_NAME}"
                self._run_heal(ssh_holder[0], f"rm -f {shlex.quote(remote_zip_path)}")
                if tmpdir:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                    tmpdir = None
                _cb("zipping", "📦 [ZIPPING] Rebuilding corrupt zip…")
                zip_path, tmpdir, file_count = _make_nexus_payload_zip()
                local_size = zip_path.stat().st_size
                size_mb = local_size / (1024 * 1024)
                _cb(
                    "uploading",
                    f"🚀 [UPLOADING] Payload.zip ({size_mb:.2f} MB) — retry",
                )
                _upload_nexus_zip_sftp(ssh_holder, connect_params, zip_path, remote_root)
                # Re-verify size after rebuild
                stat_cmd = f"stat -c%s {shlex.quote(remote_zip_path)} 2>/dev/null || echo 0"
                _, stat_out = self._run_heal(ssh_holder[0], stat_cmd)
                try:
                    remote_size = int(stat_out.strip().splitlines()[-1])
                except (ValueError, IndexError):
                    remote_size = 0
                if remote_size != local_size:
                    raise RuntimeError(
                        f"nexus_payload.zip rebuild size mismatch: local={local_size} remote={remote_size}"
                    )
                _cb("extracting", "⚡ [EXTRACTING] Unpacking on Worker…")
                exit_code, tail = self._run_heal(ssh_holder[0], unzip_cmd)

            if exit_code != 0:
                raise RuntimeError(
                    f"remote unzip failed (exit {exit_code}): {tail[-800:]}"
                )

            # ── Post-extraction file count verification ──────────────────────
            _cb("verifying", "🔍 [VERIFYING] Counting extracted files…")
            count_cmd = _remote_verify_file_count_cmd(remote_root, remote_os)
            _, count_out = self._run_heal(ssh_holder[0], count_cmd)
            try:
                remote_file_count = int(count_out.strip().splitlines()[-1])
            except (ValueError, IndexError):
                remote_file_count = -1
            log.info(
                "nexus_zip_extraction_verified",
                remote_os=remote_os,
                remote_root=remote_root,
                remote_file_count=remote_file_count,
            )
            _cb(
                "verifying",
                f"✅ [VERIFIED] {remote_file_count} files extracted to {remote_root}",
            )
            if remote_file_count == 0:
                raise RuntimeError(
                    f"nexus_payload.zip extracted 0 files into {remote_root} — "
                    "check remote path and zip contents"
                )

        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

    def _run_heal(self, ssh, cmd: str) -> tuple[int, str]:
        """
        Execute the self-healing command and wait for it to complete.
        Returns (exit_code, combined_output_tail).
        """
        _, stdout, stderr = ssh.exec_command(cmd, timeout=300)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        exit_code = stdout.channel.recv_exit_status()
        combined = (out + err)[-1200:]
        log.info("deployer_heal_output", combined=combined, exit_code=exit_code)
        if exit_code != 0 and err.strip():
            print(
                f"[Deployer] SSH remote stderr (exit={exit_code}) cmd={cmd!r}:\n{err}",
                file=sys.stderr,
                flush=True,
            )
        return exit_code, combined

    async def get_progress(self, node_id: str) -> list[dict]:
        key = f"{PROGRESS_KEY_PREFIX}{node_id}"
        raw_events = await self._redis.lrange(key, 0, -1)
        return [json.loads(e) for e in raw_events]

    async def clear_progress(self, node_id: str) -> None:
        await self._redis.delete(f"{PROGRESS_KEY_PREFIX}{node_id}")

    # ── Target resolution ──────────────────────────────────────────────────────

    async def _build_target_list(self) -> list[str]:
        """
        Combine Redis-discovered workers with any WORKER_IP static entry.
        Deduplicates by node_id.
        """
        targets: list[str] = []

        # Windows gaming laptop — always a deploy target; IP from config, heartbeat,
        # or ``_fallback_worker_windows_ip`` so Nexus-Push does not IDLE-skip.
        targets.append("worker_windows")

        # Linux worker — IP from WORKER_IP / heartbeat (no implicit LAN host)
        targets.append("worker_linux")

        # Redis-discovered workers
        redis_workers = await self._discover_worker_nodes()
        for nid in redis_workers:
            if nid not in targets:
                targets.append(nid)

        return targets

    async def dispatch_scraping_to_live_nodes(self) -> dict[str, str]:
        """
        Immediately dispatch super_scrape tasks to all LIVE nodes.

        For each node whose heartbeat shows ``status == "ok"`` (LIVE), assign
        a segment of the ``telefix.db`` targets to scan via the ARQ task queue.
        Uses ``telegram.super_scrape`` task type for maximum throughput.
        Nodes that are IDLE or unreachable are skipped.

        Returns a mapping of ``{node_id: "dispatched" | "skipped: <reason>"}``.
        """
        from arq import create_pool
        from arq.connections import RedisSettings

        results: dict[str, str] = {}
        redis_url = ""
        if self._settings is not None:
            redis_url = (getattr(self._settings, "redis_url", None) or "").strip()
        if not redis_url:
            redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

        # Collect all live nodes from heartbeats
        live_nodes: list[str] = []
        pattern = "nexus:heartbeat:*"
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                raw = await self._redis.get(key)
                if not raw:
                    continue
                try:
                    hb = json.loads(raw)
                    node_status = str(hb.get("status") or "").lower()
                    if node_status in ("ok", "live", "active"):
                        live_nodes.append(hb["node_id"])
                except Exception:
                    pass
            if cursor == 0:
                break

        if not live_nodes:
            log.warning("dispatch_scraping_no_live_nodes")
            return {"all": "skipped: no LIVE nodes found"}

        # Fetch telefix.db target segments — prefer live DB read, fall back to Redis cache
        telefix_targets: list[str] = []
        try:
            from nexus.shared.db_util import get_all_telefix_targets  # noqa: PLC0415
            telefix_targets = get_all_telefix_targets()
        except Exception:
            pass

        if not telefix_targets:
            telefix_targets_raw = await self._redis.get("nexus:telefix:targets")
            if telefix_targets_raw:
                try:
                    telefix_targets = json.loads(telefix_targets_raw)
                except Exception:
                    telefix_targets = []

        # Divide targets evenly across live nodes (round-robin segments)
        n = len(live_nodes)
        segments: list[list[str]] = [[] for _ in range(n)]
        for i, target in enumerate(telefix_targets):
            segments[i % n].append(target)

        try:
            pool = await create_pool(
                RedisSettings.from_dsn(redis_url),
                default_queue_name="nexus:tasks",
            )
        except Exception as exc:
            log.error("dispatch_scraping_pool_failed", error=str(exc))
            return {"all": f"error: {exc}"}

        try:
            for idx, node_id in enumerate(live_nodes):
                segment = segments[idx] if idx < len(segments) else []
                try:
                    from nexus.shared.schemas import TaskPayload  # noqa: PLC0415

                    task = TaskPayload(
                        task_type="telegram.super_scrape",
                        parameters={
                            "node_id": node_id,
                            "targets": segment,
                            "source": "telefix.db",
                            "cpu_limit": 0.9,
                        },
                        project_id="telefix",
                        priority=5,
                        required_capabilities=[node_id],
                    )
                    await pool.enqueue_job(
                        "execute_task",
                        task_payload=task.model_dump_for_wire(),
                        _job_id=task.task_id,
                        _queue_name="nexus:tasks",
                    )
                    log.info(
                        "dispatch_super_scrape_enqueued",
                        node_id=node_id,
                        segment_size=len(segment),
                        task_id=task.task_id,
                    )
                    results[node_id] = "dispatched"
                except Exception as exc:
                    log.warning("dispatch_scraping_node_failed", node_id=node_id, error=str(exc))
                    results[node_id] = f"error: {exc}"
        finally:
            await pool.aclose()

        return results

    async def _discover_worker_nodes(self) -> list[str]:
        pattern = "nexus:heartbeat:*"
        worker_ids: list[str] = []
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor, match=pattern, count=100
            )
            for key in keys:
                raw = await self._redis.get(key)
                if not raw:
                    continue
                try:
                    hb = json.loads(raw)
                    if hb.get("role") == "worker":
                        worker_ids.append(hb["node_id"])
                except Exception:
                    pass
            if cursor == 0:
                break
        return worker_ids

    async def _resolve_ip(self, node_id: str) -> str | None:
        """
        Resolve the IP for a node.

        Priority:
        0. If ``node_id`` is already a literal IP address, use it as the host
           (Sentinel auto-deploy and ad-hoc targets).
        1. ``worker_windows`` + ``WORKER_IP_WINDOWS`` / settings.
        2. ``worker_linux`` + ``WORKER_IP`` (any value), else heartbeat ``local_ip``,
           else stripped ``WORKER_IP`` (may be empty if unset).
        3. Otherwise ``local_ip`` from the Redis heartbeat.
        """
        stripped = node_id.strip()
        try:
            ipaddress.ip_address(stripped)
            return stripped
        except ValueError:
            pass

        worker_ip = self._get_setting("worker_ip")
        win_ip = self._get_worker_ip_windows()
        fb_win = self._fallback_worker_windows_ip()
        if node_id == "worker_windows" and win_ip:
            return win_ip
        if node_id == "worker_linux":
            raw_w = (worker_ip or "").strip()
            if raw_w and not _is_loopback_deploy_host(raw_w):
                return raw_w

        key = f"nexus:heartbeat:{node_id}"
        raw = await self._redis.get(key)
        if not raw:
            if node_id == "worker_windows":
                return win_ip or fb_win
            if node_id == "worker_linux":
                return _effective_worker_linux_ssh_host((worker_ip or "").strip())
            # Last resort: if only one static IP is configured, use it
            return worker_ip or None
        try:
            hb = json.loads(raw)
            ip = hb.get("local_ip")
            if node_id == "worker_windows" and (
                not ip or str(ip).strip() in ("", "unknown")
            ):
                return win_ip or fb_win
            if node_id == "worker_linux":
                hb_ip = str(ip).strip() if ip else ""
                if hb_ip and not _is_loopback_deploy_host(hb_ip):
                    return hb_ip
                return _effective_worker_linux_ssh_host((worker_ip or "").strip())
            return ip
        except Exception:
            if node_id == "worker_windows":
                return win_ip or fb_win
            if node_id == "worker_linux":
                return _effective_worker_linux_ssh_host((worker_ip or "").strip())
            return None

    # ── Per-node deployment ────────────────────────────────────────────────────

    def _infer_remote_os_for_local_deploy(self, node_id: str) -> str:
        if node_id == "worker_windows":
            return "Windows"
        if node_id == "worker_linux":
            # Same Windows box often registers as worker_linux via WORKER_IP=127.0.0.1;
            # Linux/bash paths would fail here — treat as Windows local deploy.
            if sys.platform == "win32":
                return "Windows"
            return "Linux"
        return "Windows" if sys.platform == "win32" else "Linux"

    def _loopback_project_destination(
        self, node_id: str, project_name: str, remote_dest: str | None
    ) -> Path:
        """Map default Linux Desktop paths to the local Windows tree when needed."""
        default_unix = f"/home/yadmin/Desktop/{project_name}"
        rd = (remote_dest or default_unix).strip()
        if sys.platform == "win32" and rd.replace("\\", "/").startswith("/"):
            base = self._resolve_local_deploy_root(node_id, "Windows")
            return base / project_name
        return Path(rd).expanduser()

    def _resolve_local_deploy_root(self, node_id: str, remote_os: str) -> Path:
        if remote_os == "Linux":
            cfg = (self._get_setting("worker_deploy_root_linux") or "").strip()
            if cfg:
                return Path(cfg).expanduser()
            return Path.home() / "Desktop" / "Nexus-Orchestrator"
        cfg = (self._get_setting("worker_deploy_root_win") or "").strip()
        if cfg:
            return Path(cfg)
        return Path.home() / "Desktop" / "Nexus-Orchestrator"

    def _local_stop_worker_blocking(self, remote_os: str, remote_root: str) -> None:
        if remote_os == "Linux":
            inner = (
                "REMOTE_ROOT="
                + shlex.quote(remote_root)
                + "; "
                'if [ -f "$REMOTE_ROOT/worker.pid" ]; then '
                'kill -SIGTERM $(cat "$REMOTE_ROOT/worker.pid") 2>/dev/null || true; '
                "sleep 2; "
                'kill -SIGKILL $(cat "$REMOTE_ROOT/worker.pid") 2>/dev/null || true; '
                'rm -f "$REMOTE_ROOT/worker.pid"; fi; '
                "pkill -SIGTERM -f 'start_worker.py' 2>/dev/null || true; sleep 1"
            )
            subprocess.run(
                ["bash", "-lc", inner],
                capture_output=True,
                timeout=60,
                check=False,
            )
        else:
            subprocess.run(
                'taskkill /F /FI "WINDOWTITLE eq nexus-worker*" 2>nul & '
                'taskkill /F /IM python.exe /FI "WINDOWTITLE eq nexus*" 2>nul & '
                "timeout /t 2 /nobreak >nul",
                shell=True,
                capture_output=True,
                timeout=45,
                cwd=remote_root,
                check=False,
            )

    def _local_sync_payload_blocking(self, dest_root: Path, remote_os: str) -> None:
        dest_root = dest_root.resolve()
        if dest_root == NEXUS_ROOT.resolve():
            log.info("local_deploy_skip_copy_same_root", dest=str(dest_root))
            return
        dest_root.mkdir(parents=True, exist_ok=True)
        skip = frozenset({"__pycache__", ".venv", ".git", "node_modules", ".mypy_cache"})

        if remote_os == "Windows" and sys.platform == "win32":
            for dir_name in SYNC_DIRS:
                src = (NEXUS_ROOT / dir_name).resolve()
                if not src.is_dir():
                    continue
                dst = dest_root / dir_name
                dst.mkdir(parents=True, exist_ok=True)
                cmd = [
                    "robocopy",
                    str(src),
                    str(dst),
                    "/E",
                    "/XD",
                    "__pycache__",
                    ".venv",
                    ".git",
                    "node_modules",
                    ".mypy_cache",
                    "/XF",
                    "*.pyc",
                    "*.pyo",
                    "/NFL",
                    "/NDL",
                    "/NJH",
                    "/NJS",
                    "/NC",
                    "/NS",
                    "/NP",
                ]
                r = subprocess.run(cmd, capture_output=True, timeout=1200)
                if r.returncode >= 8:
                    err = (r.stderr or b"").decode(errors="replace")[-500:]
                    raise RuntimeError(f"robocopy {dir_name!r} failed (exit {r.returncode}): {err}")
            for fn in SYNC_FILES:
                sf = NEXUS_ROOT / fn
                if sf.is_file():
                    shutil.copy2(sf, dest_root / fn)
            return

        for dir_name in SYNC_DIRS:
            src = NEXUS_ROOT / dir_name
            if not src.is_dir():
                continue
            dst = dest_root / dir_name
            for item in src.rglob("*"):
                if not item.is_file():
                    continue
                if item.suffix in (".pyc", ".pyo"):
                    continue
                if skip.intersection(item.parts):
                    continue
                rel = item.relative_to(src)
                out = dst / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, out)
        for fn in SYNC_FILES:
            sf = NEXUS_ROOT / fn
            if sf.is_file():
                shutil.copy2(sf, dest_root / fn)

    def _local_install_deps_blocking(
        self, remote_root: str, venv_python: str, remote_os: str
    ) -> bool:
        if remote_os == "Linux":
            inner = (
                "REMOTE_ROOT="
                + shlex.quote(remote_root)
                + "; "
                'if [ ! -f "$REMOTE_ROOT/.venv/bin/python" ]; then '
                'python3 -m venv "$REMOTE_ROOT/.venv"; fi && '
                'cd "$REMOTE_ROOT/tools" && '
                'source "$REMOTE_ROOT/.venv/bin/activate" && '
                "pip install --quiet --upgrade pip && "
                "pip install --quiet -r ../requirements.txt "
                "|| pip install --quiet --no-cache-dir -r ../requirements.txt; "
                "echo EXIT_CODE:$?"
            )
            r = subprocess.run(
                ["bash", "-lc", inner],
                capture_output=True,
                text=True,
                timeout=600,
            )
            return r.returncode == 0 and "EXIT_CODE:0" in (r.stdout or "")
        req = str(Path(remote_root) / "requirements.txt")
        cmd = (
            f'"{venv_python}" -m pip install --quiet --upgrade pip && '
            f'"{venv_python}" -m pip install --quiet -r "{req}"'
        )
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
        return r.returncode == 0

    def _local_restart_worker_blocking(
        self, remote_root: str, venv_python: str, remote_os: str
    ) -> None:
        if remote_os == "Linux":
            inner = (
                "REMOTE_ROOT="
                + shlex.quote(remote_root)
                + "; "
                "pkill -f 'start_worker.py' 2>/dev/null || true; sleep 1; "
                'chmod +x "$REMOTE_ROOT/start_nexus.sh"; cd "$REMOTE_ROOT" && '
                'nohup bash "$REMOTE_ROOT/start_nexus.sh" >> "$REMOTE_ROOT/worker.log" 2>&1 &'
            )
            subprocess.run(
                ["bash", "-lc", inner],
                capture_output=True,
                timeout=30,
                check=False,
            )
        else:
            rr = Path(remote_root)
            worker_script = rr / "tools" / "start_worker.py"
            log_file = rr / "worker.log"
            cmd = (
                f'cd /d "{remote_root}" && start /B "" "{venv_python}" '
                f'"{worker_script}" >> "{log_file}" 2>&1'
            )
            subprocess.Popen(
                cmd,
                shell=True,
                cwd=str(rr),
                creationflags=subprocess.DETACHED_PROCESS if sys.platform == "win32" else 0,
            )

    async def _deploy_node_loopback(self, node_id: str, ip: str) -> str:
        """Same-machine deploy: no SSH; copy tree locally then pip + restart."""
        loop = asyncio.get_running_loop()
        remote_os = self._infer_remote_os_for_local_deploy(node_id)
        dest = self._resolve_local_deploy_root(node_id, remote_os)
        remote_root = str(dest)
        if remote_os == "Linux":
            venv_python = f"{remote_root}/.venv/bin/python"
        else:
            venv_python = str(dest / ".venv" / "Scripts" / "python.exe")

        try:
            await self._emit(
                node_id,
                "connecting",
                "running",
                f"Local deploy — skipping SSH ({ip} → {remote_root})",
            )
            await self._emit(
                node_id,
                "connecting",
                "done",
                "Same machine — no SSH",
            )

            await self._emit(
                node_id,
                "stopping_worker",
                "running",
                "Stopping local worker…",
            )
            await loop.run_in_executor(
                None,
                lambda: self._local_stop_worker_blocking(remote_os, remote_root),
            )
            await self._emit(node_id, "stopping_worker", "done", "Worker stopped")

            await self._emit(
                node_id,
                "uploading",
                "running",
                f"[{node_id}: ACTIVE] Copying src/nexus tools/ → {remote_root}",
            )
            await loop.run_in_executor(
                None,
                lambda: self._local_sync_payload_blocking(dest, remote_os),
            )
            await self._emit(node_id, "uploading", "done", "Payload synced locally")

            await self._maybe_early_management_ahu(1.0, node_id)

            await self._emit(
                node_id,
                "installing_deps",
                "running",
                "pip install -r requirements.txt",
            )
            deps_ok = await loop.run_in_executor(
                None,
                lambda: self._local_install_deps_blocking(
                    remote_root, venv_python, remote_os
                ),
            )
            if not deps_ok:
                await self._emit(
                    node_id,
                    "installing_deps",
                    "error",
                    "pip install had errors — check worker.log",
                )
                return "error: pip install failed"
            await self._emit(
                node_id, "installing_deps", "done", "Dependencies installed"
            )

            await self._emit(node_id, "restarting", "running", "Starting worker…")
            await loop.run_in_executor(
                None,
                lambda: self._local_restart_worker_blocking(
                    remote_root, venv_python, remote_os
                ),
            )
            await self._emit(node_id, "restarting", "done", "Worker started")
            await self._emit(node_id, "done", "done", "Deployment complete — Worker Live")
            await self._clear_deploy_degraded_for_node(node_id)
            return "ok"
        except Exception as exc:
            detail = str(exc)
            log.exception(
                "deployer_loopback_error", node_id=node_id, error=detail
            )
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

    async def _deploy_node(self, node_id: str) -> str:
        await self.clear_progress(node_id)

        ip = await self._resolve_ip(node_id)
        if not ip or ip in ("unknown", ""):
            detail = (
                f"No IP for '{node_id}'. "
                "Set WORKER_IP in .env or ensure the worker is sending heartbeats."
            )
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        if _is_loopback_deploy_host(ip):
            return await self._deploy_node_loopback(node_id, ip)

        if node_id == "worker_windows":
            await self._emit(
                node_id,
                "uploading",
                "running",
                f"[{node_id}: UPLOADING] Target {ip} — deploy starting (heartbeat optional)",
            )

        ssh_user = (
            self._vault._backend.get("WORKER_SSH_USER")
            or os.environ.get("WORKER_SSH_USER")
            or self._get_setting("worker_ssh_user")
            or "yadmin"
        )
        ssh_pass = (
            self._vault._backend.get("WORKER_SSH_PASSWORD")
            or os.environ.get("WORKER_SSH_PASSWORD")
            or self._get_setting("worker_ssh_password")
            or ""
        )
        ssh_key_files = self._resolve_ssh_key_files()

        if not ssh_pass:
            log.info(
                "deployer_no_password_key_auth_fallback",
                node_id=node_id,
                key_files=ssh_key_files,
                msg="WORKER_SSH_PASSWORD not set — attempting key-based auth",
            )
            if ssh_key_files:
                await self._emit(
                    node_id,
                    "connecting",
                    "running",
                    f"No password set — using key auth ({', '.join(ssh_key_files)})",
                )
            else:
                await self._emit(
                    node_id,
                    "connecting",
                    "running",
                    "No password set and no SSH key found — set WORKER_SSH_PASSWORD or WORKER_SSH_KEY_FILE in .env",
                )

        try:
            import paramiko  # type: ignore[import-untyped]
        except ImportError:
            detail = "paramiko not installed — run: pip install paramiko"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        ssh = paramiko.SSHClient()
        _configure_ssh_security(ssh)

        connect_params = {
            "hostname": ip,
            "username": ssh_user,
            "password": ssh_pass or None,
            "key_filename": ssh_key_files or None,
            "timeout": int(CONNECT_PHASE_TIMEOUT_SEC),
            "banner_timeout": int(CONNECT_PHASE_TIMEOUT_SEC),
            **_KEX_CONNECT_KWARGS,
        }
        ssh_h = [ssh]
        loop = asyncio.get_running_loop()

        try:
            # ── 1. Connect (hard time cap — skip target on expiry) ────────────
            await self._emit(node_id, "connecting", "running", f"SSH → {ssh_user}@{ip}")
            pf_err = await self._preflight_remote_ssh_executor(ip)
            if pf_err:
                await self._emit(
                    node_id,
                    "skipped",
                    "done",
                    f"[SKIPPED] {pf_err}",
                )
                log.warning(
                    "deployer_deploy_node_skipped_preflight",
                    node_id=node_id,
                    detail=(pf_err or "")[:500],
                )
                return f"skipped: {pf_err}"
            print_ssh_debug_command(ssh_user, ip)
            try:
                await self._connect_ssh_with_deadline(
                    ssh_h[0],
                    hostname=ip,
                    username=ssh_user,
                    password=ssh_pass,
                    node_id=node_id,
                    key_files=ssh_key_files or None,
                )
            except Exception as connect_exc:
                return await self._handle_deploy_connect_failure(
                    node_id, connect_exc
                )
            await self._emit(node_id, "connecting", "done", f"Connected to {ip}")

            # ── 2. Detect remote OS and resolve destination path ───────────────
            remote_os, remote_root, venv_python = await loop.run_in_executor(
                None, lambda: self._detect_remote_env(ssh_h[0], ssh_user)
            )
            log.info("deployer_remote_env", node_id=node_id, remote_os=remote_os, remote_root=remote_root)

            # ── 3. Stop worker ────────────────────────────────────────────────
            await self._emit(node_id, "stopping_worker", "running", "Sending SIGTERM to worker")
            await loop.run_in_executor(
                None, lambda: self._stop_worker(ssh_h[0], remote_os, remote_root)
            )
            await self._emit(node_id, "stopping_worker", "done", "Worker stopped")

            # ── 4. Upload payload (Linux: single zip; Windows: SFTP tree) ───────
            if remote_os == "Linux":
                await self._emit(
                    node_id,
                    "purging",
                    "running",
                    f"Deep purge {remote_root}/src/nexus before transfer…",
                )
                await loop.run_in_executor(
                    None,
                    lambda: self._pre_sync_purge_linux_remote(
                        ssh_h[0], remote_root, ssh_user
                    ),
                )
                await self._emit(
                    node_id,
                    "purging",
                    "done",
                    "Purge finished (warnings ignored if paths were missing)",
                )
                def _zip_progress_cb_node(step: str, detail: str) -> None:
                    try:
                        loop.call_soon_threadsafe(
                            lambda s=step, d=detail: asyncio.ensure_future(
                                self._emit(node_id, s, "running", d)
                            )
                        )
                    except Exception:
                        pass

                await loop.run_in_executor(
                    None,
                    lambda: self._compressed_nexus_transfer_and_extract(
                        ssh_h, connect_params, remote_root,
                        remote_os="Linux",
                        progress_cb=_zip_progress_cb_node,
                    ),
                )
                await self._emit(
                    node_id, "uploading", "done", "Archive uploaded and extracted"
                )
                await self._maybe_early_management_ahu(1.0, node_id)
            else:
                # Windows worker — zip-based deploy (bypasses per-file SFTP lock-ups)
                await self._emit(
                    node_id,
                    "uploading",
                    "running",
                    f"[{node_id}: ACTIVE] Building {NEXUS_PAYLOAD_ZIP_NAME} → {remote_root}",
                )

                def _zip_progress_cb_win(step: str, detail: str) -> None:
                    try:
                        loop.call_soon_threadsafe(
                            lambda s=step, d=detail: asyncio.ensure_future(
                                self._emit(node_id, s, "running", d)
                            )
                        )
                    except Exception:
                        pass

                await loop.run_in_executor(
                    None,
                    lambda: self._compressed_nexus_transfer_and_extract(
                        ssh_h, connect_params, remote_root,
                        remote_os="Windows",
                        progress_cb=_zip_progress_cb_win,
                    ),
                )
                await self._emit(
                    node_id, "uploading", "done",
                    f"{NEXUS_PAYLOAD_ZIP_NAME} deployed and extracted (Windows)"
                )
                await self._maybe_early_management_ahu(1.0, node_id)

            # ── 5. Install dependencies ───────────────────────────────────────
            await self._emit(node_id, "installing_deps", "running",
                             "pip install -r requirements.txt")
            deps_ok = await loop.run_in_executor(
                None, lambda: self._install_deps(ssh_h[0], remote_root, venv_python, remote_os)
            )
            if not deps_ok:
                await self._emit(node_id, "installing_deps", "error",
                                 "pip install had errors — check worker.log")
            else:
                await self._emit(node_id, "installing_deps", "done",
                                 "Dependencies installed")

            # ── 6. Restart worker via start_nexus.sh ──────────────────────────
            await self._emit(node_id, "restarting", "running", "bash start_nexus.sh")
            await loop.run_in_executor(
                None, lambda: self._restart_worker(ssh_h[0], remote_root, venv_python, remote_os)
            )
            await self._emit(node_id, "restarting", "done", "Worker restarted")
            await self._emit(node_id, "done", "done", "Deployment complete — Worker Live")
            await self._clear_deploy_degraded_for_node(node_id)
            return "ok"

        except Exception as exc:
            detail = str(exc)
            log.exception("deployer_node_error", node_id=node_id, error=detail)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"
        finally:
            try:
                ssh_h[0].close()
            except Exception:
                pass

    # ── SSH helpers (blocking — run in executor) ───────────────────────────────

    def _detect_remote_env(self, ssh, ssh_user: str) -> tuple[str, str, str]:
        """
        Returns (remote_os, remote_root, venv_python).

        Uses WORKER_DEPLOY_ROOT_LINUX / WIN from settings when set.
        Falls back to auto-detection via `find` on Linux.
        """
        _, stdout, _ = ssh.exec_command("uname -s 2>/dev/null || echo Windows")
        uname = stdout.read().decode().strip()
        remote_os = "Linux" if "linux" in uname.lower() else "Windows"

        if remote_os == "Linux":
            # 1. Prefer the explicit config value
            configured = self._get_setting("worker_deploy_root_linux")
            if configured:
                remote_root = configured
            else:
                # 2. Auto-detect by finding pyproject.toml
                _, out, _ = ssh.exec_command(
                    "find /home -maxdepth 5 -name 'pyproject.toml' 2>/dev/null | head -1"
                )
                found = out.read().decode().strip()
                remote_root = str(Path(found).parent) if found else f"/home/{ssh_user}/Desktop/Nexus-Orchestrator"
            venv_python = f"{remote_root}/.venv/bin/python"
        else:
            configured = self._get_setting("worker_deploy_root_win")
            remote_root = configured or rf"C:\Users\{ssh_user}\Desktop\Nexus-Orchestrator"
            venv_python = rf"{remote_root}\.venv\Scripts\python.exe"

        return remote_os, remote_root, venv_python

    def _stop_worker(self, ssh, remote_os: str, remote_root: str) -> None:
        """Gracefully stop the running worker using the PID file, then pkill."""
        if remote_os == "Linux":
            pid_file = f"{remote_root}/worker.pid"
            # Try PID file first for a clean SIGTERM
            ssh.exec_command(
                f"if [ -f {pid_file} ]; then "
                f"  kill -SIGTERM $(cat {pid_file}) 2>/dev/null || true; "
                f"  sleep 2; "
                f"  kill -SIGKILL $(cat {pid_file}) 2>/dev/null || true; "
                f"  rm -f {pid_file}; "
                f"fi; "
                f"pkill -SIGTERM -f 'start_worker.py' 2>/dev/null || true; "
                f"sleep 1"
            )
        else:
            ssh.exec_command(
                'taskkill /F /FI "WINDOWTITLE eq nexus-worker*" 2>nul & '
                'taskkill /F /IM python.exe /FI "WINDOWTITLE eq nexus*" 2>nul & '
                'timeout /t 2 /nobreak >nul'
            )

    def _upload_dirs(
        self,
        ssh_holder: list,
        connect_params: dict[str, Any],
        remote_root: str,
        remote_os: str,
        node_id: str,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """SFTP-upload src/nexus/, tools/, and root-level files (per-file put timeout + reconnect)."""
        sep = "/" if remote_os == "Linux" else "\\"
        total = self._count_tree_upload_files()
        sftp0 = ssh_holder[0].open_sftp()
        ctx: dict[str, Any] = {
            "ssh_holder": ssh_holder,
            "connect_params": connect_params,
            "sftp_holder": [sftp0],
            "node_id": node_id,
            "loop": loop,
            "done": [0],
            "total": total,
        }
        try:
            _sftp_mkdir_p(ctx["sftp_holder"][0], remote_root)

            for dir_name in SYNC_DIRS:
                local_dir = NEXUS_ROOT / dir_name
                if local_dir.exists():
                    remote_dir = f"{remote_root}{sep}{dir_name}"
                    self._sftp_put_dir_resilient(ctx, local_dir, remote_dir, remote_os)

            for file_name in SYNC_FILES:
                local_file = NEXUS_ROOT / file_name
                if not local_file.exists():
                    log.debug("deployer_skip_missing_file", file=file_name)
                    continue
                remote_path = f"{remote_root}{sep}{file_name}"
                try:
                    self._upload_tree_file_ctx(ctx, str(local_file), remote_path)
                except Exception as exc:
                    log.warning("deployer_file_upload_error", file=file_name, error=str(exc))

            if remote_os == "Linux":
                for sh in ("start_nexus.sh", "run_worker.sh"):
                    try:
                        ctx["sftp_holder"][0].chmod(f"{remote_root}/{sh}", 0o755)
                    except Exception:
                        pass
        finally:
            try:
                ctx["sftp_holder"][0].close()
            except Exception:
                pass

    def _install_deps(
        self,
        ssh,
        remote_root: str,
        venv_python: str,
        remote_os: str,
    ) -> bool:
        """
        Install dependencies on the remote node.

        Linux sequence (matches the spec exactly):
          1. Create .venv if missing.
          2. cd <root>/tools && source ../.venv/bin/activate
          3. pip install --upgrade pip
          4. pip install -r ../requirements.txt
          5. Retry with --no-cache-dir on failure.

        Returns True on success.
        """
        if remote_os == "Linux":
            venv_dir = f"{remote_root}/.venv"
            scripts_dir = f"{remote_root}/tools"
            cmd = (
                # 1. Create venv if absent
                f"if [ ! -f {venv_dir}/bin/python ]; then "
                f"  python3 -m venv {venv_dir}; "
                f"fi && "
                # 2. cd into tools/, activate, upgrade pip, install deps
                f"cd {scripts_dir} && "
                f"source {venv_dir}/bin/activate && "
                f"pip install --quiet --upgrade pip && "
                f"pip install --quiet -r ../requirements.txt "
                # 3. Retry with --no-cache-dir if first attempt failed
                f"|| pip install --quiet --no-cache-dir -r ../requirements.txt; "
                f"echo EXIT_CODE:$?"
            )
        else:
            req_file = rf"{remote_root}\requirements.txt"
            cmd = (
                f'"{venv_python}" -m pip install --quiet --upgrade pip && '
                f'"{venv_python}" -m pip install --quiet -r "{req_file}"'
            )

        _, stdout, _ = ssh.exec_command(cmd, timeout=300)
        output = stdout.read().decode()
        exit_status = stdout.channel.recv_exit_status()
        log.info("deployer_deps_output", output=output[-1000:])

        if remote_os == "Linux" and "EXIT_CODE:0" in output:
            return True
        return exit_status == 0

    def _restart_worker(
        self,
        ssh,
        remote_root: str,
        venv_python: str,
        remote_os: str,
    ) -> None:
        """
        Kill any existing start_worker.py processes, then launch start_nexus.sh
        detached so it survives the SSH session.
        """
        if remote_os == "Linux":
            start_sh = f"{remote_root}/start_nexus.sh"
            log_file = f"{remote_root}/worker.log"
            cmd = (
                # Kill existing worker processes
                f"pkill -f 'start_worker.py' 2>/dev/null || true; "
                f"sleep 1; "
                # Ensure script is executable
                f"chmod +x {start_sh}; "
                # Launch detached — nohup + & ensures it outlives the SSH channel
                f"cd {remote_root} && "
                f"nohup bash {start_sh} >> {log_file} 2>&1 &"
            )
        else:
            sep = "\\"
            worker_script = f"{remote_root}{sep}tools{sep}start_worker.py"
            log_file = rf"{remote_root}\worker.log"
            cmd = (
                f'cd /d "{remote_root}" && '
                f'start /B "{venv_python}" "{worker_script}" '
                f'>> "{log_file}" 2>&1'
            )

        ssh.exec_command(cmd)

    # ── Redis progress emitter ─────────────────────────────────────────────────

    async def _emit(
        self,
        node_id: str,
        step: DeployStep,
        status: DeployStatus,
        detail: str = "",
        *,
        extra: dict[str, object] | None = None,
    ) -> None:
        event = _event(node_id, step, status, detail, extra=extra)
        key = f"{PROGRESS_KEY_PREFIX}{node_id}"
        payload = json.dumps(event)
        try:
            await asyncio.wait_for(
                self._redis_rpush_trim_expire(key, payload),
                timeout=4.0,
            )
        except asyncio.TimeoutError:
            log.warning("deployer_emit_redis_timeout", node_id=node_id, step=step)
        log.debug("deployer_progress", **event)

    async def _redis_rpush_trim_expire(self, key: str, payload: str) -> None:
        await self._redis.rpush(key, payload)
        await self._redis.ltrim(key, -PROGRESS_MAX_LEN, -1)
        await self._redis.expire(key, 3600)

    # ── Settings helper ────────────────────────────────────────────────────────

    def _get_setting(self, key: str) -> str:
        """Read a value from the injected settings object, or fall back to env."""
        if self._settings is not None:
            return getattr(self._settings, key, "") or ""
        return os.environ.get(key.upper(), "")

    def _resolve_master_ip(self) -> str:
        """
        Return the LAN IP of this master machine (Jacob-PC) so workers can
        reach the Redis broker.

        Priority:
        1. ``MASTER_IP`` env var (explicit override).
        2. ``REDIS_HOST`` env var when it is not loopback.
        3. Auto-detect the first non-loopback IPv4 address on this host.
        """
        explicit = os.environ.get("MASTER_IP", "").strip()
        if explicit and not _is_loopback_deploy_host(explicit):
            return explicit

        redis_host = os.environ.get("REDIS_HOST", "").strip()
        if redis_host and not _is_loopback_deploy_host(redis_host):
            return redis_host

        # Auto-detect: connect a UDP socket to a public address (no packet sent)
        # to learn which local interface the OS would use for LAN traffic.
        try:
            import socket as _socket
            with _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                detected = s.getsockname()[0]
            if detected and not _is_loopback_deploy_host(detected):
                return detected
        except Exception:
            pass

        log.warning("master_ip_unresolved_skipping_redis_inject")
        return ""


# ── SFTP helpers ───────────────────────────────────────────────────────────────

def _sftp_put_dir(sftp, local_dir: Path, remote_dir: str, remote_os: str) -> None:
    """
    Recursively upload `local_dir` to `remote_dir` via SFTP.
    Skips __pycache__, .venv, .git, node_modules, and compiled bytecode.
    """
    sep = "/" if remote_os == "Linux" else "\\"
    _sftp_mkdir_p(sftp, remote_dir)

    for item in local_dir.iterdir():
        if item.name in ("__pycache__", ".venv", ".git", "node_modules", ".mypy_cache"):
            continue
        if item.suffix in (".pyc", ".pyo"):
            continue

        remote_path = f"{remote_dir}{sep}{item.name}"
        if item.is_dir():
            _sftp_put_dir(sftp, item, remote_path, remote_os)
        else:
            try:
                sftp.put(str(item), remote_path)
            except Exception as exc:
                log.warning("sftp_put_error", local=str(item), remote=remote_path, error=str(exc))


def _sftp_mkdir_p(sftp, remote_path: str) -> None:
    """Create remote directory and all parents (like mkdir -p)."""
    parts = remote_path.replace("\\", "/").split("/")
    current = ""
    for part in parts:
        if not part:
            current = "/"
            continue
        current = f"{current}/{part}" if current != "/" else f"/{part}"
        try:
            sftp.stat(current)
        except FileNotFoundError:
            try:
                sftp.mkdir(current)
            except Exception:
                pass


def _deployer_startup_script_path() -> Path:
    """Resolve ``scripts/start_deployer.py`` for runpy (exe dir when frozen, else package layout)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "scripts" / "start_deployer.py"
    return NEXUS_ROOT / "scripts" / "start_deployer.py"


if __name__ == "__main__":
    import argparse
    import runpy

    _cli = argparse.ArgumentParser(description="Run the standalone Nexus deployer HTTP API (uvicorn).")
    _cli.add_argument("--port", type=int, default=None, help="Listen port (default: 8001 or NEXUS_DEPLOYER_PORT).")
    _cli.add_argument(
        "--host",
        default=None,
        help="Bind host (sets NEXUS_DEPLOYER_BIND_HOST; default 0.0.0.0).",
    )
    _args, _unknown = _cli.parse_known_args()
    if _args.port is not None:
        os.environ["NEXUS_DEPLOYER_PORT"] = str(_args.port)
    if _args.host:
        os.environ["NEXUS_DEPLOYER_BIND_HOST"] = _args.host
    _start_script = _deployer_startup_script_path()
    sys.argv = [str(_start_script)]
    runpy.run_path(str(_start_script), run_name="__main__")
