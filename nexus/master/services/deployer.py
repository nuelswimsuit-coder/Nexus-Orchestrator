"""
Nexus Auto-Deployer Service  (Phase 17 — Zero-Touch Cluster Update)
====================================================================

Syncs `nexus/`, `scripts/`, `requirements.txt`, `start_nexus.sh`, and
`run_worker.sh` from the Master to every worker node over SSH/SCP, installs
dependencies, then restarts the worker via `start_nexus.sh`.

Deployment sequence per node
-----------------------------
1. Resolve the target IP:
   a. If WORKER_IP is set in .env / settings, use it directly.
   b. Otherwise look up local_ip from the Redis heartbeat for that node_id.
2. Open SSH connection (paramiko) using WORKER_SSH_USER / WORKER_SSH_PASSWORD.
   Transport options: curve25519-sha256 + diffie-hellman-group14-sha256 KEX,
   StrictHostKeyChecking disabled, UserKnownHostsFile=/dev/null equivalent.
3. Kill all remote python3 processes to prevent file-locking (pkill -f python3).
4. ZIP-bundle nexus/, scripts/, and root-level files into a single archive,
   SFTP-upload it as one transfer, then run `unzip -o` on the remote.
5. Run: pip install -r requirements.txt  (inside the remote .venv)
6. Re-launch the worker via `bash start_nexus.sh` so PYTHONPATH is set.
7. Publish progress events to Redis for the dashboard SSE stream.

Progress events
---------------
Each step emits a JSON event to  nexus:deploy:progress:<node_id>  ::

    {
        "node_id": "worker_laptop_01",
        "step":    "installing_deps",     # see DeployStep
        "status":  "running",             # "running" | "done" | "error"
        "detail":  "pip install -r …",
        "ts":      "2025-01-01T00:00:00Z"
    }

Configuration (.env)
---------------------
    WORKER_SSH_USER=yadmin
    WORKER_SSH_PASSWORD=<password>
    WORKER_IP=192.168.1.42            # direct IP — no Redis heartbeat needed
    WORKER_DEPLOY_ROOT_LINUX=/home/yadmin/Desktop/Nexus-Orchestrator
    WORKER_DEPLOY_ROOT_WIN=C:\\Users\\Yarin\\Desktop\\Nexus-Orchestrator
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import structlog

from nexus.shared.deploy_preflight import (
    preflight_remote_ssh,
    print_ssh_debug_command,
)

log = structlog.get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

NEXUS_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # project root

# #region agent log
def _agent_dbg_deploy(payload: dict) -> None:
    try:
        import time as _time

        row = {"sessionId": "7f9550", "timestamp": int(_time.time() * 1000), **payload}
        with open(NEXUS_ROOT / "debug-7f9550.log", "a", encoding="utf-8") as _f:
            _f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass


# #endregion

# Directories to sync (relative to project root)
SYNC_DIRS = ["nexus", "scripts"]

# Individual root-level files to sync alongside the directories
SYNC_FILES = [
    "requirements.txt",
    "start_nexus.sh",
    "run_worker.sh",
    "pyproject.toml",
]

# Redis key prefix for progress events
PROGRESS_KEY_PREFIX = "nexus:deploy:progress:"
PROGRESS_MAX_LEN = 250

DeployStep = Literal[
    "connecting",
    "stopping_worker",
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
    "uploading":       "Syncing files…",
    "installing_deps": "Installing deps…",
    "restarting":      "Restarting worker…",
    "done":            "Worker Live ✓",
    "skipped":         "Skipped (unreachable)",
    "error":           "Error ✗",
}


# ── SSH hardening helper ───────────────────────────────────────────────────────

def _harden_ssh_transport(transport) -> None:
    """
    Apply security/compatibility options to a live paramiko Transport:
    - Prefer curve25519-sha256 and diffie-hellman-group14-sha256 KEX algorithms
      (equivalent to -o KexAlgorithms=+curve25519-sha256,diffie-hellman-group14-sha256)
    - StrictHostKeyChecking=no / UserKnownHostsFile=/dev/null are handled by
      using AutoAddPolicy on the SSHClient before connect().
    """
    try:
        preferred_kex = [
            "curve25519-sha256",
            "curve25519-sha256@libssh.org",
            "diffie-hellman-group14-sha256",
            "diffie-hellman-group14-sha1",
            "diffie-hellman-group-exchange-sha256",
        ]
        if hasattr(transport, "_preferred_kex"):
            existing = list(getattr(transport, "_preferred_kex", []))
            merged = preferred_kex + [k for k in existing if k not in preferred_kex]
            transport._preferred_kex = tuple(merged)
    except Exception:
        pass


def _configure_ssh_client(ssh) -> None:
    """
    Configure an SSHClient before connect() for maximum Linux worker compatibility.
    Equivalent to: -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
    """
    import paramiko  # type: ignore[import-untyped]
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


# ── ZIP bundle helper ──────────────────────────────────────────────────────────

_ZIP_EXCLUDE_DIRS = frozenset(
    ("__pycache__", ".venv", ".git", "node_modules", ".mypy_cache", "venv", ".next", "dist", "build")
)
_ZIP_EXCLUDE_EXTS = frozenset((".pyc", ".pyo"))


def _build_deployment_zip() -> bytes:
    """
    Bundle SYNC_DIRS + SYNC_FILES into an in-memory ZIP archive.
    Returns the raw ZIP bytes ready for SFTP upload.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for dir_name in SYNC_DIRS:
            local_dir = NEXUS_ROOT / dir_name
            if not local_dir.exists():
                continue
            for item in local_dir.rglob("*"):
                if any(part in _ZIP_EXCLUDE_DIRS for part in item.parts):
                    continue
                if item.suffix in _ZIP_EXCLUDE_EXTS:
                    continue
                if item.is_file():
                    arcname = str(item.relative_to(NEXUS_ROOT)).replace("\\", "/")
                    zf.write(str(item), arcname)

        for file_name in SYNC_FILES:
            local_file = NEXUS_ROOT / file_name
            if local_file.exists():
                zf.write(str(local_file), file_name)

    return buf.getvalue()


# ── Progress event helper ──────────────────────────────────────────────────────

def _event(
    node_id: str,
    step: DeployStep,
    status: DeployStatus,
    detail: str = "",
) -> dict:
    return {
        "node_id": node_id,
        "step": step,
        "status": status,
        "detail": detail,
        "label": STEP_LABELS.get(step, step),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


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

    # ── Public API ─────────────────────────────────────────────────────────────

    async def deploy_all(self, node_ids: list[str] | None = None) -> dict[str, str]:
        """
        Deploy to all active workers (or a specific subset).

        If WORKER_IP is set in settings, a synthetic node entry is added so
        the laptop is always reachable even when it has no Redis heartbeat.

        Returns {node_id: "ok" | "error: <reason>"}.
        """
        targets = node_ids or await self._build_target_list()
        if not targets:
            log.warning("deployer_no_targets")
            return {}

        log.info("deployer_start", targets=targets)
        results: dict[str, str] = {}

        sem = asyncio.Semaphore(3)

        async def _deploy_one(nid: str) -> None:
            async with sem:
                try:
                    results[nid] = await self._deploy_node(nid)
                except Exception as exc:
                    log.exception(
                        "deployer_node_unhandled_exception",
                        node_id=nid,
                        error=str(exc),
                    )
                    detail = f"SSH/deploy crashed for {nid!r}: {exc}"
                    try:
                        await self._emit(nid, "error", "error", detail)
                    except Exception:
                        pass
                    results[nid] = f"error: {exc}"

        await asyncio.gather(*[_deploy_one(nid) for nid in targets])
        log.info("deployer_done", results=results)
        return results

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
        ip = (self._get_setting("worker_ip") or "").strip()
        if not ip:
            detail = (
                "WORKER_IP is not set — set WORKER_IP in .env to your Linux worker "
                "IP (reachable on port 22)."
            )
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        if not os.path.exists(project_path):
            detail = f"Project path does not exist: {project_path}"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        remote_dest = remote_path or f"/home/yadmin/Desktop/{project_name}"
        ssh_user = self._get_setting("worker_ssh_user") or "yadmin"
        ssh_pass = (
            self._vault._backend.get("WORKER_SSH_PASSWORD")
            or os.environ.get("WORKER_SSH_PASSWORD")
            or self._get_setting("worker_ssh_password")
            or ""
        )
        ssh_key = (
            os.environ.get("WORKER_SSH_KEY_FILE")
            or self._get_setting("worker_ssh_key_file")
            or ""
        )

        if not ssh_pass and not ssh_key:
            detail = "WORKER_SSH_PASSWORD is not set in .env or Vault"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        try:
            import paramiko  # type: ignore[import-untyped]
        except ImportError:
            detail = "paramiko not installed — run: pip install paramiko"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        await self.clear_progress(node_id)
        ssh = paramiko.SSHClient()
        # StrictHostKeyChecking=no / UserKnownHostsFile=/dev/null equivalent
        _configure_ssh_client(ssh)

        try:
            # ── 1. Connect ────────────────────────────────────────────────────
            await self._emit(node_id, "connecting", "running",
                             f"SSH → {ssh_user}@{ip}")
            _loop_sp = asyncio.get_event_loop()
            _pf_sp = await _loop_sp.run_in_executor(None, preflight_remote_ssh, ip)
            if _pf_sp:
                await self._emit(
                    node_id,
                    "skipped",
                    "done",
                    f"[SKIPPED] {_pf_sp}",
                )
                log.warning(
                    "deployer_project_sync_skipped_preflight",
                    node_id=node_id,
                    detail=_pf_sp[:500],
                )
                return f"skipped: {_pf_sp}"
            print_ssh_debug_command(ssh_user, ip)
            _connect_kwargs: dict = dict(hostname=ip, username=ssh_user, timeout=15, banner_timeout=15)
            if ssh_pass:
                _connect_kwargs["password"] = ssh_pass
            if ssh_key and os.path.isfile(ssh_key):
                _connect_kwargs["key_filename"] = ssh_key
            await _loop_sp.run_in_executor(
                None,
                lambda: ssh.connect(**_connect_kwargs),
            )
            _t = ssh.get_transport()
            # Apply KexAlgorithms preference after transport is established
            _harden_ssh_transport(_t)
            _t.set_keepalive(30)
            if _t.sock:
                try: _t.sock.settimeout(60)
                except Exception: pass
            await self._emit(node_id, "connecting", "done",
                             f"Connected to {ip}")

            # ── 2. Sync project files ─────────────────────────────────────────
            await self._emit(node_id, "uploading", "running",
                             f"Syncing {project_name} → {remote_dest}")
            
            file_count = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._sync_single_project(ssh, project_path, remote_dest, project_name),
            )
            
            await self._emit(node_id, "uploading", "done",
                             f"{file_count} files synced to {remote_dest}")

            await self._emit(node_id, "done", "done",
                             f"Project '{project_name}' deployed — ready on worker")
            return "ok"

        except Exception as exc:
            detail = str(exc)
            log.exception("deployer_project_sync_error", 
                         project=project_name, node_id=node_id, error=detail)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"
        finally:
            ssh.close()

    def _sync_single_project(
        self,
        ssh,
        local_project_path: str,
        remote_dest: str,
        project_name: str,
    ) -> int:
        """
        SFTP-upload an entire desktop project to the worker.
        Returns the number of files transferred.
        """
        from pathlib import Path

        sftp = ssh.open_sftp()
        sftp.get_channel().settimeout(120)
        local_path = Path(local_project_path)
        file_count = 0

        try:
            _sftp_mkdir_p(sftp, remote_dest)

            # Recursive upload with exclusions
            for item in local_path.rglob("*"):
                # Skip large/unnecessary directories
                if any(skip in item.parts for skip in 
                      ("node_modules", ".venv", ".git", "__pycache__", 
                       ".mypy_cache", "venv", "vendor", ".next", "dist", "build")):
                    continue
                
                if item.is_file():
                    try:
                        # Calculate relative path
                        rel_path = item.relative_to(local_path)
                        remote_file_path = f"{remote_dest}/{str(rel_path).replace(os.sep, '/')}"
                        
                        # Ensure parent directory exists
                        remote_parent = "/".join(remote_file_path.split("/")[:-1])
                        _sftp_mkdir_p(sftp, remote_parent)
                        
                        # Upload the file
                        sftp.put(str(item), remote_file_path)
                        file_count += 1
                        
                        # Log progress every 50 files
                        if file_count % 50 == 0:
                            log.debug("project_sync_progress", 
                                     project=project_name, files_done=file_count)
                                     
                    except Exception as exc:
                        log.debug("project_sync_file_error", 
                                 file=str(item), error=str(exc))

        finally:
            sftp.close()

        log.info("project_sync_complete", 
                 project=project_name, files_transferred=file_count,
                 remote_dest=remote_dest)
        return file_count

    async def sync_to_worker(self) -> str:
        """
        Phase 19 — Nexus-Push direct sync.

        Connects to WORKER_IP via paramiko, uploads nexus/, scripts/,
        and requirements.txt via SFTP (emitting per-file progress events),
        then executes the exact self-healing command:

            bash -c 'cd /home/yadmin/Desktop/Nexus-Orchestrator/scripts
              && source .venv/bin/activate
              && pip install -r ../requirements.txt
              && pkill -f start_worker.py || true
              && nohup python3 start_worker.py > worker.log 2>&1 &'

        Every step emits JSON events to nexus:deploy:progress:worker_linux
        so the dashboard terminal streams them live.

        Returns "ok" or "error: <reason>".
        """
        node_id = "worker_linux"
        ip = (self._get_setting("worker_ip") or "").strip()
        if not ip:
            detail = (
                "WORKER_IP is not set — set WORKER_IP in .env to your Linux worker "
                "IP (reachable on port 22)."
            )
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

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
        ssh_key = (
            os.environ.get("WORKER_SSH_KEY_FILE")
            or self._get_setting("worker_ssh_key_file")
            or ""
        )

        # #region agent log
        _st_ip = ""
        if self._settings is not None:
            _st_ip = str(getattr(self._settings, "worker_ip", "") or "")
        _agent_dbg_deploy(
            {
                "location": "deployer.py:sync_to_worker:pre_tcp_probe",
                "message": "nexus-push target resolved before TCP probe",
                "hypothesisId": "H2",
                "runId": "pre-fix",
                "data": {
                    "connect_ip": ip,
                    "env_WORKER_IP": (os.environ.get("WORKER_IP") or "").strip(),
                    "settings_worker_ip": _st_ip.strip(),
                    "ssh_user": ssh_user,
                    "platform": sys.platform,
                    "auth_password_set": bool(ssh_pass),
                    "auth_key_configured": bool(ssh_key and os.path.isfile(ssh_key)),
                },
            }
        )
        # #endregion

        if not ssh_pass and not ssh_key:
            detail = "WORKER_SSH_PASSWORD is not set in .env or Vault"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        try:
            import paramiko  # type: ignore[import-untyped]
        except ImportError:
            detail = "paramiko not installed — run: pip install paramiko"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        await self.clear_progress(node_id)
        ssh = paramiko.SSHClient()
        # StrictHostKeyChecking=no / UserKnownHostsFile=/dev/null equivalent
        _configure_ssh_client(ssh)

        loop = asyncio.get_event_loop()

        try:
            # ── 1. Connect ────────────────────────────────────────────────────
            await self._emit(node_id, "connecting", "running",
                             f"SSH → {ssh_user}@{ip}")
            _ckw2: dict = dict(hostname=ip, username=ssh_user, timeout=15, banner_timeout=15)
            if ssh_pass:
                _ckw2["password"] = ssh_pass
            if ssh_key and os.path.isfile(ssh_key):
                _ckw2["key_filename"] = ssh_key

            _pf_sw = await loop.run_in_executor(None, preflight_remote_ssh, ip)
            # #region agent log
            _agent_dbg_deploy(
                {
                    "location": "deployer.py:sync_to_worker:post_preflight",
                    "message": "ICMP+TCP preflight finished",
                    "hypothesisId": "H1,H3,H4",
                    "runId": "pre-fix",
                    "data": {
                        "connect_ip": ip,
                        "preflight_ok": _pf_sw is None,
                        "preflight_err": (str(_pf_sw or "")[:320]),
                    },
                }
            )
            # #endregion
            if _pf_sw:
                await self._emit(
                    node_id,
                    "skipped",
                    "done",
                    f"[SKIPPED] {_pf_sw}",
                )
                log.warning(
                    "deployer_sync_skipped_preflight",
                    node_id=node_id,
                    detail=_pf_sw[:500],
                )
                return f"skipped: {_pf_sw}"

            print_ssh_debug_command(ssh_user, ip)

            await loop.run_in_executor(
                None,
                lambda: ssh.connect(**_ckw2),
            )
            _t2 = ssh.get_transport()
            # Apply KexAlgorithms preference after transport is established
            _harden_ssh_transport(_t2)
            _t2.set_keepalive(30)
            if _t2.sock:
                try: _t2.sock.settimeout(60)
                except Exception: pass
            await self._emit(node_id, "connecting", "done",
                             f"Connected to {ip}")

            # ── 2. Kill all python3 to prevent file-locking ───────────────────
            await self._emit(node_id, "stopping_worker", "running",
                             "pkill -f python3 to clear file locks")
            await loop.run_in_executor(
                None,
                lambda: self._stop_worker(ssh, "Linux", remote_root),
            )
            await self._emit(node_id, "stopping_worker", "done", "Processes cleared")

            # ── 3. Build ZIP bundle and upload as single transfer ─────────────
            await self._emit(node_id, "uploading", "running",
                             "Building ZIP bundle…")
            zip_bytes = await loop.run_in_executor(None, _build_deployment_zip)
            zip_size_kb = len(zip_bytes) // 1024
            await self._emit(node_id, "uploading", "running",
                             f"Uploading {zip_size_kb} KB ZIP → {remote_root}")

            try:
                await loop.run_in_executor(
                    None,
                    lambda: self._upload_zip_and_extract(ssh, remote_root, "Linux", zip_bytes),
                )
            except Exception as exc:
                upload_err = str(exc)
                await self._emit(node_id, "error", "error", upload_err)
                return f"error: {upload_err}"

            await self._emit(node_id, "uploading", "done",
                             f"ZIP extracted to {remote_root}")

            # ── 3. Self-healing command ───────────────────────────────────────
            # Use bash -c so `source` works in the non-interactive exec_command shell
            heal_cmd = (
                f"bash -c 'cd {remote_root}/scripts"
                f" && source .venv/bin/activate"
                f" && pip install -r ../requirements.txt"
                f" && pkill -f start_worker.py || true"
                f" && nohup python3 start_worker.py > worker.log 2>&1 &'"
            )

            await self._emit(node_id, "installing_deps", "running",
                             "pip install -r requirements.txt")
            exit_code, stdout_tail = await loop.run_in_executor(
                None,
                lambda: self._run_heal(ssh, heal_cmd),
            )
            if exit_code != 0:
                pip_msg = f"pip install exited {exit_code} — {stdout_tail[-200:]}"
                await self._emit(node_id, "installing_deps", "error", pip_msg)
                await self._emit(node_id, "error", "error", pip_msg)
                return f"error: {pip_msg}"

            await self._emit(node_id, "installing_deps", "done",
                             "Dependencies installed")

            await self._emit(node_id, "restarting", "done",
                             "start_worker.py launched in background")
            await self._emit(node_id, "done", "done",
                             "Deployment complete — Worker Live ✓")
            return "ok"

        except Exception as exc:
            detail = str(exc)
            log.exception("deployer_sync_error", node_id=node_id, error=detail)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"
        finally:
            ssh.close()

    def _upload_with_progress(
        self,
        ssh,
        remote_root: str,
        progress_cb: "Callable[[str, int, int], None]",
    ) -> None:
        """
        Upload nexus/, scripts/, and requirements.txt via SFTP.
        Calls progress_cb(filename, files_done_so_far, total_files) after each file.
        """
        from typing import Callable  # local import to avoid circular

        # Set socket-level timeout so dead TCP connections are detected quickly
        transport = ssh.get_transport()
        if transport and transport.sock:
            try:
                transport.sock.settimeout(60)
            except Exception:
                pass

        sftp = ssh.open_sftp()
        sftp.get_channel().settimeout(60)
        try:
            _sftp_mkdir_p(sftp, remote_root)

            # Collect all (local_path, remote_path) pairs up front
            pairs: list[tuple[Path, str]] = []

            for dir_name in SYNC_DIRS:
                local_dir = NEXUS_ROOT / dir_name
                if local_dir.exists():
                    remote_dir = f"{remote_root}/{dir_name}"
                    _collect_files(local_dir, remote_dir, pairs)

            for file_name in SYNC_FILES:
                local_file = NEXUS_ROOT / file_name
                if local_file.exists():
                    pairs.append((local_file, f"{remote_root}/{file_name}"))

            total = len(pairs)
            for idx, (local_path, remote_path) in enumerate(pairs, start=1):
                remote_parent = "/".join(remote_path.split("/")[:-1])
                _sftp_mkdir_p(sftp, remote_parent)
                try:
                    sftp.put(str(local_path), remote_path)
                except Exception as exc:
                    log.warning("sftp_put_error",
                                local=str(local_path), remote=remote_path,
                                error=str(exc))
                    if not (transport and transport.is_active()):
                        raise
                rel = str(local_path.relative_to(NEXUS_ROOT))
                progress_cb(rel, idx, total)

            # Make shell scripts executable
            for sh in ("start_nexus.sh", "run_worker.sh"):
                try:
                    sftp.chmod(f"{remote_root}/{sh}", 0o755)
                except Exception:
                    pass
        finally:
            sftp.close()

    def _run_heal(self, ssh, cmd: str) -> tuple[int, str]:
        """
        Execute the self-healing command and wait for it to complete.
        Returns (exit_code, combined_output_tail).
        """
        _, stdout, stderr = ssh.exec_command(cmd, timeout=300)
        stdout.channel.settimeout(300)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        exit_code = stdout.channel.recv_exit_status()
        combined = (out + err)[-1200:]
        log.info("deployer_heal_output", combined=combined, exit_code=exit_code)
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

        # Static WORKER_IP entry — always included when configured
        worker_ip = (self._get_setting("worker_ip") or "").strip()
        if worker_ip:
            targets.append("worker_linux")  # canonical ID for the static laptop

        # Redis-discovered workers
        redis_workers = await self._discover_worker_nodes()
        for nid in redis_workers:
            if nid not in targets:
                targets.append(nid)

        return targets

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
        1. If node_id == "worker_linux" and WORKER_IP is set → use it directly.
        2. Otherwise look up local_ip from the Redis heartbeat.
        """
        worker_ip = (self._get_setting("worker_ip") or "").strip()
        if node_id == "worker_linux" and worker_ip:
            return worker_ip

        key = f"nexus:heartbeat:{node_id}"
        raw = await self._redis.get(key)
        if not raw:
            # Last resort: if only one static IP is configured, use it
            return worker_ip or None
        try:
            hb = json.loads(raw)
            return hb.get("local_ip")
        except Exception:
            return None

    # ── Per-node deployment ────────────────────────────────────────────────────

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
        ssh_key = (
            os.environ.get("WORKER_SSH_KEY_FILE")
            or self._get_setting("worker_ssh_key_file")
            or ""
        )

        if not ssh_pass and not ssh_key:
            detail = "WORKER_SSH_PASSWORD is not set in .env or Vault"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        try:
            import paramiko  # type: ignore[import-untyped]
        except ImportError:
            detail = "paramiko not installed — run: pip install paramiko"
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"

        ssh = paramiko.SSHClient()
        # StrictHostKeyChecking=no / UserKnownHostsFile=/dev/null equivalent
        _configure_ssh_client(ssh)

        try:
            # ── 1. Connect ────────────────────────────────────────────────────
            await self._emit(node_id, "connecting", "running", f"SSH → {ssh_user}@{ip}")
            _loop_dn = asyncio.get_event_loop()
            _pf_dn = await _loop_dn.run_in_executor(None, preflight_remote_ssh, ip)
            if _pf_dn:
                await self._emit(
                    node_id,
                    "skipped",
                    "done",
                    f"[SKIPPED] {_pf_dn}",
                )
                log.warning(
                    "deployer_deploy_node_skipped_preflight",
                    node_id=node_id,
                    detail=_pf_dn[:500],
                )
                return f"skipped: {_pf_dn}"
            print_ssh_debug_command(ssh_user, ip)
            _ckw3: dict = dict(hostname=ip, username=ssh_user, timeout=15, banner_timeout=15)
            if ssh_pass:
                _ckw3["password"] = ssh_pass
            if ssh_key and os.path.isfile(ssh_key):
                _ckw3["key_filename"] = ssh_key
            await _loop_dn.run_in_executor(
                None,
                lambda: ssh.connect(**_ckw3),
            )
            _t3 = ssh.get_transport()
            # Apply KexAlgorithms preference after transport is established
            _harden_ssh_transport(_t3)
            _t3.set_keepalive(30)
            if _t3.sock:
                try: _t3.sock.settimeout(60)
                except Exception: pass
            await self._emit(node_id, "connecting", "done", f"Connected to {ip}")

            # ── 2. Detect remote OS and resolve destination path ───────────────
            remote_os, remote_root, venv_python = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._detect_remote_env(ssh, ssh_user)
            )
            log.info("deployer_remote_env", node_id=node_id, remote_os=remote_os, remote_root=remote_root)

            # ── 3. Stop worker — kill all python3 to prevent file-locking ─────
            await self._emit(node_id, "stopping_worker", "running",
                             "pkill -f python3 + SIGTERM worker")
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._stop_worker(ssh, remote_os, remote_root)
            )
            await self._emit(node_id, "stopping_worker", "done", "Worker stopped")

            # ── 4. ZIP-based upload + remote unzip ────────────────────────────
            await self._emit(node_id, "uploading", "running",
                             f"Building ZIP bundle → {remote_root}")
            zip_bytes = await asyncio.get_event_loop().run_in_executor(
                None, _build_deployment_zip
            )
            zip_size_kb = len(zip_bytes) // 1024
            await self._emit(node_id, "uploading", "running",
                             f"Uploading {zip_size_kb} KB ZIP → {remote_root}")
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._upload_zip_and_extract(ssh, remote_root, remote_os, zip_bytes)
            )
            await self._emit(node_id, "uploading", "done", "Files synced via ZIP")

            # ── 5. Install dependencies ───────────────────────────────────────
            await self._emit(node_id, "installing_deps", "running",
                             "pip install -r requirements.txt")
            deps_ok = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._install_deps(ssh, remote_root, venv_python, remote_os)
            )
            if not deps_ok:
                await self._emit(node_id, "installing_deps", "error",
                                 "pip install had errors — check worker.log")
            else:
                await self._emit(node_id, "installing_deps", "done",
                                 "Dependencies installed")

            # ── 6. Restart worker via start_nexus.sh ──────────────────────────
            await self._emit(node_id, "restarting", "running", "bash start_nexus.sh")
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._restart_worker(ssh, remote_root, venv_python, remote_os)
            )
            await self._emit(node_id, "restarting", "done", "Worker restarted")
            await self._emit(node_id, "done", "done", "Deployment complete — Worker Live")
            return "ok"

        except Exception as exc:
            detail = str(exc)
            log.exception("deployer_node_error", node_id=node_id, error=detail)
            await self._emit(node_id, "error", "error", detail)
            return f"error: {detail}"
        finally:
            ssh.close()

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
        """
        Kill all python3 processes to prevent file-locking, then gracefully
        stop the worker via PID file / pkill.
        """
        if remote_os == "Linux":
            pid_file = f"{remote_root}/worker.pid"
            cmd = (
                # Broad kill — prevents .pyc / .py file-locking during unzip
                "pkill -f python3 2>/dev/null || true; "
                "sleep 1; "
                # Targeted SIGTERM via PID file
                f"if [ -f {pid_file} ]; then "
                f"  kill -SIGTERM $(cat {pid_file}) 2>/dev/null || true; "
                f"  sleep 2; "
                f"  kill -SIGKILL $(cat {pid_file}) 2>/dev/null || true; "
                f"  rm -f {pid_file}; "
                f"fi; "
                "pkill -SIGTERM -f 'start_worker.py' 2>/dev/null || true; "
                "sleep 1"
            )
            _, stdout, _ = ssh.exec_command(cmd)
            stdout.channel.recv_exit_status()
        else:
            ssh.exec_command(
                'taskkill /F /FI "WINDOWTITLE eq nexus-worker*" 2>nul & '
                'taskkill /F /IM python.exe /FI "WINDOWTITLE eq nexus*" 2>nul & '
                'timeout /t 2 /nobreak >nul'
            )

    def _upload_dirs(self, ssh, remote_root: str, remote_os: str) -> None:
        """SFTP-upload nexus/, scripts/, and root-level files."""
        sftp = ssh.open_sftp()
        sftp.get_channel().settimeout(120)
        try:
            sep = "/" if remote_os == "Linux" else "\\"

            # Ensure the destination root exists
            _sftp_mkdir_p(sftp, remote_root)

            # Upload directories
            for dir_name in SYNC_DIRS:
                local_dir = NEXUS_ROOT / dir_name
                if local_dir.exists():
                    remote_dir = f"{remote_root}{sep}{dir_name}"
                    _sftp_put_dir(sftp, local_dir, remote_dir, remote_os)

            # Upload individual root-level files
            for file_name in SYNC_FILES:
                local_file = NEXUS_ROOT / file_name
                if not local_file.exists():
                    log.debug("deployer_skip_missing_file", file=file_name)
                    continue
                remote_path = f"{remote_root}{sep}{file_name}"
                try:
                    sftp.put(str(local_file), remote_path)
                    log.debug("deployer_file_uploaded", file=file_name)
                except Exception as exc:
                    log.warning("deployer_file_upload_error", file=file_name, error=str(exc))

            # Make shell scripts executable on Linux
            if remote_os == "Linux":
                for sh in ("start_nexus.sh", "run_worker.sh"):
                    try:
                        sftp.chmod(f"{remote_root}/{sh}", 0o755)
                    except Exception:
                        pass
        finally:
            sftp.close()

    def _upload_zip_and_extract(
        self,
        ssh,
        remote_root: str,
        remote_os: str,
        zip_bytes: bytes,
    ) -> None:
        """
        Upload the deployment ZIP as a single SFTP transfer, then extract it
        on the remote with `unzip -o` (overwrite without prompting).

        This replaces the old file-by-file SFTP approach and avoids partial-
        upload failures caused by per-file locking on Windows remotes.
        """
        remote_zip = f"{remote_root}/_nexus_deploy.zip"

        sftp = ssh.open_sftp()
        sftp.get_channel().settimeout(120)
        try:
            _sftp_mkdir_p(sftp, remote_root)
            import io as _io
            sftp.putfo(_io.BytesIO(zip_bytes), remote_zip)
        finally:
            sftp.close()

        if remote_os == "Linux":
            # unzip -o: overwrite existing files without prompting
            unzip_cmd = (
                f"cd {remote_root} && "
                f"unzip -o {remote_zip} && "
                f"rm -f {remote_zip}"
            )
        else:
            # Windows: use PowerShell Expand-Archive
            unzip_cmd = (
                f'powershell -Command "'
                f'Expand-Archive -Force -Path \\"{remote_zip}\\" '
                f'-DestinationPath \\"{remote_root}\\"; '
                f'Remove-Item -Force \\"{remote_zip}\\"'
                f'"'
            )

        _, stdout, stderr = ssh.exec_command(unzip_cmd, timeout=120)
        stdout.channel.settimeout(120)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            raise RuntimeError(
                f"Remote unzip failed (exit {exit_code}): {(out + err)[-500:]}"
            )
        log.info("deployer_zip_extracted", remote_root=remote_root, exit_code=exit_code)

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
          2. cd <root>/scripts && source ../.venv/bin/activate
          3. pip install --upgrade pip
          4. pip install -r ../requirements.txt
          5. Retry with --no-cache-dir on failure.

        Returns True on success.
        """
        if remote_os == "Linux":
            venv_dir = f"{remote_root}/.venv"
            scripts_dir = f"{remote_root}/scripts"
            cmd = (
                # 1. Create venv if absent
                f"if [ ! -f {venv_dir}/bin/python ]; then "
                f"  python3 -m venv {venv_dir}; "
                f"fi && "
                # 2. cd into scripts/, activate, upgrade pip, install deps
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
            worker_script = f"{remote_root}{sep}scripts{sep}start_worker.py"
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
    ) -> None:
        event = _event(node_id, step, status, detail)
        key = f"{PROGRESS_KEY_PREFIX}{node_id}"
        await self._redis.rpush(key, json.dumps(event))
        await self._redis.ltrim(key, -PROGRESS_MAX_LEN, -1)
        await self._redis.expire(key, 3600)
        log.debug("deployer_progress", **event)

    # ── Settings helper ────────────────────────────────────────────────────────

    def _get_setting(self, key: str) -> str:
        """Read a value from the injected settings object, or fall back to env."""
        if self._settings is not None:
            return getattr(self._settings, key, "") or ""
        return os.environ.get(key.upper(), "")


# ── File collection helpers ────────────────────────────────────────────────────

def _collect_files(
    local_dir: Path,
    remote_dir: str,
    pairs: "list[tuple[Path, str]]",
) -> None:
    """
    Recursively collect (local_path, remote_path) pairs from local_dir.
    Skips __pycache__, .venv, .git, node_modules, and compiled bytecode.
    """
    for item in local_dir.iterdir():
        if item.name in ("__pycache__", ".venv", ".git", "node_modules", ".mypy_cache"):
            continue
        if item.suffix in (".pyc", ".pyo"):
            continue
        remote_path = f"{remote_dir}/{item.name}"
        if item.is_dir():
            _collect_files(item, remote_path, pairs)
        else:
            pairs.append((item, remote_path))


def _count_sync_files() -> int:
    """Count the total number of files that will be uploaded."""
    pairs: list[tuple[Path, str]] = []
    for dir_name in SYNC_DIRS:
        local_dir = NEXUS_ROOT / dir_name
        if local_dir.exists():
            _collect_files(local_dir, f"/{dir_name}", pairs)
    for file_name in SYNC_FILES:
        if (NEXUS_ROOT / file_name).exists():
            pairs.append((NEXUS_ROOT / file_name, file_name))
    return len(pairs)


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


def _check_rsync_available() -> bool:
    """Return True if rsync and sshpass are both available on this machine."""
    import shutil
    return shutil.which("rsync") is not None and shutil.which("sshpass") is not None


def _rsync_upload(
    ip: str,
    ssh_user: str,
    ssh_pass: str,
    ssh_key: str,
    remote_root: str,
    progress_cb: "Callable[[str, int, int], None]",
) -> str:
    """
    Use rsync over SSH to sync nexus/ and scripts/ to the worker.
    Returns "ok" on success, or an error string.
    rsync is resumable, delta-syncs only changed files, and respects OS timeouts.
    """
    import subprocess
    from typing import Callable

    sync_sources = []
    for dir_name in SYNC_DIRS:
        local_dir = NEXUS_ROOT / dir_name
        if local_dir.exists():
            sync_sources.append(str(local_dir) + "/")

    # Count total files for progress reporting
    total = _count_sync_files()
    done = 0

    ssh_opts = (
        "ssh -o StrictHostKeyChecking=no "
        "-o ConnectTimeout=15 "
        "-o ServerAliveInterval=10 "
        "-o ServerAliveCountMax=3 "
        "-o BatchMode=no"
    )
    if ssh_key and os.path.isfile(ssh_key):
        ssh_opts += f" -i {ssh_key}"

    for dir_name in SYNC_DIRS:
        local_dir = NEXUS_ROOT / dir_name
        if not local_dir.exists():
            continue

        remote_dir = f"{ssh_user}@{ip}:{remote_root}/{dir_name}"

        if ssh_pass:
            cmd = [
                "sshpass", f"-p{ssh_pass}",
                "rsync", "-az", "--delete",
                "--exclude=__pycache__", "--exclude=*.pyc", "--exclude=.git",
                "--exclude=.venv", "--exclude=node_modules",
                "-e", ssh_opts,
                str(local_dir) + "/",
                remote_dir,
            ]
        else:
            cmd = [
                "rsync", "-az", "--delete",
                "--exclude=__pycache__", "--exclude=*.pyc", "--exclude=.git",
                "--exclude=.venv", "--exclude=node_modules",
                "-e", ssh_opts,
                str(local_dir) + "/",
                remote_dir,
            ]

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300
            )
            if result.returncode != 0:
                return f"rsync failed (rc={result.returncode}): {result.stderr[-500:]}"
        except subprocess.TimeoutExpired:
            return f"rsync timed out syncing {dir_name}/"
        except Exception as exc:
            return f"rsync error: {exc}"

        done += 1
        progress_cb(f"{dir_name}/", done, max(total, 2))

    # Sync individual root files
    for file_name in SYNC_FILES:
        local_file = NEXUS_ROOT / file_name
        if not local_file.exists():
            continue
        remote_path = f"{ssh_user}@{ip}:{remote_root}/{file_name}"
        if ssh_pass:
            cmd = ["sshpass", f"-p{ssh_pass}", "rsync", "-az", "-e", ssh_opts,
                   str(local_file), remote_path]
        else:
            cmd = ["rsync", "-az", "-e", ssh_opts, str(local_file), remote_path]
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except Exception:
            pass

    progress_cb("done", total, total)
    return "ok"
