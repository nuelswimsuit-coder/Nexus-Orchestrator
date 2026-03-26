"""
nexus.scale_worker — Worker scaling task.

Dispatched by the Decision Engine when it determines that additional worker
capacity is needed.  The handler attempts to SSH-restart the remote worker
or signals the master to spawn an additional worker process.

Registered task type: "nexus.scale_worker"
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import structlog

from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)


@registry.register("nexus.scale_worker")
async def scale_worker(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Scale out worker capacity.

    Parameters
    ----------
    action : str
        "restart" | "spawn" | "status" (default: "restart")
    worker_ip : str
        Override the target worker IP (defaults to WORKER_IP env var).
    reason : str
        Human-readable reason for the scale action (for logging).

    Returns
    -------
    dict with keys: success, action, worker_ip, message
    """
    action    = parameters.get("action", "restart")
    worker_ip = parameters.get("worker_ip") or os.getenv("WORKER_IP", "")
    reason    = parameters.get("reason", "Decision engine scale-out")

    log.info(
        "scale_worker_task_started",
        action=action,
        worker_ip=worker_ip,
        reason=reason,
    )

    if action == "status":
        return {
            "success":   True,
            "action":    "status",
            "worker_ip": worker_ip,
            "message":   f"Worker status check requested for {worker_ip or 'local'}",
        }

    if action in ("restart", "spawn"):
        if not worker_ip:
            log.warning("scale_worker_no_ip", reason="WORKER_IP not configured")
            return {
                "success":   False,
                "action":    action,
                "worker_ip": "",
                "message":   "WORKER_IP not configured — cannot scale remotely",
            }

        ssh_user     = os.getenv("WORKER_SSH_USER", "yadmin")
        ssh_password = os.getenv("WORKER_SSH_PASSWORD", "")
        deploy_root  = os.getenv(
            "WORKER_REMOTE_PATH",
            "/home/yadmin/Desktop/Nexus-Orchestrator",
        )

        remote_cmd = (
            f"cd {deploy_root} && "
            "nohup python scripts/start_worker.py > /tmp/nexus_worker.log 2>&1 &"
        )

        if ssh_password:
            cmd = [
                "sshpass", "-p", ssh_password,
                "ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                f"{ssh_user}@{worker_ip}",
                remote_cmd,
            ]
        else:
            cmd = [
                "ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                f"{ssh_user}@{worker_ip}",
                remote_cmd,
            ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

            if proc.returncode == 0:
                log.info(
                    "scale_worker_ssh_success",
                    worker_ip=worker_ip,
                    action=action,
                )
                return {
                    "success":   True,
                    "action":    action,
                    "worker_ip": worker_ip,
                    "message":   f"Worker {action} succeeded on {worker_ip}",
                }
            else:
                err = stderr.decode(errors="replace")[:300]
                log.error(
                    "scale_worker_ssh_failed",
                    worker_ip=worker_ip,
                    returncode=proc.returncode,
                    stderr=err,
                )
                return {
                    "success":   False,
                    "action":    action,
                    "worker_ip": worker_ip,
                    "message":   f"SSH {action} failed (exit {proc.returncode}): {err}",
                }

        except asyncio.TimeoutError:
            log.error("scale_worker_ssh_timeout", worker_ip=worker_ip)
            return {
                "success":   False,
                "action":    action,
                "worker_ip": worker_ip,
                "message":   f"SSH connection to {worker_ip} timed out",
            }
        except FileNotFoundError:
            log.error("scale_worker_ssh_not_found")
            return {
                "success":   False,
                "action":    action,
                "worker_ip": worker_ip,
                "message":   "ssh/sshpass not found — install openssh-client",
            }
        except Exception as exc:
            log.error("scale_worker_error", error=str(exc))
            return {
                "success":   False,
                "action":    action,
                "worker_ip": worker_ip,
                "message":   f"Unexpected error: {exc}",
            }

    return {
        "success":   False,
        "action":    action,
        "worker_ip": worker_ip,
        "message":   f"Unknown action: {action}",
    }
