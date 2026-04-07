"""
Deploy router — Phase 17: Zero-Touch Cluster Update

Endpoints
---------
POST /api/deploy/cluster
    Trigger a rolling deployment to all active worker nodes (or a subset).
    Also targets the static WORKER_IP laptop even without a Redis heartbeat.
    Returns immediately with a job_id; work runs in the background.

GET  /api/deploy/progress/{node_id}
    Server-Sent Events (SSE) stream of deployment progress for a single node.
    Each event carries: node_id, step, status, detail, label, ts.

GET  /api/deploy/status
    Snapshot of the latest progress event for every node in the last deployment.

DELETE /api/deploy/progress/{node_id}
    Clear the progress buffer for a node (housekeeping).
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from nexus.api.dependencies import RedisDep
from nexus.master.services.deployer import DeployerService
from nexus.master.services.vault import Vault
from nexus.shared.config import settings

log = structlog.get_logger(__name__)

# #region agent log
_AGENT_DEBUG_LOG = Path(__file__).resolve().parents[3] / "debug-9dd305.log"


def _agent_dbg(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    try:
        line = json.dumps(
            {
                "sessionId": "9dd305",
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data,
                "timestamp": int(time.time() * 1000),
            },
            default=str,
        )
        with _AGENT_DEBUG_LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# #endregion

router = APIRouter(prefix="/deploy", tags=["deploy"])

# ── Schemas ────────────────────────────────────────────────────────────────────

class DeployRequest(BaseModel):
    node_ids: list[str] | None = Field(
        default=None,
        description="Specific node IDs to deploy to. Omit to deploy to all active workers.",
    )


class DeployResponse(BaseModel):
    job_id: str
    targets: list[str] | None
    message: str
    started_at: str


class DeployProgressEvent(BaseModel):
    node_id: str
    step: str
    status: str    # "running" | "done" | "error"
    detail: str
    label: str = ""
    ts: str


class DeployStatusResponse(BaseModel):
    nodes: dict[str, DeployProgressEvent | None]
    queried_at: str


# ── Active deploy tracking ─────────────────────────────────────────────────────

_active_deploys: dict[str, asyncio.Task] = {}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_deployer(redis) -> DeployerService:  # type: ignore[type-arg]
    """Build a DeployerService with credentials seeded from settings."""
    vault = Vault()
    if settings.worker_ssh_user:
        vault._backend.set("WORKER_SSH_USER", settings.worker_ssh_user)
    if settings.worker_ssh_password:
        vault._backend.set("WORKER_SSH_PASSWORD", settings.worker_ssh_password)
    return DeployerService(redis=redis, vault=vault, settings=settings)


async def _run_deploy(redis, node_ids: list[str] | None, job_id: str) -> None:
    deployer = _make_deployer(redis)
    try:
        results = await deployer.deploy_all(node_ids=node_ids)
        hard_failures = {k: v for k, v in results.items() if str(v).startswith("error:")}
        log.info(
            "deploy_job_complete",
            job_id=job_id,
            results=results,
            overall_pipeline_ok=len(hard_failures) == 0,
            failed_nodes=list(hard_failures.keys()),
        )
        if hard_failures:
            log.warning(
                "deploy_job_finished_with_errors",
                job_id=job_id,
                failed_nodes=list(hard_failures.keys()),
            )
    except Exception as exc:
        log.exception("deploy_job_error", job_id=job_id, error=str(exc))
    finally:
        _active_deploys.pop(job_id, None)


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.post(
    "/cluster",
    response_model=None,
    summary="SYNC & RESTART CLUSTER — push code to all workers",
)
async def trigger_deploy(
    body: DeployRequest,
    request: Request,
    redis: RedisDep,
) -> DeployResponse | JSONResponse:
    """
    Start a background deployment job.  Returns immediately with a job_id.

    Stream live progress via GET /api/deploy/progress/{node_id} (SSE).
    Poll a snapshot via GET /api/deploy/status.

    Targets:
    - All node_ids listed in body.node_ids (if provided).
    - Otherwise: all Redis-heartbeat workers + the static WORKER_IP laptop.
    """
    try:
        job_id = f"deploy_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

        # Cancel any stale deploy task that is no longer running
        stale = [jid for jid, t in _active_deploys.items() if t.done()]
        for jid in stale:
            _active_deploys.pop(jid, None)

        if _active_deploys:
            raise HTTPException(
                status_code=409,
                detail="A deployment is already in progress. Wait for it to finish.",
            )

        task = asyncio.create_task(_run_deploy(redis, body.node_ids, job_id))
        _active_deploys[job_id] = task

        log.info("deploy_triggered", job_id=job_id, targets=body.node_ids)

        return DeployResponse(
            job_id=job_id,
            targets=body.node_ids,
            message="Deployment started — stream via /api/deploy/progress/{node_id}",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        log.exception("deploy_cluster_route_failed", error=str(e))
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)},
        )


@router.get(
    "/progress/{node_id}",
    summary="SSE stream of deployment progress for a node",
    response_class=StreamingResponse,
    response_model=None,
)
async def stream_progress(
    node_id: str,
    redis: RedisDep,
) -> StreamingResponse | JSONResponse:
    """
    Server-Sent Events stream.  Emits one `data: <json>\\n\\n` per progress
    event.  Closes automatically when the `done` or `error` step is received,
    or after 5 minutes of inactivity.
    """

    async def _generator() -> AsyncGenerator[str, None]:
        try:
            key = f"nexus:deploy:progress:{node_id}"
            cursor = 0
            idle_ticks = 0
            max_idle = 300  # 5 min × 1 s ticks

            while idle_ticks < max_idle:
                # Read any new events appended since last poll
                events = await redis.lrange(key, cursor, -1)
                if events:
                    for raw in events:
                        cursor += 1
                        idle_ticks = 0
                        yield f"data: {raw}\n\n"
                        # Stop streaming once terminal step is reached
                        try:
                            ev = json.loads(raw)
                            # Close on any error status (e.g. installing_deps + error) or final done/done.
                            if ev.get("status") == "error":
                                return
                            if ev.get("step") == "done" and ev.get("status") == "done":
                                return
                            # Unreachable host preflight — terminal for this leg (sync continues other target).
                            if ev.get("step") == "skipped" and ev.get("status") == "done":
                                return
                        except Exception:
                            pass
                else:
                    idle_ticks += 1
                    # Keep-alive comment every 15 s
                    if idle_ticks % 15 == 0:
                        yield ": keep-alive\n\n"

                await asyncio.sleep(1)
        except Exception as e:
            traceback.print_exc()
            log.exception("deploy_progress_sse_failed", node_id=node_id, error=str(e))
            err = json.dumps({"status": "error", "message": str(e)})
            yield f"data: {err}\n\n"

    try:
        return StreamingResponse(
            _generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    except Exception as e:
        traceback.print_exc()
        log.exception("deploy_progress_route_failed", node_id=node_id, error=str(e))
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)},
        )


@router.get(
    "/status",
    response_model=None,
    summary="Latest progress snapshot for all nodes",
)
async def get_deploy_status(redis: RedisDep) -> DeployStatusResponse | JSONResponse:
    """
    Returns the most recent progress event for every node that has a deploy
    progress buffer in Redis.
    """
    try:
        pattern = "nexus:deploy:progress:*"
        nodes: dict[str, DeployProgressEvent | None] = {}
        cursor = 0

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                kstr = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
                node_id = kstr.split("nexus:deploy:progress:")[-1]
                raw = await redis.lindex(key, -1)  # last event
                if raw:
                    try:
                        ev = json.loads(raw)
                        nodes[node_id] = DeployProgressEvent(**ev)
                    except Exception:
                        nodes[node_id] = None
                else:
                    nodes[node_id] = None
            if cursor == 0:
                break

        return DeployStatusResponse(
            nodes=nodes,
            queried_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as e:
        traceback.print_exc()
        log.exception("deploy_status_route_failed", error=str(e))
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)},
        )


@router.delete(
    "/progress/{node_id}",
    response_model=None,
    summary="Clear deploy progress buffer for a node",
)
async def clear_progress(node_id: str, redis: RedisDep) -> dict[str, str] | JSONResponse:
    try:
        await redis.delete(f"nexus:deploy:progress:{node_id}")
        return {"status": "cleared", "node_id": node_id}
    except Exception as e:
        traceback.print_exc()
        log.exception("deploy_clear_progress_failed", node_id=node_id, error=str(e))
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)},
        )


# ── Phase 18: Nexus-Push direct sync ──────────────────────────────────────────

async def _run_sync(redis) -> None:  # type: ignore[type-arg]
    deployer = _make_deployer(redis)
    try:
        result = await deployer.sync_to_worker()
        ok = isinstance(result, str) and not result.startswith("error:")
        log.info("sync_job_complete", result=result, overall_pipeline_ok=ok)
        if not ok:
            log.warning("sync_job_failed", result=result)
    except Exception as exc:
        log.exception("sync_job_error", error=str(exc))
    finally:
        _active_deploys.pop("sync", None)


@router.post(
    "/sync",
    response_model=None,
    summary="🚀 SYNC & RESTART — push code directly to WORKER_IP laptop",
)
async def trigger_sync(
    redis: RedisDep,
) -> DeployResponse | JSONResponse:
    """
    Phase 18 — Nexus-Push.

    Connects directly to the laptop at WORKER_IP using WORKER_SSH_USER /
    WORKER_SSH_PASSWORD from .env, uploads nexus/ scripts/ api/ to
    WORKER_REMOTE_PATH, then runs the self-healing command:

        cd <path>/scripts
        && python3 -m venv .venv --system-site-packages
        && source .venv/bin/activate
        && pip install -r ../requirements.txt
        && pkill -f start_worker.py || true
        && nohup python3 start_worker.py > worker.log 2>&1 &

    Stream live progress via GET /api/deploy/progress/{node_id} (SSE) for each
    target (``worker_linux``, ``worker_windows``). Sync succeeds if at least one
    target completes successfully.
    """
    try:
        # #region agent log
        _agent_dbg(
            "H1",
            "deploy.py:trigger_sync:entry",
            "trigger_sync entered",
            {"active_keys": list(_active_deploys.keys())},
        )
        # #endregion
        # Cancel stale tasks
        stale = [jid for jid, t in _active_deploys.items() if t.done()]
        for jid in stale:
            _active_deploys.pop(jid, None)

        if "sync" in _active_deploys and not _active_deploys["sync"].done():
            raise HTTPException(
                status_code=409,
                detail="A sync is already in progress.",
            )

        task = asyncio.create_task(_run_sync(redis))
        _active_deploys["sync"] = task

        started = datetime.now(timezone.utc).isoformat()
        log.info("sync_triggered", worker_ip=settings.worker_ip)

        # #region agent log
        _agent_dbg(
            "H2",
            "deploy.py:trigger_sync:success",
            "returning DeployResponse",
            {"job_id": "sync"},
        )
        # #endregion
        return DeployResponse(
            job_id="sync",
            targets=["worker_linux", "worker_windows"],
            message=(
                "Sync started — stream via GET /api/deploy/progress/worker_linux "
                "and .../worker_windows"
            ),
            started_at=started,
        )
    except HTTPException:
        raise
    except Exception as e:
        # #region agent log
        _agent_dbg(
            "H3",
            "deploy.py:trigger_sync:except",
            "route caught exception",
            {"error_type": type(e).__name__, "error": str(e)[:500]},
        )
        # #endregion
        traceback.print_exc()
        log.exception("deploy_sync_route_failed", error=str(e))
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)},
        )
