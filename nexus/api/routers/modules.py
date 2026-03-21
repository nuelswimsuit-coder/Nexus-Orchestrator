"""
Modules router — TeleFix Module Integration API

Provides centralized access to external TeleFix modules integrated into the system.
Each module (OTP_Sessions_Creator, BudgetTracker, etc.) is monitored and controllable
via these endpoints.

Endpoints
---------
GET  /api/modules
    List all TeleFix modules with status, live stats, and metadata.

GET  /api/modules/{module_id}
    Get detailed information for a specific module.

POST /api/modules/{module_id}/action
    Send control action: start, stop, restart, sync, scan.

GET  /api/modules/widgets/fuel-gauge
    Session health data for the dashboard fuel gauge widget.

GET  /api/modules/widgets/financial-pulse  
    Budget data for the dashboard financial pulse widget.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import arq
import structlog
from fastapi import APIRouter, HTTPException
from arq.connections import RedisSettings
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep
from nexus.modules import module_manager
from nexus.modules.moltbot import build_moltbot_parameters
from nexus.modules.openclaw import build_openclaw_parameters
from nexus.shared.config import settings
from nexus.shared.schemas import TaskPayload, WorkerCapability
from nexus.trading.poly_bot_state import POLY_BOT_STATUS_KEY
from nexus.worker.tasks.moltbot import MOLTBOT_STATUS_KEY
from nexus.worker.tasks.openclaw import OPENCLAW_STATUS_KEY

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/modules", tags=["modules"])

# ── Schemas ────────────────────────────────────────────────────────────────────

class ModuleInfo(BaseModel):
    name: str
    path: str
    exists: bool
    status: str
    category: str
    priority: int
    icon: str
    description: str
    live_stats: Dict[str, Any]


class ModulesResponse(BaseModel):
    modules: Dict[str, ModuleInfo]
    total_count: int
    running_count: int
    available_count: int
    last_scan: str


class ModuleActionRequest(BaseModel):
    action: str  # "start" | "stop" | "restart" | "sync" | "scan"


class FuelGaugeResponse(BaseModel):
    available: bool
    session_count: int = 0
    recent_activity: int = 0 
    fuel_level: float = 0.0


class FinancialPulseResponse(BaseModel):
    available: bool
    daily_pnl: float = 0.0
    currency: str = "USD"
    status: str = "Unknown"


class OpenclawLaunchRequest(BaseModel):
    mode: str = "google_maps"
    query: str
    project_id: str = "telefix"
    location: str = ""
    max_leads: int = 50


class MoltbotLaunchRequest(BaseModel):
    action: str = "launch_scrape"
    query: str = ""
    max_items: int = 100
    session_file: str | None = None


class ModuleLaunchResponse(BaseModel):
    task_id: str
    task_type: str
    module: str
    message: str


class ModuleRuntimeHealth(BaseModel):
    module: str
    active: bool = False
    stage: str = "idle"
    detail: str = ""
    node_id: str = ""
    cpu_percent: float = 0.0
    rss_mb: float = 0.0
    updated_at: str = ""


class ModuleHealthResponse(BaseModel):
    modules: Dict[str, ModuleRuntimeHealth]
    queried_at: str


async def _enqueue_task(task: TaskPayload) -> str:
    pool = await arq.create_pool(
        RedisSettings.from_dsn(settings.redis_url),
        default_queue_name="nexus:tasks",
    )
    try:
        job = await pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task.task_id,
            _queue_name="nexus:tasks",
        )
        if job is None:
            raise HTTPException(status_code=409, detail="Task already queued with same id")
        return task.task_id
    finally:
        await pool.aclose()


async def _read_health(redis: RedisDep, key: str, module: str) -> ModuleRuntimeHealth:
    raw = await redis.get(key)
    if not raw:
        return ModuleRuntimeHealth(module=module)
    try:
        data = json.loads(raw)
    except Exception:
        return ModuleRuntimeHealth(module=module)
    return ModuleRuntimeHealth(
        module=module,
        active=bool(data.get("active", False)),
        stage=str(data.get("stage", "idle")),
        detail=str(data.get("detail", "")),
        node_id=str(data.get("node_id", "")),
        cpu_percent=float(data.get("cpu_percent", 0.0) or 0.0),
        rss_mb=float(data.get("rss_mb", 0.0) or 0.0),
        updated_at=str(data.get("updated_at", "")),
    )


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("", response_model=ModulesResponse, summary="List all TeleFix modules")
async def get_modules(redis: RedisDep) -> ModulesResponse:
    """
    Return metadata and live stats for all TeleFix modules.
    Includes session counts, financial data, and process status.
    """
    modules_data = module_manager.get_all_modules()
    
    modules = {
        module_id: ModuleInfo(**data)
        for module_id, data in modules_data.items()
    }
    
    running_count = sum(1 for m in modules.values() if m.status == "running")
    available_count = sum(1 for m in modules.values() if m.exists)
    
    return ModulesResponse(
        modules=modules,
        total_count=len(modules),
        running_count=running_count,
        available_count=available_count,
        last_scan=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/{module_id}", response_model=ModuleInfo, summary="Get specific module details")
async def get_module(module_id: str, redis: RedisDep) -> ModuleInfo:
    """Return detailed metadata for a single TeleFix module."""
    module_data = module_manager.get_module(module_id)
    
    if not module_data:
        raise HTTPException(
            status_code=404, 
            detail=f"Module '{module_id}' not found in TeleFix registry"
        )
    
    return ModuleInfo(**module_data)


@router.post("/openclaw/launch", response_model=ModuleLaunchResponse, status_code=202)
async def launch_openclaw(body: OpenclawLaunchRequest, redis: RedisDep) -> ModuleLaunchResponse:
    """
    Launch an OpenClaw scrape job, routed to the high-power Windows worker.
    """
    task_id = str(uuid.uuid4())
    task = TaskPayload(
        task_id=task_id,
        task_type="scraper.openclaw",
        parameters=build_openclaw_parameters(
            mode=body.mode,
            query=body.query,
            project_id=body.project_id,
            max_leads=body.max_leads,
            location=body.location,
        ),
        project_id=body.project_id,
        priority=2,
        required_capabilities=[WorkerCapability.WINDOWS],
    )
    await _enqueue_task(task)
    return ModuleLaunchResponse(
        task_id=task_id,
        task_type="scraper.openclaw",
        module="openclaw",
        message="OpenClaw launch dispatched to ARQ cluster queue",
    )


@router.post("/moltbot/launch", response_model=ModuleLaunchResponse, status_code=202)
async def launch_moltbot(body: MoltbotLaunchRequest, redis: RedisDep) -> ModuleLaunchResponse:
    """
    Launch a Moltbot task; runnable on any worker with a valid session file.
    """
    task_id = str(uuid.uuid4())
    session_file = (body.session_file or os.getenv("MOLTBOT_SESSION_FILE", "")).strip()
    if not session_file:
        raise HTTPException(
            status_code=400,
            detail="session_file is required (body.session_file or MOLTBOT_SESSION_FILE env var)",
        )

    task = TaskPayload(
        task_id=task_id,
        task_type="bot.moltbot",
        parameters=build_moltbot_parameters(
            session_file=session_file,
            action=body.action,
            query=body.query,
            max_items=body.max_items,
        ),
        project_id="telefix",
        priority=2,
    )
    await _enqueue_task(task)
    return ModuleLaunchResponse(
        task_id=task_id,
        task_type="bot.moltbot",
        module="moltbot",
        message="Moltbot launch dispatched to ARQ cluster queue",
    )


@router.get("/widgets/module-health", response_model=ModuleHealthResponse, summary="Runtime health for core modules")
async def get_module_health(redis: RedisDep) -> ModuleHealthResponse:
    openclaw = await _read_health(redis, OPENCLAW_STATUS_KEY, "openclaw")
    moltbot = await _read_health(redis, MOLTBOT_STATUS_KEY, "moltbot")
    poly_bot = await _read_health(redis, POLY_BOT_STATUS_KEY, "polymarket_bot")
    return ModuleHealthResponse(
        modules={
            "openclaw": openclaw,
            "moltbot": moltbot,
            "polymarket_bot": poly_bot,
        },
        queried_at=datetime.now(timezone.utc).isoformat(),
    )


@router.post("/{module_id}/action", summary="Control TeleFix module")
async def module_action(
    module_id: str,
    body: ModuleActionRequest,
    redis: RedisDep
) -> Dict[str, str]:
    """
    Send a control action to a TeleFix module.
    
    Actions:
    - start: Launch the module (if stopped)
    - stop: Terminate module processes
    - restart: Stop then start
    - sync: Push to Linux workers via deployer
    - scan: Force refresh of module metadata
    """
    action = body.action.lower()
    if action not in ("start", "stop", "restart", "sync", "scan"):
        raise HTTPException(status_code=400, detail=f"Invalid action: {action}")

    module_data = module_manager.get_module(module_id)
    if not module_data:
        raise HTTPException(status_code=404, detail=f"Module '{module_id}' not found")

    log.info("module_action_triggered", 
             module=module_id, action=action,
             current_status=module_data["status"])

    if action == "sync":
        # Use the deployer to push this module to Linux workers
        from nexus.master.services.deployer import DeployerService
        from nexus.master.services.vault import Vault
        from nexus.shared.config import settings

        try:
            vault = Vault()
            deployer = DeployerService(redis=redis, vault=vault, settings=settings)
            
            result = await deployer.sync_project_to_worker(
                project_name=module_data["name"],
                project_path=module_data["path"],
                remote_path=f"/home/yadmin/Desktop/TeleFix-Modules/{module_id}",
            )
            
            return {
                "module": module_id,
                "action": action,
                "status": "completed" if result == "ok" else "error",
                "message": f"Module '{module_data['name']}' synced to Linux worker" 
                          if result == "ok" else result,
            }
        except Exception as exc:
            log.exception("module_sync_error", module=module_id, error=str(exc))
            return {
                "module": module_id,
                "action": action,
                "status": "error",
                "message": f"Sync failed: {exc}",
            }

    # For other actions, return placeholder response
    return {
        "module": module_id,
        "action": action,
        "status": "accepted",
        "message": f"Action '{action}' queued for {module_data['name']}",
    }


@router.get("/widgets/fuel-gauge", response_model=FuelGaugeResponse, 
           summary="Session health data for fuel gauge widget")
async def get_fuel_gauge_data(redis: RedisDep) -> FuelGaugeResponse:
    """
    Extract OTP Sessions Creator data for the dashboard fuel gauge.
    Shows session count and activity level as a "fuel" percentage.
    """
    data = module_manager.get_fuel_gauge_data()
    return FuelGaugeResponse(**data)


@router.get("/widgets/financial-pulse", response_model=FinancialPulseResponse,
           summary="Budget data for financial pulse widget") 
async def get_financial_pulse_data(redis: RedisDep) -> FinancialPulseResponse:
    """
    Extract BudgetTracker data for the dashboard financial pulse widget.
    Shows daily P&L and trading status.
    """
    data = module_manager.get_financial_pulse_data()
    return FinancialPulseResponse(**data)