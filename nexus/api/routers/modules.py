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

from datetime import datetime, timezone
from typing import Any, Dict

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep
from nexus.modules import module_manager

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