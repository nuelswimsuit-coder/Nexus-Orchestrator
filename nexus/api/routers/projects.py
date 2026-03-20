"""
Projects router — Desktop Project Hub API

Endpoints
---------
GET  /api/projects
    Return metadata for all monitored desktop projects (OTP Creator, BudgetTracker, etc.)
    Includes language, status, config keys, live stats, and running processes.

GET  /api/projects/{project_name}
    Get detailed info for a specific project.

POST /api/projects/{project_name}/action
    Send a control action to a project: "start", "stop", "restart", "sync"

GET  /api/projects/budget/widget
    Extract BudgetTracker data for the dashboard widget.
    Returns {"available": False} if BudgetTracker is not accessible.

POST /api/projects/scan
    Trigger a fresh scan of all desktop projects (bypasses Redis cache).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict

import structlog
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep
from nexus.master.services.explorer import (
    get_cached_projects,
    get_budget_widget_data,
    scan_desktop_projects,
)

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/projects", tags=["projects"])

# ── Schemas ────────────────────────────────────────────────────────────────────

class ProjectInfo(BaseModel):
    name: str
    path: str
    exists: bool
    language: str
    stack: list[str]
    status: str
    running_processes: list[str]
    config_keys: list[str]
    env_file: str
    live_stats: Dict[str, Any]
    last_modified: str
    size_mb: float
    scanned_at: str


class ProjectsResponse(BaseModel):
    projects: Dict[str, ProjectInfo]
    total_count: int
    running_count: int
    total_size_mb: float
    last_scan: str


class ProjectActionRequest(BaseModel):
    action: str  # "start" | "stop" | "restart" | "sync"


class BudgetWidgetResponse(BaseModel):
    available: bool
    daily_pnl: float = 0.0
    currency: str = "USD"
    status: str = "Unknown"
    project_path: str = ""
    last_transaction: str = ""


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("", response_model=ProjectsResponse, summary="Get all desktop projects")
async def get_projects(redis: RedisDep) -> ProjectsResponse:
    """
    Return metadata for all monitored desktop projects.
    Uses Redis cache; triggers fresh scan if cache is empty.
    """
    projects_dict = await get_cached_projects(redis)
    
    projects = {
        name: ProjectInfo(**data) 
        for name, data in projects_dict.items()
    }
    
    running_count = sum(1 for p in projects.values() if p.status == "Running")
    total_size = sum(p.size_mb for p in projects.values() if p.exists)
    
    # Get last scan timestamp
    last_scan_raw = await redis.get("nexus:explorer:last_scan")
    last_scan = last_scan_raw or datetime.now(timezone.utc).isoformat()

    return ProjectsResponse(
        projects=projects,
        total_count=len(projects),
        running_count=running_count,
        total_size_mb=round(total_size, 1),
        last_scan=last_scan,
    )


@router.get("/{project_name}", response_model=ProjectInfo, summary="Get specific project details")
async def get_project(project_name: str, redis: RedisDep) -> ProjectInfo:
    """Return detailed metadata for a single project."""
    projects_dict = await get_cached_projects(redis)
    
    if project_name not in projects_dict:
        raise HTTPException(status_code=404, detail=f"Project '{project_name}' not found")
    
    return ProjectInfo(**projects_dict[project_name])


@router.post("/{project_name}/action", summary="Control project (start/stop/sync)")
async def project_action(
    project_name: str, 
    body: ProjectActionRequest,
    request: Request,
    redis: RedisDep
) -> Dict[str, str]:
    """
    Send a control action to a project.
    
    Actions:
    - start: Launch the project (if stopped)
    - stop: Terminate project processes  
    - restart: Stop then start
    - sync: Push to workers via deployer
    """
    action = body.action.lower()
    if action not in ("start", "stop", "restart", "sync"):
        raise HTTPException(status_code=400, detail=f"Invalid action: {action}")

    projects_dict = await get_cached_projects(redis)
    if project_name not in projects_dict:
        raise HTTPException(status_code=404, detail=f"Project '{project_name}' not found")

    project_data = projects_dict[project_name]
    
    log.info("project_action_triggered", 
             project=project_name, action=action, 
             current_status=project_data["status"])

    if action == "sync":
        # Use the deployer to push this project to the Linux worker
        from nexus.master.services.deployer import DeployerService
        from nexus.master.services.vault import Vault
        from nexus.shared.config import settings

        try:
            vault = Vault()
            deployer = DeployerService(redis=redis, vault=vault, settings=settings)
            
            result = await deployer.sync_project_to_worker(
                project_name=project_name,
                project_path=project_data["path"],
                remote_path=f"/home/yadmin/Desktop/{project_name}",
            )
            
            if result == "ok":
                return {
                    "project": project_name,
                    "action": action,
                    "status": "completed",
                    "message": f"'{project_name}' synced to Linux worker successfully",
                }
            else:
                return {
                    "project": project_name,
                    "action": action,
                    "status": "error",
                    "message": result,
                }
        except Exception as exc:
            log.exception("project_sync_error", project=project_name, error=str(exc))
            return {
                "project": project_name,
                "action": action,
                "status": "error", 
                "message": f"Sync failed: {exc}",
            }

    else:
        # For start/stop/restart actions, return a placeholder for now
        # In a full implementation, this would actually control processes
        return {
            "project": project_name,
            "action": action,
            "status": "accepted",
            "message": f"Action '{action}' queued for {project_name}",
        }


@router.get("/budget/widget", response_model=BudgetWidgetResponse, 
           summary="BudgetTracker widget data for dashboard")
async def get_budget_widget(redis: RedisDep) -> BudgetWidgetResponse:
    """
    Extract BudgetTracker financial data for the main dashboard widget.
    Returns {"available": False} if BudgetTracker is not found/accessible.
    """
    stats = await get_budget_widget_data(redis)
    return BudgetWidgetResponse(**stats)


@router.get("/architect/audit", summary="Self-Architect audit stats")
async def get_architect_audit(redis: RedisDep) -> Dict[str, Any]:
    """Return the latest Self-Architect audit summary and pending prompts."""
    from nexus.master.services.architect_agent import ArchitectAgent
    agent = ArchitectAgent(redis=redis)
    return await agent.get_audit_stats()


@router.post("/architect/run", summary="Trigger a Self-Architect audit cycle now")
async def run_architect_audit(redis: RedisDep) -> Dict[str, Any]:
    """Run a full audit cycle immediately and return the summary."""
    from nexus.master.services.architect_agent import ArchitectAgent
    agent = ArchitectAgent(redis=redis)
    return await agent.run_once()


@router.get("/architect/otp-optimizations", summary="OTP Sessions Creator optimization report")
async def get_otp_optimizations(redis: RedisDep) -> Dict[str, Any]:
    """Return the 3 targeted optimizations for OTP Sessions Creator."""
    from nexus.master.services.architect_agent import ArchitectAgent
    agent = ArchitectAgent(redis=redis)
    opts = await agent.get_otp_optimizations()
    return {"optimizations": opts, "count": len(opts)}


@router.get("/architect/prompts", summary="Pending optimization prompts")
async def get_pending_prompts(redis: RedisDep) -> Dict[str, Any]:
    """Return all pending optimization prompts generated by the Self-Architect."""
    from nexus.master.services.architect_agent import ArchitectAgent
    agent = ArchitectAgent(redis=redis)
    prompts = await agent.get_pending_prompts()
    return {"prompts": prompts, "count": len(prompts)}


@router.post("/scan", summary="Trigger fresh scan of all desktop projects")
async def trigger_scan(redis: RedisDep) -> Dict[str, str]:
    """Force a fresh scan of all desktop projects, bypassing Redis cache."""
    try:
        projects_dict = await scan_desktop_projects(redis)
        
        # Persist the fresh results
        payload = json.dumps(projects_dict)
        await redis.set("nexus:explorer:projects", payload, ex=24*3600)
        await redis.set("nexus:explorer:last_scan", 
                       datetime.now(timezone.utc).isoformat())
        
        project_count = len(projects_dict)
        running_count = sum(
            1 for p in projects_dict.values() 
            if p.get("status") == "Running"
        )
        
        return {
            "status": "completed",
            "projects_scanned": project_count,
            "running_projects": running_count,
            "scanned_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.exception("project_scan_trigger_error", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Scan failed: {exc}")