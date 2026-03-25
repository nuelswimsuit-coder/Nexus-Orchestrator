"""
TeleFix Modules — External Project Integration System

This package provides seamless integration of external desktop projects
into the TeleFix ecosystem. Each external project becomes a "TeleFix Module"
with standardized interfaces for monitoring, control, and deployment.

Integrated Projects
-------------------
• OTP_Sessions_Creator — Telegram session management and creation
• 1XPanel_API — Control panel and API management system  
• BudgetTracker — Financial tracking and P&L calculation
• CryptoSellsBot — Cryptocurrency trading bot automation
• fix-express-labs-invoicing — Invoice processing and automation
• Reporter — Telegram reporting / Archivist automation toolkit

Module Architecture
-------------------
Each external project is wrapped with a TeleFix Module Adapter that provides:
  • Standardized status monitoring (running/stopped/error)
  • Configuration reading and validation
  • Live metrics extraction (sessions, balances, etc.)
  • Process control (start/stop/restart)
  • Deployment synchronization to workers

The modules are dynamically loaded and monitored by the Explorer Service,
displayed in the Project Hub dashboard, and can be deployed to Linux workers
via the enhanced Deployer service.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional

# Base paths
DESKTOP_BASE = Path(r"C:\Users\Yarin\Desktop")
MODULES_ROOT = Path(__file__).parent

# Module registry
TELEFIX_MODULES = {
    "otp_sessions": {
        "name": "OTP Sessions Creator",
        "path": DESKTOP_BASE / "OTP_Sessions_Creator", 
        "description": "Telegram session management and creation",
        "category": "communication",
        "priority": 1,
        "icon": "📱",
    },
    "panel_api": {
        "name": "1XPanel API",
        "path": DESKTOP_BASE / "1XPanel_API",
        "description": "Control panel and API management system",
        "category": "infrastructure", 
        "priority": 2,
        "icon": "🎛️",
    },
    "budget_tracker": {
        "name": "BudgetTracker",
        "path": DESKTOP_BASE / "BudgetTracker",
        "description": "Financial tracking and P&L calculation",
        "category": "financial",
        "priority": 1,
        "icon": "💰",
    },
    "crypto_bot": {
        "name": "CryptoSellsBot", 
        "path": DESKTOP_BASE / "CryptoSellsBot",
        "description": "Cryptocurrency trading bot automation",
        "category": "trading",
        "priority": 2,
        "icon": "🚀",
    },
    "invoicing": {
        "name": "Express Labs Invoicing",
        "path": DESKTOP_BASE / "fix-express-labs-invoicing", 
        "description": "Invoice processing and automation",
        "category": "business",
        "priority": 3,
        "icon": "🧾",
    },
    "reporter": {
        "name": "Reporter",
        "path": DESKTOP_BASE / "Reporter",
        "description": "Telegram reporting & Archivist automation toolkit",
        "category": "intelligence",
        "priority": 1,
        "icon": "🗄️",
    },
    "openclaw": {
        "name": "OpenClaw Core",
        "path": MODULES_ROOT / "openclaw.py",
        "description": "Browser-heavy scraping engine orchestrated by Nexus",
        "category": "intelligence",
        "priority": 1,
        "icon": "🕷️",
    },
    "moltbot": {
        "name": "Moltbot Core",
        "path": MODULES_ROOT / "moltbot.py",
        "description": "Telegram-heavy automation and scrape execution module",
        "category": "communication",
        "priority": 1,
        "icon": "🤖",
    },
}


class ModuleAdapter:
    """Base adapter class for external TeleFix modules."""
    
    def __init__(self, module_id: str, config: Dict[str, Any]):
        self.module_id = module_id
        self.config = config
        self.name = config["name"]
        self.path = Path(config["path"])
        self.category = config["category"]
        
    def exists(self) -> bool:
        """Check if the module path exists."""
        return self.path.exists()
        
    def get_status(self) -> str:
        """Get module status: running/stopped/error/not_found."""
        if not self.exists():
            return "not_found"
            
        # Check for running processes related to this module
        import psutil
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if str(self.path) in cmdline or self.name in cmdline:
                        return "running"
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return "stopped"
        except Exception:
            return "error"
    
    def get_live_stats(self) -> Dict[str, Any]:
        """Extract live statistics specific to this module type."""
        if not self.exists():
            return {"available": False}
            
        stats = {"available": True, "path": str(self.path)}
        
        # Module-specific stat extraction
        if self.module_id == "otp_sessions":
            stats.update(self._get_otp_stats())
        elif self.module_id == "budget_tracker":
            stats.update(self._get_budget_stats())
        elif self.module_id == "crypto_bot":
            stats.update(self._get_crypto_stats())
        elif self.module_id == "openclaw":
            stats.update(self._get_openclaw_stats())
        elif self.module_id == "moltbot":
            stats.update(self._get_moltbot_stats())
            
        return stats
    
    def _get_otp_stats(self) -> Dict[str, Any]:
        """Extract OTP Sessions Creator statistics."""
        try:
            # Count session files
            session_files = list(self.path.rglob("*.session"))
            json_sessions = list(self.path.rglob("*session*.json"))
            
            total_sessions = len(session_files) + len(json_sessions)
            
            # Check for recent activity (files modified in last 24h)
            from datetime import datetime, timedelta
            cutoff = datetime.now() - timedelta(hours=24)
            recent_activity = sum(
                1 for f in (session_files + json_sessions)
                if f.stat().st_mtime > cutoff.timestamp()
            )
            
            return {
                "session_count": total_sessions,
                "recent_activity": recent_activity,
                "session_files": len(session_files),
                "json_sessions": len(json_sessions),
            }
        except Exception:
            return {"session_count": 0}
    
    def _get_budget_stats(self) -> Dict[str, Any]:
        """Extract BudgetTracker financial data."""
        try:
            # Look for database or JSON files
            db_files = list(self.path.rglob("*.db"))
            json_files = list(self.path.rglob("*budget*.json"))
            
            # Simple P&L extraction (would need customization based on actual schema)
            daily_pnl = 0.0
            currency = "USD"
            
            # Try to read a simple budget.json if it exists
            budget_json = self.path / "budget.json"
            if budget_json.exists():
                import json
                try:
                    with open(budget_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        daily_pnl = data.get("daily_pnl", 0.0)
                        currency = data.get("currency", "USD")
                except Exception:
                    pass
            
            return {
                "daily_pnl": daily_pnl,
                "currency": currency,
                "db_files": len(db_files),
                "json_files": len(json_files),
                "last_update": self._get_last_modified(),
            }
        except Exception:
            return {"daily_pnl": 0.0, "currency": "USD"}
    
    def _get_crypto_stats(self) -> Dict[str, Any]:
        """Extract CryptoSellsBot status."""
        try:
            # Look for bot configuration and status files
            config_files = list(self.path.rglob("config.*"))
            log_files = list(self.path.rglob("*.log"))
            
            # Check if bot is configured
            has_config = len(config_files) > 0
            has_logs = len(log_files) > 0
            
            return {
                "configured": has_config,
                "has_logs": has_logs,
                "config_files": len(config_files),
                "log_files": len(log_files),
            }
        except Exception:
            return {"configured": False}
    
    def _get_last_modified(self) -> str:
        """Get the most recent modification time."""
        try:
            if not self.path.exists():
                return ""
            latest = max(
                (f.stat().st_mtime for f in self.path.rglob("*") 
                 if f.is_file() and ".git" not in f.parts),
                default=0
            )
            from datetime import datetime
            return datetime.fromtimestamp(latest).isoformat() if latest else ""
        except Exception:
            return ""

    def _get_openclaw_stats(self) -> Dict[str, Any]:
        """Basic local availability metadata for OpenClaw core."""
        try:
            return {
                "entrypoint": str(self.path),
                "last_update": self._get_last_modified(),
            }
        except Exception:
            return {"entrypoint": str(self.path)}

    def _get_moltbot_stats(self) -> Dict[str, Any]:
        """Session awareness metadata for Moltbot."""
        try:
            session_path = os.getenv("MOLTBOT_SESSION_FILE", "")
            has_session = bool(session_path and Path(session_path).exists())
            return {
                "entrypoint": str(self.path),
                "session_file": session_path,
                "has_valid_session": has_session,
                "last_update": self._get_last_modified(),
            }
        except Exception:
            return {"entrypoint": str(self.path), "has_valid_session": False}


class ModuleManager:
    """Central manager for all TeleFix modules."""
    
    def __init__(self):
        self._adapters: Dict[str, ModuleAdapter] = {}
        self._initialize_adapters()
    
    def _initialize_adapters(self):
        """Initialize module adapters for all registered modules."""
        for module_id, config in TELEFIX_MODULES.items():
            self._adapters[module_id] = ModuleAdapter(module_id, config)
    
    def get_all_modules(self) -> Dict[str, Dict[str, Any]]:
        """Get status and metadata for all modules."""
        modules = {}
        for module_id, adapter in self._adapters.items():
            modules[module_id] = {
                "name": adapter.name,
                "path": str(adapter.path),
                "exists": adapter.exists(),
                "status": adapter.get_status(),
                "category": adapter.category,
                "priority": adapter.config["priority"],
                "icon": adapter.config["icon"],
                "description": adapter.config["description"],
                "live_stats": adapter.get_live_stats(),
            }
        return modules
    
    def get_module(self, module_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed info for a specific module."""
        if module_id not in self._adapters:
            return None
        return self.get_all_modules()[module_id]
    
    def get_fuel_gauge_data(self) -> Dict[str, Any]:
        """Extract session health data for the dashboard fuel gauge."""
        otp_adapter = self._adapters.get("otp_sessions")
        if not otp_adapter or not otp_adapter.exists():
            return {"available": False, "session_count": 0}
        
        stats = otp_adapter.get_live_stats()
        return {
            "available": True,
            "session_count": stats.get("session_count", 0),
            "recent_activity": stats.get("recent_activity", 0),
            "fuel_level": min(100, (stats.get("session_count", 0) / 50) * 100),
        }
    
    def get_financial_pulse_data(self) -> Dict[str, Any]:
        """Extract budget data for the financial pulse widget."""
        budget_adapter = self._adapters.get("budget_tracker")
        if not budget_adapter or not budget_adapter.exists():
            return {"available": False, "daily_pnl": 0.0}
        
        stats = budget_adapter.get_live_stats()
        return {
            "available": True,
            "daily_pnl": stats.get("daily_pnl", 0.0),
            "currency": stats.get("currency", "USD"),
            "status": budget_adapter.get_status(),
        }


# Global module manager instance
module_manager = ModuleManager()