"""
Hardware detection utility for Worker (and Master) nodes.

Collects machine identity information that is included in NodeHeartbeat
payloads so the dashboard can display a "Digital Twin" HUD for each node.

Detection strategy
------------------
- CPU model  : platform.processor() → cleaned up string.
               Falls back to psutil CPU count if processor() is empty.
- GPU model  : Tries nvidia-smi via subprocess first (works on any OS with
               NVIDIA drivers).  Falls back to GPUtil if installed.
               Falls back to "N/A" gracefully — never raises.
- Local IP   : Connects a UDP socket to 8.8.8.8 (no data sent) to discover
               the outbound interface IP.  Falls back to socket.gethostbyname().
- RAM total  : psutil.virtual_memory().total
- OS info    : platform.system() + platform.release()

All detection is synchronous and cached at import time — the values do not
change during a process lifetime.  The `get_hardware_info()` function returns
a frozen dict that is safe to embed in a Pydantic model.
"""

from __future__ import annotations

import platform
import socket
import subprocess
import sys
from functools import lru_cache
from typing import Any

import psutil
import structlog

log = structlog.get_logger(__name__)


@lru_cache(maxsize=1)
def get_hardware_info() -> dict[str, Any]:
    """
    Detect and return hardware specs for this node.

    Returns a dict with keys:
        local_ip, cpu_model, gpu_model, ram_total_mb, os_info, motherboard

    All values are strings or floats; never None.
    Cached after first call — safe to call repeatedly.
    """
    return {
        "local_ip": _detect_local_ip(),
        "cpu_model": _detect_cpu_model(),
        "gpu_model": _detect_gpu_model(),
        "ram_total_mb": _detect_ram_total_mb(),
        "os_info": _detect_os_info(),
        "motherboard": _detect_motherboard(),
    }


# ── Individual detectors ───────────────────────────────────────────────────────

def _detect_local_ip() -> str:
    """Return the LAN IP of the primary outbound interface."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


def _detect_cpu_model() -> str:
    """Return a cleaned CPU model string."""
    raw = platform.processor()

    # Windows: platform.processor() often returns a full Intel/AMD string.
    # Linux:   may return "x86_64" — fall back to /proc/cpuinfo.
    if not raw or raw in ("x86_64", "i686", "AMD64"):
        raw = _read_proc_cpuinfo_model() or raw

    if not raw:
        count = psutil.cpu_count(logical=False) or psutil.cpu_count()
        freq = psutil.cpu_freq()
        freq_str = f" @ {freq.max / 1000:.1f} GHz" if freq else ""
        return f"{count}-core CPU{freq_str}"

    # Trim common noise from Windows strings.
    raw = raw.replace("(R)", "").replace("(TM)", "").replace("  ", " ").strip()
    # Truncate very long strings.
    return raw[:60] if len(raw) > 60 else raw


def _read_proc_cpuinfo_model() -> str:
    """Read CPU model from /proc/cpuinfo (Linux only)."""
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""


def _detect_gpu_model() -> str:
    """
    Detect GPU model.  Tries three methods in order:
    1. nvidia-smi (subprocess) — works on any OS with NVIDIA drivers.
    2. GPUtil Python library — if installed.
    3. Returns "N/A".
    """
    # Method 1: nvidia-smi
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            lines = [ln.strip() for ln in result.stdout.strip().splitlines() if ln.strip()]
            if lines:
                return lines[0][:60]
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        pass

    # Method 2: GPUtil
    try:
        import GPUtil  # type: ignore[import-untyped]
        gpus = GPUtil.getGPUs()
        if gpus:
            return gpus[0].name[:60]
    except ImportError:
        pass
    except Exception:
        pass

    # Method 3: wmic (Windows fallback)
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["wmic", "path", "win32_VideoController", "get", "name"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                lines = [
                    ln.strip()
                    for ln in result.stdout.strip().splitlines()
                    if ln.strip() and ln.strip().lower() != "name"
                ]
                if lines:
                    return lines[0][:60]
        except Exception:
            pass

    return "N/A"


def _detect_ram_total_mb() -> float:
    """Return total installed RAM in MB."""
    try:
        return round(psutil.virtual_memory().total / (1024 * 1024), 1)
    except Exception:
        return 0.0


def _detect_os_info() -> str:
    """Return a human-readable OS string."""
    system = platform.system()
    release = platform.release()

    if system == "Windows":
        return f"Windows {release}"
    if system == "Linux":
        try:
            with open("/etc/os-release", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("PRETTY_NAME="):
                        return line.split("=", 1)[1].strip().strip('"')[:40]
        except Exception:
            pass
        return f"Linux {release}"
    if system == "Darwin":
        return f"macOS {platform.mac_ver()[0]}"
    return f"{system} {release}".strip()


def _detect_motherboard() -> str:
    """Detect motherboard manufacturer + product name."""
    system = platform.system()

    if system == "Windows":
        try:
            result = subprocess.run(
                ["wmic", "baseboard", "get", "Manufacturer,Product"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                lines = [
                    ln.strip()
                    for ln in result.stdout.strip().splitlines()
                    if ln.strip() and ln.strip().lower() not in ("manufacturer product", "manufacturer,product")
                ]
                if lines:
                    return " ".join(lines[0].split())[:60]
        except Exception:
            pass

    elif system == "Linux":
        try:
            vendor, name = "", ""
            for path, attr in [("/sys/class/dmi/id/board_vendor", "v"), ("/sys/class/dmi/id/board_name", "n")]:
                try:
                    with open(path, encoding="utf-8") as f:
                        val = f.read().strip()
                    if attr == "v":
                        vendor = val
                    else:
                        name = val
                except Exception:
                    pass
            if vendor or name:
                return f"{vendor} {name}".strip()[:60]
        except Exception:
            pass

    elif system == "Darwin":
        try:
            result = subprocess.run(
                ["system_profiler", "SPHardwareDataType"],
                capture_output=True, text=True, timeout=8,
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "Model Identifier" in line:
                        return line.split(":", 1)[1].strip()[:60]
        except Exception:
            pass

    return "N/A"
