#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Nexus Orchestrator — Worker Launcher with CPU Limiting
#
#  Usage:
#    chmod +x launch_worker.sh   # once, to make it executable
#    ./launch_worker.sh
#
#  What it does:
#    1. Configures Git credential.helper store and runs git pull (PAT once, then automatic)
#    2. Checks if 'venv' directory exists, creates it if not and installs requirements
#    3. Activates the virtual environment
#    4. Installs redis, rich, and psutil via pip
#    5. Exports PYTHONPATH to current directory
#    6. Exports NEXUS_SKIP_INHIBIT=true (before node monitor and worker)
#    7. Prompts for Master IP address (defaults to 10.100.102.8)
#    8. Starts node_monitor.py in background (Redis + compact Rich layout)
#    9. Starts start_worker.py — muscle mode ~90% CPU (cpulimit when installed)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Resolve project root (works even when called from another directory) ──────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

VENV_DIR="$PROJECT_ROOT/venv"
VENV_PYTHON="$VENV_DIR/bin/python"
VENV_ACTIVATE="$VENV_DIR/bin/activate"
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements.txt"
NODE_MONITOR_SCRIPT="$PROJECT_ROOT/scripts/node_monitor.py"
WORKER_SCRIPT="$PROJECT_ROOT/scripts/start_worker.py"

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
RESET='\033[0m'

ok()   { echo -e "  ${GREEN}✓${RESET}  $*"; }
warn() { echo -e "  ${YELLOW}⚠${RESET}  $*"; }
err()  { echo -e "  ${RED}✗${RESET}  $*"; }
info() { echo -e "  ${CYAN}▸${RESET}  $*"; }

# ── Configure Git credential helper and perform git pull ───────────────────────
echo ""
info "Configuring Git credential helper..."
git config --global credential.helper store
ok "Git credential helper configured"

echo ""
echo -e "  ${YELLOW}If this is your first time, please enter your Username and PAT (Token) once.${RESET}"
echo -e "  ${YELLOW}From now on, it will be automatic.${RESET}"
echo ""

info "Performing git pull..."
cd "$PROJECT_ROOT"
git pull || {
    warn "Git pull failed. Continuing anyway..."
}

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  NEXUS ORCHESTRATOR — WORKER LAUNCHER"
echo "══════════════════════════════════════════════════════════"
info "Project root : $PROJECT_ROOT"

# ── Check if venv exists, create if not ───────────────────────────────────────
if [[ ! -d "$VENV_DIR" ]]; then
    warn "Virtual environment not found at $VENV_DIR"
    info "Creating virtual environment..."
    
    if ! command -v python3 &> /dev/null; then
        err "python3 not found. Please install Python 3 first."
        exit 1
    fi
    
    python3 -m venv "$VENV_DIR"
    ok "Created virtual environment: $VENV_DIR"
    
    # Activate venv to install requirements
    # shellcheck disable=SC1090
    source "$VENV_ACTIVATE"
    
    # Upgrade pip first
    info "Upgrading pip..."
    "$VENV_PYTHON" -m pip install --upgrade pip --quiet
    
    # Install requirements
    if [[ -f "$REQUIREMENTS_FILE" ]]; then
        info "Installing requirements from $REQUIREMENTS_FILE..."
        "$VENV_PYTHON" -m pip install -r "$REQUIREMENTS_FILE" --quiet
        ok "Installed requirements from requirements.txt"
    else
        warn "requirements.txt not found at $REQUIREMENTS_FILE"
    fi
else
    ok "Virtual environment found: $VENV_DIR"
    # Activate venv
    # shellcheck disable=SC1090
    source "$VENV_ACTIVATE"
fi

# ── Install/ensure redis, rich, and psutil are installed ─────────────────────
info "Ensuring redis, rich, and psutil are installed..."
"$VENV_PYTHON" -m pip install --quiet redis rich psutil
ok "Verified redis, rich, and psutil are installed"

# ── Set PYTHONPATH to current directory ───────────────────────────────────────
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
info "PYTHONPATH    : $PYTHONPATH"

# ── Export NEXUS_SKIP_INHIBIT ──────────────────────────────────────────────────
export NEXUS_SKIP_INHIBIT=true
info "NEXUS_SKIP_INHIBIT : $NEXUS_SKIP_INHIBIT"
export NEXUS_WORKER_CPU_UTIL_TARGET=90
info "NEXUS_WORKER_CPU_UTIL_TARGET : $NEXUS_WORKER_CPU_UTIL_TARGET"

# ── Get Master IP address from user ───────────────────────────────────────────
DEFAULT_MASTER_IP="10.100.102.8"
echo ""
read -r -p "Enter Master IP address [${DEFAULT_MASTER_IP}]: " MASTER_IP
MASTER_IP="${MASTER_IP:-$DEFAULT_MASTER_IP}"
info "Using Master IP: $MASTER_IP"

# ── Check if node_monitor.py exists ───────────────────────────────────────────
if [[ ! -f "$NODE_MONITOR_SCRIPT" ]]; then
    warn "node_monitor.py not found at $NODE_MONITOR_SCRIPT"
    warn "Skipping node monitor startup"
    NODE_MONITOR_PID=""
else
    # ── Start node_monitor.py in background ───────────────────────────────────
    info "Starting node_monitor.py in background..."
    nohup "$VENV_PYTHON" "$NODE_MONITOR_SCRIPT" --redis-host "$MASTER_IP" > /dev/null 2>&1 &
    NODE_MONITOR_PID=$!
    ok "Started node_monitor.py (PID: $NODE_MONITOR_PID)"
fi

# ── Check for cpulimit command ────────────────────────────────────────────────
CPU_LIMIT_PERCENT=90
if command -v cpulimit &> /dev/null; then
    # Use cpulimit if available (muscle workers — align with NEXUS_WORKER_CPU_UTIL_TARGET)
    info "Starting worker with ${CPU_LIMIT_PERCENT}% CPU ceiling using cpulimit..."
    ok "Starting worker..."
    echo "──────────────────────────────────────────────────────────"
    exec cpulimit -l "$CPU_LIMIT_PERCENT" -- "$VENV_PYTHON" "$WORKER_SCRIPT" --master-host "$MASTER_IP"
elif command -v nice &> /dev/null; then
    # Fallback to nice (doesn't limit CPU percentage, but lowers priority)
    warn "cpulimit not found. Using nice as fallback (reduces priority, not CPU limit)"
    warn "For true CPU limiting, install cpulimit: sudo apt-get install cpulimit"
    info "Starting worker with reduced priority..."
    ok "Starting worker..."
    echo "──────────────────────────────────────────────────────────"
    exec nice -n 10 "$VENV_PYTHON" "$WORKER_SCRIPT" --master-host "$MASTER_IP"
else
    # No CPU limiting available
    warn "Neither cpulimit nor nice found. Starting worker without CPU limiting."
    warn "For CPU limiting, install cpulimit: sudo apt-get install cpulimit"
    ok "Starting worker..."
    echo "──────────────────────────────────────────────────────────"
    exec "$VENV_PYTHON" "$WORKER_SCRIPT" --master-host "$MASTER_IP"
fi
