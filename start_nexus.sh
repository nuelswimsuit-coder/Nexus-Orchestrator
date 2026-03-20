#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Nexus Orchestrator — start_nexus.sh
#
#  The canonical entry-point for Linux workers.
#  Called by the Master's deployer after every code push.
#
#  What it does (in order):
#    1. Resolves PROJECT_ROOT from the script's own location.
#    2. Sets PYTHONPATH so `import nexus` always works.
#    3. Activates the .venv (creates it first if missing).
#    4. Runs `pip install -r requirements.txt` to ensure deps are current.
#    5. Kills any existing worker process gracefully.
#    6. Launches scripts/start_worker.py in the background.
#
#  Usage:
#    chmod +x start_nexus.sh        # once
#    ./start_nexus.sh               # start / restart worker
#    ./start_nexus.sh --foreground  # run in foreground (for systemd / debugging)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
VENV_DIR="$PROJECT_ROOT/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"
VENV_PIP="$VENV_DIR/bin/pip"
VENV_ACTIVATE="$VENV_DIR/bin/activate"
REQUIREMENTS="$PROJECT_ROOT/requirements.txt"
WORKER_SCRIPT="$PROJECT_ROOT/scripts/start_worker.py"
LOG_FILE="$PROJECT_ROOT/worker.log"
PID_FILE="$PROJECT_ROOT/worker.pid"

FOREGROUND=false
[[ "${1:-}" == "--foreground" ]] && FOREGROUND=true

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
CYAN='\033[0;36m';  BOLD='\033[1m';      RESET='\033[0m'
ok()   { echo -e "  ${GREEN}✓${RESET}  $*"; }
warn() { echo -e "  ${YELLOW}⚠${RESET}  $*"; }
err()  { echo -e "  ${RED}✗${RESET}  $*"; exit 1; }
info() { echo -e "  ${CYAN}▸${RESET}  $*"; }

echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  TELEFIX OS — start_nexus.sh${RESET}"
echo -e "${BOLD}══════════════════════════════════════════════════════════${RESET}"
info "Project root : $PROJECT_ROOT"

# ── 1. Ensure .venv exists ────────────────────────────────────────────────────
if [[ ! -f "$VENV_PYTHON" ]]; then
    warn ".venv not found — creating …"
    python3 -m venv "$VENV_DIR" || err "python3 -m venv failed. Run: sudo apt install python3-venv python3-full"
    ok "Created .venv at $VENV_DIR"
else
    ok ".venv exists at $VENV_DIR"
fi

# ── 2. Set PYTHONPATH (includes TeleFix module paths) ────────────────────────
# Core project root so `import nexus` always resolves
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"

# TeleFix external modules — add Desktop project paths if they exist
TELEFIX_MODULES_BASE="/home/yadmin/Desktop"
for MODULE_DIR in \
    "$TELEFIX_MODULES_BASE/OTP_Sessions_Creator" \
    "$TELEFIX_MODULES_BASE/1XPanel_API" \
    "$TELEFIX_MODULES_BASE/BudgetTracker" \
    "$TELEFIX_MODULES_BASE/CryptoSellsBot" \
    "$TELEFIX_MODULES_BASE/fix-express-labs-invoicing" \
    "$TELEFIX_MODULES_BASE/Reporter" \
    "$TELEFIX_MODULES_BASE/TeleFix-Modules"; do
    if [[ -d "$MODULE_DIR" ]]; then
        export PYTHONPATH="$MODULE_DIR:$PYTHONPATH"
        ok "Module path added : $MODULE_DIR"
    fi
done

info "PYTHONPATH   : $PYTHONPATH"

# ── 3. Activate venv ─────────────────────────────────────────────────────────
# shellcheck disable=SC1090
source "$VENV_ACTIVATE"
ok "Activated     : $(python --version)"

# ── 4. Install / update dependencies ─────────────────────────────────────────
if [[ -f "$REQUIREMENTS" ]]; then
    info "Installing deps from requirements.txt …"
    pip install --quiet --upgrade pip
    pip install --quiet -r "$REQUIREMENTS" \
        || { warn "First attempt failed — retrying with --no-cache-dir …"
             pip install --quiet --no-cache-dir -r "$REQUIREMENTS"; }
    ok "Dependencies up to date"

    # Install Playwright browser (OpenClaw requires headless Chromium)
    if python -c "import playwright" 2>/dev/null; then
        info "Installing Playwright Chromium browser …"
        python -m playwright install chromium --with-deps 2>/dev/null || true
        ok "Playwright Chromium ready"
    fi
else
    warn "requirements.txt not found — skipping pip install"
fi

# ── 5. Kill existing worker ───────────────────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        info "Stopping existing worker (PID $OLD_PID) …"
        kill -SIGTERM "$OLD_PID" 2>/dev/null || true
        sleep 2
        kill -0 "$OLD_PID" 2>/dev/null && kill -SIGKILL "$OLD_PID" 2>/dev/null || true
        ok "Old worker stopped"
    fi
    rm -f "$PID_FILE"
fi
# Belt-and-suspenders: kill any stray start_worker.py processes
pkill -SIGTERM -f "start_worker.py" 2>/dev/null || true
sleep 1

# ── 6. Launch worker ─────────────────────────────────────────────────────────
echo "──────────────────────────────────────────────────────────"
if $FOREGROUND; then
    ok "Starting worker in foreground …"
    exec python "$WORKER_SCRIPT"
else
    ok "Starting worker in background → $LOG_FILE"
    nohup python "$WORKER_SCRIPT" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    ok "Worker started (PID $(cat "$PID_FILE"))"
    echo ""
    info "Tail logs : tail -f $LOG_FILE"
    info "Stop      : kill \$(cat $PID_FILE)"
fi
