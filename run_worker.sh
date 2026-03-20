#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Nexus Orchestrator — One-Click Worker Launcher (Linux / macOS)
#
#  Usage:
#    chmod +x run_worker.sh   # once, to make it executable
#    ./run_worker.sh
#
#  What it does:
#    1. Resolves the project root (the directory containing this script).
#    2. Sets PYTHONPATH so `import nexus` works without installing the package.
#    3. Activates the .venv created by system_bootstrap.py.
#    4. Runs scripts/start_worker.py.
#
#  If the .venv does not exist yet, it offers to run system_bootstrap.py first.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Resolve project root (works even when called from another directory) ──────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

VENV_DIR="$PROJECT_ROOT/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"
VENV_ACTIVATE="$VENV_DIR/bin/activate"
WORKER_SCRIPT="$PROJECT_ROOT/scripts/start_worker.py"
BOOTSTRAP_SCRIPT="$PROJECT_ROOT/scripts/system_bootstrap.py"

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

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  NEXUS ORCHESTRATOR — WORKER LAUNCHER"
echo "══════════════════════════════════════════════════════════"
info "Project root : $PROJECT_ROOT"

# ── Check .venv exists ────────────────────────────────────────────────────────
if [[ ! -f "$VENV_PYTHON" ]]; then
    warn ".venv not found at $VENV_DIR"
    echo ""
    read -r -p "  Run system_bootstrap.py now to create it? [Y/n] " answer
    answer="${answer:-Y}"
    if [[ "$answer" =~ ^[Yy]$ ]]; then
        info "Running bootstrap …"
        python3 "$BOOTSTRAP_SCRIPT" --no-mangement-ahu
    else
        err "Cannot start worker without a .venv. Run:"
        err "  python3 $BOOTSTRAP_SCRIPT --no-mangement-ahu"
        exit 1
    fi
fi

# ── Set PYTHONPATH so `import nexus` resolves without editable install ────────
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
info "PYTHONPATH   : $PYTHONPATH"

# ── Activate venv ─────────────────────────────────────────────────────────────
# shellcheck disable=SC1090
source "$VENV_ACTIVATE"
ok "Activated venv : $VENV_DIR"
info "Python        : $(python --version)"

# ── Launch worker ─────────────────────────────────────────────────────────────
ok "Starting worker …"
echo "──────────────────────────────────────────────────────────"
exec python "$WORKER_SCRIPT" "$@"
