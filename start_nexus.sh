#!/usr/bin/env bash
# Nexus Worker startup script for Linux nodes.
# Launched by the deployer after each sync; also usable manually.
#
# Usage:
#   bash start_nexus.sh
#   bash start_nexus.sh --master-host 10.100.102.8

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Activate virtual environment if present ────────────────────────────────────
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# ── Resolve master Redis host ──────────────────────────────────────────────────
# Default: 10.100.102.8 (fleet master Windows PC).
# Override via --master-host arg or MASTER_IP env var.
MASTER_HOST="${MASTER_IP:-10.100.102.8}"
for arg in "$@"; do
    case "$arg" in
        --master-host=*) MASTER_HOST="${arg#*=}" ;;
        --master-ip=*)   MASTER_HOST="${arg#*=}" ;;
    esac
done

# Force the correct Redis URL for the worker — overrides any [::1] from .env
export REDIS_URL="redis://${MASTER_HOST}:6379/0"
export MASTER_IP="$MASTER_HOST"
export REDIS_HOST="$MASTER_HOST"

echo "[start_nexus.sh] Master Redis: ${REDIS_URL}"
echo "[start_nexus.sh] Starting worker..."

exec python scripts/start_worker.py --master-host "$MASTER_HOST" "$@"
