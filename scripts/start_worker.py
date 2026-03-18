"""
Worker Node entrypoint.

Usage
-----
    python scripts/start_worker.py

Or via the installed CLI entrypoint (after `pip install -e .`):
    nexus-worker

Deploy this script (along with the full `nexus/` package) to each Worker Node.
Set REDIS_URL and NODE_ID in the .env file on each machine.

What this script does
---------------------
1. Loads settings from .env.
2. Configures structured logging.
3. Starts the ARQ worker process which polls Redis for tasks and executes them
   by calling `execute_task` in nexus/worker/listener.py.
"""

from __future__ import annotations

import structlog
from arq import run_worker

from nexus.shared.config import settings
from nexus.shared.logging_config import configure_logging
from nexus.worker.listener import WorkerSettings

log = structlog.get_logger(__name__)


def main() -> None:
    configure_logging(level=settings.log_level, node_id=settings.node_id)
    log.info("nexus_worker_starting", node_id=settings.node_id, redis=settings.redis_url)

    # `run_worker` is ARQ's blocking worker loop.  It handles:
    #   - Connecting to Redis
    #   - Polling the queue
    #   - Calling execute_task for each job
    #   - Graceful shutdown on SIGTERM / Ctrl-C
    run_worker(WorkerSettings)


if __name__ == "__main__":
    main()
