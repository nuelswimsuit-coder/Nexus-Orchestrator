"""
Temporary swarm test — enqueue five system.echo tasks via ARQ to verify
the Windows laptop worker consumes jobs from Redis.

Usage (from repo root):
    python scripts/test_swarm_injection.py

Redis defaults to 10.100.102.8 unless REDIS_URL is set (e.g. in .env).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.getcwd())

from arq import create_pool
from arq.connections import RedisSettings

from nexus.shared.schemas import TaskPayload

DEFAULT_REDIS_URL = "redis://10.100.102.8:6379/0"
QUEUE_NAME = "nexus:tasks"

ECHO_MESSAGE = "HELLO FROM MASTER! Jacob Hatan is in control."


async def main() -> None:
    redis_url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL).strip()
    if not redis_url:
        print("REDIS_URL is empty; set it or use default broker.", file=sys.stderr)
        raise SystemExit(1)

    base_ts_ms = int(time.time() * 1000)
    pool = await create_pool(
        RedisSettings.from_dsn(redis_url),
        default_queue_name=QUEUE_NAME,
    )
    try:
        for i in range(5):
            job_id = f"swarm-echo-{base_ts_ms}-{i}"
            payload = TaskPayload(
                task_id=job_id,
                task_type="system.echo",
                parameters={"message": ECHO_MESSAGE},
                project_id="swarm-injection-test",
            )
            wire = payload.model_dump_for_wire()
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=wire,
                _job_id=job_id,
                _queue_name=QUEUE_NAME,
            )
            status = "enqueued" if job is not None else "duplicate_skipped"
            print(f"[{i + 1}/5] job_id={job_id} -> {status}")
    finally:
        await pool.aclose()


if __name__ == "__main__":
    asyncio.run(main())
