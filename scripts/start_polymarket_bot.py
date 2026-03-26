"""
Polymarket Bot Tick Loop — dispatches trading.polymarket_bot_tick every N seconds.

Runs as a standalone service inside the Nexus launcher (the "polymarket" panel).
Replaces the one-shot nexus_core --task dispatch with a proper recurring loop
so the CLOB heartbeat stays alive and the dashboard shows "מחובר".
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Ensure repo root is on sys.path regardless of cwd.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Load .env before importing nexus packages.
_env_file = _ROOT / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _, _v = _line.partition("=")
        _k = _k.strip()
        _v = _v.strip().split("#")[0].strip()
        if _k and _k not in os.environ:
            os.environ[_k] = _v

_TICK_INTERVAL_S = float(os.environ.get("POLYMARKET_BOT_TICK_INTERVAL_S", "20"))
_STARTUP_DELAY_S = float(os.environ.get("POLYMARKET_BOT_STARTUP_DELAY_S", "10"))


async def _dispatch_loop() -> None:
    from arq import create_pool
    from arq.connections import RedisSettings

    from nexus.shared.redis_util import apply_redis_url_to_environment
    apply_redis_url_to_environment()

    from nexus.shared.config import settings

    ARQ_QUEUE_NAME = "nexus:tasks"

    redis_url = os.environ.get("REDIS_URL", settings.redis_url)

    # Parse host from URL for arq RedisSettings.
    from urllib.parse import urlparse as _urlparse
    _parsed = _urlparse(redis_url)
    _host = (_parsed.hostname or "127.0.0.1").strip("[]")
    _port = _parsed.port or 6379
    _db   = int((_parsed.path or "/0").lstrip("/") or "0")

    rs = RedisSettings(host=_host, port=_port, database=_db)

    print(f"[polymarket-bot] Starting tick loop — interval={_TICK_INTERVAL_S}s  redis={_host}:{_port}/{_db}", flush=True)
    await asyncio.sleep(_STARTUP_DELAY_S)

    while True:
        pool = None
        try:
            pool = await create_pool(rs, default_queue_name=ARQ_QUEUE_NAME)
            params = {
                "max_bet_usd":   float(os.environ.get("POLYMARKET_BOT_MAX_BET_USD", "10")),
                "yes_ceiling":   float(os.environ.get("POLYMARKET_BOT_YES_CEILING", "0.40")),
                "proximity_pct": float(os.environ.get("POLYMARKET_BOT_PROXIMITY", "0.005")),
                "stop_loss_pct": float(os.environ.get("POLYMARKET_BOT_STOP_LOSS", "0.20")),
            }
            from nexus.shared.schemas import TaskPayload
            payload = TaskPayload(
                task_type="trading.polymarket_bot_tick",
                parameters=params,
                project_id="nexus-poly-trader",
                priority=8,
            )
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=payload.model_dump_for_wire(),
                _job_id=payload.task_id,
                _queue_name=ARQ_QUEUE_NAME,
            )
            jid = job.job_id if job else payload.task_id
            print(f"[polymarket-bot] tick dispatched job_id={jid}", flush=True)
        except Exception as exc:
            print(f"[polymarket-bot] dispatch error: {exc}", flush=True)
        finally:
            if pool is not None:
                try:
                    await pool.aclose()
                except Exception:
                    pass

        await asyncio.sleep(_TICK_INTERVAL_S)


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(_dispatch_loop())
    except KeyboardInterrupt:
        print("[polymarket-bot] stopped.", flush=True)


if __name__ == "__main__":
    main()
