"""
CLI entrypoint for management.sentinel_seo (Telethon sync in worker thread).

Usage:
  python scripts/sentinel_seo_check.py

Requires NEXUS_SEO_PROBE_SESSION (session stem under data/staged_accounts) and
populated group_metadata in telefix.db (run management.group_health_scan first).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


async def _main() -> int:
    from nexus.worker.tasks.sentinel_seo import sentinel_seo

    out = await sentinel_seo({})
    print(out)
    return 0 if out.get("status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
