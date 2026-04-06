"""
nexus-push — rolling cluster deploy over SSH (CLI).

Runs the same :meth:`DeployerService.deploy_all` path as ``POST /api/deploy/cluster``.
One unreachable worker (closed port 22, bad auth, etc.) must not abort the rest:
each target is isolated; this process exits with a non-zero code only if any node
returns a result starting with ``error:``.

Usage
-----
    nexus-push
    nexus-push --node-ids worker_linux worker_other
    python scripts/nexus_push.py --force   # ``--force`` accepted for supervisor compatibility

Environment: same as the API deployer (``REDIS_URL``, ``WORKER_SSH_*``, ``WORKER_IP``, …).
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env", override=False)
_configs = _PROJECT_ROOT / "configs" / ".env"
if _configs.is_file():
    load_dotenv(_configs, override=False)

from redis.asyncio import Redis  # noqa: E402

from nexus.master.services.deployer import DeployerService  # noqa: E402
from nexus.master.services.vault import Vault  # noqa: E402
from nexus.shared import redis_util  # noqa: E402
from nexus.shared.config import settings  # noqa: E402


def _build_redis() -> Redis:
    url = redis_util.coerce_redis_url_for_platform(settings.redis_url)
    return Redis.from_url(url, decode_responses=True)


def _make_deployer(redis: Redis) -> DeployerService:
    vault = Vault()
    if settings.worker_ssh_user:
        vault._backend.set("WORKER_SSH_USER", settings.worker_ssh_user)
    if settings.worker_ssh_password:
        vault._backend.set("WORKER_SSH_PASSWORD", settings.worker_ssh_password)
    return DeployerService(redis=redis, vault=vault, settings=settings)


def _result_is_hard_failure(value: str) -> bool:
    return str(value).startswith("error:")


async def _async_main(node_ids: list[str] | None) -> int:
    redis = _build_redis()
    try:
        await redis.ping()
    except Exception as exc:
        print(f"[nexus-push] Redis unreachable: {exc}", file=sys.stderr)
        return 2

    try:
        deployer = _make_deployer(redis)
        results = await deployer.deploy_all(node_ids=node_ids)
        failures = {k: v for k, v in results.items() if _result_is_hard_failure(v)}

        for nid, res in sorted(results.items()):
            tag = "FAIL" if _result_is_hard_failure(res) else "ok"
            print(f"[nexus-push] {tag}  {nid}: {res}", file=sys.stderr)

        if failures:
            print(
                f"[nexus-push] Finished with {len(failures)} error(s); not a full success.",
                file=sys.stderr,
            )
            return 1

        print("[nexus-push] Success — no hard failures (skipped hosts are OK).", file=sys.stderr)
        return 0
    finally:
        await redis.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Nexus cluster SSH deploy (nexus-push).")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignored; kept for compatibility with supervisor / recovery wrappers.",
    )
    parser.add_argument(
        "--node-ids",
        nargs="*",
        default=None,
        help="Optional explicit worker node IDs; default = all active targets.",
    )
    args = parser.parse_args()
    if args.force:
        print("[nexus-push] --force set (full cluster deploy).", file=sys.stderr)

    rc = asyncio.run(_async_main(args.node_ids))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
