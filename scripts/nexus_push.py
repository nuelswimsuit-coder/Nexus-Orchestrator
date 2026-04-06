"""
CLI entrypoint for ``nexus-push`` — cluster deploy via the same DeployerService
used by POST /api/deploy/cluster.

Exits with code 0 only when every targeted worker returns ``ok`` (not ``skipped:``
or ``error:``). Unreachable hosts are logged per node and do not abort other workers.
"""

from __future__ import annotations

import argparse
import asyncio
import sys


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Push Nexus worker bundle to all cluster nodes over SSH (nexus-push).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Ignored compatibility flag (supervisor); always runs a full deploy.",
    )
    p.add_argument(
        "--node",
        action="append",
        dest="node_ids",
        metavar="NODE_ID",
        help="Restrict to specific heartbeat node_id(s). May be passed multiple times.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        print("[nexus-push] interrupted", file=sys.stderr)
        raise SystemExit(130) from None


async def _async_main(args: argparse.Namespace) -> None:
    from redis.asyncio import Redis as AIORedis

    from nexus.master.services.deployer import DeployerService
    from nexus.master.services.vault import Vault
    from nexus.shared.config import settings

    redis = AIORedis.from_url(settings.redis_url, decode_responses=True)
    try:
        vault = Vault()
        if settings.worker_ssh_user:
            vault._backend.set("WORKER_SSH_USER", settings.worker_ssh_user)
        if settings.worker_ssh_password:
            vault._backend.set("WORKER_SSH_PASSWORD", settings.worker_ssh_password)

        deployer = DeployerService(redis=redis, vault=vault, settings=settings)
        targets = args.node_ids if args.node_ids else None
        results = await deployer.deploy_all(node_ids=targets)

        for nid, res in sorted(results.items()):
            line = f"  {nid}: {res}"
            print(f"[nexus-push] {line}")

        if not results:
            print("[nexus-push] Failure: no deploy targets (check WORKER_IP / Redis heartbeats).", file=sys.stderr)
            raise SystemExit(1)

        bad = {k: v for k, v in results.items() if v != "ok"}
        if bad:
            print(
                "[nexus-push] Failure: not all workers returned ok — "
                f"failed or skipped: {bad}",
                file=sys.stderr,
            )
            raise SystemExit(1)

        print("[nexus-push] Success: all workers deployed.")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    main()
