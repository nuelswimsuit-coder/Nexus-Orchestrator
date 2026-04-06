"""
CLI entrypoint for ``nexus-push`` — cluster deploy via the same DeployerService
used by POST /api/deploy/cluster.

Unreachable workers produce ``skipped:`` results; other nodes still deploy.
Exits 0 when at least one worker returns ``ok`` and none return ``error:``.
Exits 1 if every target failed, skipped, or returned an error.
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
            print("[nexus-push] Failure: no deploy targets (check workers.json / WORKER_IP / Redis).", file=sys.stderr)
            raise SystemExit(1)

        errors = {k: v for k, v in results.items() if str(v).startswith("error:")}
        if errors:
            print(
                "[nexus-push] Failure: one or more workers returned an error — "
                f"{errors}",
                file=sys.stderr,
            )
            raise SystemExit(1)

        if not any(v == "ok" for v in results.values()):
            print(
                "[nexus-push] Failure: no worker returned ok (all unreachable or skipped).",
                file=sys.stderr,
            )
            raise SystemExit(1)

        skipped = {k: v for k, v in results.items() if str(v).startswith("skipped:")}
        if skipped:
            print(
                "[nexus-push] Warning: some workers were skipped (offline / SSH unreachable): "
                f"{skipped}",
                file=sys.stderr,
            )

        print("[nexus-push] Success: deploy finished (ok on all reachable workers).")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    main()
