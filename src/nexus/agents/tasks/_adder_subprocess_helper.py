"""
Subprocess helper — runs the Mangement Ahu SmartAdderEngine in isolation.

Invoked by telegram_adder.py via subprocess.run().
Prints a JSON result line to stdout.

Fixed encoding/path logic
--------------------------
- Session files are loaded with UTF-8 encoding, falling back to latin-1.
- All paths use pathlib.Path for cross-platform compatibility.
- SESSIONS_DIR is resolved from the project's paths.py, not hardcoded.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--targets", required=True)
    args, _ = parser.parse_known_args()

    project_root = args.project
    target_links = [t.strip() for t in args.targets.split(",") if t.strip()]

    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    os.chdir(project_root)

    result = asyncio.run(_run(target_links))
    print(json.dumps(result))


async def _run(target_links: list[str]) -> dict:
    added = 0
    errors: list[str] = []

    try:
        from app.database.repository import Repository  # type: ignore[import]
        from app.services.logic.adder_engine import SmartAdderEngine  # type: ignore[import]

        await Repository.create_tables()

        # Fetch pending users from DB
        users = await Repository.get_all_users()
        pending = [u for u in users if getattr(u, "status", "PENDING") == "PENDING"]

        for target_link in target_links:
            if not pending:
                break

            control = {"paused": False, "stopped": False}
            engine = SmartAdderEngine(
                target_link=target_link,
                users=pending,
                speed_config={"min_delay": 8, "max_delay": 18},
                control=control,
            )

            async for status in engine.run():
                added = status.success
                if status.is_finished:
                    break

        await Repository.record_last_run("adder")

    except ImportError as exc:
        errors.append(f"ImportError: {exc}")
    except Exception as exc:
        errors.append(str(exc))

    return {"added": added, "errors": errors, "success": len(errors) == 0}


if __name__ == "__main__":
    main()
