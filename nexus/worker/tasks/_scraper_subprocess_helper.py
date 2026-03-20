"""
Subprocess helper — runs inside the Mangement Ahu project's Python environment.

This script is invoked by auto_scrape.py via subprocess.run() with:
    python _scraper_subprocess_helper.py --project <path> --sources <comma-list>

It adds the Mangement Ahu project to sys.path, imports the scraper engine,
runs a headless scrape of the given source groups, and prints a JSON result
line to stdout.

Running as a subprocess keeps Telethon and the bot's dependencies fully
isolated from the Nexus worker's venv.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="Path to Mangement Ahu root")
    parser.add_argument("--sources", required=True, help="Comma-separated group links")
    args = parser.parse_args()

    project_root = args.project
    source_links = [s.strip() for s in args.sources.split(",") if s.strip()]

    # Add the project to sys.path so its imports resolve.
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Change CWD so relative paths inside the project work.
    os.chdir(project_root)

    result = asyncio.run(_run(source_links))
    # Print JSON result as the last line of stdout — auto_scrape.py reads this.
    print(json.dumps(result))


async def _run(source_links: list[str]) -> dict:
    total_saved = 0
    errors: list[str] = []

    try:
        from app.database.repository import Repository  # type: ignore[import]
        from app.services.logic.scraper_engine import ScraperEngine  # type: ignore[import]

        # Ensure tables exist (idempotent).
        await Repository.create_tables()

        # Fetch full target objects for the requested links.
        all_sources = await Repository.get_targets(role="source")
        sources_to_scrape = [s for s in all_sources if s.link in source_links]

        if not sources_to_scrape:
            # Fall back to using the raw links directly.
            sources_to_scrape = source_links  # type: ignore[assignment]

        control = {"paused": False, "stopped": False}
        engine = ScraperEngine(
            sources=sources_to_scrape,
            filter_mode="all",
            chunk_size=200,
            max_parallel_sources=3,
            control=control,
        )

        async for status in engine.run():
            total_saved = status.total_saved

        # Record the run timestamp.
        await Repository.record_last_run("scraper")

    except ImportError as exc:
        errors.append(f"ImportError: {exc}")
    except Exception as exc:
        errors.append(str(exc))

    return {
        "users_saved": total_saved,
        "errors": errors,
        "success": len(errors) == 0,
    }


if __name__ == "__main__":
    main()
