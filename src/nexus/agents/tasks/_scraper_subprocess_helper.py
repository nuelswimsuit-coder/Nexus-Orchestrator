"""
Subprocess helper — runs inside the Mangement Ahu project's Python environment.

This script is invoked by auto_scrape.py via subprocess.run() with:
    python _scraper_subprocess_helper.py --project <path> --sources <comma-list>
                                         [--task-id <id>]

It adds the Mangement Ahu project to sys.path, imports the scraper engine,
runs a headless scrape of the given source groups, and prints a JSON result
line to stdout.

Running as a subprocess keeps Telethon and the bot's dependencies fully
isolated from the Nexus worker's venv.

Checkpoint / Resume
-------------------
When ``--task-id`` is supplied the helper writes per-source checkpoints via
``nexus.shared.checkpoint_store`` so that a crash mid-scrape can be resumed:

- Before scraping a source  → mark_running("source:<link>")
- After success             → mark_done("source:<link>", {"users_saved": N})
- After failure             → mark_failed("source:<link>", "<error>")

On restart, sources that are already ``done`` are skipped automatically.
Sources that were ``running`` (i.e. the process was killed mid-scrape) are
reset to ``pending`` and re-scraped.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path


def _add_nexus_to_path() -> None:
    """Ensure the Nexus src/ tree is importable from this subprocess."""
    here = Path(__file__).resolve()
    # Walk up to find the src/ directory that contains nexus/
    for parent in here.parents:
        candidate = parent / "src"
        if (candidate / "nexus").is_dir():
            src_str = str(candidate)
            if src_str not in sys.path:
                sys.path.insert(0, src_str)
            return
    # Fallback: add repo root
    repo_root = str(here.parents[4])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="Path to Mangement Ahu root")
    parser.add_argument("--sources", required=True, help="Comma-separated group links")
    parser.add_argument("--task-id", default="", help="ARQ task_id for checkpoint tracking")
    args, _ = parser.parse_known_args()

    project_root = args.project
    source_links = [s.strip() for s in args.sources.split(",") if s.strip()]
    task_id = args.task_id.strip()

    # Add the project to sys.path so its imports resolve.
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Ensure Nexus shared modules are importable for checkpoint_store.
    _add_nexus_to_path()

    # Change CWD so relative paths inside the project work.
    os.chdir(project_root)

    result = asyncio.run(_run(source_links, task_id=task_id))
    # Print JSON result as the last line of stdout — auto_scrape.py reads this.
    print(json.dumps(result))


async def _run(source_links: list[str], *, task_id: str = "") -> dict:
    total_saved = 0
    errors: list[str] = []

    # ── Checkpoint store (no-op when task_id is empty) ────────────────────────
    store = None
    if task_id:
        try:
            from nexus.shared.checkpoint_store import CheckpointStore  # type: ignore[import]
            store = CheckpointStore(task_id)
            # Reset any sources that were "running" when the process last crashed.
            stale = store.reset_stale_running()
            if stale:
                print(f"[checkpoint] reset {stale} stale-running source(s) → pending", flush=True)
        except Exception as cp_err:
            print(f"[checkpoint] init failed (non-fatal): {cp_err}", flush=True)
            store = None

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

        # ── Resume: skip sources already completed in a previous run ──────────
        if store is not None:
            done_keys = store.get_done_steps(prefix="source:")
            done_links = {k[len("source:"):] for k in done_keys}
            if done_links:
                before = len(sources_to_scrape)
                sources_to_scrape = [
                    s for s in sources_to_scrape
                    if (s.link if hasattr(s, "link") else str(s)) not in done_links
                ]
                skipped = before - len(sources_to_scrape)
                if skipped:
                    print(
                        f"[checkpoint] resuming — skipping {skipped} already-done source(s)",
                        flush=True,
                    )
                    # Count previously saved users from checkpoint payloads
                    for key in done_keys:
                        step = store.get_step(key)
                        if step and step.get("payload"):
                            try:
                                prev = json.loads(step["payload"])
                                total_saved += int(prev.get("users_saved", 0))
                            except Exception:
                                pass

        # ── Scrape each source with per-source checkpointing ──────────────────
        for source in sources_to_scrape:
            link = source.link if hasattr(source, "link") else str(source)
            step_key = f"source:{link}"

            if store is not None:
                store.mark_running(step_key)

            source_saved = 0
            source_error: str | None = None
            try:
                control = {"paused": False, "stopped": False}
                engine = ScraperEngine(
                    sources=[source],
                    filter_mode="all",
                    chunk_size=200,
                    max_parallel_sources=1,
                    control=control,
                )
                async for status in engine.run():
                    source_saved = status.total_saved

                total_saved += source_saved
                if store is not None:
                    store.mark_done(step_key, {"users_saved": source_saved})

            except Exception as exc:
                source_error = str(exc)
                errors.append(f"{link}: {source_error}")
                if store is not None:
                    store.mark_failed(step_key, source_error)

        # Record the run timestamp (only if at least one source succeeded).
        if len(errors) < len(sources_to_scrape) or not sources_to_scrape:
            await Repository.record_last_run("scraper")

        # ── Clear checkpoints on full success ─────────────────────────────────
        if store is not None and not errors:
            store.clear()

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
