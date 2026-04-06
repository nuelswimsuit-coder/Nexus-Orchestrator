"""
Subprocess helper — searches Telegram for groups matching keywords.

Uses Telethon's SearchRequest to find public groups/channels.
Filters by minimum member count.
Prints a JSON list of {title, link, member_count, niche} to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",     required=True)
    parser.add_argument("--keywords",    required=True)
    parser.add_argument("--min-members", type=int, default=500)
    parser.add_argument("--max-per-kw",  type=int, default=5)
    parser.add_argument("--api-id",      default="")
    parser.add_argument("--api-hash",    default="")
    args = parser.parse_args()

    if args.project not in sys.path:
        sys.path.insert(0, args.project)
    os.chdir(args.project)

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    results = asyncio.run(_search(
        keywords=keywords,
        min_members=args.min_members,
        max_per_kw=args.max_per_kw,
        api_id=args.api_id,
        api_hash=args.api_hash,
    ))
    print(json.dumps(results))


async def _search(
    keywords: list[str],
    min_members: int,
    max_per_kw: int,
    api_id: str,
    api_hash: str,
) -> list[dict]:
    """
    Search Telegram for groups matching each keyword.
    Uses the first available manager session for authentication.
    """
    results: list[dict] = []
    seen_links: set[str] = set()

    try:
        from pathlib import Path

        from app.utils.paths import SESSIONS_DIR  # type: ignore[import]
        from telethon import TelegramClient, functions, types  # type: ignore[import]
        from telethon.errors import FloodWaitError  # type: ignore[import]

        # Resolve API credentials
        resolved_api_id = int(api_id) if api_id else 0
        resolved_api_hash = api_hash or ""

        if not resolved_api_id or not resolved_api_hash:
            # Try to load from the project's config
            try:
                from app.config.settings import config  # type: ignore[import]
                resolved_api_id   = int(getattr(config, "API_ID", 0) or 0)
                resolved_api_hash = str(getattr(config, "API_HASH", "") or "")
            except Exception:
                pass

        if not resolved_api_id or not resolved_api_hash:
            print(json.dumps([]))
            return []

        # Find a manager session file
        managers_dir = Path(SESSIONS_DIR) / "managers"
        session_files = list(managers_dir.glob("*.session")) if managers_dir.exists() else []
        if not session_files:
            session_files = list(Path(SESSIONS_DIR).glob("*.session"))
        if not session_files:
            print(json.dumps([]))
            return []

        session_path = str(session_files[0].with_suffix(""))

        async with TelegramClient(session_path, resolved_api_id, resolved_api_hash) as client:
            import logging

            log_discovery = logging.getLogger("discovery")

            for keyword in keywords:
                await asyncio.sleep(1.0)
                for attempt in range(8):
                    try:
                        if not client.is_connected():
                            await client.connect()
                        result = await client(functions.contacts.SearchRequest(
                            q=keyword,
                            limit=max_per_kw * 2,  # fetch extra, then filter
                        ))
                        for chat in result.chats:
                            if not isinstance(chat, (types.Channel, types.Chat)):
                                continue
                            member_count = getattr(chat, "participants_count", 0) or 0
                            if member_count < min_members:
                                continue

                            username = getattr(chat, "username", None)
                            if not username:
                                continue

                            link = f"https://t.me/{username}"
                            if link in seen_links:
                                continue
                            seen_links.add(link)

                            results.append({
                                "title":        getattr(chat, "title", username),
                                "link":         link,
                                "member_count": member_count,
                                "niche":        keyword,
                            })

                            if len([r for r in results if r["niche"] == keyword]) >= max_per_kw:
                                break
                        break
                    except FloodWaitError as exc:
                        wait_s = min(
                            max(1, int(getattr(exc, "seconds", 60) or 60)),
                            3600,
                        )
                        log_discovery.warning(
                            "FloodWait on SearchRequest for %r — sleeping %ss (attempt %s)",
                            keyword,
                            wait_s,
                            attempt + 1,
                        )
                        await asyncio.sleep(wait_s)
                    except (ConnectionError, OSError) as exc:
                        log_discovery.warning(
                            "Connection lost during search for %r: %s — reconnecting (attempt %s)",
                            keyword,
                            exc,
                            attempt + 1,
                        )
                        try:
                            await client.connect()
                        except Exception:
                            pass
                        await asyncio.sleep(0.75)
                    except Exception as exc:
                        log_discovery.warning("Search failed for %r: %s", keyword, exc)
                        break

    except ImportError as exc:
        import logging
        logging.getLogger("discovery").error(f"ImportError: {exc}")
    except Exception as exc:
        import logging
        logging.getLogger("discovery").error(f"Discovery error: {exc}")

    return results


if __name__ == "__main__":
    main()
