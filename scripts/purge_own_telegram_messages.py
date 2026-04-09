#!/usr/bin/env python3
"""
מחיקת הודעות שנשלחו *מאותו חשבון* (הודעות יוצאות) בצ׳אטים שנבחרו.

שימוש אחרי חשד לסשן שנפרץ — מריץ מקומית עם קובץ סשן תקין ב-vault.
לא מוחק הודעות של משתמשים אחרים; לא מוחק אם אין הרשאת מחיקה.

דוגמאות
-------
  # יבש — רק ספירה
  python scripts/purge_own_telegram_messages.py --session-stem 12367085997 --chats fixy0rnt0z neiajakkso --dry-run

  # מחיקה בפועל
  python scripts/purge_own_telegram_messages.py --session-stem 12367085997 --chats fixy0rnt0z neiajakkso

  # כל מגה־קבוצות/ערוצים בדיאלוגים (זהירות — הרבה קריאות API)
  python scripts/purge_own_telegram_messages.py --session-stem 12367085997 --all-megagroups --limit 200

נדרש: TELEGRAM_API_ID + TELEGRAM_API_HASH (או TELEFIX_*) ב-.env, וקובץ <stem>.session + <stem>.json לידו.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Repo root on sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _is_own_message(msg: object, me_id: int) -> bool:
    """הודעה יוצאת או עם sender_id של המשתמש (סופרגרופ / פרטי)."""
    if not msg:
        return False
    if bool(getattr(msg, "out", False)):
        return True
    sid = getattr(msg, "sender_id", None)
    if sid is None:
        return False
    try:
        return int(sid) == int(me_id)
    except (TypeError, ValueError):
        return False


async def _purge_one_chat(
    client,
    entity,
    *,
    me: object,
    limit: int,
    dry_run: bool,
    verbose: bool,
) -> tuple[int, list[str]]:
    errs: list[str] = []
    collected: list[int] = []
    scanned = 0
    me_id = int(getattr(me, "id", 0) or 0)
    try:
        # Telethon: from_user=User מחזיר רק הודעות שנשלחו מהחשבון הזה
        async for msg in client.iter_messages(entity, from_user=me, limit=limit):
            scanned += 1
            if not msg or not msg.id:
                continue
            collected.append(msg.id)
            if verbose and len(collected) <= 3:
                print(f"    [verbose] own msg id={msg.id} out={getattr(msg, 'out', None)} sender_id={getattr(msg, 'sender_id', None)}")
    except TypeError:
        # גרסאות ישנות / ישות בלי from_user — נסה סריקה ידנית
        try:
            async for msg in client.iter_messages(entity, limit=min(limit * 5, 20000)):
                scanned += 1
                if not msg or not msg.id:
                    continue
                if not _is_own_message(msg, me_id):
                    continue
                collected.append(msg.id)
                if len(collected) >= limit:
                    break
        except Exception as exc:
            return 0, [f"iter_fallback:{exc!s}"]
    except Exception as exc:
        return 0, [f"iter:{exc!s}"]

    if verbose:
        print(f"    scanned={scanned} own_match={len(collected)}")

    deleted = 0
    if dry_run:
        return len(collected), [f"dry_run would delete {len(collected)} msgs"]

    for i in range(0, len(collected), 100):
        batch = collected[i : i + 100]
        try:
            await client.delete_messages(entity, batch)
            deleted += len(batch)
            if verbose:
                print(f"    deleted batch ids {batch[0]}..{batch[-1]} ({len(batch)} msgs)")
            await asyncio.sleep(0.5)
        except Exception as exc:
            errs.append(f"delete:{exc!s}")
    return deleted, errs


async def _run(args: argparse.Namespace) -> int:
    from telethon import utils as tg_utils  # type: ignore[import-untyped]
    from telethon.tl.types import Channel  # type: ignore[import-untyped]

    from nexus.worker.services.tg_session import async_telegram_client

    print(
        "NOTE: This script runs only on your PC. "
        "Nothing in Cursor/Cloud deletes Telegram messages for you.\n",
        flush=True,
    )

    stem = args.session_stem.strip()
    vault = _ROOT / "vault" / "sessions"
    session_base = str(vault / stem)
    if not (vault / f"{stem}.session").is_file():
        print(f"ERROR: missing {vault / (stem + '.session')}", file=sys.stderr)
        return 2

    params: dict = {"session_stem": stem, "__secrets__": {}}

    async with async_telegram_client(session_base, params) as client:
        if not await client.is_user_authorized():
            print("ERROR: session not authorized", file=sys.stderr)
            return 2

        me = await client.get_me()
        print(f"Logged in as: {getattr(me, 'id', '?')} @{getattr(me, 'username', '') or 'no_username'}")

        targets: list[tuple[str, object]] = []

        if args.chats:
            for raw in args.chats:
                u = raw.strip().lstrip("@")
                if not u:
                    continue
                try:
                    ent = await client.get_entity(u)
                    targets.append((u, ent))
                except Exception as exc:
                    print(f"WARN: skip @{u}: {exc}", file=sys.stderr)

        if args.all_megagroups:
            async for d in client.iter_dialogs():
                e = d.entity
                if isinstance(e, Channel) and (e.megagroup or e.broadcast):
                    label = getattr(e, "username", None) or str(getattr(e, "id", ""))
                    targets.append((str(label), e))

        if not targets:
            print("No chats to process.", file=sys.stderr)
            return 1

        total_del = 0
        seen_peers: set[int] = set()
        for label, entity in targets:
            peer_id = tg_utils.get_peer_id(entity)
            if peer_id in seen_peers:
                continue
            seen_peers.add(peer_id)

            print(f"--- {label} (dry_run={args.dry_run}) ---")
            n, errs = await _purge_one_chat(
                client,
                entity,
                me=me,
                limit=args.limit,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            total_del += n
            print(f"  deleted_or_counted: {n}")
            for e in errs[:12]:
                print(f"  err: {e}")
            await asyncio.sleep(0.5)

        print(f"Done. Total messages removed/counted: {total_del}")
        if total_del == 0 and not args.dry_run:
            print(
                "\nIf zero deleted: wrong session stem (wrong account), "
                "spam was from other users/bots, or Telegram blocked delete (Flood/rights). "
                "Try --verbose and ensure --session-stem matches the account that posted.",
                file=sys.stderr,
            )
    return 0


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env", override=True)
    load_dotenv(_ROOT / "configs" / ".env", override=False)

    p = argparse.ArgumentParser(description="Purge own Telegram messages in selected chats.")
    p.add_argument(
        "--session-stem",
        required=True,
        help="Basename under vault/sessions/ (without .session), e.g. phone or session id",
    )
    p.add_argument(
        "--chats",
        nargs="*",
        default=[],
        help="Usernames without @ (e.g. fixy0rnt0z neiajakkso)",
    )
    p.add_argument(
        "--all-megagroups",
        action="store_true",
        help="Also iterate all megagroups/broadcast channels in dialogs",
    )
    p.add_argument("--limit", type=int, default=2000, help="Max own messages per chat (default 2000)")
    p.add_argument("--dry-run", action="store_true", help="Only count, do not delete")
    p.add_argument("--verbose", "-v", action="store_true", help="Print per-batch and sample ids")
    args = p.parse_args()

    if not args.chats and not args.all_megagroups:
        print("Specify --chats and/or --all-megagroups", file=sys.stderr)
        sys.exit(2)

    if not os.getenv("TELEGRAM_API_ID") and not os.getenv("TELEFIX_API_ID"):
        print("Set TELEGRAM_API_ID / TELEGRAM_API_HASH (or TELEFIX_*) in .env", file=sys.stderr)
        sys.exit(2)

    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
