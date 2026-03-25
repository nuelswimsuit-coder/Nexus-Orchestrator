"""
_openclaw_verify_helper.py — Subprocess helper for Telegram account verification.

Called by openclaw.browser_scrape to check whether scraped leads have active
Telegram accounts.  Runs in a subprocess so Telethon's dependencies stay
isolated from the main Nexus venv (same pattern as _scraper_subprocess_helper.py).

Usage (internal — called by openclaw.py)
-----------------------------------------
    python _openclaw_verify_helper.py \
        --project /path/to/Mangement-Ahu \
        --leads   /tmp/leads_in.json \
        --output  /tmp/leads_verified.json

Input JSON (--leads)
--------------------
[
  {"name": "...", "phone": "+972501234567", "username": "johndoe", ...},
  ...
]

Output JSON (--output)
----------------------
[
  {"telegram_id": 123456789},   # verified — has Telegram
  {"telegram_id": null},        # not found
  ...
]
One entry per input lead, in the same order.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _setup_path(project_path: str) -> None:
    """Add Mangement Ahu to sys.path so we can import its Telethon sessions."""
    p = str(Path(project_path).resolve())
    if p not in sys.path:
        sys.path.insert(0, p)


def _load_session(project_path: str):  # type: ignore[return]
    """
    Load the first available Telethon session from the project's sessions dir.
    Returns a connected TelegramClient or None.
    """
    try:
        from telethon.sync import TelegramClient  # type: ignore[import-untyped]
        import glob

        sessions_dir = Path(project_path) / "sessions"
        session_files = glob.glob(str(sessions_dir / "**" / "*.json"), recursive=True)

        if not session_files:
            print("[openclaw_verify] No session files found", file=sys.stderr)
            return None

        # Load the first valid session JSON
        for sf in session_files[:3]:
            try:
                with open(sf, encoding="utf-8") as f:
                    sess = json.load(f)
                api_id   = int(sess.get("api_id", 0))
                api_hash = sess.get("api_hash", "")
                session_name = Path(sf).stem

                if not api_id or not api_hash:
                    continue

                session_path = str(Path(sf).parent / session_name)
                client = TelegramClient(session_path, api_id, api_hash)
                client.connect()
                if client.is_user_authorized():
                    return client
                client.disconnect()
            except Exception as e:
                print(f"[openclaw_verify] Session load error: {e}", file=sys.stderr)

        return None
    except ImportError:
        print("[openclaw_verify] telethon not installed", file=sys.stderr)
        return None


def _verify_phone(client, phone: str) -> int | None:
    """Try to resolve a phone number to a Telegram user_id."""
    try:
        from telethon.tl.functions.contacts import ImportContactsRequest  # type: ignore
        from telethon.tl.types import InputPhoneContact  # type: ignore

        contact = InputPhoneContact(
            client_id=0,
            phone=phone,
            first_name="Lead",
            last_name="",
        )
        result = client(ImportContactsRequest([contact]))
        if result.users:
            user_id = result.users[0].id
            # Clean up — delete the imported contact immediately
            try:
                from telethon.tl.functions.contacts import DeleteContactsRequest  # type: ignore
                client(DeleteContactsRequest(id=[result.users[0]]))
            except Exception:
                pass
            return user_id
    except Exception as e:
        print(f"[openclaw_verify] phone lookup error: {e}", file=sys.stderr)
    return None


def _verify_username(client, username: str) -> int | None:
    """Try to resolve a @username to a Telegram user_id."""
    try:
        from telethon.tl.functions.contacts import ResolveUsernameRequest  # type: ignore

        result = client(ResolveUsernameRequest(username))
        if result.users:
            return result.users[0].id
    except Exception as e:
        print(f"[openclaw_verify] username lookup error: {e}", file=sys.stderr)
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--leads",   required=True)
    parser.add_argument("--output",  required=True)
    args, _ = parser.parse_known_args()

    _setup_path(args.project)

    with open(args.leads, encoding="utf-8") as f:
        leads = json.load(f)

    results: list[dict] = [{"telegram_id": None}] * len(leads)

    client = _load_session(args.project)
    if client is None:
        print("[openclaw_verify] No usable Telethon session — all leads unverified",
              file=sys.stderr)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f)
        return

    try:
        for i, lead in enumerate(leads):
            phone    = (lead.get("phone") or "").strip()
            username = (lead.get("username") or "").strip()

            tg_id: int | None = None

            # Priority: phone number first (more reliable), then username
            if phone and phone.startswith("+"):
                tg_id = _verify_phone(client, phone)

            if tg_id is None and username:
                tg_id = _verify_username(client, username)

            results[i] = {"telegram_id": tg_id}

            if tg_id:
                print(f"[openclaw_verify] {i+1}/{len(leads)} ✓ {lead.get('name','?')} → {tg_id}")
            else:
                print(f"[openclaw_verify] {i+1}/{len(leads)} ✗ {lead.get('name','?')}")

    finally:
        try:
            client.disconnect()
        except Exception:
            pass

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f)

    print(f"[openclaw_verify] Done. {sum(1 for r in results if r['telegram_id'])} verified.")


if __name__ == "__main__":
    main()
