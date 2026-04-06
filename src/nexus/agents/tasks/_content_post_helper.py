"""
Subprocess helper — posts AI-generated content to a Telegram group.

Invoked by content_factory.py via subprocess.run().
Adds the Mangement Ahu project to sys.path, loads a Telethon session,
and sends the message + optional photo.
Prints a JSON result line to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path


def _nexus_repo_root_for_media_opsec() -> Path | None:
    p = Path(__file__).resolve().parent
    for _ in range(12):
        if (p / "nexus" / "services" / "media_opsec.py").is_file():
            return p
        if p.parent == p:
            break
        p = p.parent
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",  required=True)
    parser.add_argument("--group",    required=True)
    parser.add_argument("--text",     required=True)
    parser.add_argument("--image",    default="")
    parser.add_argument("--api-key",  default="")
    args, _ = parser.parse_known_args()

    if args.project not in sys.path:
        sys.path.insert(0, args.project)
    os.chdir(args.project)

    result = asyncio.run(_post(
        group=args.group,
        text=args.text,
        image=args.image or None,
    ))
    print(json.dumps(result))


async def _post(group: str, text: str, image: str | None) -> dict:
    try:
        from io import BytesIO

        from app.services.telegram.manager import SessionManager  # type: ignore[import]
        from app.utils.paths import SESSIONS_DIR  # type: ignore[import]

        # Use manager sessions (owner accounts) for posting
        managers_dir = Path(SESSIONS_DIR) / "managers"
        sessions_dir = managers_dir if managers_dir.exists() else SESSIONS_DIR
        mgr = SessionManager(sessions_dir=sessions_dir)
        clients = await mgr.load_sessions()

        if not clients:
            return {"success": False, "message_id": None, "error": "No manager sessions found"}

        client = clients[0]
        entity = await client.get_entity(group)

        if image and Path(image).exists():
            img_path = Path(image)
            root = _nexus_repo_root_for_media_opsec()
            if root is not None:
                if str(root) not in sys.path:
                    sys.path.insert(0, str(root))
                try:
                    from nexus.services.media_opsec import (  # type: ignore[import-untyped]
                        make_image_upload_salt_seed,
                        prepare_jpeg_png_for_telegram_upload,
                    )
                    from nexus.services.recent_news_digest import (  # type: ignore[import-untyped]
                        telegram_image_filename_from_bytes,
                    )

                    raw_b = img_path.read_bytes()
                    salt = make_image_upload_salt_seed(img_path.stem)
                    raw_b, _ = prepare_jpeg_png_for_telegram_upload(raw_b, salt_seed=salt)
                    fn = telegram_image_filename_from_bytes(raw_b)
                    msg = await client.send_file(
                        entity, file=(fn, BytesIO(raw_b)), caption=text
                    )
                except Exception:
                    msg = await client.send_file(entity, str(img_path), caption=text)
            else:
                msg = await client.send_file(entity, str(img_path), caption=text)
        else:
            msg = await client.send_message(entity, text)

        # Gracefully disconnect all sessions
        for c in clients:
            try:
                await c.disconnect()
            except Exception:
                pass

        return {"success": True, "message_id": getattr(msg, "id", None), "error": None}

    except ImportError as exc:
        return {"success": False, "message_id": None, "error": f"ImportError: {exc}"}
    except Exception as exc:
        return {"success": False, "message_id": None, "error": str(exc)}


if __name__ == "__main__":
    main()
