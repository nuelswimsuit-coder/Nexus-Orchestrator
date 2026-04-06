"""
Detect whether a Telegram group message was sent by a user who is a channel/group
creator or admin (for de-escalation when generating swarm replies).

Anonymous admin posts: the visible sender may be the channel entity — treated as privileged.
"""

from __future__ import annotations

from typing import Any


async def sender_of_message_is_owner_or_admin(client: Any, entity: Any, message_id: int) -> bool:
    """
    Return True if the author of ``message_id`` in ``entity`` is owner, admin, or a channel actor.
    On RPC errors or missing data, returns False (fail-open for normal behavior).
    """
    try:
        mid = int(message_id)
    except (TypeError, ValueError):
        return False
    if mid <= 0:
        return False

    try:
        msgs = await client.get_messages(entity, ids=mid)
    except Exception:
        return False
    m0 = msgs[0] if msgs else None
    if m0 is None:
        return False

    try:
        sender = await m0.get_sender()
    except Exception:
        sender = None
    if sender is None:
        return False

    try:
        from telethon.tl.types import Channel, User
    except Exception:
        return False

    if isinstance(sender, Channel):
        return True
    if not isinstance(sender, User):
        return True

    try:
        perms = await client.get_permissions(entity, sender)
    except Exception:
        return False
    if perms is None:
        return False
    return bool(getattr(perms, "is_creator", False) or getattr(perms, "is_admin", False))
