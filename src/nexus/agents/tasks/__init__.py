"""Worker task implementations — imported by listener.py to register handlers."""

# OpenClaw browser scraping (Phase 19) — Linux worker only
# Import is guarded so the master process doesn't fail if playwright is absent
try:
    from nexus.agents.tasks import openclaw as _openclaw  # noqa: F401
except Exception:
    pass

# Phase 11: Archivist — full Telegram backup with worker media offload
try:
    from nexus.agents.tasks import archivist as _archivist  # noqa: F401
except Exception:
    pass

# Phase 12: Sentinel — mass-reporter using Telethon session rotation
try:
    from nexus.agents.tasks import sentinel as _sentinel  # noqa: F401
except Exception:
    pass

# Moltbot integration — Telegram-heavy scrape/action runner
try:
    from nexus.agents.tasks import moltbot as _moltbot  # noqa: F401
except Exception:
    pass

try:
    from nexus.agents.tasks import polymarket_bot as _polymarket_bot  # noqa: F401
except Exception:
    pass

# Account mapper — staged Telethon sessions → channel/group/bot asset map
try:
    from nexus.agents.tasks import account_mapper as _account_mapper  # noqa: F401
except Exception:
    pass

try:
    from nexus.agents.tasks import group_warmer as _group_warmer  # noqa: F401
except Exception:
    pass

try:
    from nexus.agents.tasks import retention_monitor as _retention_monitor  # noqa: F401
except Exception:
    pass

try:
    from nexus.agents.tasks import llm_gemini as _llm_gemini  # noqa: F401
except Exception:
    pass
