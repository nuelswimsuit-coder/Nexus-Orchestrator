"""Worker task implementations — imported by listener.py to register handlers."""

# OpenClaw browser scraping (Phase 19) — Linux worker only
# Import is guarded so the master process doesn't fail if playwright is absent
try:
    from nexus.worker.tasks import openclaw as _openclaw  # noqa: F401
except Exception:
    pass

# Phase 11: Archivist — full Telegram backup with worker media offload
try:
    from nexus.worker.tasks import archivist as _archivist  # noqa: F401
except Exception:
    pass

# Phase 12: Sentinel — mass-reporter using Telethon session rotation
try:
    from nexus.worker.tasks import sentinel as _sentinel  # noqa: F401
except Exception:
    pass

# Moltbot integration — Telegram-heavy scrape/action runner
try:
    from nexus.worker.tasks import moltbot as _moltbot  # noqa: F401
except Exception:
    pass

try:
    from nexus.worker.tasks import polymarket_bot as _polymarket_bot  # noqa: F401
except Exception:
    pass

# Account mapper — staged Telethon sessions → channel/group/bot asset map
try:
    from nexus.worker.tasks import account_mapper as _account_mapper  # noqa: F401
except Exception:
    pass

try:
    from nexus.worker.tasks import group_warmer as _group_warmer  # noqa: F401
except Exception:
    pass

try:
    from nexus.worker.tasks import swarm as _swarm_factory  # noqa: F401 — community factory
except Exception:
    pass

try:
    from nexus.worker.tasks import reactions as _reactions  # noqa: F401 — passive native reactions
except Exception:
    pass

try:
    from nexus.worker.tasks import swarm_onboarding as _swarm_onboarding  # noqa: F401
except Exception:
    pass

try:
    from nexus.worker.tasks import retention_monitor as _retention_monitor  # noqa: F401
except Exception:
    pass

try:
    from nexus.worker.tasks import seo_group_factory as _seo_group_factory  # noqa: F401
except Exception:
    pass

try:
    from nexus.worker.tasks import lurkers as _lurkers  # noqa: F401 — swarm.lurkers.tick
except Exception:
    pass

try:
    from nexus.worker.tasks import poll_generator as _poll_generator  # noqa: F401 — swarm.poll*
except Exception:
    pass
