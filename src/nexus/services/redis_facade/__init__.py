"""Redis integration facade — re-exports shared broker helpers.

Named ``redis_facade`` (not ``redis``) so running scripts from this directory
does not shadow the PyPI ``redis`` package on ``sys.path[0]``.
"""

from __future__ import annotations

from nexus.shared.redis_util import *  # noqa: F403
