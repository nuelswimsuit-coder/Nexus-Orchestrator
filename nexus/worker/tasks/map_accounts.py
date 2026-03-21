"""
Backward-compatible import path — registers ``account_mapper.map`` on load.
"""

from __future__ import annotations

import nexus.worker.tasks.account_mapper  # noqa: F401
