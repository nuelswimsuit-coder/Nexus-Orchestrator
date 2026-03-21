"""
Master node entry mirror.

The canonical CLI is ``python scripts/start_master.py`` or the ``nexus-master``
console script. This module exists so tooling and docs can reference
``nexus.master.main`` without duplicating startup logic.
"""

from __future__ import annotations


def main() -> None:
    from scripts.start_master import main as _start_master_main

    _start_master_main()


if __name__ == "__main__":
    main()
