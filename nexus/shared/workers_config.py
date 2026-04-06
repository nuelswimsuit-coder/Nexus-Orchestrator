"""Load static worker targets (node_id, IP, optional deploy root) from JSON."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StaticWorker:
    node_id: str
    ip: str
    deploy_root: str | None = None


def default_workers_config_paths(repo_root: Path) -> list[Path]:
    return [
        repo_root / "configs" / "workers.json",
        repo_root / "workers.json",
    ]


def resolve_workers_config_path(repo_root: Path, explicit: str | None) -> Path | None:
    if explicit and explicit.strip():
        p = Path(explicit.strip()).expanduser()
        return p if p.is_file() else None
    for candidate in default_workers_config_paths(repo_root):
        if candidate.is_file():
            return candidate
    return None


def load_static_workers(
    *,
    repo_root: Path,
    explicit_path: str | None = None,
) -> list[StaticWorker]:
    """
    Parse ``workers.json`` (see ``configs/workers.json.example``).

    Path resolution: ``explicit_path`` (e.g. from settings), then
    ``NEXUS_WORKERS_CONFIG`` / ``WORKERS_JSON``, then ``configs/workers.json``,
    then repo-root ``workers.json``.
    """
    env_path = (os.environ.get("NEXUS_WORKERS_CONFIG") or os.environ.get("WORKERS_JSON") or "").strip()
    chosen = (explicit_path or "").strip() or env_path
    path = resolve_workers_config_path(repo_root, chosen or None)
    if path is None:
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    entries = raw.get("workers") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return []

    out: list[StaticWorker] = []
    for item in entries:
        if isinstance(item, str):
            ip = item.strip()
            if not ip:
                continue
            out.append(StaticWorker(node_id=f"worker_{ip.replace('.', '_')}", ip=ip))
            continue
        if not isinstance(item, dict):
            continue
        nid = str(item.get("node_id") or item.get("id") or "").strip()
        ip = str(item.get("ip") or item.get("host") or "").strip()
        if not nid or not ip:
            continue
        root = item.get("deploy_root") or item.get("remote_root")
        dr = str(root).strip() if root else None
        out.append(StaticWorker(node_id=nid, ip=ip, deploy_root=dr or None))
    return out
