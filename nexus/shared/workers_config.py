"""Optional static worker list for deploy (``configs/workers.json``)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkerEntry:
    node_id: str
    host: str
    deploy_root: str | None = None


@dataclass(frozen=True)
class WorkersFileConfig:
    entries: tuple[WorkerEntry, ...]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_workers_json_path(repo_root: Path | None = None) -> Path:
    env = (os.environ.get("NEXUS_WORKERS_CONFIG") or "").strip()
    if env:
        return Path(env)
    root = repo_root or _repo_root()
    p = root / "configs" / "workers.json"
    if p.is_file():
        return p
    return root / "workers.json"


def load_workers_config(repo_root: Path | None = None) -> WorkersFileConfig:
    path = default_workers_json_path(repo_root)
    if not path.is_file():
        return WorkersFileConfig(())
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return WorkersFileConfig(())
    raw = data.get("workers") or data.get("nodes") or []
    if not isinstance(raw, list):
        return WorkersFileConfig(())
    entries: list[WorkerEntry] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        nid = str(item.get("node_id") or item.get("id") or "").strip()
        host = str(item.get("host") or item.get("ip") or "").strip()
        if not nid or not host:
            continue
        dr = item.get("deploy_root") or item.get("remote_path")
        dr_s = str(dr).strip() if dr else ""
        entries.append(
            WorkerEntry(node_id=nid, host=host, deploy_root=dr_s or None)
        )
    return WorkersFileConfig(tuple(entries))


def worker_entry_by_id(
    cfg: WorkersFileConfig, node_id: str
) -> WorkerEntry | None:
    for e in cfg.entries:
        if e.node_id == node_id:
            return e
    return None
