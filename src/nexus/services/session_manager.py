"""
Global session registry in Redis (``session:*``).

Processes (Nexus core supervisor, ARQ workers, etc.) publish JSON blobs on a
fixed interval; dashboards scan matching keys for fleet-wide session visibility.

Payload (per key)::

    {
        "computer_name": str,
        "session_id": str,
        "status": "active" | "idle",
        "last_seen": float (unix),
        "started_at": float (unix, process boot on publisher)
    }

Environment:

* ``NEXUS_SESSION_ROLE`` — disambiguates multiple publishers sharing ``NODE_ID``
  (e.g. ``nexus_core`` vs ``arq_worker``).
* ``NEXUS_GLOBAL_SESSION_INCLUDE_VAULT`` — if truthy, also publishes one row per
  Telethon vault stem (best-effort; status ``idle``).
"""

from __future__ import annotations

import json
import os
import re
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any

SESSION_SCAN_PATTERN = "session:*"
SESSION_KEY_PREFIX = "session:"
TTL_SECONDS = 120
HEARTBEAT_INTERVAL_SEC = 30

_PROCESS_BOOT_WALL = time.time()


def _slug(s: str, max_len: int = 72) -> str:
    t = re.sub(r"[^a-zA-Z0-9._-]+", "_", (s or "").strip()) or "unknown"
    return t[:max_len]


def session_redis_key(computer_name: str, session_id: str) -> str:
    return f"{SESSION_KEY_PREFIX}{_slug(computer_name)}:{_slug(session_id)}"


def _infer_active_idle() -> str:
    try:
        import psutil

        cpu = float(psutil.cpu_percent(interval=None))
        return "active" if cpu >= 12.0 else "idle"
    except Exception:
        return "active"


@dataclass(frozen=True)
class GlobalSessionRecord:
    redis_key: str
    computer_name: str
    session_id: str
    status: str
    last_seen: float
    started_at: float


def build_session_payload(
    *,
    computer_name: str,
    session_id: str,
    status: str | None = None,
    last_seen: float | None = None,
    started_at: float | None = None,
) -> dict[str, Any]:
    ts = time.time() if last_seen is None else float(last_seen)
    st = (status or _infer_active_idle()).strip().lower()
    if st not in ("active", "idle"):
        st = "active"
    boot = float(started_at) if started_at is not None else _PROCESS_BOOT_WALL
    return {
        "computer_name": computer_name,
        "session_id": session_id,
        "status": st,
        "last_seen": ts,
        "started_at": boot,
    }


def publish_session(
    redis_client: Any,
    *,
    computer_name: str,
    session_id: str,
    status: str | None = None,
    started_at: float | None = None,
) -> None:
    payload = build_session_payload(
        computer_name=computer_name,
        session_id=session_id,
        status=status,
        started_at=started_at,
    )
    key = session_redis_key(computer_name, session_id)
    redis_client.set(key, json.dumps(payload), ex=TTL_SECONDS)


def publish_local_sessions_bundle(redis_client: Any) -> None:
    """Write this process's global session row(s) to Redis."""
    computer = socket.gethostname()
    node_id = (os.getenv("NODE_ID") or computer).strip() or computer
    role = (os.getenv("NEXUS_SESSION_ROLE") or "runtime").strip() or "runtime"
    session_id = f"{node_id}:{role}"
    publish_session(
        redis_client,
        computer_name=computer,
        session_id=session_id,
        started_at=_PROCESS_BOOT_WALL,
    )
    if os.getenv("NEXUS_GLOBAL_SESSION_INCLUDE_VAULT", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            from nexus.services.session_vault import (
                discover_meta_paths_from_session_sqlite,
            )

            for meta in discover_meta_paths_from_session_sqlite()[:200]:
                stem = meta.stem
                publish_session(
                    redis_client,
                    computer_name=computer,
                    session_id=f"vault:{stem}",
                    status="idle",
                    started_at=_PROCESS_BOOT_WALL,
                )
        except Exception:
            pass


def scan_global_sessions(redis_client: Any) -> list[GlobalSessionRecord]:
    """Load and parse all ``session:*`` keys (sync Redis client)."""
    out: list[GlobalSessionRecord] = []
    try:
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(
                cursor=cursor, match=SESSION_SCAN_PATTERN, count=200
            )
            for key in keys:
                ks = key.decode() if isinstance(key, bytes) else str(key)
                raw = redis_client.get(ks)
                if not raw:
                    continue
                try:
                    d = json.loads(str(raw))
                except json.JSONDecodeError:
                    continue
                if not isinstance(d, dict):
                    continue
                cn = str(d.get("computer_name") or "").strip()
                sid = str(d.get("session_id") or "").strip()
                st = str(d.get("status") or "active").lower()
                if st not in ("active", "idle"):
                    st = "active"
                try:
                    ls = float(d.get("last_seen") or 0.0)
                except (TypeError, ValueError):
                    ls = 0.0
                try:
                    sa = float(d.get("started_at") or ls)
                except (TypeError, ValueError):
                    sa = ls
                if cn and sid:
                    out.append(
                        GlobalSessionRecord(
                            redis_key=ks,
                            computer_name=cn,
                            session_id=sid,
                            status=st,
                            last_seen=ls,
                            started_at=sa,
                        )
                    )
            if cursor == 0:
                break
    except Exception:
        pass
    out.sort(key=lambda r: (r.computer_name.lower(), r.session_id.lower()))
    return out


def start_heartbeat_daemon(dsn: str, stop: threading.Event) -> threading.Thread:
    """
    Background thread: publish ``publish_local_sessions_bundle`` every
    :data:`HEARTBEAT_INTERVAL_SEC` until ``stop`` is set.
    """

    def _run() -> None:
        try:
            import redis as redis_sync  # type: ignore[import-untyped]
        except ImportError:
            return
        client: Any = None
        try:
            client = redis_sync.Redis.from_url(dsn, decode_responses=True)
            while True:
                try:
                    publish_local_sessions_bundle(client)
                except Exception:
                    pass
                if stop.wait(HEARTBEAT_INTERVAL_SEC):
                    break
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    t = threading.Thread(target=_run, name="nexus-global-session-hb", daemon=True)
    t.start()
    return t


__all__ = [
    "GlobalSessionRecord",
    "HEARTBEAT_INTERVAL_SEC",
    "SESSION_SCAN_PATTERN",
    "build_session_payload",
    "publish_local_sessions_bundle",
    "publish_session",
    "scan_global_sessions",
    "session_redis_key",
    "start_heartbeat_daemon",
    "SessionHarvester",
    "harvest_sessions",
]


# ── Session Harvester & ZIP Extractor ─────────────────────────────────────────

import glob as _glob
import shutil as _shutil
import string
import zipfile as _zipfile
from dataclasses import dataclass, field
from pathlib import Path as _Path


@dataclass
class HarvestResult:
    """Summary of a session harvest run."""
    total_found: int = 0
    total_moved: int = 0
    total_skipped: int = 0
    sources: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _all_drive_roots() -> list[_Path]:
    """Return all mounted drive roots on Windows, or [/] on Linux/macOS."""
    roots: list[_Path] = []
    if os.name == "nt":
        for letter in string.ascii_uppercase:
            p = _Path(f"{letter}:\\")
            if p.exists():
                roots.append(p)
    else:
        roots.append(_Path("/"))
    return roots


def _is_session_file(p: _Path) -> bool:
    return p.suffix.lower() == ".session" and p.is_file()


def _is_json_companion(p: _Path) -> bool:
    return p.suffix.lower() == ".json" and p.is_file()


def _extract_zip_for_sessions(
    zip_path: _Path,
    vault_dir: _Path,
    result: HarvestResult,
) -> None:
    """Extract .session (and companion .json) files from a zip archive."""
    try:
        with _zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            session_names = [n for n in names if n.lower().endswith(".session")]
            for sname in session_names:
                stem = _Path(sname).stem
                dest = vault_dir / _Path(sname).name
                if dest.exists():
                    result.total_skipped += 1
                    continue
                try:
                    data = zf.read(sname)
                    dest.write_bytes(data)
                    result.total_moved += 1
                    result.total_found += 1
                    result.sources.append(f"zip:{zip_path}!{sname}")
                except Exception as exc:
                    result.errors.append(f"zip extract {zip_path}!{sname}: {exc}")
                # Try companion JSON
                json_name = sname.replace(".session", ".json")
                if json_name in names:
                    jdest = vault_dir / _Path(json_name).name
                    if not jdest.exists():
                        try:
                            jdest.write_bytes(zf.read(json_name))
                        except Exception:
                            pass
    except Exception as exc:
        result.errors.append(f"zip open {zip_path}: {exc}")


def _extract_raw_for_sessions(
    raw_path: _Path,
    vault_dir: _Path,
    result: HarvestResult,
) -> None:
    """Treat .raw files as potential zip archives and try to extract sessions from them."""
    try:
        if _zipfile.is_zipfile(raw_path):
            _extract_zip_for_sessions(raw_path, vault_dir, result)
        else:
            result.errors.append(f"raw not a zip: {raw_path}")
    except Exception as exc:
        result.errors.append(f"raw probe {raw_path}: {exc}")


class SessionHarvester:
    """
    Scans all subdirectories, .zip, and .raw files for Telegram session files
    (.session + companion .json) and consolidates them into a unified vault directory.

    Usage
    -----
        harvester = SessionHarvester(vault_dir=Path("vault/sessions"))
        result = harvester.run()
        print(f"Gathered {result.total_moved} new sessions")
    """

    def __init__(
        self,
        vault_dir: _Path | str | None = None,
        scan_all_drives: bool = False,
        extra_scan_roots: list[_Path] | None = None,
        skip_dirs: tuple[str, ...] = (
            "node_modules", ".venv", "venv", ".git", "__pycache__",
            "Windows", "Program Files", "Program Files (x86)", "ProgramData",
        ),
    ) -> None:
        if vault_dir is None:
            _here = _Path(os.path.dirname(os.path.abspath(__file__)))
            _repo_root = _here
            for _ in range(6):
                if (_repo_root / "vault").exists() or (_repo_root / ".git").exists():
                    break
                _repo_root = _repo_root.parent
            vault_dir = _repo_root / "vault" / "sessions"
        self.vault_dir = _Path(vault_dir).resolve()
        self.scan_all_drives = scan_all_drives
        self.extra_scan_roots: list[_Path] = extra_scan_roots or []
        self.skip_dirs = frozenset(skip_dirs)

    def _scan_roots(self) -> list[_Path]:
        roots: list[_Path] = []
        if self.scan_all_drives:
            roots.extend(_all_drive_roots())
        roots.extend(self.extra_scan_roots)
        if not roots:
            _here = _Path(os.path.dirname(os.path.abspath(__file__)))
            _repo_root = _here
            for _ in range(6):
                if (_repo_root / ".git").exists():
                    break
                _repo_root = _repo_root.parent
            roots.append(_repo_root)
        return roots

    def _should_skip(self, p: _Path) -> bool:
        return bool(self.skip_dirs.intersection(p.parts))

    def run(self) -> HarvestResult:
        """Execute the harvest and return a summary."""
        self.vault_dir.mkdir(parents=True, exist_ok=True)
        result = HarvestResult()

        for root in self._scan_roots():
            if not root.exists():
                continue
            try:
                self._walk(root, result)
            except PermissionError:
                pass
            except Exception as exc:
                result.errors.append(f"walk {root}: {exc}")

        return result

    def _walk(self, root: _Path, result: HarvestResult) -> None:
        try:
            for item in root.rglob("*"):
                if self._should_skip(item):
                    continue
                if item.is_dir():
                    continue
                suffix = item.suffix.lower()
                if suffix == ".session":
                    self._move_session(item, result)
                elif suffix == ".zip":
                    _extract_zip_for_sessions(item, self.vault_dir, result)
                elif suffix == ".raw":
                    _extract_raw_for_sessions(item, self.vault_dir, result)
        except PermissionError:
            pass

    def _move_session(self, src: _Path, result: HarvestResult) -> None:
        if src.resolve().parent == self.vault_dir:
            result.total_found += 1
            result.total_skipped += 1
            return
        dest = self.vault_dir / src.name
        if dest.exists():
            result.total_found += 1
            result.total_skipped += 1
            return
        try:
            _shutil.copy2(src, dest)
            result.total_found += 1
            result.total_moved += 1
            result.sources.append(str(src))
            # Copy companion JSON if present
            companion = src.with_suffix(".json")
            if companion.exists():
                jdest = self.vault_dir / companion.name
                if not jdest.exists():
                    _shutil.copy2(companion, jdest)
        except Exception as exc:
            result.errors.append(f"copy {src}: {exc}")


def harvest_sessions(
    vault_dir: _Path | str | None = None,
    scan_all_drives: bool = False,
    extra_scan_roots: list[_Path] | None = None,
) -> HarvestResult:
    """
    Convenience wrapper: run a full session harvest and return the result.

    Parameters
    ----------
    vault_dir:
        Destination directory (defaults to ``vault/sessions`` in the repo root).
    scan_all_drives:
        If True, scan all mounted drive letters (Windows) or ``/`` (Linux).
    extra_scan_roots:
        Additional directories to scan in addition to the repo root.
    """
    harvester = SessionHarvester(
        vault_dir=vault_dir,
        scan_all_drives=scan_all_drives,
        extra_scan_roots=extra_scan_roots,
    )
    result = harvester.run()
    print(
        f"[SessionHarvester] found={result.total_found} "
        f"moved={result.total_moved} skipped={result.total_skipped} "
        f"errors={len(result.errors)}",
        flush=True,
    )
    return result
