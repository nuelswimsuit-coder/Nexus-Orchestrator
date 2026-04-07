"""
OpenClaw (Moltbot) auto-editor: apply file patches under a mapped workspace with backups,
optional HTTP ingress for patch requests, and a watchdog that triggers process reload on ``.py`` changes.

Workspace mapping
-----------------
Set ``OPENCLAW_WORKSPACE`` or ``NEXUS_OPENCLAW_WORKSPACE`` to a comma-separated list of absolute
paths (Nexus-Orchestrator repo root, sibling ``Trackerr``, etc.). If unset, the repo root is
inferred from this file and a sibling ``../Trackerr`` directory is included when it exists.

Permissions
-----------
The OS user running this process must already have write access to those trees (run the terminal
or OpenClaw host as Administrator on Windows or with appropriate ``sudo``/ownership on Linux).
``check-perms`` verifies create/write under each root's backup directory.

Auto-reload
-----------
When ``--watch`` is used (or ``serve`` with ``--watch``), ``.py`` edits under the workspace
trigger a debounced shell command from ``NEXUS_OPENCLAW_RELOAD_CMD`` (e.g. ``systemctl restart
nexus-worker`` or a script that signals your supervisor). Without that variable, only a log line
is emitted — there is no safe default that fits every process tree (e.g. ``nexus_core`` tears
down all children if one worker exits).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

# ---------------------------------------------------------------------------
# Workspace roots (OpenClaw "Workspace" = allowed write roots)
# ---------------------------------------------------------------------------

_ENV_KEYS = ("OPENCLAW_WORKSPACE", "NEXUS_OPENCLAW_WORKSPACE")


def _infer_repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in (here.parent, *here.parents):
        if (p / "pyproject.toml").is_file():
            return p
    return here.parent.parent.parent


def _default_workspace_roots() -> list[Path]:
    root = _infer_repo_root()
    out = [root.resolve()]
    sibling = (root.parent / "Trackerr").resolve()
    if sibling.is_dir():
        out.append(sibling)
    return out


def resolve_workspace_roots(explicit: str | None = None) -> list[Path]:
    raw = (explicit or "").strip()
    if not raw:
        for key in _ENV_KEYS:
            raw = (os.environ.get(key) or "").strip()
            if raw:
                break
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return [Path(p).expanduser().resolve() for p in parts]
    return _default_workspace_roots()


def path_is_under_any(path: Path, roots: Iterable[Path]) -> bool:
    rp = path.resolve()
    for r in roots:
        try:
            rp.relative_to(r.resolve())
            return True
        except ValueError:
            continue
    return False


def resolve_target_path(rel_or_abs: str, roots: list[Path]) -> Path:
    s = (rel_or_abs or "").strip()
    if not s:
        raise ValueError("empty path")
    p = Path(s).expanduser()
    if p.is_absolute():
        cand = p.resolve()
    else:
        if len(roots) != 1:
            raise ValueError(
                "relative paths require exactly one workspace root; "
                "use an absolute path or set a single OPENCLAW_WORKSPACE."
            )
        cand = (roots[0] / p).resolve()
    if not path_is_under_any(cand, roots):
        raise ValueError(f"path escapes workspace: {cand}")
    return cand


_BACKUP_DIRNAME = ".openclaw_backups"
_SKIP_DIR_PARTS = frozenset(
    {".git", ".venv", "venv", "__pycache__", _BACKUP_DIRNAME, ".openclaw_scratch"}
)


def _should_ignore_watch_path(path: Path, roots: list[Path]) -> bool:
    try:
        parts = path.resolve().parts
    except OSError:
        return True
    if any(x in _SKIP_DIR_PARTS for x in parts):
        return True
    if path.suffix.lower() != ".py":
        return True
    return not path_is_under_any(path, roots)


def backup_dir_for_root(root: Path) -> Path:
    d = (root / _BACKUP_DIRNAME).resolve()
    return d


def make_backup(target: Path, roots: list[Path]) -> Path | None:
    """
    Copy ``target`` to ``<root>/.openclaw_backups/...`` using shutil.copy2.
    Returns the backup path, or None if the file did not exist (new file).
    """
    if not target.exists():
        return None
    if not target.is_file():
        raise IsADirectoryError(f"not a file: {target}")
    root_used: Path | None = None
    tr = target.resolve()
    for r in roots:
        try:
            tr.relative_to(r.resolve())
            root_used = r.resolve()
            break
        except ValueError:
            continue
    if root_used is None:
        raise ValueError(f"cannot pick backup root for {target}")

    bdir = backup_dir_for_root(root_used)
    os.makedirs(bdir, exist_ok=True)
    rel = tr.relative_to(root_used)
    safe = str(rel).replace(os.sep, "_").replace("/", "_")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_name = f"{ts}_{safe}"
    dest = bdir / backup_name
    shutil.copy2(tr, dest)
    return dest


@dataclass
class ApplyResult:
    ok: bool
    path: str
    backup: str | None
    error: str | None = None


def apply_patch(
    *,
    rel_or_abs: str,
    content: str,
    roots: list[Path],
    encoding: str = "utf-8",
) -> ApplyResult:
    """
    Write ``content`` to the resolved path (full replace). Creates parent dirs as needed.
    """
    try:
        target = resolve_target_path(rel_or_abs, roots)
    except ValueError as exc:
        return ApplyResult(False, rel_or_abs, None, str(exc))

    try:
        backup_path = make_backup(target, roots)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding=encoding, newline="")
    except OSError as exc:
        return ApplyResult(False, str(target), None, str(exc))

    return ApplyResult(True, str(target), str(backup_path) if backup_path else None, None)


def check_workspace_permissions(roots: list[Path]) -> dict[str, Any]:
    """Verify each root allows writes under its backup dir (and optional scratch)."""
    results: list[dict[str, Any]] = []
    for r in roots:
        r = r.resolve()
        entry: dict[str, Any] = {"root": str(r), "writable": False, "detail": ""}
        if not r.is_dir():
            entry["detail"] = "not a directory"
            results.append(entry)
            continue
        bdir = backup_dir_for_root(r)
        try:
            os.makedirs(bdir, exist_ok=True)
            probe = bdir / f".openclaw_perm_probe_{uuid.uuid4().hex}"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            entry["writable"] = True
            entry["detail"] = "backup dir writable"
        except OSError as exc:
            entry["detail"] = str(exc)
        results.append(entry)

    admin_hint = (
        "On Windows, run the OpenClaw host terminal as Administrator if writes fail. "
        "On Linux, use sudo or fix ownership (chown) for the workspace trees."
    )
    return {"roots": results, "permission_hint": admin_hint}


def _run_reload_command(log: Callable[[str], None]) -> None:
    cmd = (os.environ.get("NEXUS_OPENCLAW_RELOAD_CMD") or "").strip()
    if not cmd:
        log(
            "openclaw_auto_editor: .py change detected but NEXUS_OPENCLAW_RELOAD_CMD is unset; "
            "set it to a shell command that restarts your worker/master (see module docstring)."
        )
        return
    try:
        # shell=True so users can pass e.g. `systemctl restart nexus-worker && true`
        subprocess.run(cmd, shell=True, check=False, cwd=str(_infer_repo_root()))
        log(f"openclaw_auto_editor: executed NEXUS_OPENCLAW_RELOAD_CMD after .py change")
    except OSError as exc:
        log(f"openclaw_auto_editor: reload command failed: {exc}")


class _DebouncedPyReloadHandler(FileSystemEventHandler):
    def __init__(
        self,
        roots: list[Path],
        debounce_s: float,
        on_fire: Callable[[], None],
    ) -> None:
        self._roots = roots
        self._debounce_s = debounce_s
        self._on_fire = on_fire
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    def _schedule(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self._debounce_s, self._fire_once)
            self._timer.daemon = True
            self._timer.start()

    def _fire_once(self) -> None:
        with self._lock:
            self._timer = None
        self._on_fire()

    def _handle_path(self, path: Path) -> None:
        if _should_ignore_watch_path(path, self._roots):
            return
        self._schedule()

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._handle_path(Path(event.src_path))

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._handle_path(Path(event.src_path))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._handle_path(Path(event.dest_path))


def run_watchdog(roots: list[Path], debounce_s: float, log: Callable[[str], None]) -> None:
    observer = Observer()
    handler = _DebouncedPyReloadHandler(roots, debounce_s, lambda: _run_reload_command(log))
    for r in roots:
        if r.is_dir():
            observer.schedule(handler, str(r), recursive=True)
    observer.start()
    log(f"openclaw_auto_editor: watching {len(roots)} root(s) for .py changes (debounce={debounce_s}s)")
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        log("openclaw_auto_editor: stopping watchdog")
    finally:
        observer.stop()
        observer.join(timeout=5)


# ---------------------------------------------------------------------------
# HTTP server (POST /patch or /apply)
# ---------------------------------------------------------------------------

def _json_response(handler: BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _make_http_handler(roots: list[Path]) -> type[BaseHTTPRequestHandler]:
    class OpenClawPatchHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write(f"[openclaw_auto_editor] {self.address_string()} - {fmt % args}\n")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path not in ("/patch", "/apply", "/"):
                _json_response(self, 404, {"ok": False, "error": "not found"})
                return
            length = int(self.headers.get("Content-Length") or "0")
            if length <= 0 or length > 32 * 1024 * 1024:
                _json_response(self, 400, {"ok": False, "error": "invalid Content-Length"})
                return
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                _json_response(self, 400, {"ok": False, "error": f"invalid json: {exc}"})
                return
            if not isinstance(data, dict):
                _json_response(self, 400, {"ok": False, "error": "body must be a JSON object"})
                return
            rel = str(data.get("path") or data.get("file") or "").strip()
            content = data.get("content")
            if not rel or not isinstance(content, str):
                _json_response(
                    self,
                    400,
                    {"ok": False, "error": "required fields: path (or file), content (string)"},
                )
                return
            res = apply_patch(rel_or_abs=rel, content=content, roots=roots)
            status = 200 if res.ok else 400
            _json_response(
                self,
                status,
                {
                    "ok": res.ok,
                    "path": res.path,
                    "backup": res.backup,
                    "error": res.error,
                },
            )

    return OpenClawPatchHandler


def serve_http(
    host: str,
    port: int,
    roots: list[Path],
    watch: bool,
    debounce_s: float,
) -> None:
    handler_cls = _make_http_handler(roots)
    server = HTTPServer((host, port), handler_cls)
    log = lambda m: print(m, flush=True)
    log(f"openclaw_auto_editor: HTTP PATCH server on http://{host}:{port}/patch (POST JSON)")

    obs_thread: threading.Thread | None = None
    if watch:

        def _watch_loop() -> None:
            run_watchdog(roots, debounce_s, log)

        obs_thread = threading.Thread(target=_watch_loop, name="openclaw-py-watch", daemon=True)
        obs_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("openclaw_auto_editor: shutting down HTTP server")
    finally:
        server.server_close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _read_stdin_text() -> str:
    return sys.stdin.read()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OpenClaw workspace file editor: backups, patch apply, optional HTTP + .py reload hook.",
    )
    parser.add_argument(
        "--workspace",
        default="",
        help="Comma-separated workspace roots (default: env OPENCLAW_WORKSPACE / NEXUS_OPENCLAW_WORKSPACE or auto)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_apply = sub.add_parser("apply", help="Write content to a file under the workspace (full replace)")
    p_apply.add_argument("--path", required=True, help="Absolute path, or relative if a single workspace root")
    p_apply.add_argument("--encoding", default="utf-8")
    g = p_apply.add_mutually_exclusive_group(required=True)
    g.add_argument("--content", default="", help="New file contents")
    g.add_argument(
        "--content-file",
        default="",
        help="Read new contents from this file",
    )

    sub.add_parser("check-perms", help="Verify write access to backup dirs under each workspace root")

    p_watch = sub.add_parser("watch", help="Watch workspace for .py changes and run NEXUS_OPENCLAW_RELOAD_CMD")
    p_watch.add_argument("--debounce", type=float, default=1.5, help="Seconds to debounce reload (default 1.5)")

    p_serve = sub.add_parser("serve", help="HTTP POST /patch for JSON {path, content}; optional --watch")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=9849)
    p_serve.add_argument("--watch", action="store_true", help="Also run the .py file watchdog in a thread")
    p_serve.add_argument("--debounce", type=float, default=1.5)

    p_json = sub.add_parser("apply-json", help="Read one JSON object from stdin: {path, content}")
    p_json.add_argument("--encoding", default="utf-8")

    args = parser.parse_args()
    roots = resolve_workspace_roots(args.workspace or None)

    if args.cmd == "check-perms":
        report = check_workspace_permissions(roots)
        print(json.dumps(report, indent=2))
        if not all(r.get("writable") for r in report["roots"]):
            raise SystemExit(1)
        return

    if args.cmd == "apply":
        if args.content_file:
            content = Path(args.content_file).read_text(encoding=args.encoding)
        else:
            content = args.content
        res = apply_patch(rel_or_abs=args.path, content=content, roots=roots, encoding=args.encoding)
        print(json.dumps({"ok": res.ok, "path": res.path, "backup": res.backup, "error": res.error}, indent=2))
        raise SystemExit(0 if res.ok else 1)

    if args.cmd == "apply-json":
        data = json.loads(_read_stdin_text())
        if not isinstance(data, dict):
            raise SystemExit("stdin must be a JSON object")
        path = str(data.get("path") or data.get("file") or "").strip()
        content = data.get("content")
        if not path or not isinstance(content, str):
            raise SystemExit("JSON requires path (or file) and content (string)")
        res = apply_patch(rel_or_abs=path, content=content, roots=roots, encoding=args.encoding)
        print(json.dumps({"ok": res.ok, "path": res.path, "backup": res.backup, "error": res.error}, indent=2))
        raise SystemExit(0 if res.ok else 1)

    if args.cmd == "watch":

        def _log(m: str) -> None:
            print(m, flush=True)

        run_watchdog(roots, args.debounce, _log)
        return

    if args.cmd == "serve":
        serve_http(args.host, args.port, roots, watch=args.watch, debounce_s=args.debounce)
        return

    raise SystemExit(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    # Windows: SIGINT works; help Ctrl+C in threads
    if sys.platform == "win32":

        def _win_sigint(_signum: int, _frame: Any) -> None:
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, _win_sigint)

    main()
