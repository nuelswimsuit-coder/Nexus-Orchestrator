"""
Deep local scan for Telegram-style account artifacts (.session, co-located .json,
tdata trees), including nested .zip archives. Stages bundles under
``data/staged_accounts/acc_[phone_or_hash]/`` (phone taken from sibling JSON when
present) with per-file SHA-256 deduplication.

Complementary Desktop **writer** loop (new ``AI_*`` Python trees on a timer) lives
in :mod:`nexus.core.engine.project_genesis` and is started from
``scripts/nexus_core.py`` alongside the core services.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Sequence

import structlog

log = structlog.get_logger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STAGED_ROOT = _REPO_ROOT / "data" / "staged_accounts"
DEFAULT_SCAN_ROOTS: tuple[Path, ...] = (Path.home(),)

_ZIP_EXTENSIONS = frozenset({".zip"})
_SESSION_SUFFIX = ".session"
_JSON_SUFFIX = ".json"
_TDATA_NAME = "tdata"


def _norm_key(path: str) -> str:
    return path.replace("\\", "/").rstrip("/")


def _parent_key(path: str) -> str:
    key = _norm_key(path)
    if "/" not in key:
        return ""
    return key.rsplit("/", 1)[0]


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_segment(name: str) -> str:
    name = name.strip().replace("\\", "_").replace("/", "_")
    name = re.sub(r'[<>:"|?*]', "_", name)
    return name[:120] if len(name) > 120 else name


@dataclass(slots=True)
class ScavengeResult:
    staged_root: Path
    accounts_staged: int
    files_written: int
    files_deduplicated: int
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _FsRef:
    abs_path: Path

    def display(self) -> str:
        return str(self.abs_path)


@dataclass(slots=True)
class _ZipRef:
    """Member path inside the innermost archive (after optional nested zip chain)."""

    root_zip: Path
    inner_open: tuple[str, ...]
    member: str

    def display(self) -> str:
        if self.inner_open:
            chain = ">".join(self.inner_open)
            return f"{self.root_zip}::{chain}|{self.member}"
        return f"{self.root_zip}::{self.member}"

    def same_container(self, other: _ZipRef) -> bool:
        return self.root_zip == other.root_zip and self.inner_open == other.inner_open


@dataclass(slots=True)
class _DiscoveredFile:
    ref: _FsRef | _ZipRef
    rel_parent: str

    @property
    def name(self) -> str:
        if isinstance(self.ref, _FsRef):
            return self.ref.abs_path.name
        return Path(self.ref.member.replace("\\", "/")).name

    @property
    def is_session(self) -> bool:
        return self.name.lower().endswith(_SESSION_SUFFIX)

    @property
    def is_json(self) -> bool:
        return self.name.lower().endswith(_JSON_SUFFIX)

    def display(self) -> str:
        return self.ref.display() if isinstance(self.ref, _ZipRef) else self.ref.display()


@dataclass(slots=True)
class _DiscoveredTdata:
    ref: _FsRef | _ZipRef
    root_prefix: str  # tdata root path inside FS or innermost zip

    def member_prefix(self) -> str:
        if isinstance(self.ref, _FsRef):
            return _norm_key(str(self.ref.abs_path)) + "/"
        return _norm_key(self.root_prefix).rstrip("/") + "/"

    def display(self) -> str:
        return self.ref.display() if isinstance(self.ref, _ZipRef) else self.ref.display()


def _is_zip_path(path: Path) -> bool:
    return path.suffix.lower() in _ZIP_EXTENSIONS


@contextmanager
def _open_innermost_zip(root: Path, inner_open: tuple[str, ...]) -> Iterator[zipfile.ZipFile]:
    outer = zipfile.ZipFile(root, "r")
    stack: list[zipfile.ZipFile] = [outer]
    current = outer
    try:
        for seg in inner_open:
            data = current.read(seg)
            inner = zipfile.ZipFile(io.BytesIO(data), "r")
            stack.append(inner)
            current = inner
        yield current
    finally:
        for z in reversed(stack):
            z.close()


def _read_zip_bytes(zr: _ZipRef) -> bytes:
    with _open_innermost_zip(zr.root_zip, zr.inner_open) as zf:
        return zf.read(zr.member)


def _scan_inner_zip(
    root_zip: Path,
    inner_open: tuple[str, ...],
    inner: zipfile.ZipFile,
    *,
    max_nested_zip_depth: int,
) -> Iterator[_DiscoveredFile | _DiscoveredTdata]:
    for name in inner.namelist():
        norm = _norm_key(name)
        if not norm:
            continue
        if norm.endswith("/"):
            parts = norm.rstrip("/").split("/")
            if parts and parts[-1].lower() == _TDATA_NAME:
                root = norm.rstrip("/")
                yield _DiscoveredTdata(
                    ref=_ZipRef(root_zip=root_zip, inner_open=inner_open, member=root),
                    root_prefix=root,
                )
            continue

        parts = norm.split("/")
        if parts and parts[-1].lower() == _TDATA_NAME:
            yield _DiscoveredTdata(
                ref=_ZipRef(root_zip=root_zip, inner_open=inner_open, member=norm),
                root_prefix=norm,
            )
            continue

        parent = _parent_key(norm)
        low = norm.lower()
        if low.endswith(_SESSION_SUFFIX):
            yield _DiscoveredFile(
                ref=_ZipRef(root_zip=root_zip, inner_open=inner_open, member=name),
                rel_parent=parent,
            )
        elif low.endswith(_JSON_SUFFIX):
            yield _DiscoveredFile(
                ref=_ZipRef(root_zip=root_zip, inner_open=inner_open, member=name),
                rel_parent=parent,
            )

        if _is_zip_path(Path(norm)):
            if len(inner_open) >= max_nested_zip_depth:
                continue
            try:
                data = inner.read(name)
            except (KeyError, RuntimeError, OSError) as exc:
                log.warning("zip_nested_read_failed", outer=str(root_zip), member=name, error=str(exc))
                continue
            try:
                deeper = zipfile.ZipFile(io.BytesIO(data), "r")
            except zipfile.BadZipFile:
                continue
            with deeper:
                yield from _scan_inner_zip(
                    root_zip,
                    (*inner_open, name),
                    deeper,
                    max_nested_zip_depth=max_nested_zip_depth,
                )


def _iter_zip_files(zip_path: Path, *, max_nested_zip_depth: int) -> Iterator[_DiscoveredFile | _DiscoveredTdata]:
    try:
        outer = zipfile.ZipFile(zip_path, "r")
    except (zipfile.BadZipFile, OSError) as exc:
        log.warning("zip_open_failed", path=str(zip_path), error=str(exc))
        return

    with outer:
        yield from _scan_inner_zip(zip_path, (), outer, max_nested_zip_depth=max_nested_zip_depth)


def _walk_filesystem(root: Path, *, max_nested_zip_depth: int) -> Iterator[_DiscoveredFile | _DiscoveredTdata]:
    root = root.resolve()
    if not root.exists():
        log.warning("scan_root_missing", path=str(root))
        return

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(dirpath)
        if base.name.lower() == _TDATA_NAME:
            yield _DiscoveredTdata(
                ref=_FsRef(abs_path=base),
                root_prefix=_norm_key(str(base)),
            )

        for fn in filenames:
            path = base / fn
            low = fn.lower()
            if low.endswith(_SESSION_SUFFIX):
                yield _DiscoveredFile(
                    ref=_FsRef(abs_path=path),
                    rel_parent=_norm_key(str(base)),
                )
            elif low.endswith(_JSON_SUFFIX):
                yield _DiscoveredFile(
                    ref=_FsRef(abs_path=path),
                    rel_parent=_norm_key(str(base)),
                )
            elif _is_zip_path(path):
                yield from _iter_zip_files(path, max_nested_zip_depth=max_nested_zip_depth)

        for d in list(dirnames):
            if d.lower() == _TDATA_NAME:
                td = base / d
                yield _DiscoveredTdata(
                    ref=_FsRef(abs_path=td),
                    root_prefix=_norm_key(str(td)),
                )


def _tdata_matches_session(
    t: _DiscoveredTdata,
    session_parent: str,
    s_ref: _FsRef | _ZipRef,
) -> bool:
    """Sibling tdata (same parent dir) or session path inside this tdata tree."""
    if isinstance(s_ref, _FsRef) and isinstance(t.ref, _FsRef):
        t_root = _norm_key(str(t.ref.abs_path))
        t_parent = _norm_key(str(t.ref.abs_path.parent))
        if t_parent == session_parent:
            return True
        return session_parent == t_root or session_parent.startswith(t_root + "/")

    if isinstance(s_ref, _ZipRef) and isinstance(t.ref, _ZipRef):
        if not s_ref.same_container(t.ref):
            return False
        t_root = _norm_key(t.root_prefix)
        t_parent = _parent_key(t_root)
        if t_parent == session_parent:
            return True
        return session_parent == t_root or session_parent.startswith(t_root + "/")

    return False


def _content_for_discovered_file(df: _DiscoveredFile) -> bytes:
    if isinstance(df.ref, _FsRef):
        return df.ref.abs_path.read_bytes()
    return _read_zip_bytes(df.ref)


def _stage_file_bytes_resolved(
    staged_root: Path,
    account_dir: Path,
    relative_logical: str,
    data: bytes,
    content_hashes: dict[str, Path],
    manifest_files: list[dict[str, str]],
) -> bool:
    """Returns True if a new blob was written (not deduplicated)."""
    h = _sha256_bytes(data)
    if h in content_hashes:
        manifest_files.append(
            {
                "path": relative_logical,
                "sha256": h,
                "deduplicated_from": content_hashes[h].as_posix(),
            }
        )
        return False

    blob_rel = Path("_blobs") / h[:2] / h
    blob_path = staged_root / blob_rel
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(data)
    content_hashes[h] = blob_rel

    parts = [p for p in relative_logical.replace("\\", "/").split("/") if p]
    out_rel = Path("files").joinpath(*[_safe_segment(p) for p in parts])
    out_path = account_dir / out_rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(data)

    manifest_files.append({"path": relative_logical, "sha256": h, "stored_as": out_rel.as_posix()})
    return True


def _copy_fs_tree_under_tdata(
    tdata: _DiscoveredTdata,
    staged_root: Path,
    account_dir: Path,
    content_hashes: dict[str, Path],
    manifest_files: list[dict[str, str]],
    written: list[int],
    deduped: list[int],
) -> None:
    root = tdata.ref.abs_path
    troot = _norm_key(str(root))
    for dirpath, _, filenames in os.walk(root, followlinks=False):
        for fn in filenames:
            fp = Path(dirpath) / fn
            rel = _norm_key(str(fp))[len(troot) + 1 :]
            try:
                data = fp.read_bytes()
            except OSError as exc:
                log.warning("tdata_file_read_failed", path=str(fp), error=str(exc))
                continue
            logical = f"tdata/{rel}"
            if _stage_file_bytes_resolved(staged_root, account_dir, logical, data, content_hashes, manifest_files):
                written[0] += 1
            else:
                deduped[0] += 1


def _copy_zip_tdata_tree(
    tdata: _DiscoveredTdata,
    staged_root: Path,
    account_dir: Path,
    content_hashes: dict[str, Path],
    manifest_files: list[dict[str, str]],
    written: list[int],
    deduped: list[int],
) -> None:
    zr = tdata.ref
    assert isinstance(zr, _ZipRef)
    prefix = tdata.member_prefix()
    with _open_innermost_zip(zr.root_zip, zr.inner_open) as zf:
        for name in zf.namelist():
            n = _norm_key(name)
            if not n.startswith(prefix) or n.endswith("/"):
                continue
            rel = n[len(prefix) :]
            if not rel:
                continue
            try:
                data = zf.read(name)
            except (KeyError, RuntimeError, OSError) as exc:
                log.warning("tdata_zip_read_failed", member=name, error=str(exc))
                continue
            logical = f"tdata/{rel}"
            if _stage_file_bytes_resolved(staged_root, account_dir, logical, data, content_hashes, manifest_files):
                written[0] += 1
            else:
                deduped[0] += 1


def _phone_hint_from_sibling_jsons(files: list[_DiscoveredFile]) -> str | None:
    """Digits from staged Telethon meta JSON (``phone`` / ``phone_number``)."""
    for df in files:
        if not df.is_json:
            continue
        try:
            raw = _content_for_discovered_file(df)
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        for key in ("phone", "phone_number", "Phone"):
            v = data.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if not s:
                continue
            digits = re.sub(r"\D", "", s)
            return digits[:20] if digits else _safe_segment(s)[:20]
    return None


def _session_account_id(
    session_bytes: bytes,
    label: str,
    phone_hint: str | None = None,
) -> str:
    h = _sha256_bytes(session_bytes)[:10]
    if phone_hint:
        safe = _safe_segment(phone_hint[:24])
        return f"acc_{safe}_{h}"
    stem = _safe_segment(Path(label.replace("\\", "/")).stem)[:40]
    return f"acc_{h}_{stem}"


def _dedup_key(item: _DiscoveredFile | _DiscoveredTdata) -> str:
    if isinstance(item, _DiscoveredFile):
        if isinstance(item.ref, _FsRef):
            return f"fs:{item.ref.abs_path.resolve()}"
        z = item.ref
        return f"zip:{z.root_zip.resolve()}|{z.inner_open}|{z.member}"
    if isinstance(item.ref, _FsRef):
        return f"fs-td:{item.ref.abs_path.resolve()}"
    z = item.ref
    return f"zip-td:{z.root_zip.resolve()}|{z.inner_open}|{item.root_prefix}"


def run_account_scavenge(
    scan_roots: Sequence[Path | str] | None = None,
    staged_root: Path | str | None = None,
    *,
    max_nested_zip_depth: int = 8,
) -> ScavengeResult:
    """
    Scan local paths for .session, co-located .json, and tdata directories (on
    disk or inside .zip). Stage each logical account under ``staged_root``.

    JSON files are included when they share a parent directory with a .session.
    ``tdata`` is attached when it is a sibling of the session or when the
    session path lies inside that ``tdata`` tree (including zip paths).

    Parameters
    ----------
    scan_roots:
        Directories to walk (default: user home).
    staged_root:
        Output root (default: ``<repo>/data/staged_accounts``).
    max_nested_zip_depth:
        Maximum nested ``.zip`` archives opened from a single filesystem zip
        (enforced while descending).
    """
    depth = max(0, int(max_nested_zip_depth))
    roots = [Path(p).expanduser().resolve() for p in (scan_roots or DEFAULT_SCAN_ROOTS)]
    out_root = Path(staged_root).expanduser().resolve() if staged_root else DEFAULT_STAGED_ROOT.resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    discovered: dict[str, _DiscoveredFile | _DiscoveredTdata] = {}
    errors: list[str] = []

    for r in roots:
        try:
            for item in _walk_filesystem(r, max_nested_zip_depth=depth):
                discovered[_dedup_key(item)] = item
        except OSError as exc:
            errors.append(f"os.walk failed for {r}: {exc}")
            log.warning("scan_root_walk_failed", path=str(r), error=str(exc))

    items = list(discovered.values())
    sessions = [d for d in items if isinstance(d, _DiscoveredFile) and d.is_session]
    all_json = [d for d in items if isinstance(d, _DiscoveredFile) and d.is_json]
    tdatas = [d for d in items if isinstance(d, _DiscoveredTdata)]

    consumed_tdata: set[int] = set()
    bundles: list[tuple[str, list[_DiscoveredFile], list[_DiscoveredTdata]]] = []

    for s in sessions:
        try:
            payload = _content_for_discovered_file(s)
        except (OSError, KeyError, RuntimeError) as exc:
            errors.append(f"read session failed {s.display()}: {exc}")
            continue
        parent = s.rel_parent
        group_files: list[_DiscoveredFile] = [s]
        for j in all_json:
            if j.rel_parent == parent:
                group_files.append(j)
        phone_hint = _phone_hint_from_sibling_jsons(group_files)
        aid = _session_account_id(payload, s.name, phone_hint)
        group_tdata: list[_DiscoveredTdata] = []
        for i, t in enumerate(tdatas):
            if _tdata_matches_session(t, parent, s.ref):
                group_tdata.append(t)
                consumed_tdata.add(i)
        bundles.append((aid, group_files, group_tdata))

    for i, t in enumerate(tdatas):
        if i in consumed_tdata:
            continue
        label = t.display()
        aid = f"acc_tdata_{_sha256_bytes(label.encode('utf-8', errors='replace'))[:12]}"
        bundles.append((aid, [], [t]))

    merged: dict[str, tuple[dict[str, _DiscoveredFile], dict[str, _DiscoveredTdata]]] = {}
    for aid, files, tds in bundles:
        fmap, tmap = merged.setdefault(aid, ({}, {}))
        for f in files:
            fmap[_dedup_key(f)] = f
        for t in tds:
            tmap[_dedup_key(t)] = t
    flat_bundles = [(aid, list(fm.values()), list(tm.values())) for aid, (fm, tm) in merged.items()]

    content_hashes: dict[str, Path] = {}
    written = [0]
    deduped = [0]
    accounts = 0

    for aid, files, tds in flat_bundles:
        if not files and not tds:
            continue
        account_dir = out_root / _safe_segment(aid)
        account_dir.mkdir(parents=True, exist_ok=True)
        manifest_files: list[dict[str, str]] = []
        accounts += 1

        for df in files:
            try:
                data = _content_for_discovered_file(df)
            except (OSError, KeyError, RuntimeError) as exc:
                errors.append(f"read failed {df.display()}: {exc}")
                continue
            logical = df.name
            if _stage_file_bytes_resolved(out_root, account_dir, logical, data, content_hashes, manifest_files):
                written[0] += 1
            else:
                deduped[0] += 1

        for td in tds:
            if isinstance(td.ref, _FsRef):
                _copy_fs_tree_under_tdata(td, out_root, account_dir, content_hashes, manifest_files, written, deduped)
            else:
                _copy_zip_tdata_tree(td, out_root, account_dir, content_hashes, manifest_files, written, deduped)

        meta = {
            "account_id": aid,
            "sources": [x.display() for x in files] + [x.display() for x in tds],
            "files": manifest_files,
        }
        (account_dir / "manifest.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    log.info(
        "account_scavenge_complete",
        staged_root=str(out_root),
        accounts=accounts,
        files_written=written[0],
        files_deduplicated=deduped[0],
        errors=len(errors),
    )

    return ScavengeResult(
        staged_root=out_root,
        accounts_staged=accounts,
        files_written=written[0],
        files_deduplicated=deduped[0],
        errors=errors,
    )
