"""
TeleFix scrape-vault router — serves vault/data/scrapes/*.json files.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import Response
from pydantic import BaseModel

router = APIRouter(prefix="/telefix", tags=["telefix"])

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRAPES_DIR = _REPO_ROOT / "vault" / "data" / "scrapes"


def _ensure_dirs() -> None:
    _SCRAPES_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_scrape_record(data: dict[str, Any], filename: str) -> dict[str, Any]:
    return {
        "filename": filename,
        "scraped_at": data.get("scraped_at", ""),
        "source_group": data.get("source_group", ""),
        "ai_relevance": data.get("ai_relevance", 0.0),
        "keywords": data.get("keywords", []),
        "users_count": len(data.get("users", [])),
        "messages_count": len(data.get("selected_messages", [])),
        "raw": data,
    }


def _iter_scrape_files() -> list[dict[str, Any]]:
    _ensure_dirs()
    rows: list[dict[str, Any]] = []
    if not _SCRAPES_DIR.is_dir():
        return rows
    for p in sorted(_SCRAPES_DIR.glob("*.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows.append(_normalize_scrape_record(data, p.name))
    return rows


class ScrapeVaultResponse(BaseModel):
    files: list[dict[str, Any]]
    count: int


@router.get("/scrapes", response_model=ScrapeVaultResponse)
async def list_scrapes() -> ScrapeVaultResponse:
    rows = _iter_scrape_files()
    return ScrapeVaultResponse(files=rows, count=len(rows))


@router.get("/scrapes/export")
async def export_scrapes_csv(
    date_from: str | None = Query(None),
    keyword: str | None = Query(None),
    min_relevance: float | None = Query(None, ge=0.0, le=1.0),
) -> Response:
    rows = _iter_scrape_files()

    def _matches(r: dict[str, Any]) -> bool:
        if date_from and date_from not in str(r.get("scraped_at", "")):
            return False
        if min_relevance is not None and float(r.get("ai_relevance", 0)) < min_relevance:
            return False
        if keyword:
            blob = json.dumps(r, ensure_ascii=False).lower()
            if keyword.lower() not in blob:
                return False
        return True

    filtered = [r for r in rows if _matches(r)]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["filename", "scraped_at", "source_group", "ai_relevance", "keywords", "users_count", "messages_count"])
    for r in filtered:
        writer.writerow([
            r["filename"],
            r["scraped_at"],
            r["source_group"],
            r["ai_relevance"],
            "|".join(r["keywords"]) if isinstance(r["keywords"], list) else r["keywords"],
            r["users_count"],
            r["messages_count"],
        ])

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Response(
        content=buf.getvalue().encode("utf-8-sig"),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="scrapes_export_{ts}.csv"'},
    )
