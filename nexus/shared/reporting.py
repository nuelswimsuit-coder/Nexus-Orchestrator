"""
Fleet Reporter — account-level audit from the Telefix Account-Mapper.

The *Account-Mapper* is the Telefix SQLite schema: each row in
``managed_groups`` ties a Telegram asset to ``owner_session`` (the phone /
session name that owns or operates it).  This module aggregates those rows,
enriches them with member and premium counts, writes JSON + CSV, and prints
a Rich summary table.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import structlog
from rich.console import Console
from rich.table import Table

from nexus.api.services.telefix_bridge import DB_PATH as _DEFAULT_TELEFIX_DB

log = structlog.get_logger(__name__)

AUDIT_JSON_NAME = "account_fleet_audit.json"
AUDIT_CSV_NAME = "account_fleet_audit.csv"

MASTER_FLEET_JSON_NAME = "master_fleet_report.json"
MASTER_FLEET_CSV_NAME = "master_fleet_report.csv"

_CHAT_KINDS_MAPPER = frozenset({"group", "supergroup", "channel"})


def aggregate_mapper_session_power(session_row: dict[str, Any]) -> dict[str, Any]:
    """One row: Session ID, phone, groups managed, reach, premium density."""
    st = str(session_row.get("status") or "ok")
    assets = session_row.get("assets") or []
    if not isinstance(assets, list):
        assets = []
    filtered = [
        a for a in assets
        if isinstance(a, dict) and a.get("kind") in _CHAT_KINDS_MAPPER
    ]
    total_groups = len(filtered)
    total_reach = sum(int(a.get("member_count") or 0) for a in filtered)
    prem_sum = 0
    reach_for_prem = 0
    for a in filtered:
        pm = a.get("premium_members")
        mc = int(a.get("member_count") or 0)
        if pm is not None:
            prem_sum += int(pm)
            reach_for_prem += mc
    premium_density: float | None
    if reach_for_prem > 0:
        premium_density = round(prem_sum / reach_for_prem, 6)
    else:
        premium_density = None
    uid = session_row.get("user_id")
    sid = str(uid) if uid is not None else str(session_row.get("session_file") or "")
    phone_raw = session_row.get("phone")
    phone_out = str(phone_raw).strip() if phone_raw else None
    return {
        "session_id": sid,
        "session_label": str(session_row.get("session_file") or ""),
        "phone": phone_out,
        "total_groups": total_groups,
        "total_reach": total_reach,
        "premium_density": premium_density,
        "mapper_status": st,
    }


def build_master_fleet_document_from_mapper_payload(
    payload: dict[str, Any],
) -> dict[str, Any]:
    sessions_raw = payload.get("sessions") or []
    rows: list[dict[str, Any]] = []
    for s in sessions_raw:
        if isinstance(s, dict):
            rows.append(aggregate_mapper_session_power(s))
    rows.sort(key=lambda r: int(r.get("total_reach") or 0), reverse=True)
    return {
        "generated_at": payload.get("generated_at")
        or datetime.now(timezone.utc).isoformat(),
        "source_map_path": payload.get("output_path"),
        "staged_dir": payload.get("staged_dir"),
        "sessions": rows,
        "grand_total_reach": sum(int(r.get("total_reach") or 0) for r in rows),
        "mapper_available": True,
    }


def write_master_fleet_from_mapper_payload(
    payload: dict[str, Any],
    out_dir: str | os.PathLike[str] | None = None,
) -> Path:
    root = Path(out_dir) if out_dir is not None else Path(str(payload.get("staged_dir") or "."))
    root = root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    doc = build_master_fleet_document_from_mapper_payload(payload)
    json_path = root / MASTER_FLEET_JSON_NAME
    csv_path = root / MASTER_FLEET_CSV_NAME
    _write_json(json_path, doc)
    _write_master_fleet_csv(csv_path, doc)
    log.info("master_fleet_report_written", json=str(json_path), csv=str(csv_path))
    return json_path


def _write_master_fleet_csv(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "session_id",
        "phone",
        "total_groups",
        "total_reach",
        "premium_density",
        "mapper_status",
        "session_label",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in doc.get("sessions", []):
            w.writerow({
                "session_id": row.get("session_id", ""),
                "phone": row.get("phone") or "",
                "total_groups": row.get("total_groups", 0),
                "total_reach": row.get("total_reach", 0),
                "premium_density": row.get("premium_density", ""),
                "mapper_status": row.get("mapper_status", ""),
                "session_label": row.get("session_label", ""),
            })


def _find_latest_map_json(root: Path) -> Path | None:
    if not root.is_dir():
        return None
    candidates = [p for p in root.glob("map_*.json") if p.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def load_mapper_fleet_snapshot(
    staged_root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    from nexus.shared.staged_accounts import staged_accounts_root

    root = (
        Path(staged_root).expanduser().resolve()
        if staged_root
        else staged_accounts_root()
    )
    master = root / MASTER_FLEET_JSON_NAME
    if master.is_file():
        try:
            data = json.loads(master.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("sessions"), list):
                data.setdefault("mapper_available", True)
                return data
        except (OSError, json.JSONDecodeError):
            pass

    latest = _find_latest_map_json(root)
    if latest is None:
        return {
            "generated_at": None,
            "sessions": [],
            "grand_total_reach": 0,
            "mapper_available": False,
            "source_map_path": None,
        }
    try:
        raw = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "generated_at": None,
            "sessions": [],
            "grand_total_reach": 0,
            "mapper_available": False,
            "source_map_path": None,
        }
    if not isinstance(raw, dict):
        return {
            "generated_at": None,
            "sessions": [],
            "grand_total_reach": 0,
            "mapper_available": False,
            "source_map_path": None,
        }
    merged = {**raw, "output_path": str(latest)}
    return build_master_fleet_document_from_mapper_payload(merged)


async def _scalper_settled_pnl_last_hours(redis: Any, hours: float) -> float:
    """Sum simulation `settled` ledger PnL within the last ``hours`` (UTC)."""
    if redis is None or hours <= 0:
        return 0.0
    try:
        from nexus.master.services.ultimate_scalper import LEDGER_KEY
    except Exception:
        LEDGER_KEY = "nexus:scalper:virtual_ledger"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    total = 0.0
    try:
        raws = await redis.lrange(LEDGER_KEY, 0, -1)
    except Exception:
        return 0.0
    for raw in raws or []:
        try:
            row = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if str(row.get("event") or "") != "settled":
            continue
        ts_raw = row.get("settled_at") or row.get("opened_at")
        if not ts_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt < cutoff:
            continue
        try:
            total += float(row.get("pnl_usd") or 0.0)
        except (TypeError, ValueError):
            continue
    return round(total, 4)


def _resolve_db_path(explicit: str | None) -> str:
    return (explicit or os.environ.get("TELEFIX_DB_PATH") or _DEFAULT_TELEFIX_DB).strip()


async def _table_columns(db: aiosqlite.Connection, table: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        rows = await cur.fetchall()
    return {str(r[1]) for r in rows}


def _as_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _premium_sql_expr() -> str:
    """SQLite expression counting a truthy is_premium value."""
    return (
        "SUM(CASE WHEN COALESCE(is_premium, 0) IN (1, '1', 'true', 'True', 'TRUE') "
        "THEN 1 ELSE 0 END)"
    )


async def _scraped_stats_for_group(
    db: aiosqlite.Connection,
    title: str | None,
    username: str | None,
    group_id: Any,
) -> tuple[int, int]:
    """
    Best-effort (member_count, premium_count) from scraped_users.source_group
    matching title, @username, or group_id string.
    """
    keys: list[str] = []
    if title and str(title).strip():
        keys.append(str(title).strip())
    if username and str(username).strip():
        u = str(username).strip().lstrip("@")
        keys.extend([u, f"@{u}"])
    if group_id is not None and str(group_id).strip():
        keys.append(str(group_id).strip())

    best_m, best_p = 0, 0
    expr = _premium_sql_expr()
    for key in keys:
        async with db.execute(
            f"""
            SELECT COUNT(*) AS c, {expr} AS p
            FROM scraped_users
            WHERE lower(trim(COALESCE(source_group, ''))) = lower(trim(?))
            """,
            (key,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                continue
            c = _as_int(row["c"])
            p = _as_int(row["p"])
            if c > best_m:
                best_m, best_p = c, p
    return best_m, best_p


async def collect_account_fleet_from_mapper(
    db_path: str | None = None,
) -> dict[str, Any]:
    """
    Aggregate Telefix ``managed_groups`` by ``owner_session`` (account name).

    Returns a dict suitable for JSON serialization with keys:
    generated_at, db_path, db_available, accounts, grand_total_reach,
    grand_total_premium.
    """
    path = _resolve_db_path(db_path)
    now = datetime.now(timezone.utc).isoformat()
    base: dict[str, Any] = {
        "generated_at": now,
        "db_path": path,
        "db_available": False,
        "accounts": [],
        "grand_total_reach": 0,
        "grand_total_premium": 0,
    }

    if not os.path.exists(path):
        log.warning("fleet_audit_db_missing", path=path)
        return base

    try:
        uri = f"file:{path.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")

            mg_cols = await _table_columns(db, "managed_groups")
            has_members_col = "member_count" in mg_cols or "participants_count" in mg_cols
            member_col = "member_count" if "member_count" in mg_cols else (
                "participants_count" if "participants_count" in mg_cols else None
            )
            kind_col = None
            for candidate in ("chat_type", "kind", "group_type", "role"):
                if candidate in mg_cols:
                    kind_col = candidate
                    break

            async with db.execute(
                "SELECT * FROM managed_groups ORDER BY owner_session, title"
            ) as cur:
                raw_rows = await cur.fetchall()
            rows = [dict(r) for r in raw_rows]

            by_account: dict[str, list[dict[str, Any]]] = defaultdict(list)

            for row in rows:
                owner = (row.get("owner_session") or "").strip() or "(unassigned)"
                title = row.get("title")
                username = row.get("username")
                gid = row.get("group_id")

                if has_members_col and member_col:
                    members = _as_int(row.get(member_col))
                    if "premium_count" in mg_cols:
                        prem = _as_int(row.get("premium_count"))
                    else:
                        _, prem = await _scraped_stats_for_group(db, title, username, gid)
                else:
                    members, prem = await _scraped_stats_for_group(db, title, username, gid)

                kind = "channel" if kind_col and str(row.get(kind_col) or "").lower() in (
                    "channel", "broadcast", "c"
                ) else "group"

                asset = {
                    "kind": kind,
                    "group_id": gid,
                    "title": title,
                    "username": username,
                    "member_count": members,
                    "premium_count": prem,
                    "last_automation": row.get("last_automation"),
                }
                by_account[owner].append(asset)

        accounts_out: list[dict[str, Any]] = []
        grand_reach = 0
        grand_prem = 0

        for account_name in sorted(by_account.keys(), key=str.lower):
            assets = by_account[account_name]
            total_reach = sum(_as_int(a["member_count"]) for a in assets)
            total_prem = sum(_as_int(a["premium_count"]) for a in assets)
            grand_reach += total_reach
            grand_prem += total_prem
            accounts_out.append({
                "account_name": account_name,
                "owned_assets": assets,
                "total_reach": total_reach,
                "total_premium": total_prem,
            })

        base["db_available"] = True
        base["accounts"] = accounts_out
        base["grand_total_reach"] = grand_reach
        base["grand_total_premium"] = grand_prem

        log.info(
            "fleet_audit_collected",
            accounts=len(accounts_out),
            grand_reach=grand_reach,
        )
        return base

    except Exception as exc:
        log.error("fleet_audit_collect_error", error=str(exc), path=path)
        base["error"] = str(exc)
        return base


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _write_csv(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "account_name",
        "account_total_reach",
        "account_total_premium",
        "asset_kind",
        "group_id",
        "title",
        "username",
        "member_count",
        "premium_count",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for acc in payload.get("accounts", []):
            name = acc.get("account_name", "")
            tr = acc.get("total_reach", 0)
            tp = acc.get("total_premium", 0)
            for asset in acc.get("owned_assets", []):
                w.writerow({
                    "account_name": name,
                    "account_total_reach": tr,
                    "account_total_premium": tp,
                    "asset_kind": asset.get("kind", ""),
                    "group_id": asset.get("group_id", ""),
                    "title": asset.get("title", ""),
                    "username": asset.get("username", ""),
                    "member_count": asset.get("member_count", 0),
                    "premium_count": asset.get("premium_count", 0),
                })


def print_fleet_audit_summary(payload: dict[str, Any], console: Console | None = None) -> None:
    """Rich table: one row per account plus fleet totals."""
    con = console or Console()
    table = Table(
        title="[bold cyan]Account Fleet Audit[/bold cyan]",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Account (session)", overflow="fold")
    table.add_column("Assets", justify="right")
    table.add_column("Total reach", justify="right")
    table.add_column("Premium", justify="right")

    for acc in payload.get("accounts", []):
        assets = acc.get("owned_assets") or []
        table.add_row(
            str(acc.get("account_name", "")),
            str(len(assets)),
            str(acc.get("total_reach", 0)),
            str(acc.get("total_premium", 0)),
        )

    table.add_section()
    table.add_row(
        "[bold]Fleet total[/bold]",
        "",
        str(payload.get("grand_total_reach", 0)),
        str(payload.get("grand_total_premium", 0)),
    )

    con.print()
    con.print(table)
    con.print(
        f"[dim]DB: {payload.get('db_path', '')}  •  "
        f"available={payload.get('db_available')}  •  "
        f"{payload.get('generated_at', '')}[/dim]"
    )


async def run_account_fleet_audit(
    db_path: str | None = None,
    output_dir: str | os.PathLike[str] | None = None,
    *,
    write_reports: bool = True,
    show_console: bool = True,
    console: Console | None = None,
) -> dict[str, Any]:
    """
    Collect mapper data, optionally write ``account_fleet_audit.json`` and
    ``account_fleet_audit.csv``, and print the Rich summary.
    """
    payload = await collect_account_fleet_from_mapper(db_path=db_path)
    out = Path(output_dir) if output_dir is not None else Path.cwd()

    if write_reports:
        _write_json(out / AUDIT_JSON_NAME, payload)
        _write_csv(out / AUDIT_CSV_NAME, payload)

    if show_console:
        print_fleet_audit_summary(payload, console=console)

    return payload


def run_account_fleet_audit_sync(
    db_path: str | None = None,
    output_dir: str | os.PathLike[str] | None = None,
    *,
    write_reports: bool = True,
    show_console: bool = True,
    console: Console | None = None,
) -> dict[str, Any]:
    """Blocking wrapper for scripts and CLI."""
    return asyncio.run(
        run_account_fleet_audit(
            db_path=db_path,
            output_dir=output_dir,
            write_reports=write_reports,
            show_console=show_console,
            console=console,
        )
    )


# ── Daily Hustle — Telegram summary at local midnight (00:00) and 08:00 ───────


class DailyHustleReporter:
    """
    Sends a compact “bottom line” report to all notification providers.
    Schedules: local **00:00** (24h hustle) and **08:00** (night-watch morning).
    Lazy-imports master reporting collectors to avoid loading the master stack
    for simple fleet-audit CLI runs.
    """

    def __init__(self, notifier: Any, redis: Any) -> None:
        self._notifier = notifier
        self._redis = redis
        self._running = False

    async def send_report(self, *, morning_night_watch: bool = False) -> dict[str, Any]:
        from datetime import datetime, timezone

        from nexus.api.services.telefix_bridge import get_windowed_stats
        from nexus.master.services.reporting import (
            LAST_REPORT_KEY,
            LAST_REPORT_TTL,
            REPORT_SENDING_KEY,
            REPORT_SENDING_TTL,
            _collect_report_data,
            _collect_trading_stats,
        )

        period_label = "DAILY_HUSTLE_MORNING" if morning_night_watch else "DAILY_HUSTLE"
        if self._redis:
            await self._redis.set(
                REPORT_SENDING_KEY,
                __import__("json").dumps({
                    "sending": True,
                    "period": period_label,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }),
                ex=REPORT_SENDING_TTL,
            )

        try:
            window_minutes = 480 if morning_night_watch else 1440
            window = await get_windowed_stats(window_minutes=window_minutes)
            window_hours = 8 if morning_night_watch else 24
            base_24h, trading = await asyncio.gather(
                _collect_report_data(window_hours=window_hours),
                _collect_trading_stats(self._redis),
            )
            hustle = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "new_scraped_24h": int(window.get("new_scraped_users_window", 0)),
                "new_pipeline_24h": int(window.get("new_pipeline_users_window", 0)),
                "trading": trading,
                "sessions": {
                    "active": base_24h.get("active_sessions", 0),
                    "frozen": base_24h.get("frozen_sessions", 0),
                },
                "window_minutes": window_minutes,
                "morning_night_watch": morning_night_watch,
            }

            openclaw: dict[str, Any] = {}
            if self._redis:
                try:
                    raw_oc = await self._redis.get("nexus:openclaw:news_sentiment")
                    if raw_oc:
                        openclaw = json.loads(raw_oc)
                except Exception:
                    openclaw = {}

            alpha = str(openclaw.get("channel_title") or "(none)").strip() or "(none)"
            sim_pnl = float(trading.get("virtual_pnl", 0.0) or 0.0)
            real_hint = 0.0
            if self._redis:
                try:
                    raw_p = await self._redis.get("nexus:poly:pnl")
                    if raw_p:
                        pj = json.loads(raw_p)
                        real_hint = float(pj.get("session_pnl_usd", pj.get("pnl_usd", 0.0)) or 0.0)
                except Exception:
                    pass

            sim_mode = os.getenv("SIMULATION_MODE", "true").lower() in {"1", "true", "yes"}
            slept_profit = await _scalper_settled_pnl_last_hours(self._redis, 8.0)
            hourly_rate = slept_profit / 8.0 if morning_night_watch else 0.0

            if morning_night_watch:
                fleet_label = "FLEET REACH (last 8h)"
                lines = [
                    "☀️ *NEXUS — GOOD MORNING / NIGHT WATCH*",
                    "────────────────────────",
                    f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC",
                    "",
                    f"Good morning. While you slept (last 8 hours), the system generated "
                    f"${slept_profit:+.2f} profit at a rate of ${hourly_rate:+.2f} per hour.",
                    "",
                    "💰 *NET PROFIT (context)*",
                    f"• Sim (paper, session): `${sim_pnl:+.2f}`",
                    f"• Real hint (Poly): `${real_hint:+.4f}`",
                    f"• Mode: {'SIM' if sim_mode else 'LIVE'}",
                    "",
                    f"🛰 *{fleet_label}*",
                    f"• New scraped users: {hustle['new_scraped_24h']}",
                    f"• New pipeline users: {hustle['new_pipeline_24h']}",
                    "",
                    "📡 *TOP ALPHA SOURCE*",
                    f"• {alpha}",
                    "",
                    "⚡ *SYSTEM HEALTH*",
                    f"• Master online: {'yes' if trading.get('master_online') else 'no'}",
                    f"• Workers seen: {trading.get('worker_count', 0)}",
                    f"• Active sessions: {hustle['sessions']['active']} · "
                    f"Frozen: {hustle['sessions']['frozen']}",
                    "",
                    "────────────────────────",
                    "_Operator summary — Nexus Orchestrator_",
                ]
            else:
                lines = [
                    "🌑 *NEXUS — DAILY HUSTLE*",
                    "────────────────────────",
                    f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC",
                    "",
                    "💰 *NET PROFIT*",
                    f"• Sim (paper): `${sim_pnl:+.2f}`",
                    f"• Real hint (Poly): `${real_hint:+.4f}`",
                    f"• Mode: {'SIM' if sim_mode else 'LIVE'}",
                    "",
                    "🛰 *FLEET REACH (24h)*",
                    f"• New scraped users: {hustle['new_scraped_24h']}",
                    f"• New pipeline users: {hustle['new_pipeline_24h']}",
                    "",
                    "📡 *TOP ALPHA SOURCE*",
                    f"• {alpha}",
                    "",
                    "⚡ *SYSTEM HEALTH*",
                    f"• Master online: {'yes' if trading.get('master_online') else 'no'}",
                    f"• Workers seen: {trading.get('worker_count', 0)}",
                    f"• Active sessions: {hustle['sessions']['active']} · "
                    f"Frozen: {hustle['sessions']['frozen']}",
                    "",
                    "────────────────────────",
                    "_Operator summary — Nexus Orchestrator_",
                ]
            hustle["slept_scalper_pnl_8h_usd"] = slept_profit
            hustle["slept_hourly_rate_usd"] = round(hourly_rate, 4) if morning_night_watch else None
            body = "\n".join(lines)

            if self._notifier:
                from nexus.shared.notifications.base import Alert, AlertLevel  # noqa: PLC0415

                alert_title = (
                    "☀️ Nexus Night Watch — Good Morning"
                    if morning_night_watch
                    else "🌑 Nexus Daily Hustle"
                )
                await self._notifier.notify(
                    Alert(
                        title=alert_title,
                        body=body,
                        level=AlertLevel.INFO,
                        metadata={
                            "kind": "daily_hustle_morning" if morning_night_watch else "daily_hustle",
                        },
                    )
                )

            hustle["telegram_body"] = body
            if self._redis:
                merged = {**base_24h, **{"daily_hustle": hustle}}
                await self._redis.set(
                    LAST_REPORT_KEY,
                    __import__("json").dumps(merged),
                    ex=LAST_REPORT_TTL,
                )
            return hustle
        finally:
            if self._redis:
                await asyncio.sleep(REPORT_SENDING_TTL)
                await self._redis.delete(REPORT_SENDING_KEY)

    async def run_loop(self) -> None:
        from datetime import datetime

        self._running = True
        log.info("daily_hustle_reporter_started", at_local=["00:00", "08:00"])
        last_midnight: str = ""
        last_morning: str = ""
        while self._running:
            await asyncio.sleep(60)
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            if now.hour == 0 and now.minute == 0 and last_midnight != today:
                last_midnight = today
                try:
                    await self.send_report(morning_night_watch=False)
                except Exception as exc:
                    log.error("daily_hustle_send_error", error=str(exc))
            if now.hour == 8 and now.minute == 0 and last_morning != today:
                last_morning = today
                try:
                    await self.send_report(morning_night_watch=True)
                except Exception as exc:
                    log.error("daily_hustle_morning_send_error", error=str(exc))

    def stop(self) -> None:
        self._running = False


if __name__ == "__main__":
    run_account_fleet_audit_sync()
