"""
לוח בקרה — Dashboard Tab
Real-time system health via Redis-direct polling (2 s) + FastAPI fallback.

Data flow:
  RedisLivePoller (QThread, 2 s)
    → telemetry(list[dict])      → _on_telemetry()   → worker rows + cards
    → queue_depth(int)           → _on_queue_depth()  → task card
    → redis_latency_ms(float)    → _on_redis_ping()   → Redis card
    → redis_error(str)           → _on_redis_error()  → log panel

  ApiPoller (QThread, 10 s) — fallback for data not in Redis
    → /api/business/stats        → _on_api_stats()

UI mutations happen exclusively in the main thread via Signal→Slot connections.
"""
from __future__ import annotations

import os
from datetime import datetime

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QColor, QTextCursor
from PyQt6.QtWidgets import (
    QFrame, QGroupBox, QHBoxLayout, QLabel,
    QPlainTextEdit, QPushButton, QScrollArea,
    QSplitter, QVBoxLayout, QWidget,
)

from ..theme import C
from ..workers import ApiPoller, RedisLivePoller

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
API_BASE  = os.environ.get("NEXUS_API_BASE", "http://localhost:8001")
API_KEY   = os.environ.get("NEXUS_API_KEY",  "")

_TAG_COLOR = {
    "error":   C["danger"],
    "warn":    C["warn"],
    "success": C["success"],
    "accent":  C["accent"],
    "dim":     C["text3"],
}


# ── Stat card helper ──────────────────────────────────────────────────────────

class StatCard(QFrame):
    """Small metric card: large value label + small title label."""

    def __init__(self, title: str, value: str = "—",
                 color: str = C["silver"], parent=None) -> None:
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet(
            f"QFrame {{ background:{C['surface2']}; border:1px solid {C['border2']}; }}"
        )
        v = QVBoxLayout(self)
        v.setContentsMargins(16, 12, 16, 12)
        v.setSpacing(4)

        self._val = QLabel(value)
        self._val.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._set_color(color)

        self._ttl = QLabel(title.upper())
        self._ttl.setStyleSheet(
            f"color:{C['silver3']}; font-size:9px; letter-spacing:2px; background:transparent;"
        )
        self._ttl.setAlignment(Qt.AlignmentFlag.AlignCenter)

        v.addWidget(self._val)
        v.addWidget(self._ttl)

    def set_value(self, text: str, color: str | None = None) -> None:
        self._val.setText(text)
        if color:
            self._set_color(color)

    def _set_color(self, color: str) -> None:
        self._val.setStyleSheet(
            f"color:{color}; font-size:24px; font-weight:bold; background:transparent;"
        )


# ── Worker row helper ─────────────────────────────────────────────────────────

class WorkerRow(QFrame):
    """Compact row showing one node's live stats."""

    def __init__(self, node_id: str, parent=None) -> None:
        super().__init__(parent)
        self.node_id = node_id
        self.setStyleSheet(
            f"QFrame {{ background:{C['surface2']}; border:1px solid {C['border']}; }}"
        )
        h = QHBoxLayout(self)
        h.setContentsMargins(10, 6, 10, 6)

        self._dot = QLabel("●")
        self._dot.setFixedWidth(16)
        self._dot.setStyleSheet(f"color:{C['success']}; font-size:10px; background:transparent;")

        self._name  = QLabel(node_id)
        self._name.setStyleSheet(f"color:{C['silver']}; font-weight:bold; background:transparent;")
        self._name.setFixedWidth(160)

        self._role  = QLabel("")
        self._role.setStyleSheet(f"color:{C['silver3']}; font-size:10px; background:transparent;")
        self._role.setFixedWidth(56)

        self._cpu   = self._metric("CPU")
        self._ram   = self._metric("RAM")
        self._jobs  = self._metric("JOBS")
        self._temp  = self._metric("TEMP")

        for w in (self._dot, self._name, self._role,
                  self._cpu, self._ram, self._jobs, self._temp):
            h.addWidget(w)
        h.addStretch()

    def _metric(self, label: str) -> QLabel:
        w = QLabel(f"{label}: —")
        w.setStyleSheet(f"color:{C['text2']}; font-size:11px; background:transparent;")
        w.setFixedWidth(90)
        return w

    def update(self, hb: dict) -> None:
        """Refresh from a NodeHeartbeat dict. Must be called from main thread."""
        online = hb.get("status", "online") == "online"
        cpu    = float(hb.get("cpu_percent", 0))
        ram    = float(hb.get("ram_used_mb", 0))
        jobs   = int(hb.get("active_jobs",  0))
        temp   = hb.get("cpu_temp_c")
        role   = str(hb.get("role", "")).upper()[:6]

        self._dot.setStyleSheet(
            f"color:{ C['success'] if online else C['danger']}; "
            f"font-size:10px; background:transparent;"
        )
        self._role.setText(role)

        cpu_color = (C["danger"] if cpu > 85
                     else C["warn"] if cpu > 60
                     else C["text2"])
        self._cpu.setText(f"CPU: {cpu:.0f}%")
        self._cpu.setStyleSheet(f"color:{cpu_color}; font-size:11px; background:transparent;")

        ram_gb = ram / 1024
        self._ram.setText(f"RAM: {ram_gb:.1f}GB" if ram_gb >= 1 else f"RAM: {ram:.0f}MB")

        job_color = C["accent"] if jobs > 0 else C["text3"]
        self._jobs.setText(f"JOBS: {jobs}")
        self._jobs.setStyleSheet(f"color:{job_color}; font-size:11px; background:transparent;")

        if temp is not None:
            t_color = C["danger"] if temp > 85 else C["warn"] if temp > 70 else C["text2"]
            self._temp.setText(f"TEMP: {temp:.0f}°C")
            self._temp.setStyleSheet(f"color:{t_color}; font-size:11px; background:transparent;")
        else:
            self._temp.setText("TEMP: —")


# ── Dashboard Tab ─────────────────────────────────────────────────────────────

class DashboardTab(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._worker_rows: dict[str, WorkerRow] = {}
        self._redis_poller: RedisLivePoller | None = None
        self._api_poller:   ApiPoller | None = None
        self._setup_ui()
        self._start_pollers()

    # ── Build UI ───────────────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(14)

        # ── Header ─────────────────────────────────────────────────────────────
        hdr = QHBoxLayout()
        v_hdr = QVBoxLayout()
        t = QLabel("לוח בקרה")
        t.setStyleSheet(f"color:{C['silver']}; font-size:22px; font-weight:bold;")
        s = QLabel("NEXUS SUPREME — LIVE SYSTEM DASHBOARD")
        s.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:3px;")
        v_hdr.addWidget(t)
        v_hdr.addWidget(s)
        self._last_update = QLabel("")
        self._last_update.setStyleSheet(f"color:{C['text3']}; font-size:10px;")
        hdr.addLayout(v_hdr)
        hdr.addStretch()
        hdr.addWidget(self._last_update)
        root.addLayout(hdr)

        # ── Stat cards ─────────────────────────────────────────────────────────
        cards_row = QHBoxLayout()
        cards_row.setSpacing(10)
        self._card_master  = StatCard("מאסטר",        "—")
        self._card_workers = StatCard("עובדים",        "—")
        self._card_queue   = StatCard("תור משימות",    "—", C["accent"])
        self._card_jobs    = StatCard("עבודות (24h)",  "—")
        self._card_redis   = StatCard("Redis Ping",    "—", C["success"])
        for c in (self._card_master, self._card_workers,
                  self._card_queue, self._card_jobs, self._card_redis):
            cards_row.addWidget(c)
        root.addLayout(cards_row)

        # ── Splitter: worker list | log ─────────────────────────────────────────
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setStyleSheet(f"QSplitter::handle {{ background:{C['border']}; width:1px; }}")

        # Worker panel (left)
        wp = QGroupBox("  WORKER NODES  ")
        wp.setStyleSheet(self._grp_style())
        self._worker_vbox = QVBoxLayout(wp)
        self._worker_vbox.setSpacing(5)
        self._worker_vbox.setContentsMargins(8, 14, 8, 8)
        self._worker_vbox.addStretch()

        scroll = QScrollArea()
        scroll.setWidget(wp)
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border:none; }")
        splitter.addWidget(scroll)

        # Log panel (right)
        lp = QGroupBox("  SYSTEM LOG  ")
        lp.setStyleSheet(self._grp_style())
        lp_v = QVBoxLayout(lp)
        lp_v.setContentsMargins(8, 14, 8, 8)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setStyleSheet(
            f"background:{C['surface']}; color:{C['text2']}; border:none; "
            f"font-family:'Consolas','JetBrains Mono',monospace; font-size:11px; line-height:1.5;"
        )
        self._log.setMaximumBlockCount(3000)
        lp_v.addWidget(self._log)

        btn_row = QHBoxLayout()
        btn_clear = QPushButton("נקה לוג")
        btn_clear.setFixedWidth(80)
        btn_clear.clicked.connect(self._log.clear)
        btn_row.addStretch()
        btn_row.addWidget(btn_clear)
        lp_v.addLayout(btn_row)

        splitter.addWidget(lp)
        splitter.setSizes([290, 640])
        root.addWidget(splitter, stretch=1)

        # ── Footer actions ──────────────────────────────────────────────────────
        footer = QHBoxLayout()
        btn_refresh = QPushButton("⟳ רענן")
        btn_refresh.setObjectName("btnPrimary")
        btn_refresh.setFixedWidth(100)
        btn_refresh.clicked.connect(self._restart_pollers)
        btn_panic = QPushButton("🛑 PANIC STOP")
        btn_panic.setObjectName("btnDanger")
        btn_panic.clicked.connect(self._panic_stop)
        footer.addStretch()
        footer.addWidget(btn_refresh)
        footer.addWidget(btn_panic)
        root.addLayout(footer)

    def _grp_style(self) -> str:
        return (
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; "
            f"margin-top:14px; padding:10px; }}"
        )

    # ── Pollers ────────────────────────────────────────────────────────────────

    def _start_pollers(self) -> None:
        # Redis direct — 2 s
        self._redis_poller = RedisLivePoller(
            redis_url = REDIS_URL,
            interval  = 2.0,
        )
        self._redis_poller.telemetry.connect(self._on_telemetry)
        self._redis_poller.queue_depth.connect(self._on_queue_depth)
        self._redis_poller.redis_latency_ms.connect(self._on_redis_ping)
        self._redis_poller.redis_error.connect(self._on_redis_error)
        self._redis_poller.start()

        # API fallback — 10 s (business stats not in Redis)
        self._api_poller = ApiPoller(
            base_url  = API_BASE,
            endpoints = ["/api/business/stats"],
            interval  = 10.0,
            api_key   = API_KEY,
        )
        self._api_poller.data_ready.connect(self._on_api_data)
        self._api_poller.error.connect(
            lambda ep, e: self._log_append(f"[API] {ep}: {e}", "warn")
        )
        self._api_poller.start()

    def _stop_pollers(self) -> None:
        for p in (self._redis_poller, self._api_poller):
            if p:
                p.stop()
                p.wait(500)

    def _restart_pollers(self) -> None:
        self._stop_pollers()
        self._log_append("[רענון ידני]", "accent")
        self._start_pollers()

    # ── Slots (main-thread UI mutations) ──────────────────────────────────────

    def _on_telemetry(self, nodes: list) -> None:
        """Called from main thread via Signal. Updates all worker rows + master card."""
        ts = datetime.now().strftime("%H:%M:%S")
        self._last_update.setText(f"עדכון: {ts}")

        if not nodes:
            return

        online_workers = 0
        master_node    = None

        for hb in nodes:
            nid  = hb.get("node_id", "unknown")
            role = str(hb.get("role", "")).lower()

            if role == "master":
                master_node = hb
            else:
                online_workers += 1

            # Upsert WorkerRow
            if nid not in self._worker_rows:
                row = WorkerRow(nid)
                # Insert before the stretch spacer
                idx = self._worker_vbox.count() - 1
                self._worker_vbox.insertWidget(idx, row)
                self._worker_rows[nid] = row
            self._worker_rows[nid].update(hb)

        # Master card
        if master_node:
            cpu = float(master_node.get("cpu_percent", 0))
            color = C["danger"] if cpu > 85 else C["warn"] if cpu > 60 else C["success"]
            self._card_master.set_value(f"CPU {cpu:.0f}%", color)
        else:
            self._card_master.set_value("OFFLINE", C["danger"])

        # Workers card
        total = len(nodes) - (1 if master_node else 0)
        color = C["success"] if online_workers == total else C["warn"] if online_workers > 0 else C["danger"]
        self._card_workers.set_value(f"{online_workers}/{total}", color)

        self._log_append(
            f"[Redis] {len(nodes)} nodes · {online_workers} workers online",
            "dim",
        )

    def _on_queue_depth(self, depth: int) -> None:
        color = C["danger"] if depth > 50 else C["warn"] if depth > 10 else C["success"]
        self._card_queue.set_value(str(depth), color)

    def _on_redis_ping(self, ms: float) -> None:
        color = C["danger"] if ms > 50 else C["warn"] if ms > 15 else C["success"]
        self._card_redis.set_value(f"{ms:.1f} ms", color)

    def _on_redis_error(self, error: str) -> None:
        self._card_redis.set_value("ERR", C["danger"])
        self._card_master.set_value("N/A", C["danger"])
        self._log_append(f"[Redis] {error}", "error")

    def _on_api_data(self, endpoint: str, data: object) -> None:
        if not isinstance(data, dict):
            return
        if endpoint == "/api/business/stats":
            jobs = data.get("completed_jobs_24h", data.get("jobs_today", "—"))
            self._card_jobs.set_value(str(jobs))

    # ── Log helper ─────────────────────────────────────────────────────────────

    def _log_append(self, text: str, tag: str = "normal") -> None:
        ts    = datetime.now().strftime("%H:%M:%S")
        color = _TAG_COLOR.get(tag, C["text2"])
        self._log.appendHtml(
            f'<span style="color:{C["text3"]}">[{ts}]</span>'
            f' <span style="color:{color}">{text}</span>'
        )
        self._log.moveCursor(QTextCursor.MoveOperation.End)

    # ── Actions ────────────────────────────────────────────────────────────────

    def _panic_stop(self) -> None:
        try:
            import requests as req
            req.post(
                f"{API_BASE}/api/flight-mode/panic",
                headers={"X-Nexus-Api-Key": API_KEY},
                timeout=5,
            )
            self._log_append("🛑 PANIC STOP נשלח!", "error")
        except Exception as exc:
            self._log_append(f"שגיאה בשליחת PANIC: {exc}", "error")

    # ── Cleanup ────────────────────────────────────────────────────────────────

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._stop_pollers()
        super().closeEvent(event)
