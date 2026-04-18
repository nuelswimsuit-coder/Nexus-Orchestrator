"""
מנוע שיווק ו-AHU — Marketing & AHU Portal Tab
Full AHU feature portal: scraper control, enrollments, bot management, migration.
"""
from __future__ import annotations

import os

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtWidgets import (
    QFrame, QGroupBox, QHBoxLayout, QLabel, QPlainTextEdit,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem,
    QHeaderView, QVBoxLayout, QWidget,
)

from ..theme import C
from ..workers import ApiPoller, MigrationWorker

API_BASE = os.environ.get("NEXUS_API_BASE", "http://localhost:8001")
API_KEY  = os.environ.get("NEXUS_API_KEY",  "")


def _get(path: str) -> dict:
    import requests
    try:
        r = requests.get(
            f"{API_BASE}{path}",
            headers={"X-Nexus-Api-Key": API_KEY},
            timeout=8,
        )
        return r.json()
    except Exception:
        return {}


def _post(path: str, payload: dict | None = None) -> dict:
    import requests
    try:
        r = requests.post(
            f"{API_BASE}{path}",
            json    = payload or {},
            headers = {"X-Nexus-Api-Key": API_KEY},
            timeout = 15,
        )
        return r.json()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


class AhuPortalTab(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._migrate_worker: MigrationWorker | None = None
        self._poller: ApiPoller | None = None
        self._setup_ui()
        self._start_polling()

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(16)

        # Header
        title = QLabel("מנוע שיווק ו-AHU")
        title.setStyleSheet(f"color:{C['silver']}; font-size:22px; font-weight:bold;")
        sub = QLabel("MANAGEMENT AHU PORTAL · SESSIONS · SCRAPER · ENROLLMENT")
        sub.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:3px;")
        root.addWidget(title)
        root.addWidget(sub)

        # Stat cards
        cards = QHBoxLayout()
        self._lbl_users     = self._mini_card("משתמשים שנאספו", "—")
        self._lbl_premium   = self._mini_card("פרימיום", "—", C["accent"])
        self._lbl_sessions  = self._mini_card("סשנים", "—", C["success"])
        self._lbl_enroll    = self._mini_card("הצטרפויות", "—")
        self._lbl_bot_stat  = self._mini_card("בוט AHU", "—")
        for w in (self._lbl_users[0], self._lbl_premium[0], self._lbl_sessions[0],
                  self._lbl_enroll[0], self._lbl_bot_stat[0]):
            cards.addWidget(w)
        root.addLayout(cards)

        # Controls row
        ctrl = QHBoxLayout()
        btn_start_bot = QPushButton("▶ הפעל בוט AHU")
        btn_start_bot.setObjectName("btnSuccess")
        btn_start_bot.clicked.connect(self._start_ahu_bot)

        btn_stop_bot = QPushButton("⏹ עצור בוט AHU")
        btn_stop_bot.setObjectName("btnDanger")
        btn_stop_bot.clicked.connect(self._stop_ahu_bot)

        btn_sync = QPushButton("🔄 סנכרן סשנים")
        btn_sync.clicked.connect(self._sync_sessions)

        btn_migrate = QPushButton("📦 מיגרציה ראשונית")
        btn_migrate.setObjectName("btnPrimary")
        btn_migrate.clicked.connect(self._run_migration)

        ctrl.addWidget(btn_start_bot)
        ctrl.addWidget(btn_stop_bot)
        ctrl.addWidget(btn_sync)
        ctrl.addStretch()
        ctrl.addWidget(btn_migrate)
        root.addLayout(ctrl)

        # Splitter: sessions table | log
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Sessions table
        grp_sess = QGroupBox("  SESSIONS  ")
        grp_sess.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:10px; }}"
        )
        sess_v = QVBoxLayout(grp_sess)
        self._sess_table = QTableWidget(0, 2)
        self._sess_table.setHorizontalHeaderLabels(["תיקייה", "ספירה"])
        self._sess_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._sess_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        sess_v.addWidget(self._sess_table)
        splitter.addWidget(grp_sess)

        # Targets table
        grp_tgt = QGroupBox("  TARGET GROUPS  ")
        grp_tgt.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:10px; }}"
        )
        tgt_v = QVBoxLayout(grp_tgt)
        self._tgt_table = QTableWidget(0, 3)
        self._tgt_table.setHorizontalHeaderLabels(["שם", "קישור", "תפקיד"])
        self._tgt_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._tgt_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        tgt_v.addWidget(self._tgt_table)
        splitter.addWidget(grp_tgt)
        splitter.setSizes([300, 400])
        root.addWidget(splitter, stretch=1)

        # Migration log
        grp_log = QGroupBox("  לוג פעולות  ")
        grp_log.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:8px; }}"
        )
        log_v = QVBoxLayout(grp_log)
        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setFixedHeight(110)
        self._log.setStyleSheet(
            f"background:{C['surface']}; color:{C['text2']}; "
            f"font-family:'Consolas',monospace; font-size:11px; border:none;"
        )
        log_v.addWidget(self._log)
        root.addWidget(grp_log)

    def _mini_card(self, title: str, value: str, color: str = C["silver"]):
        frame = QFrame()
        frame.setStyleSheet(
            f"QFrame {{ background:{C['surface2']}; border:1px solid {C['border2']}; }}"
        )
        v = QVBoxLayout(frame)
        v.setContentsMargins(12, 8, 12, 8)
        lv = QLabel(value)
        lv.setStyleSheet(f"color:{color}; font-size:20px; font-weight:bold;")
        lv.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lt = QLabel(title.upper())
        lt.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:1px;")
        lt.setAlignment(Qt.AlignmentFlag.AlignCenter)
        v.addWidget(lv)
        v.addWidget(lt)
        return frame, lv

    def _start_polling(self) -> None:
        self._poller = ApiPoller(
            base_url  = API_BASE,
            endpoints = ["/api/ahu/status", "/api/ahu/stats", "/api/ahu/sessions", "/api/ahu/targets"],
            interval  = 10.0,
            api_key   = API_KEY,
        )
        self._poller.data_ready.connect(self._on_data)
        self._poller.error.connect(lambda ep, e: self._log_line(f"[שגיאה] {ep}: {e}"))
        self._poller.start()

    def _on_data(self, endpoint: str, data: object) -> None:
        if not isinstance(data, dict):
            return
        if endpoint == "/api/ahu/status":
            running = data.get("bot_running", False)
            self._lbl_bot_stat[1].setText("ONLINE" if running else "OFFLINE")
            self._lbl_bot_stat[1].setStyleSheet(
                f"color:{ C['success'] if running else C['danger']}; font-size:20px; font-weight:bold;"
            )
            self._lbl_sessions[1].setText(str(data.get("total_sessions", "—")))

        elif endpoint == "/api/ahu/stats":
            u = data.get("users", {})
            self._lbl_users[1].setText(f"{u.get('total',0):,}")
            self._lbl_premium[1].setText(f"{u.get('premium_pct',0)}%")
            self._lbl_enroll[1].setText(
                str(data.get("enrollments", {}).get("total", "—"))
            )

        elif endpoint == "/api/ahu/sessions":
            self._populate_sessions(data)

        elif endpoint == "/api/ahu/targets":
            self._populate_targets(data.get("targets", []))

    def _populate_sessions(self, data: dict) -> None:
        rows = sorted(data.items())
        self._sess_table.setRowCount(len(rows))
        for i, (folder, info) in enumerate(rows):
            cnt = info.get("count", 0) if isinstance(info, dict) else 0
            self._sess_table.setItem(i, 0, QTableWidgetItem(folder))
            self._sess_table.setItem(i, 1, QTableWidgetItem(str(cnt)))

    def _populate_targets(self, targets: list) -> None:
        self._tgt_table.setRowCount(len(targets))
        for i, t in enumerate(targets):
            self._tgt_table.setItem(i, 0, QTableWidgetItem(t.get("title", "")))
            self._tgt_table.setItem(i, 1, QTableWidgetItem(t.get("link", "")))
            self._tgt_table.setItem(i, 2, QTableWidgetItem(t.get("role", "")))

    def _log_line(self, text: str) -> None:
        from datetime import datetime
        ts = datetime.now().strftime("%H:%M:%S")
        self._log.appendPlainText(f"[{ts}] {text}")

    def _start_ahu_bot(self) -> None:
        result = _post("/api/ahu/bot/start")
        ok = result.get("ok", False)
        self._log_line(
            f"✅ בוט AHU הופעל (PID {result.get('pid')})" if ok
            else f"❌ {result.get('detail', result.get('error',''))}"
        )

    def _stop_ahu_bot(self) -> None:
        result = _post("/api/ahu/bot/stop")
        ok = result.get("ok", False)
        self._log_line(
            "✅ בוט AHU נעצר" if ok
            else f"❌ {result.get('detail', result.get('error',''))}"
        )

    def _sync_sessions(self) -> None:
        result = _post("/api/ahu/sessions/sync-scanned")
        self._log_line(
            f"🔄 סנכרון: {result.get('copied',0)} הועתקו, {result.get('skipped',0)} דולגו"
        )

    def _run_migration(self) -> None:
        if self._migrate_worker and self._migrate_worker.isRunning():
            self._log_line("מיגרציה כבר רצה...")
            return
        self._log_line("מתחיל מיגרציית AHU → Nexus Supreme...")
        self._migrate_worker = MigrationWorker(force=False)
        self._migrate_worker.progress.connect(self._log_line)
        self._migrate_worker.finished.connect(self._on_migration_done)
        self._migrate_worker.start()

    def _on_migration_done(self, result: dict) -> None:
        ok = result.get("ok", False)
        c  = result.get("copied", 0)
        s  = result.get("skipped", 0)
        self._log_line(
            f"{'✅' if ok else '❌'} מיגרציה הסתיימה: {c} הועתקו, {s} דולגו"
        )
