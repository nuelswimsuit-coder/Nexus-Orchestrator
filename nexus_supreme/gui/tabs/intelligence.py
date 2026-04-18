"""
מודיעין וגיבוי — Intelligence & Archive Tab
Authorized chat archiver + AI analysis (Claude API local queries).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QFileDialog, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QPlainTextEdit, QPushButton, QSpinBox,
    QSplitter, QVBoxLayout, QWidget,
)

from ..theme import C
from ..workers import ScraperWorker

API_ID   = int(os.environ.get("TELEGRAM_API_ID",   "0") or "0")
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")


class IntelligenceTab(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._worker: ScraperWorker | None = None
        self._setup_ui()

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(16)

        title = QLabel("מודיעין וגיבוי")
        title.setStyleSheet(f"color:{C['silver']}; font-size:22px; font-weight:bold;")
        sub = QLabel("CHAT ARCHIVE ENGINE · AI ANALYSIS · BACKUP")
        sub.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:3px;")
        root.addWidget(title)
        root.addWidget(sub)

        splitter = QSplitter(Qt.Orientation.Vertical)

        # ── Archiver config ────────────────────────────────────────────────────
        grp_arc = QGroupBox("  ארכיון צ'אט — מאורגן  ")
        grp_arc.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:12px; }}"
        )
        arc_v = QVBoxLayout(grp_arc)

        # Session selector
        row1 = QHBoxLayout()
        lbl_s = QLabel("Session:")
        lbl_s.setFixedWidth(80)
        self._session_edit = QLineEdit()
        self._session_edit.setPlaceholderText("בחר קובץ .session...")
        btn_browse = QPushButton("Browse")
        btn_browse.setFixedWidth(80)
        btn_browse.clicked.connect(self._browse_session)
        row1.addWidget(lbl_s)
        row1.addWidget(self._session_edit)
        row1.addWidget(btn_browse)
        arc_v.addLayout(row1)

        # Chat ID
        row2 = QHBoxLayout()
        lbl_c = QLabel("Chat ID:")
        lbl_c.setFixedWidth(80)
        self._chat_id_edit = QLineEdit()
        self._chat_id_edit.setPlaceholderText("מזהה צ'אט / ערוץ (מספר שלילי לקבוצות)")
        row2.addWidget(lbl_c)
        row2.addWidget(self._chat_id_edit)
        arc_v.addLayout(row2)

        # Options
        row3 = QHBoxLayout()
        self._chk_media = QCheckBox("הורד מדיה (תמונות <10MB · וידאו <50MB)")
        self._chk_media.setChecked(True)
        lbl_lim = QLabel("מגבלת הודעות:")
        self._spin_limit = QSpinBox()
        self._spin_limit.setRange(0, 1_000_000)
        self._spin_limit.setValue(0)
        self._spin_limit.setSpecialValueText("ללא הגבלה")
        self._spin_limit.setFixedWidth(130)
        row3.addWidget(self._chk_media)
        row3.addStretch()
        row3.addWidget(lbl_lim)
        row3.addWidget(self._spin_limit)
        arc_v.addLayout(row3)

        # Start / Stop
        row4 = QHBoxLayout()
        self._btn_start = QPushButton("▶ התחל ארכיון")
        self._btn_start.setObjectName("btnPrimary")
        self._btn_start.clicked.connect(self._start_archive)
        self._btn_stop = QPushButton("⏹ עצור")
        self._btn_stop.setObjectName("btnDanger")
        self._btn_stop.clicked.connect(self._stop_archive)
        self._btn_stop.setEnabled(False)
        row4.addStretch()
        row4.addWidget(self._btn_start)
        row4.addWidget(self._btn_stop)
        arc_v.addLayout(row4)
        splitter.addWidget(grp_arc)

        # ── AI Analysis ────────────────────────────────────────────────────────
        grp_ai = QGroupBox("  NEXUS BRAIN — ניתוח AI  ")
        grp_ai.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:12px; }}"
        )
        ai_v = QVBoxLayout(grp_ai)

        ai_row1 = QHBoxLayout()
        lbl_mode = QLabel("סוג ניתוח:")
        lbl_mode.setFixedWidth(90)
        self._analysis_mode = QComboBox()
        self._analysis_mode.addItems([
            "זיהוי לידים",
            "ניתוח סנטימנט",
            "זיהוי משימות",
            "סיכום ביצועים",
        ])
        btn_analyze = QPushButton("נתח ארכיב")
        btn_analyze.setObjectName("btnPrimary")
        btn_analyze.clicked.connect(self._run_analysis)
        ai_row1.addWidget(lbl_mode)
        ai_row1.addWidget(self._analysis_mode)
        ai_row1.addStretch()
        ai_row1.addWidget(btn_analyze)
        ai_v.addLayout(ai_row1)

        self._ai_result = QPlainTextEdit()
        self._ai_result.setReadOnly(True)
        self._ai_result.setPlaceholderText("תוצאות ניתוח יופיעו כאן...")
        self._ai_result.setStyleSheet(
            f"background:{C['surface']}; color:{C['text']}; "
            f"font-family:'Segoe UI',Arial; font-size:12px; border:none;"
        )
        ai_v.addWidget(self._ai_result)
        splitter.addWidget(grp_ai)

        # ── Log ────────────────────────────────────────────────────────────────
        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setFixedHeight(120)
        self._log.setStyleSheet(
            f"background:{C['surface']}; color:{C['text2']}; "
            f"font-family:'Consolas',monospace; font-size:11px; border:1px solid {C['border']};"
        )
        splitter.addWidget(self._log)
        splitter.setSizes([240, 260, 120])
        root.addWidget(splitter, stretch=1)

    def _browse_session(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "בחר קובץ Session", "", "Session Files (*.session)"
        )
        if path:
            self._session_edit.setText(path)

    def _log_line(self, text: str) -> None:
        self._log.appendPlainText(text)

    def _start_archive(self) -> None:
        session = self._session_edit.text().strip()
        chat_id_str = self._chat_id_edit.text().strip()

        if not session or not chat_id_str:
            self._log_line("שגיאה: יש למלא נתיב session ו-chat ID")
            return
        try:
            chat_id = int(chat_id_str)
        except ValueError:
            self._log_line("שגיאה: Chat ID חייב להיות מספר")
            return

        if not API_ID or not API_HASH:
            self._log_line("שגיאה: TELEGRAM_API_ID / TELEGRAM_API_HASH לא מוגדרים ב-.env")
            return

        self._btn_start.setEnabled(False)
        self._btn_stop.setEnabled(True)
        self._log_line(f"מתחיל ארכיון לצ'אט {chat_id}...")

        self._worker = ScraperWorker(
            session_path   = session,
            chat_id        = chat_id,
            api_id         = API_ID,
            api_hash       = API_HASH,
            download_media = self._chk_media.isChecked(),
        )
        self._worker.progress.connect(self._log_line)
        self._worker.finished.connect(self._on_archive_done)
        self._worker.start()

    def _stop_archive(self) -> None:
        if self._worker:
            self._worker.terminate()
            self._log_line("ארכיון הופסק.")
        self._btn_start.setEnabled(True)
        self._btn_stop.setEnabled(False)

    def _on_archive_done(self, result: dict) -> None:
        self._btn_start.setEnabled(True)
        self._btn_stop.setEnabled(False)
        if result.get("ok"):
            self._log_line(
                f"✅ הסתיים: {result.get('messages',0)} הודעות, "
                f"{result.get('media',0)} מדיה, {result.get('errors',0)} שגיאות"
            )
        else:
            self._log_line(f"❌ שגיאה: {result.get('error','')}")

    def _run_analysis(self) -> None:
        mode = self._analysis_mode.currentText()
        archives = list(Path("data/archives").glob("*/messages.jsonl"))
        if not archives:
            self._ai_result.setPlainText("לא נמצאו ארכיבים. הרץ ארכיון תחילה.")
            return

        # Load last 200 messages from most-recently-modified archive
        latest = max(archives, key=lambda p: p.stat().st_mtime)
        lines: list[str] = []
        try:
            with open(latest, encoding="utf-8", errors="replace") as f:
                for raw in f:
                    try:
                        rec = json.loads(raw)
                        text = rec.get("text", "").strip()
                        if text:
                            lines.append(text)
                    except Exception:
                        pass
        except Exception as exc:
            self._ai_result.setPlainText(f"שגיאה בקריאת ארכיב: {exc}")
            return

        if not lines:
            self._ai_result.setPlainText("הארכיב ריק.")
            return

        sample = "\n".join(lines[-200:])
        self._ai_result.setPlainText(f"מנתח {len(lines)} הודעות (200 אחרונות בתצוגה)...\n")
        self._call_claude_analyze(mode, sample)

    def _call_claude_analyze(self, mode: str, text: str) -> None:
        """Call the Nexus API /api/analyze endpoint (or direct Claude API)."""
        import requests

        NEXUS_BASE = os.environ.get("NEXUS_API_BASE", "http://localhost:8001")
        api_key    = os.environ.get("NEXUS_API_KEY", "")

        prompt_map = {
            "זיהוי לידים":     "Identify all potential sales leads, with contact info if available. Reply in Hebrew.",
            "ניתוח סנטימנט":   "Analyze the overall sentiment of these messages. Reply in Hebrew with examples.",
            "זיהוי משימות":    "Extract all action items and tasks mentioned. Reply in Hebrew as a numbered list.",
            "סיכום ביצועים":   "Summarize key performance indicators and activity patterns. Reply in Hebrew.",
        }
        instruction = prompt_map.get(mode, "Summarize the content in Hebrew.")

        try:
            resp = requests.post(
                f"{NEXUS_BASE}/api/analyze",
                json    = {"text": text[:10_000], "instruction": instruction},
                headers = {"X-Nexus-Api-Key": api_key},
                timeout = 60,
            )
            result = resp.json()
            self._ai_result.setPlainText(result.get("analysis", resp.text))
        except Exception as exc:
            self._ai_result.appendPlainText(
                f"\n[שגיאה: {exc}]\n\n"
                "הוסף endpoint /api/analyze לשרת ה-API, או הגדר GEMINI_API_KEY / OPENAI_API_KEY."
            )
