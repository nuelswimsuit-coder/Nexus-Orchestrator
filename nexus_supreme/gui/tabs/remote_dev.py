"""
פיתוח מרחוק — Remote DevOps Bridge Tab
GUI front-end for the Neural Link (dev_link.py).
Shows bridge status, audit log, and a local prompt-architect tool.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont, QTextCursor
from PyQt6.QtWidgets import (
    QGroupBox, QHBoxLayout, QLabel, QPlainTextEdit,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem,
    QHeaderView, QTextEdit, QVBoxLayout, QWidget,
)

from ..theme import C
from ..workers import ServiceLauncher

ROOT     = Path(__file__).resolve().parents[3]
API_BASE = os.environ.get("NEXUS_API_BASE", "http://localhost:8001")
API_KEY  = os.environ.get("NEXUS_API_KEY",  "")


class RemoteDevTab(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._claude_proc: ServiceLauncher | None = None
        self._setup_ui()
        self._load_audit_log()

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(16)

        # Header
        title = QLabel("פיתוח מרחוק")
        title.setStyleSheet(f"color:{C['silver']}; font-size:22px; font-weight:bold;")
        sub = QLabel("NEURAL LINK · CLAUDE CLI BRIDGE · DEVOPS")
        sub.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:3px;")
        root.addWidget(title)
        root.addWidget(sub)

        splitter = QSplitter(Qt.Orientation.Vertical)

        # ── Local Claude CLI terminal ──────────────────────────────────────────
        grp_cli = QGroupBox("  CLAUDE CLI — הרצה מקומית  ")
        grp_cli.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:12px; }}"
        )
        cli_v = QVBoxLayout(grp_cli)

        # Prompt input
        lbl_p = QLabel("הוראה לביצוע:")
        lbl_p.setStyleSheet(f"color:{C['silver2']}; font-size:11px;")
        self._prompt_input = QTextEdit()
        self._prompt_input.setPlaceholderText(
            "הכנס הוראה לביצוע ע\"י Claude Code CLI...\n"
            "דוגמה: fix the Redis timeout error in nexus/master/services/deployer.py"
        )
        self._prompt_input.setFixedHeight(80)
        self._prompt_input.setStyleSheet(
            f"background:{C['surface2']}; color:{C['text']}; "
            f"font-family:'Segoe UI',Arial; font-size:12px; border:1px solid {C['border2']};"
        )
        cli_v.addWidget(lbl_p)
        cli_v.addWidget(self._prompt_input)

        btn_row = QHBoxLayout()
        self._btn_run = QPushButton("▶ הרץ ב-Claude CLI")
        self._btn_run.setObjectName("btnPrimary")
        self._btn_run.clicked.connect(self._run_claude_locally)
        self._btn_kill = QPushButton("⏹ עצור")
        self._btn_kill.setObjectName("btnDanger")
        self._btn_kill.clicked.connect(self._kill_claude)
        self._btn_kill.setEnabled(False)
        btn_tpl = QPushButton("📋 תבנית פרומפט")
        btn_tpl.clicked.connect(self._insert_template)
        btn_row.addWidget(btn_tpl)
        btn_row.addStretch()
        btn_row.addWidget(self._btn_run)
        btn_row.addWidget(self._btn_kill)
        cli_v.addLayout(btn_row)

        # Output terminal
        self._terminal = QPlainTextEdit()
        self._terminal.setReadOnly(True)
        self._terminal.setStyleSheet(
            f"background:#080808; color:#c8ffc8; "
            f"font-family:'Consolas','JetBrains Mono',monospace; font-size:12px; "
            f"border:1px solid {C['border']};"
        )
        self._terminal.setMaximumBlockCount(3000)
        cli_v.addWidget(self._terminal)
        splitter.addWidget(grp_cli)

        # ── Audit log ─────────────────────────────────────────────────────────
        grp_audit = QGroupBox("  AUDIT LOG — היסטוריית פקודות  ")
        grp_audit.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:10px; }}"
        )
        audit_v = QVBoxLayout(grp_audit)
        self._audit_table = QTableWidget(0, 4)
        self._audit_table.setHorizontalHeaderLabels(["זמן", "פקודה", "Exit", "פלט"])
        self._audit_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._audit_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._audit_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._audit_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._audit_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        audit_v.addWidget(self._audit_table)
        splitter.addWidget(grp_audit)
        splitter.setSizes([400, 200])
        root.addWidget(splitter, stretch=1)

    # ── Actions ────────────────────────────────────────────────────────────────

    def _run_claude_locally(self) -> None:
        instruction = self._prompt_input.toPlainText().strip()
        if not instruction:
            self._term_write("[שגיאה] יש להכניס הוראה")
            return

        if self._claude_proc and self._claude_proc.isRunning():
            self._term_write("[שגיאה] פקודה כבר רצה. לחץ עצור תחילה.")
            return

        cmd = ["claude", "--print", instruction]
        self._term_write(f"\n$ claude --print \"{instruction[:80]}...\"\n", color="#6af")

        self._btn_run.setEnabled(False)
        self._btn_kill.setEnabled(True)

        self._claude_proc = ServiceLauncher(
            cmd   = cmd,
            cwd   = str(ROOT),
            shell = False,
            env   = {"PYTHONUNBUFFERED": "1"},
        )
        self._claude_proc.line_ready.connect(self._term_write)
        self._claude_proc.finished.connect(self._on_claude_done)
        self._claude_proc.start()

    def _kill_claude(self) -> None:
        if self._claude_proc:
            self._claude_proc.stop()
            self._term_write("\n[נעצר על ידי המשתמש]", color="#fa0")
        self._btn_run.setEnabled(True)
        self._btn_kill.setEnabled(False)

    def _on_claude_done(self, exit_code: int) -> None:
        self._term_write(
            f"\n[סיום — exit {exit_code}]",
            color=C["success"] if exit_code == 0 else C["danger"],
        )
        self._btn_run.setEnabled(True)
        self._btn_kill.setEnabled(False)
        QTimer.singleShot(500, self._load_audit_log)

    def _term_write(self, text: str, color: str = "#c8ffc8") -> None:
        self._terminal.appendHtml(
            f'<span style="color:{color}">{text.replace("<","&lt;").replace(">","&gt;")}</span>'
        )
        self._terminal.moveCursor(QTextCursor.MoveOperation.End)

    def _insert_template(self) -> None:
        templates = [
            "analyze the error in the last run and propose a fix",
            "add unit tests for the module src/nexus/core/dev_link.py",
            "review nexus/api/routers/ahu.py for security issues",
            "optimize the Redis connection pool settings in the master node",
            "generate a Hebrew changelog for the last 5 commits",
        ]
        import random
        self._prompt_input.setPlainText(random.choice(templates))

    def _load_audit_log(self) -> None:
        log_path = ROOT / "logs" / "claude_bridge.log"
        if not log_path.exists():
            return
        try:
            entries: list[dict] = []
            with open(log_path, encoding="utf-8", errors="replace") as f:
                lines = f.read().split("---\n")
            for block in lines:
                block = block.strip()
                if not block:
                    continue
                first = block.splitlines()[0] if block else ""
                ts_part  = first[1:first.find("]")] if "[" in first else ""
                rest     = first[first.find("]")+1:].strip()
                exit_raw = ""
                cmd_raw  = rest
                if rest.startswith("exit="):
                    parts    = rest.split(" cmd=", 1)
                    exit_raw = parts[0].replace("exit=", "")
                    cmd_raw  = parts[1] if len(parts) > 1 else ""
                output   = "\n".join(block.splitlines()[1:])[:100]
                entries.append({
                    "ts": ts_part, "cmd": cmd_raw, "exit": exit_raw, "output": output
                })

            self._audit_table.setRowCount(len(entries))
            for row, e in enumerate(reversed(entries)):
                self._audit_table.setItem(row, 0, QTableWidgetItem(e["ts"][-8:]))
                self._audit_table.setItem(row, 1, QTableWidgetItem(e["cmd"][:80]))
                exit_item = QTableWidgetItem(e["exit"])
                exit_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self._audit_table.setItem(row, 2, exit_item)
                self._audit_table.setItem(row, 3, QTableWidgetItem(e["output"][:60]))
        except Exception:
            pass
