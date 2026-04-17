"""
Nexus Supreme — Main Window (PyQt6)
Luxury dark desktop suite with:
  - 5 Hebrew tabs
  - System tray integration (minimise to tray)
  - Auto-restart watchdog thread
  - Status bar with live API connectivity indicator
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from PyQt6.QtCore import QSize, Qt, QThread, QTimer, pyqtSignal
from PyQt6.QtGui import QAction, QColor, QIcon, QPainter, QPixmap
from PyQt6.QtWidgets import (
    QApplication, QLabel, QMainWindow, QMessageBox,
    QStatusBar, QSystemTrayIcon, QTabWidget,
    QToolBar, QVBoxLayout, QWidget,
)

from .theme import C, STYLESHEET
from .tabs.dashboard    import DashboardTab
from .tabs.assets       import AssetsTab
from .tabs.intelligence import IntelligenceTab
from .tabs.ahu_portal   import AhuPortalTab
from .tabs.remote_dev   import RemoteDevTab

ROOT     = Path(__file__).resolve().parents[2]
API_BASE = os.environ.get("NEXUS_API_BASE", "http://localhost:8001")
API_KEY  = os.environ.get("NEXUS_API_KEY",  "")

# ── Tray icon (generated programmatically if .ico not present) ─────────────────

def _make_tray_icon() -> QIcon:
    ico_path = ROOT / "nexus_icon.ico"
    if ico_path.exists():
        return QIcon(str(ico_path))
    # Fallback: draw a simple 32×32 silver "N" on dark background
    pix = QPixmap(32, 32)
    pix.fill(QColor(C["bg"]))
    p = QPainter(pix)
    p.setPen(QColor(C["silver"]))
    from PyQt6.QtGui import QFont as QF
    p.setFont(QF("Segoe UI", 18, QF.Weight.Bold))
    p.drawText(pix.rect(), Qt.AlignmentFlag.AlignCenter, "N")
    p.end()
    return QIcon(pix)


# ── Watchdog ──────────────────────────────────────────────────────────────────

class ApiWatchdog(QThread):
    """
    Pings the Nexus API every 15 s.
    Emits `api_up` / `api_down` so the status bar can update.
    """
    api_up   = pyqtSignal()
    api_down = pyqtSignal(str)

    def run(self) -> None:
        import time
        import requests
        headers = {"X-Nexus-Api-Key": API_KEY} if API_KEY else {}
        while True:
            try:
                r = requests.get(f"{API_BASE}/health", headers=headers, timeout=4)
                if r.status_code < 300:
                    self.api_up.emit()
                else:
                    self.api_down.emit(f"HTTP {r.status_code}")
            except Exception as exc:
                self.api_down.emit(str(exc)[:60])
            time.sleep(15)


# ── Main Window ───────────────────────────────────────────────────────────────

class NexusSupremeWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Nexus Supreme Control")
        self.setMinimumSize(QSize(1280, 780))
        self.resize(1440, 860)
        self.setStyleSheet(STYLESHEET)
        self.setWindowIcon(_make_tray_icon())

        self._build_ui()
        self._build_tray()
        self._build_statusbar()
        self._start_watchdog()

    # ── UI ─────────────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        vbox = QVBoxLayout(central)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.setSpacing(0)

        # ── Top banner ─────────────────────────────────────────────────────────
        banner = QWidget()
        banner.setFixedHeight(56)
        banner.setStyleSheet(f"background:{C['surface']}; border-bottom:1px solid {C['border']};")
        bl = QVBoxLayout(banner)
        bl.setContentsMargins(28, 8, 28, 8)
        lbl_n = QLabel(
            '<span style="font-size:20px; font-weight:bold; letter-spacing:2px; '
            f'color:{C["silver"]}">NEXUS</span>'
            '<span style="font-size:11px; color:#555; letter-spacing:4px; margin-left:8px;"> SUPREME CONTROL</span>'
        )
        lbl_n.setTextFormat(Qt.TextFormat.RichText)
        bl.addWidget(lbl_n)
        vbox.addWidget(banner)

        # ── Tabs ────────────────────────────────────────────────────────────────
        self._tabs = QTabWidget()
        self._tabs.setStyleSheet(
            f"QTabWidget::pane {{ border:none; background:{C['surface']}; }}"
        )
        self._tabs.addTab(DashboardTab(self),    "  לוח בקרה  ")
        self._tabs.addTab(AssetsTab(self),       "  ניהול נכסים  ")
        self._tabs.addTab(IntelligenceTab(self), "  מודיעין וגיבוי  ")
        self._tabs.addTab(AhuPortalTab(self),    "  מנוע שיווק ו-AHU  ")
        self._tabs.addTab(RemoteDevTab(self),    "  פיתוח מרחוק  ")
        vbox.addWidget(self._tabs, stretch=1)

    # ── System tray ────────────────────────────────────────────────────────────

    def _build_tray(self) -> None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        self._tray = QSystemTrayIcon(self)
        self._tray.setIcon(_make_tray_icon())
        self._tray.setToolTip("Nexus Supreme Control")

        from PyQt6.QtWidgets import QMenu
        menu = QMenu()
        menu.setStyleSheet(
            f"QMenu {{ background:{C['surface2']}; color:{C['text']}; border:1px solid {C['border2']}; }}"
            f"QMenu::item:selected {{ background:{C['accent2']}; }}"
        )

        act_show  = QAction("הצג חלון", self)
        act_show.triggered.connect(self._show_window)
        act_dash  = QAction("לוח בקרה", self)
        act_dash.triggered.connect(lambda: (self._show_window(), self._tabs.setCurrentIndex(0)))
        act_quit  = QAction("יציאה", self)
        act_quit.triggered.connect(self._quit)

        menu.addAction(act_show)
        menu.addAction(act_dash)
        menu.addSeparator()
        menu.addAction(act_quit)
        self._tray.setContextMenu(menu)
        self._tray.activated.connect(self._on_tray_activated)
        self._tray.show()

    def _on_tray_activated(self, reason: QSystemTrayIcon.ActivationReason) -> None:
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            self._show_window()

    def _show_window(self) -> None:
        self.showNormal()
        self.activateWindow()
        self.raise_()

    # ── Status bar ─────────────────────────────────────────────────────────────

    def _build_statusbar(self) -> None:
        sb = self.statusBar()
        assert sb is not None
        sb.setStyleSheet(
            f"QStatusBar {{ background:{C['surface']}; border-top:1px solid {C['border']}; "
            f"color:{C['silver3']}; font-size:11px; }}"
        )
        self._api_indicator = QLabel("● API: בודק...")
        self._api_indicator.setStyleSheet(f"color:{C['warn']}; margin:0 12px;")
        sb.addPermanentWidget(self._api_indicator)

        root_lbl = QLabel(f"  {ROOT}  ")
        root_lbl.setStyleSheet(f"color:{C['text3']};")
        sb.addWidget(root_lbl)

    # ── Watchdog ───────────────────────────────────────────────────────────────

    def _start_watchdog(self) -> None:
        self._watchdog = ApiWatchdog()
        self._watchdog.api_up.connect(self._on_api_up)
        self._watchdog.api_down.connect(self._on_api_down)
        self._watchdog.start()

    def _on_api_up(self) -> None:
        self._api_indicator.setText("● API: מחובר")
        self._api_indicator.setStyleSheet(f"color:{C['success']}; margin:0 12px; font-weight:bold;")

    def _on_api_down(self, reason: str) -> None:
        self._api_indicator.setText(f"● API: מנותק ({reason})")
        self._api_indicator.setStyleSheet(f"color:{C['danger']}; margin:0 12px;")

    # ── Window events ──────────────────────────────────────────────────────────

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if hasattr(self, "_tray") and self._tray.isVisible():
            self.hide()
            self._tray.showMessage(
                "Nexus Supreme",
                "הועבר ל-System Tray. לחץ פעמיים על האייקון לפתיחה מחדש.",
                QSystemTrayIcon.MessageIcon.Information,
                2000,
            )
            event.ignore()
        else:
            event.accept()

    def _quit(self) -> None:
        if hasattr(self, "_watchdog"):
            self._watchdog.terminate()
        QApplication.quit()
