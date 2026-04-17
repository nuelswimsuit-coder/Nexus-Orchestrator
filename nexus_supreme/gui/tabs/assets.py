"""
ניהול נכסים — Assets Tab
Bot fleet tracking, SEO rank monitoring, uptime/reachability checks.
"""
from __future__ import annotations

import os

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtWidgets import (
    QFrame, QGroupBox, QHBoxLayout, QHeaderView, QLabel,
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget,
)

from ..theme import C, status_dot
from ..workers import ApiPoller

API_BASE = os.environ.get("NEXUS_API_BASE", "http://localhost:8001")
API_KEY  = os.environ.get("NEXUS_API_KEY",  "")


class AssetsTab(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._poller: ApiPoller | None = None
        self._setup_ui()
        self._start_polling()

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(16)

        # Header
        title = QLabel("ניהול נכסים")
        title.setStyleSheet(f"color:{C['silver']}; font-size:22px; font-weight:bold;")
        sub = QLabel("BOT FLEET · SEO · UPTIME MONITORING")
        sub.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:3px;")
        root.addWidget(title)
        root.addWidget(sub)

        # Summary cards
        cards = QHBoxLayout()
        self._lbl_total   = self._mini_card("סה\"כ בוטים", "—")
        self._lbl_active  = self._mini_card("פעילים", "—", C["success"])
        self._lbl_ranked  = self._mini_card("בחיפוש גלוי", "—", C["accent"])
        self._lbl_starts  = self._mini_card("/start היום", "—")
        for w in (self._lbl_total[0], self._lbl_active[0],
                  self._lbl_ranked[0], self._lbl_starts[0]):
            cards.addWidget(w)
        root.addLayout(cards)

        # Bot table
        grp = QGroupBox("  BOT FLEET  ")
        grp.setStyleSheet(
            f"QGroupBox {{ color:{C['silver2']}; border:1px solid {C['border2']}; "
            f"font-size:10px; font-weight:bold; letter-spacing:2px; margin-top:14px; padding:10px; }}"
        )
        grp_v = QVBoxLayout(grp)

        # Search bar
        search_row = QHBoxLayout()
        self._search = QLineEdit()
        self._search.setPlaceholderText("חפש בוט...")
        self._search.textChanged.connect(self._filter_table)
        btn_check = QPushButton("בדוק נגישות")
        btn_check.setObjectName("btnPrimary")
        btn_check.clicked.connect(self._check_reachability)
        btn_seo = QPushButton("עדכן SEO")
        btn_seo.clicked.connect(self._refresh_seo)
        search_row.addWidget(self._search)
        search_row.addWidget(btn_seo)
        search_row.addWidget(btn_check)
        grp_v.addLayout(search_row)

        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels([
            "בוט", "שם משתמש", "Niche", "דירוג SEO",
            "/start (24h)", "סטטוס", "בדיקה אחרונה",
        ])
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._table.setAlternatingRowColors(True)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        grp_v.addWidget(self._table)
        root.addWidget(grp, stretch=1)

        # Footer
        footer = QHBoxLayout()
        self._status_bar = QLabel("")
        self._status_bar.setStyleSheet(f"color:{C['text3']}; font-size:10px;")
        footer.addWidget(self._status_bar)
        footer.addStretch()
        root.addLayout(footer)

    def _mini_card(self, title: str, value: str, color: str = C["silver"]):
        frame = QFrame()
        frame.setStyleSheet(
            f"QFrame {{ background:{C['surface2']}; border:1px solid {C['border2']}; }}"
        )
        v = QVBoxLayout(frame)
        v.setContentsMargins(14, 10, 14, 10)
        lv = QLabel(value)
        lv.setStyleSheet(f"color:{color}; font-size:22px; font-weight:bold;")
        lv.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lt = QLabel(title.upper())
        lt.setStyleSheet(f"color:{C['silver3']}; font-size:9px; letter-spacing:2px;")
        lt.setAlignment(Qt.AlignmentFlag.AlignCenter)
        v.addWidget(lv)
        v.addWidget(lt)
        return frame, lv

    def _start_polling(self) -> None:
        self._poller = ApiPoller(
            base_url  = API_BASE,
            endpoints = ["/api/ahu/stats"],
            interval  = 15.0,
            api_key   = API_KEY,
        )
        self._poller.data_ready.connect(self._on_data)
        self._poller.start()
        # Also load bots from local DB
        QTimer.singleShot(500, self._load_bots_from_db)

    def _load_bots_from_db(self) -> None:
        try:
            from nexus_supreme.core.db.models import ManagedBot, get_session
            sess = get_session()
            bots = sess.query(ManagedBot).all()
            self._populate_table(bots)
            sess.close()
        except Exception as exc:
            self._status_bar.setText(f"שגיאת DB: {exc}")

    def _populate_table(self, bots: list) -> None:
        self._table.setRowCount(len(bots))
        total   = len(bots)
        active  = 0
        ranked  = 0
        starts  = 0

        for row, bot in enumerate(bots):
            is_active = getattr(bot, "is_active", True)
            rank      = getattr(bot, "search_rank", -1)
            start_cnt = getattr(bot, "start_count", 0)

            if is_active:
                active += 1
            if rank is not None and rank >= 0:
                ranked += 1
            starts += start_cnt or 0

            name_item = QTableWidgetItem(getattr(bot, "name", ""))
            user_item = QTableWidgetItem(f"@{getattr(bot, 'username', '')}" if bot.username else "—")
            nich_item = QTableWidgetItem(getattr(bot, "niche", ""))
            rank_item = QTableWidgetItem(str(rank) if rank and rank >= 0 else "—")
            strt_item = QTableWidgetItem(str(start_cnt or 0))

            status_text = "● פעיל" if is_active else "● כבוי"
            stat_item = QTableWidgetItem(status_text)
            stat_item.setForeground(
                QTableWidgetItem().foreground()
                if not is_active else QTableWidgetItem().foreground()
            )

            scanned = getattr(bot, "last_scanned", None)
            scan_item = QTableWidgetItem(
                scanned.strftime("%d/%m %H:%M") if scanned else "—"
            )

            for col, item in enumerate(
                [name_item, user_item, nich_item, rank_item, strt_item, stat_item, scan_item]
            ):
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self._table.setItem(row, col, item)

            # Colour status cell
            self._table.item(row, 5).setForeground(  # type: ignore[union-attr]
                __import__("PyQt6.QtGui", fromlist=["QColor"]).QColor(
                    C["success"] if is_active else C["danger"]
                )
            )

        self._lbl_total[1].setText(str(total))
        self._lbl_active[1].setText(str(active))
        self._lbl_ranked[1].setText(str(ranked))
        self._lbl_starts[1].setText(str(starts))

    def _on_data(self, endpoint: str, data: object) -> None:
        pass  # future: live SEO updates from API

    def _filter_table(self, text: str) -> None:
        text = text.lower()
        for row in range(self._table.rowCount()):
            match = False
            for col in range(self._table.columnCount()):
                item = self._table.item(row, col)
                if item and text in item.text().lower():
                    match = True
                    break
            self._table.setRowHidden(row, not match)

    def _check_reachability(self) -> None:
        import requests
        selected = self._table.selectedItems()
        if not selected:
            self._status_bar.setText("בחר שורה בטבלה תחילה")
            return
        row = self._table.currentRow()
        username_item = self._table.item(row, 1)
        if not username_item:
            return
        username = username_item.text().lstrip("@")
        if not username:
            return
        try:
            resp = requests.get(f"https://t.me/{username}", timeout=8)
            reachable = resp.status_code == 200
            color = C["success"] if reachable else C["danger"]
            self._status_bar.setStyleSheet(f"color:{color}; font-size:10px;")
            self._status_bar.setText(
                f"@{username}: {'✅ נגיש' if reachable else '❌ לא נגיש'} ({resp.status_code})"
            )
        except Exception as exc:
            self._status_bar.setText(f"שגיאה: {exc}")

    def _refresh_seo(self) -> None:
        self._status_bar.setText("רענון SEO — בקרוב...")
        QTimer.singleShot(1000, self._load_bots_from_db)
