"""
Nexus Supreme — Visual Theme
"Dark Resale Luxury" inspired by Audi B9 Carbon Fiber & Brushed Silver.

Palette:
  Carbon bg:     #0c0c0c / #141414 / #1a1a1a
  Brushed silver:#c4c4c4 / #9a9a9a / #6a6a6a
  Accent blue:   #3a7bd5  (for live / active indicators)
  Success:       #4a9a5a
  Warning:       #c89b2a
  Danger:        #c03030
  Border:        #252525 / #303030
"""
from __future__ import annotations

# ── Colour tokens ──────────────────────────────────────────────────────────────
C = {
    "bg":        "#0c0c0c",
    "surface":   "#141414",
    "surface2":  "#1c1c1c",
    "surface3":  "#222222",
    "border":    "#2a2a2a",
    "border2":   "#383838",
    "silver":    "#c4c4c4",
    "silver2":   "#8a8a8a",
    "silver3":   "#5a5a5a",
    "accent":    "#3a7bd5",
    "accent2":   "#2a5aa0",
    "success":   "#4a9a5a",
    "warn":      "#c89b2a",
    "danger":    "#c03030",
    "text":      "#d8d8d8",
    "text2":     "#909090",
    "text3":     "#505050",
}

STYLESHEET = f"""
/* ── Base ─────────────────────────────────────────────────────────────────── */
QMainWindow, QDialog {{
    background-color: {C['bg']};
    color: {C['text']};
}}
QWidget {{
    background-color: {C['bg']};
    color: {C['text']};
    font-family: 'Segoe UI', 'Arial', sans-serif;
    font-size: 13px;
}}

/* ── Tab Bar ─────────────────────────────────────────────────────────────── */
QTabWidget::pane {{
    border: 1px solid {C['border']};
    background-color: {C['surface']};
    top: -1px;
}}
QTabWidget::tab-bar {{
    alignment: left;
}}
QTabBar::tab {{
    background-color: {C['surface2']};
    color: {C['silver3']};
    padding: 10px 22px;
    margin-right: 2px;
    border: none;
    border-top: 2px solid transparent;
    font-size: 12px;
    font-weight: bold;
    letter-spacing: 0.5px;
}}
QTabBar::tab:selected {{
    background-color: {C['surface']};
    color: {C['silver']};
    border-top: 2px solid {C['silver']};
}}
QTabBar::tab:hover:!selected {{
    background-color: {C['surface3']};
    color: {C['silver2']};
}}

/* ── Buttons ─────────────────────────────────────────────────────────────── */
QPushButton {{
    background-color: {C['surface2']};
    color: {C['silver']};
    border: 1px solid {C['border2']};
    padding: 8px 18px;
    font-weight: bold;
    font-size: 12px;
    letter-spacing: 0.3px;
}}
QPushButton:hover {{
    background-color: {C['surface3']};
    border-color: {C['silver2']};
    color: {C['text']};
}}
QPushButton:pressed {{
    background-color: {C['border2']};
}}
QPushButton:disabled {{
    color: {C['text3']};
    border-color: {C['border']};
}}
QPushButton#btnPrimary {{
    background-color: {C['accent2']};
    color: #ffffff;
    border: 1px solid {C['accent']};
}}
QPushButton#btnPrimary:hover {{
    background-color: {C['accent']};
}}
QPushButton#btnDanger {{
    background-color: #3a1010;
    color: #ff6060;
    border: 1px solid {C['danger']};
}}
QPushButton#btnDanger:hover {{
    background-color: {C['danger']};
    color: #ffffff;
}}
QPushButton#btnSuccess {{
    background-color: #0f2a14;
    color: #70d080;
    border: 1px solid {C['success']};
}}
QPushButton#btnSuccess:hover {{
    background-color: {C['success']};
    color: #ffffff;
}}

/* ── Inputs ──────────────────────────────────────────────────────────────── */
QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {{
    background-color: {C['surface2']};
    border: 1px solid {C['border2']};
    color: {C['text']};
    padding: 6px 10px;
    selection-background-color: {C['accent2']};
}}
QLineEdit:focus, QSpinBox:focus, QComboBox:focus {{
    border-color: {C['silver2']};
}}
QComboBox::drop-down {{
    border: none;
    background-color: {C['surface3']};
    width: 24px;
}}
QComboBox QAbstractItemView {{
    background-color: {C['surface2']};
    border: 1px solid {C['border2']};
    color: {C['text']};
    selection-background-color: {C['accent2']};
}}

/* ── Text Areas ──────────────────────────────────────────────────────────── */
QTextEdit, QPlainTextEdit {{
    background-color: {C['surface']};
    border: 1px solid {C['border']};
    color: {C['text']};
    padding: 6px;
    selection-background-color: {C['accent2']};
    font-family: 'Consolas', 'JetBrains Mono', monospace;
    font-size: 12px;
    line-height: 1.5;
}}

/* ── Labels ──────────────────────────────────────────────────────────────── */
QLabel {{
    color: {C['text']};
    background-color: transparent;
}}
QLabel#labelTitle {{
    color: {C['silver']};
    font-size: 22px;
    font-weight: bold;
    letter-spacing: 1px;
}}
QLabel#labelSub {{
    color: {C['silver3']};
    font-size: 10px;
    letter-spacing: 2px;
}}
QLabel#labelSection {{
    color: {C['silver2']};
    font-size: 10px;
    font-weight: bold;
    letter-spacing: 3px;
}}
QLabel#statValue {{
    color: {C['silver']};
    font-size: 24px;
    font-weight: bold;
}}
QLabel#statLabel {{
    color: {C['silver3']};
    font-size: 10px;
    letter-spacing: 1px;
}}
QLabel#statusOnline  {{ color: {C['success']}; font-weight: bold; }}
QLabel#statusOffline {{ color: {C['danger']};  font-weight: bold; }}
QLabel#statusWarn    {{ color: {C['warn']};    font-weight: bold; }}

/* ── Tables ──────────────────────────────────────────────────────────────── */
QTableWidget, QTableView {{
    background-color: {C['surface']};
    border: 1px solid {C['border']};
    color: {C['text']};
    gridline-color: {C['border']};
    selection-background-color: {C['accent2']};
    alternate-background-color: {C['surface2']};
}}
QHeaderView::section {{
    background-color: {C['surface3']};
    color: {C['silver2']};
    padding: 6px 10px;
    border: none;
    border-right: 1px solid {C['border']};
    font-size: 11px;
    font-weight: bold;
    letter-spacing: 1px;
}}
QTableWidget::item:selected {{
    background-color: {C['accent2']};
}}

/* ── Scrollbars ──────────────────────────────────────────────────────────── */
QScrollBar:vertical {{
    background-color: {C['surface']};
    width: 8px;
    margin: 0;
}}
QScrollBar::handle:vertical {{
    background-color: {C['border2']};
    min-height: 30px;
}}
QScrollBar::handle:vertical:hover {{
    background-color: {C['silver3']};
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0;
}}
QScrollBar:horizontal {{
    background-color: {C['surface']};
    height: 8px;
}}
QScrollBar::handle:horizontal {{
    background-color: {C['border2']};
    min-width: 30px;
}}

/* ── Progress Bar ────────────────────────────────────────────────────────── */
QProgressBar {{
    background-color: {C['surface2']};
    border: 1px solid {C['border']};
    color: {C['text']};
    text-align: center;
    height: 12px;
    font-size: 10px;
}}
QProgressBar::chunk {{
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 {C['silver3']}, stop:1 {C['silver']});
}}

/* ── Group Box ───────────────────────────────────────────────────────────── */
QGroupBox {{
    border: 1px solid {C['border2']};
    margin-top: 14px;
    padding: 12px 10px 8px 10px;
    color: {C['silver2']};
    font-size: 11px;
    font-weight: bold;
    letter-spacing: 1px;
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
    color: {C['silver2']};
}}

/* ── Splitter ────────────────────────────────────────────────────────────── */
QSplitter::handle {{
    background-color: {C['border']};
}}

/* ── Tooltip ─────────────────────────────────────────────────────────────── */
QToolTip {{
    background-color: {C['surface3']};
    border: 1px solid {C['border2']};
    color: {C['text']};
    padding: 4px 8px;
}}

/* ── Status Bar ──────────────────────────────────────────────────────────── */
QStatusBar {{
    background-color: {C['surface']};
    border-top: 1px solid {C['border']};
    color: {C['silver3']};
    font-size: 11px;
}}
"""


def status_dot(online: bool) -> str:
    """Return a coloured HTML dot for inline status."""
    col = C["success"] if online else C["danger"]
    return f'<span style="color:{col}; font-size:16px;">&#9679;</span>'
