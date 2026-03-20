"""
Telegram Bot V2 — Hebrew/English Localization Strings

Modern, fluent Hebrew with tech-native terminology for TeleFix OS.
All bot interactions use these centralized strings.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Literal

Language = Literal["he", "en"]

# ── Menu buttons ───────────────────────────────────────────────────────────────

MENU_BUTTONS = {
    "stats": {
        "en": "📊 Statistics", 
        "he": "📊 סטטיסטיקות"
    },
    "cluster": {
        "en": "🖥️ Cluster Health", 
        "he": "🖥️ ניהול קלאסטר"
    },
    "wallet": {
        "en": "💰 Wallet", 
        "he": "💰 ארנק"
    },
    "settings": {
        "en": "⚙️ Settings", 
        "he": "⚙️ הגדרות"
    },
}

# ── Action buttons ─────────────────────────────────────────────────────────────

ACTION_BUTTONS = {
    "approve": {
        "en": "✅ Approve", 
        "he": "✅ אשר"
    },
    "reject": {
        "en": "❌ Reject", 
        "he": "❌ דחה"
    },
    "sync": {
        "en": "🚀 Sync Workers", 
        "he": "🚀 סנכרן מעבדים"
    },
    "restart": {
        "en": "🔄 Restart", 
        "he": "🔄 אתחל מחדש"
    },
    "force_run": {
        "en": "⚡ Force Run", 
        "he": "⚡ הפעל בכוח"
    },
}

# ── System messages ────────────────────────────────────────────────────────────

SYSTEM_MESSAGES = {
    # Welcome/start
    "welcome": {
        "en": "🎯 *TeleFix OS — Command Center*\n\n"
              "Your autonomous business intelligence system is online.\n"
              "Choose an option from the menu below:",
        "he": "🎯 *TeleFix OS — מרכז פיקוד*\n\n"
              "המערכת הופעלה בהצלחה — כל המעבדים מחוברים ופעילים\\.\n"
              "בחר פעולה מלוח הבקרה:",
    },
    
    # Status confirmations
    "approved": {
        "en": "✅ *Request Approved*\n\nExecuting Auto-Scrape operation...", 
        "he": "✅ *הבקשה אושרה*\n\nמבצע פעולת Auto-Scrape...",
    },
    "rejected": {
        "en": "❌ *Request Rejected*\n\nOperation cancelled by operator.",
        "he": "❌ *הבקשה נדחתה*\n\nהפעולה בוטלה על ידי המפעיל.",
    },
    
    # Processing states
    "processing": {
        "en": "⏳ *Processing...*\n\nFetching live data from TeleFix cluster.",
        "he": "⏳ *מעבד...*\n\nמושך נתונים חיים מקלאסטר TeleFix.",
    },
    "deployment_started": {
        "en": "🚀 *Deployment Started*\n\nSyncing code to all worker nodes...",
        "he": "🚀 *פריסה החלה*\n\nמסנכרן קוד לכל צמתי המעבד (Worker)...",
    },
    "deployment_complete": {
        "en": "✅ *Deployment Complete*\n\nAll workers updated successfully.",
        "he": "✅ *פריסה הושלמה*\n\nכל המעבדים (Workers) עודכנו בהצלחה.",
    },
    
    # Errors
    "api_error": {
        "en": "⚠️ *API Connection Error*\n\nCannot reach TeleFix Control Center.\nPlease check if the master is running.",
        "he": "⚠️ *שגיאת חיבור API*\n\nלא ניתן להגיע למרכז הבקרה של TeleFix.\nאנא בדוק אם המאסטר פועל.",
    },
    "permission_denied": {
        "en": "🚫 *Access Denied*\n\nThis command requires admin privileges.",
        "he": "🚫 *גישה נדחתה*\n\nפקודה זו דורשת הרשאות מנהל.",
    },
}

# ── Report templates ───────────────────────────────────────────────────────────

REPORT_TEMPLATES = {
    "stats_header": {
        "en": "📊 *TELEFIX STATISTICS*",
        "he": "📊 *סטטיסטיקות TELEFIX*",
    },
    "cluster_header": {
        "en": "🖥️ *CLUSTER HEALTH*",
        "he": "🖥️ *בריאות הקלאסטר*",
    },
    "wallet_header": {
        "en": "💰 *FINANCIAL DASHBOARD*",
        "he": "💰 *לוח מחוונים פיננסי*",
    },
    
    # Field labels
    "database": {
        "en": "Database",
        "he": "בסיס נתונים",
    },
    "sessions": {
        "en": "Sessions",
        "he": "סשנים",
    },
    "active": {
        "en": "Active",
        "he": "פעיל",
    },
    "frozen": {
        "en": "Frozen", 
        "he": "קפוא",
    },
    "workers": {
        "en": "Workers",
        "he": "מעבדים (Workers)",
    },
    "daily_roi": {
        "en": "Daily ROI",
        "he": "החזר השקעה יומי",
    },
    "total_revenue": {
        "en": "Total Revenue",
        "he": "הכנסה כוללת",
    },
    "cpu_load": {
        "en": "CPU Load",
        "he": "עומס מעבד",
    },
    "memory_usage": {
        "en": "Memory",
        "he": "זיכרון",
    },
    "network_status": {
        "en": "Network",
        "he": "רשת",
    },
}

# ── Helper functions ───────────────────────────────────────────────────────────

def get_string(key: str, lang: Language = "he", **kwargs) -> str:
    """
    Get a localized string by key.
    Supports string interpolation with {variable} syntax.
    """
    if key in SYSTEM_MESSAGES:
        text = SYSTEM_MESSAGES[key].get(lang, SYSTEM_MESSAGES[key]["en"])
    elif key in REPORT_TEMPLATES:
        text = REPORT_TEMPLATES[key].get(lang, REPORT_TEMPLATES[key]["en"])
    elif key in MENU_BUTTONS:
        text = MENU_BUTTONS[key].get(lang, MENU_BUTTONS[key]["en"])
    elif key in ACTION_BUTTONS:
        text = ACTION_BUTTONS[key].get(lang, ACTION_BUTTONS[key]["en"])
    else:
        return key
    
    # Simple string interpolation
    for var, value in kwargs.items():
        text = text.replace(f"{{{var}}}", str(value))
    
    return text

def format_stats_report(data: dict, lang: Language = "he") -> str:
    """Format business stats into a localized Telegram message."""
    if lang == "he":
        db_status = "✅ מחובר" if data.get("db_available") else "❌ מנותק"
        active = data.get("active_sessions", 0)
        frozen = data.get("frozen_sessions", 0)
        total_sessions = active + frozen
        health_icon = "🟢" if active > 0 else "🔴"
        
        lines = [
            get_string("stats_header", lang),
            f"🗄 {get_string('database', lang)}: {db_status}",
            "",
            "👥 *קבוצות ויעדים*",
            f"  • קבוצות מנוהלות: `{data.get('total_managed_groups', 0)}`",
            f"  • קבוצות מקור: `{data.get('source_groups', 0)}`",
            f"  • קבוצות יעד: `{data.get('target_groups', 0)}`",
            "",
            "👤 *משתמשים*",
            f"  • נגרדו \\(כולל\\): `{data.get('total_scraped_users', 0)}`",
            f"  • בצינור: `{data.get('total_users_pipeline', 0)}`",
            "",
            f"🤖 *{get_string('sessions', lang)}* {health_icon}",
            f"  • {get_string('active', lang)}: `{active}`",
            f"  • {get_string('frozen', lang)}: `{frozen}`",
            f"  • מנהלים: `{data.get('manager_sessions', 0)}`",
            f"  • בריאות: `{active}/{total_sessions}`",
            "",
            f"🕐 נוצר: `{datetime.now().strftime('%H:%M')}`",
        ]
    else:
        # English version (keep existing format)
        db_ok = "✅ Live" if data.get("db_available") else "❌ Offline"
        active = data.get("active_sessions", 0)
        frozen = data.get("frozen_sessions", 0)
        total_s = active + frozen
        health = f"{active}/{total_s}" if total_s else "0/0"
        health_icon = "🟢" if active > 0 else "🔴"

        lines = [
            get_string("stats_header", lang),
            f"🗄 Database: {db_ok}",
            "",
            "👥 *Groups & Targets*",
            f"  • Managed groups: `{data.get('total_managed_groups', 0)}`",
            f"  • Source groups:  `{data.get('source_groups', 0)}`",
            f"  • Target groups:  `{data.get('target_groups', 0)}`",
            "",
            "👤 *Users*",
            f"  • Scraped \\(total\\): `{data.get('total_scraped_users', 0)}`",
            f"  • Pipeline:         `{data.get('total_users_pipeline', 0)}`",
            "",
            f"🤖 *Sessions* {health_icon}",
            f"  • Active:  `{active}`",
            f"  • Frozen:  `{frozen}`",
            f"  • Managers: `{data.get('manager_sessions', 0)}`",
            f"  • Health:  `{health}`",
            "",
        ]

    return "\n".join(lines)

def format_cluster_report(data: dict, lang: Language = "he") -> str:
    """Format cluster status into a localized Telegram message."""
    nodes = data.get("nodes", [])
    master_nodes = [n for n in nodes if n.get("role") == "master"]
    worker_nodes = [n for n in nodes if n.get("role") == "worker"]
    
    if lang == "he":
        lines = [get_string("cluster_header", lang), ""]
        
        for node in master_nodes:
            status = "🟢 מחובר" if node.get("online") else "🔴 מנותק"
            cpu = node.get("cpu_percent", 0)
            ram = node.get("ram_used_mb", 0)
            lines.extend([
                f"🖥️ *מאסטר*",
                f"  • מצב: {status}",
                f"  • {get_string('cpu_load', lang)}: `{cpu:.1f}%`",
                f"  • זיכרון: `{ram:.0f} MB`",
                "",
            ])
        
        if worker_nodes:
            lines.append(f"👷 *{get_string('workers', lang)} ({len(worker_nodes)})*")
            for i, node in enumerate(worker_nodes, 1):
                status = "🟢 פעיל" if node.get("online") else "🔴 לא זמין"
                ip = node.get("local_ip", "לא ידוע")
                lines.append(f"  {i}. {status} | IP: `{ip}`")
            lines.append("")
        
        lines.append(f"🕐 עודכן: `{datetime.now().strftime('%H:%M')}`")
    else:
        # English version (existing format)
        lines = [get_string("cluster_header", lang), ""]
        
        for node in master_nodes:
            status = "🟢 Online" if node.get("online") else "🔴 Offline"
            cpu = node.get("cpu_percent", 0)
            ram = node.get("ram_used_mb", 0)
            lines.extend([
                f"🖥️ *Master Node*",
                f"  • Status: {status}",
                f"  • CPU: `{cpu:.1f}%`",
                f"  • RAM: `{ram:.0f} MB`",
                "",
            ])
        
        if worker_nodes:
            lines.append(f"👷 *Workers ({len(worker_nodes)})*")
            for i, node in enumerate(worker_nodes, 1):
                status = "🟢 Online" if node.get("online") else "🔴 Offline"
                ip = node.get("local_ip", "Unknown")
                lines.append(f"  {i}. {status} | IP: `{ip}`")
            lines.append("")
    
    return "\n".join(lines)

def format_wallet_report(data: dict, lang: Language = "he") -> str:
    """Format financial data into a localized message."""
    daily_pnl = data.get("daily_pnl", 0)
    currency = data.get("currency", "USD")
    
    if lang == "he":
        pnl_text = f"רווח: +{daily_pnl:.2f}" if daily_pnl > 0 else f"הפסד: {daily_pnl:.2f}"
        pnl_icon = "📈" if daily_pnl > 0 else "📉" if daily_pnl < 0 else "⚖️"
        
        lines = [
            get_string("wallet_header", lang),
            "",
            f"{pnl_icon} *רווח יומי*",
            f"  • {pnl_text} {currency}",
            f"  • מצב: {'רווחי' if daily_pnl > 0 else 'הפסדי' if daily_pnl < 0 else 'איזון'}",
            "",
            f"🕐 עודכן: `{datetime.now().strftime('%H:%M')}`",
        ]
    else:
        pnl_text = f"+{daily_pnl:.2f}" if daily_pnl >= 0 else f"{daily_pnl:.2f}"
        pnl_icon = "📈" if daily_pnl > 0 else "📉" if daily_pnl < 0 else "⚖️"
        
        lines = [
            get_string("wallet_header", lang),
            "",
            f"{pnl_icon} *Daily P&L*",
            f"  • {pnl_text} {currency}",
            f"  • Status: {'Profit' if daily_pnl > 0 else 'Loss' if daily_pnl < 0 else 'Break-even'}",
            "",
        ]
    
    return "\n".join(lines)

# ── Decision request templates ─────────────────────────────────────────────────

def format_decision_request(
    task_type: str, 
    context: str, 
    confidence: float,
    lang: Language = "he"
) -> str:
    """Format a decision request for HITL approval."""
    if lang == "he":
        return (
            f"🤖 *בקשת החלטה אוטונומית*\n\n"
            f"המערכת מבקשת אישור לביצוע:\n"
            f"📋 *סוג משימה:* `{task_type}`\n"
            f"🎯 *רמת ביטחון:* `{confidence:.0f}%`\n\n"
            f"📝 *הקשר:*\n{context}\n\n"
            f"❓ האם לבצע פעולה זו?"
        )
    else:
        return (
            f"🤖 *Autonomous Decision Request*\n\n"
            f"System requests approval to execute:\n"
            f"📋 *Task Type:* `{task_type}`\n"
            f"🎯 *Confidence:* `{confidence:.0f}%`\n\n"
            f"📝 *Context:*\n{context}\n\n"
            f"❓ Should this action be executed?"
        )

def format_confirmation_update(
    original_text: str,
    decision: str,
    reviewer: str,
    lang: Language = "he"
) -> str:
    """Format the updated message after a decision is made."""
    if lang == "he":
        decision_text = "✅ הבקשה אושרה" if decision == "approved" else "❌ הבקשה נדחתה"
        status_text = "מבצע Auto-Scrape..." if decision == "approved" else "הפעולה בוטלה."
        return f"{original_text}\n\n{decision_text}\n{status_text}"
    else:
        decision_text = "✅ Request Approved" if decision == "approved" else "❌ Request Rejected"
        status_text = "Executing Auto-Scrape..." if decision == "approved" else "Operation cancelled."
        return f"{original_text}\n\n{decision_text}\n{status_text}"

# ── Main menu creation ─────────────────────────────────────────────────────────

def create_main_menu(lang: Language = "he"):
    """Create the main inline keyboard menu in the specified language."""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=MENU_BUTTONS["stats"][lang],
                callback_data="menu_stats"
            ),
            InlineKeyboardButton(
                text=MENU_BUTTONS["cluster"][lang], 
                callback_data="menu_cluster"
            ),
        ],
        [
            InlineKeyboardButton(
                text=MENU_BUTTONS["wallet"][lang],
                callback_data="menu_wallet"
            ),
            InlineKeyboardButton(
                text=MENU_BUTTONS["settings"][lang],
                callback_data="menu_settings"
            ),
        ],
    ])

def create_approval_keyboard(request_id: str, lang: Language = "he"):
    """Create approval/rejection inline keyboard."""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=ACTION_BUTTONS["approve"][lang],
                callback_data=f"hitl_approve:{request_id}"
            ),
            InlineKeyboardButton(
                text=ACTION_BUTTONS["reject"][lang],
                callback_data=f"hitl_reject:{request_id}"
            ),
        ],
    ])