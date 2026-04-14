"""
Fix Express Labs — Telegram Deal Notifier
Sends profitable deal alerts via the Telegram Bot API using `requests`.

Set in .env:
    TELEGRAM_BOT_TOKEN=<your_bot_token>
    TELEGRAM_CHAT_ID=<your_channel_or_chat_id>
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
API_BASE           = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


# ─── Formatting helpers ───────────────────────────────────────────────────────

def _profit_bar(score: int) -> str:
    """Visual profit indicator bar."""
    bars = min(10, max(1, score // 200))
    return "█" * bars + "░" * (10 - bars)


def _confidence_badge(conf: str) -> str:
    return {"high": "🟢 גבוה", "medium": "🟡 בינוני", "low": "🔴 נמוך"}.get(conf, "🟡")


def _issues_str(issues: list) -> str:
    labels = {
        "screen": "מסך שבור",
        "battery": "סוללה",
        "motherboard": "לוח אם / IC",
        "hdmi": "HDMI",
        "water": "נזקי נוזלים",
        "charging": "פורט טעינה",
        "camera": "מצלמה",
    }
    mapped = [labels.get(i, i) for i in issues]
    return " | ".join(mapped) if mapped else "לא זוהה ספציפית"


def build_html_message(analysis, url: str) -> str:
    """
    Build a richly formatted HTML message for Telegram.
    Uses HTML parse mode for bold/italic/links.
    """
    profit_min = analysis.flip_score_min
    profit_max = analysis.flip_score_max
    bar        = _profit_bar(profit_min)

    profit_emoji = "🔥🔥" if profit_min >= 1500 else ("🔥" if profit_min >= 700 else "⚡")

    html = (
        f"{profit_emoji} <b>Fix Express Labs — Deal Alert</b>\n\n"
        f"📱 <b>{analysis.item_title}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💸 <b>מחיר נמצא:</b> ₪{analysis.scraped_price or '?'}\n"
        f"🔧 <b>עלות תיקון:</b> ₪{analysis.estimated_repair_min}–₪{analysis.estimated_repair_max}\n"
        f"📈 <b>שווי שוק תקין:</b> ₪{analysis.estimated_market_value}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>רווח פוטנציאלי: ₪{profit_min}–₪{profit_max}</b>\n"
        f"<code>{bar}</code> {profit_min} NIS\n\n"
        f"🔬 <b>תקלות שזוהו:</b> {_issues_str(analysis.detected_issues)}\n"
        f"{_confidence_badge(analysis.confidence)} <b>ביטחון</b>\n\n"
        f"💡 <i>{analysis.reasoning}</i>\n\n"
        f"🔗 <a href=\"{url}\">פתח מודעה</a>"
    )
    return html


def send_message(text: str, parse_mode: str = "HTML") -> dict:
    """
    Send a plain message via Telegram Bot API using requests.
    Returns the API JSON response.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[Notifier] ⚠️  Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env")
        return {"ok": False, "error": "missing credentials"}

    url     = f"{API_BASE}/sendMessage"
    payload = {
        "chat_id":                  TELEGRAM_CHAT_ID,
        "text":                     text,
        "parse_mode":               parse_mode,
        "disable_web_page_preview": False,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok"):
            print(f"[Notifier] ✅ Telegram message sent (msg_id={data['result']['message_id']})")
        else:
            print(f"[Notifier] ❌ Telegram API error: {data.get('description')}")
        return data
    except requests.exceptions.Timeout:
        print("[Notifier] ❌ Telegram request timed out.")
        return {"ok": False, "error": "timeout"}
    except requests.exceptions.RequestException as e:
        print(f"[Notifier] ❌ Telegram request failed: {e}")
        return {"ok": False, "error": str(e)}


def send_deal_alert(analysis, url: str) -> bool:
    """
    Format and send a deal alert to Telegram.
    Returns True if the message was sent successfully.
    """
    text = build_html_message(analysis, url)
    result = send_message(text, parse_mode="HTML")
    return result.get("ok", False)


def send_startup_ping() -> bool:
    """Send a startup confirmation message."""
    text = (
        "🚀 <b>Fix Express Labs Deal Hunter</b>\n"
        "Bot started ✓ — scanning for profitable deals...\n"
        f"<i>Profit threshold: ₪500</i>"
    )
    result = send_message(text)
    return result.get("ok", False)


def send_summary(run_stats: dict) -> bool:
    """Send a run summary report."""
    text = (
        f"📊 <b>סיכום ריצה — Deal Hunter</b>\n\n"
        f"🔍 פריטים שנסרקו: <b>{run_stats.get('scraped', 0)}</b>\n"
        f"📊 פריטים שנוחלו: <b>{run_stats.get('evaluated', 0)}</b>\n"
        f"💰 עסקאות רווחיות: <b>{run_stats.get('profitable', 0)}</b>\n"
        f"⏱ זמן ריצה: <b>{run_stats.get('duration_sec', 0):.1f}s</b>"
    )
    result = send_message(text)
    return result.get("ok", False)


# ─── CLI test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from evaluator import FlipAnalysis

    dummy = FlipAnalysis(
        item_title="iPhone 14 Pro — מסך שבור לאחר נפילה",
        scraped_price=800,
        estimated_repair_min=420,
        estimated_repair_max=520,
        estimated_market_value=4000,
        flip_score_min=2680,
        flip_score_max=2780,
        detected_issues=["screen"],
        confidence="high",
        reasoning="שווי שוק: ₪4000 | מחיר: ₪800 | תיקון: ₪420–₪520",
        is_profitable=True,
    )

    print("Message preview:")
    print("-" * 50)
    print(build_html_message(dummy, "https://www.facebook.com/marketplace/item/example"))
    print("-" * 50)
    print("\nSending test alert to Telegram...")
    ok = send_deal_alert(dummy, "https://www.facebook.com/marketplace/item/example")
    print("Sent:", ok)
