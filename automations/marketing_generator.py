"""
Fix Express Labs — Marketing Content Generator
Generates Hebrew Instagram captions and technical explanations for repair cases.
Usage:
    python marketing_generator.py
    # or import and call generate_content(repair_case)
"""

import json
import textwrap
from dataclasses import dataclass, field
from typing import Optional
from datetime import date


# ─── Data Model ───────────────────────────────────────────────────────────────

@dataclass
class RepairCase:
    device_type: str          # e.g. "iPhone 14 Pro", "MacBook M2", "PS5"
    repair_type: str          # e.g. "החלפת מסך OLED", "מיקרו-לחמה BGA", "תיקון HDMI"
    brand: str                # e.g. "Apple", "Samsung", "Sony"
    symptom: str              # e.g. "מסך שחור לאחר נפילה"
    solution: str             # e.g. "החלפת פאנל OLED מקורי"
    equipment_used: list[str] = field(default_factory=list)  # e.g. ["JBC Soldering", "FLIR Camera"]
    before_state: Optional[str] = None
    after_state: Optional[str] = None
    repair_time_minutes: Optional[int] = None
    price: Optional[int] = None


# ─── Templates & Data ─────────────────────────────────────────────────────────

EQUIPMENT_DESCRIPTIONS = {
    "JBC Soldering": "תחנת לחמה JBC ברמה תעשייתית עם בקרת טמפרטורה מדויקת עד 1°C",
    "FLIR Camera": "מצלמת הדמיה תרמית FLIR לאיתור נקודות חום ורכיבים פגומים",
    "Microscope 40X": "מיקרוסקופ דיגיטלי 40X לעבודת מיקרו-לחמה על רכיבי SMD",
    "DCPS": "ספק כוח DC מדויק לאבחון צריכת זרם בזמן אמת",
    "Ultrasonic Cleaner": "מנקה אולטרה-סוני לניקוי לוחות אם מנזקי נוזלים",
    "Laser Unit": "יחידת לייזר לניקוי ותיקון מסלולי מעגל מודפס",
    "Hot Air Station": "תחנת אוויר חם לפירוק ולחמה מחדש של רכיבי BGA",
}

HASHTAG_SETS = {
    "phone": "#תיקוןסמארטפון #תיקוןאייפון #תיקוןסמסונג #מעבדהניידת #fixexpresslabs #תיקוןנייד #שירותעדהבית",
    "laptop": "#תיקוןמחשב #תיקוןלפטופ #תיקוןמקבוק #מיקרולחמה #fixexpresslabs #מעבדהניידת #שדרוגמחשב",
    "console": "#תיקוןPS5 #תיקוןקונסולה #תיקוןXbox #PS5תיקון #fixexpresslabs #מעבדהניידת #גיימינג",
    "tablet": "#תיקוןאייפד #תיקוןטאבלט #fixexpresslabs #מעבדהניידת #שירותעדהבית",
    "default": "#תיקוןאלקטרוניקה #מעבדהניידת #fixexpresslabs #שירותעדהבית #תיקוןמהיר",
}

DEVICE_CATEGORY_MAP = {
    "iphone": "phone", "samsung": "phone", "pixel": "phone", "xiaomi": "phone", "oneplus": "phone",
    "macbook": "laptop", "dell": "laptop", "lenovo": "laptop", "hp": "laptop", "asus": "laptop",
    "ps5": "console", "ps4": "console", "xbox": "console", "nintendo": "console", "switch": "console",
    "ipad": "tablet", "apple watch": "tablet",
}

OPENING_HOOKS = [
    "⚡ כשאחרים אמרו 'לא ניתן לתיקון' — אנחנו קיבלנו אתגר.",
    "🔬 זה לא תיקון רגיל. זו הנדסה בזעיר אנפין.",
    "🛠️ מעבדה ניידת. ציוד תעשייתי. תוצאה מושלמת.",
    "⚙️ כשהלקוח חשב שהמכשיר גמור — אנחנו רק התחלנו.",
    "🚀 {device} — מתקלה להחייאה מלאה. זה מה שאנחנו עושים.",
]


# ─── Generator ────────────────────────────────────────────────────────────────

def _get_category(case: RepairCase) -> str:
    combined = f"{case.brand} {case.device_type}".lower()
    for keyword, cat in DEVICE_CATEGORY_MAP.items():
        if keyword in combined:
            return cat
    return "default"


def _build_equipment_block(equipment: list[str]) -> str:
    if not equipment:
        return "ציוד מעבדה מתקדם"
    lines = []
    for eq in equipment:
        desc = EQUIPMENT_DESCRIPTIONS.get(eq, eq)
        lines.append(f"  • {desc}")
    return "\n".join(lines)


def generate_instagram_caption(case: RepairCase) -> str:
    """Generate a Hebrew Instagram caption for a repair case."""
    import random

    category = _get_category(case)
    hashtags = HASHTAG_SETS.get(category, HASHTAG_SETS["default"])

    hook_template = random.choice(OPENING_HOOKS)
    hook = hook_template.format(device=case.device_type)

    time_str = f" תוך {case.repair_time_minutes} דקות בלבד!" if case.repair_time_minutes else ""
    before_after = ""
    if case.before_state and case.after_state:
        before_after = f"\n\n📍 לפני: {case.before_state}\n✅ אחרי: {case.after_state}"

    equipment_line = ""
    if case.equipment_used:
        eq_names = " + ".join(case.equipment_used[:3])
        equipment_line = f"\n\n🔬 ציוד שהשתמשנו: {eq_names}"

    caption = textwrap.dedent(f"""
{hook}

📱 מכשיר: {case.brand} {case.device_type}
🔧 תקלה: {case.symptom}
✅ פתרון: {case.solution}{time_str}{before_after}{equipment_line}

💬 יש לך מכשיר שצריך תיקון? שלח הודעה עכשיו!
📲 wa.me/972504951109 | 050-495-1109

{hashtags}
    """).strip()

    return caption


def generate_technical_explanation(case: RepairCase) -> str:
    """Generate a professional technical explanation of why advanced equipment was needed."""

    equipment_block = _build_equipment_block(case.equipment_used)

    explanation = textwrap.dedent(f"""
# דוח טכני — {case.brand} {case.device_type}
**תאריך:** {date.today().strftime('%d/%m/%Y')}
**סוג תיקון:** {case.repair_type}

## תיאור התקלה
**תסמין מוצהר:** {case.symptom}

## מדוע נדרש ציוד מתקדם?

{case.repair_type} בדגם {case.device_type} דורש רמת דיוק שלא ניתן להשיג עם כלים סטנדרטיים:

{''.join([f"- **{eq}:** {EQUIPMENT_DESCRIPTIONS.get(eq, 'ציוד מתקדם')}" + chr(10) for eq in case.equipment_used]) if case.equipment_used else "- ציוד מעבדה מתקדם ברמה תעשייתית"}

## תהליך התיקון
1. **אבחון ראשוני** — בדיקת זרם עם DCPS + סריקה תרמית
2. **פירוק מדויק** — פתיחה עם כלים מותאמים למכשיר ספציפי
3. **ביצוע:** {case.solution}
4. **בקרת איכות** — בדיקת כל הפונקציות לפני מסירה

## תוצאה
**{case.after_state or "המכשיר חזר לפעולה מלאה"}**
{'**זמן תיקון:** ' + str(case.repair_time_minutes) + ' דקות' if case.repair_time_minutes else ''}
{'**מחיר:** ₪' + str(case.price) if case.price else ''}

---
*Fix Express Labs | ירין הלילי | 050-495-1109*
    """).strip()

    return explanation


def generate_content(repair_case: RepairCase) -> dict:
    """Main entry point — returns both caption and technical report."""
    return {
        "instagram_caption": generate_instagram_caption(repair_case),
        "technical_report": generate_technical_explanation(repair_case),
    }


# ─── CLI Demo ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example repair cases
    demo_cases = [
        RepairCase(
            device_type="iPhone 14 Pro",
            repair_type="החלפת מסך OLED",
            brand="Apple",
            symptom="מסך שחור לאחר נפילה — מסך מת",
            solution="החלפת פאנל OLED מקורי Apple עם כיול FaceID",
            equipment_used=["Microscope 40X", "Hot Air Station"],
            before_state="מסך שחור לחלוטין, מסגרת שבורה",
            after_state="מסך OLED חדש, FaceID עובד, גוון צבע מקורי",
            repair_time_minutes=45,
            price=480,
        ),
        RepairCase(
            device_type="MacBook Pro M2",
            repair_type="מיקרו-לחמה BGA — תיקון PMIC",
            brand="Apple",
            symptom="לא נדלק — זרם 0mA בחיבור לחשמל",
            solution="זיהוי ותיקון IC ניהול כוח (PMIC) שנשרף",
            equipment_used=["JBC Soldering", "FLIR Camera", "DCPS", "Microscope 40X"],
            before_state="מחשב מת לחלוטין, לא מגיב לשום לחיצה",
            after_state="מחשב חזר לחיים — macOS עולה תקין, כל הפורטים עובדים",
            repair_time_minutes=90,
            price=650,
        ),
        RepairCase(
            device_type="PS5",
            repair_type="החלפת פורט HDMI",
            brand="Sony",
            symptom="אין תמונה בטלוויזיה, פין HDMI שבור",
            solution="הסרת פורט HDMI פגום ולחמה מחדש של פורט מקורי",
            equipment_used=["JBC Soldering", "Hot Air Station", "Microscope 40X"],
            before_state="אין תמונה, פין אחד כפוף, ניסיון חיבור כוחני",
            after_state="4K/120Hz — הכל עובד מושלם",
            repair_time_minutes=60,
            price=350,
        ),
    ]

    for i, case in enumerate(demo_cases, 1):
        print(f"\n{'='*60}")
        print(f"תיק מספר {i}: {case.brand} {case.device_type}")
        print(f"{'='*60}\n")

        content = generate_content(case)

        print("📸 INSTAGRAM CAPTION:")
        print("-" * 40)
        print(content["instagram_caption"])

        print("\n\n📋 TECHNICAL REPORT:")
        print("-" * 40)
        print(content["technical_report"])

    # JSON export example
    last_content = generate_content(demo_cases[0])
    with open("last_generated_content.json", "w", encoding="utf-8") as f:
        json.dump(last_content, f, ensure_ascii=False, indent=2)
    print(f"\n\n✅ Content also exported to last_generated_content.json")
