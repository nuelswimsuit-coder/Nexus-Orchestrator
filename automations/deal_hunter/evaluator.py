"""
Fix Express Labs — LLM Deal Evaluator
Passes scraped item data to Claude/OpenAI and calculates a "Flip Score".

Flip Score = Estimated Market Value (working) - Scraped Price - Estimated Repair Cost

Requirements:
    pip install anthropic python-dotenv
"""

import os
import json
import re
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# ─── Repair cost database (NIS) ──────────────────────────────────────────────
# Used as fallback when LLM estimate is unavailable.
REPAIR_COST_DB = {
    # (device_pattern_lower, issue_pattern_lower) → (min, max) NIS
    ("iphone 15", "screen"): (450, 560),
    ("iphone 14", "screen"): (420, 520),
    ("iphone 13", "screen"): (360, 450),
    ("iphone 12", "screen"): (320, 400),
    ("iphone",    "battery"): (180, 250),
    ("iphone",    "charging"): (180, 260),
    ("iphone",    "motherboard"): (450, 800),
    ("iphone",    "water"): (350, 650),
    ("samsung",   "screen"): (250, 450),
    ("samsung",   "battery"): (150, 220),
    ("macbook",   "screen"): (550, 900),
    ("macbook",   "motherboard"): (600, 1200),
    ("macbook",   "battery"): (350, 500),
    ("ps5",       "hdmi"): (280, 380),
    ("ps5",       "motherboard"): (400, 700),
    ("xbox",      "hdmi"): (250, 350),
}

MARKET_VALUE_DB = {
    "iphone 15 pro max": 5500,
    "iphone 15 pro": 4800,
    "iphone 15": 4200,
    "iphone 14 pro max": 4500,
    "iphone 14 pro": 4000,
    "iphone 14": 3200,
    "iphone 13": 2500,
    "iphone 12": 1800,
    "samsung s24 ultra": 5500,
    "samsung s24": 4200,
    "samsung s23": 3200,
    "macbook pro m3": 9000,
    "macbook pro m2": 7500,
    "macbook air m2": 5500,
    "macbook air m1": 4200,
    "ps5": 2300,
    "ps4 pro": 1200,
    "xbox series x": 2000,
}


def _lookup_repair_cost(title: str, description: str) -> tuple[int, int]:
    """Fast local lookup before calling LLM."""
    combined = (title + " " + description).lower()
    best = (300, 600)  # default fallback
    for (device, issue), cost_range in REPAIR_COST_DB.items():
        if device in combined and issue in combined:
            best = cost_range
            break
    return best


def _lookup_market_value(title: str) -> int:
    """Fast local market value lookup."""
    title_lower = title.lower()
    for device, value in MARKET_VALUE_DB.items():
        if device in title_lower:
            return value
    return 2000  # conservative default


@dataclass
class FlipAnalysis:
    item_title: str
    scraped_price: Optional[int]
    estimated_repair_min: int
    estimated_repair_max: int
    estimated_market_value: int
    flip_score_min: int       # market_value - price - repair_max
    flip_score_max: int       # market_value - price - repair_min
    detected_issues: list[str]
    confidence: str           # 'high' | 'medium' | 'low'
    reasoning: str
    is_profitable: bool       # flip_score_min >= PROFIT_THRESHOLD


PROFIT_THRESHOLD = 500  # NIS


def evaluate_with_local_db(item_title: str, item_desc: str, price: Optional[int]) -> FlipAnalysis:
    """
    Evaluate a deal using the local pricing database.
    Use this as fallback or when no LLM API key is set.
    """
    repair_min, repair_max = _lookup_repair_cost(item_title, item_desc)
    market_value = _lookup_market_value(item_title)
    effective_price = price or 0

    flip_min = market_value - effective_price - repair_max
    flip_max = market_value - effective_price - repair_min

    # Detect issues from text
    issue_keywords = {
        "screen": ["מסך", "screen", "display", "שבור"],
        "battery": ["סוללה", "battery", "ניקוז", "לא נטעין"],
        "motherboard": ["לוח", "motherboard", "board", "לא נדלק"],
        "hdmi": ["hdmi", "אין תמונה"],
        "water": ["נוזל", "water", "rain", "שפכתי"],
        "charging": ["טעינה", "charging", "USB", "לא מטעין"],
    }
    combined = (item_title + " " + item_desc).lower()
    detected = [issue for issue, kws in issue_keywords.items() if any(kw in combined for kw in kws)]

    confidence = "high" if price and price > 0 and detected else "medium"

    reasoning = (
        f"שווי שוק מוערך: ₪{market_value} | "
        f"מחיר נמצא: ₪{effective_price} | "
        f"עלות תיקון: ₪{repair_min}–₪{repair_max} | "
        f"רווח פוטנציאלי: ₪{flip_min}–₪{flip_max}"
    )

    return FlipAnalysis(
        item_title=item_title,
        scraped_price=price,
        estimated_repair_min=repair_min,
        estimated_repair_max=repair_max,
        estimated_market_value=market_value,
        flip_score_min=flip_min,
        flip_score_max=flip_max,
        detected_issues=detected,
        confidence=confidence,
        reasoning=reasoning,
        is_profitable=flip_min >= PROFIT_THRESHOLD,
    )


def evaluate_with_llm(item_title: str, item_desc: str, price: Optional[int]) -> FlipAnalysis:
    """
    Use Claude API to evaluate a deal. Falls back to local DB on failure.
    Set ANTHROPIC_API_KEY in .env to enable.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("[Evaluator] No ANTHROPIC_API_KEY found, using local DB.")
        return evaluate_with_local_db(item_title, item_desc, price)

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)

        prompt = f"""You are an expert electronics repair technician and secondhand market dealer in Israel.

Analyze this listing and return a JSON object only — no prose.

Item title: {item_title}
Description: {item_desc}
Listed price: {price} NIS (0 = price unknown)

Return JSON with these exact keys:
{{
  "detected_issues": ["list of detected hardware problems in English"],
  "estimated_repair_cost_min": <int NIS>,
  "estimated_repair_cost_max": <int NIS>,
  "estimated_market_value": <int NIS for working device in Israel>,
  "confidence": "high" | "medium" | "low",
  "reasoning": "<short Hebrew explanation>"
}}

Base estimates on current Israeli market prices (2024-2025).
Consider that this is for a professional mobile repair lab with advanced equipment (BGA microsoldering, FLIR thermal camera).
"""

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = message.content[0].text.strip()
        # Extract JSON block if wrapped in markdown
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        data = json.loads(json_match.group() if json_match else raw)

        effective_price = price or 0
        repair_min = data.get("estimated_repair_cost_min", 300)
        repair_max = data.get("estimated_repair_cost_max", 600)
        market_value = data.get("estimated_market_value", 2000)

        flip_min = market_value - effective_price - repair_max
        flip_max = market_value - effective_price - repair_min

        return FlipAnalysis(
            item_title=item_title,
            scraped_price=price,
            estimated_repair_min=repair_min,
            estimated_repair_max=repair_max,
            estimated_market_value=market_value,
            flip_score_min=flip_min,
            flip_score_max=flip_max,
            detected_issues=data.get("detected_issues", []),
            confidence=data.get("confidence", "medium"),
            reasoning=data.get("reasoning", ""),
            is_profitable=flip_min >= PROFIT_THRESHOLD,
        )

    except Exception as e:
        print(f"[Evaluator] LLM call failed ({e}), falling back to local DB.")
        return evaluate_with_local_db(item_title, item_desc, price)


def evaluate_item(item_title: str, item_desc: str, price: Optional[int], use_llm: bool = True) -> FlipAnalysis:
    """Main evaluation entry point."""
    if use_llm:
        return evaluate_with_llm(item_title, item_desc, price)
    return evaluate_with_local_db(item_title, item_desc, price)


if __name__ == "__main__":
    # Demo
    test_cases = [
        ("iPhone 14 Pro שבור", "מסך שבור לאחר נפילה, הכל שאר עובד", 800),
        ("PS5 Disc Edition", "HDMI שבור, אין תמונה, קניתי חדש", 1200),
        ("MacBook Pro M2 לא נדלק", "ספק כוח לא עובד, לוח בסדר", 2500),
    ]

    for title, desc, price in test_cases:
        result = evaluate_item(title, desc, price, use_llm=False)
        print(f"\n{'='*50}")
        print(f"📱 {title}")
        print(f"💰 מחיר: ₪{price}")
        print(f"🔧 תיקון: ₪{result.estimated_repair_min}–₪{result.estimated_repair_max}")
        print(f"📊 שווי שוק: ₪{result.estimated_market_value}")
        print(f"🎯 Flip Score: ₪{result.estimated_repair_min}–₪{result.estimated_repair_max}")
        print(f"✅ רווחי: {'כן!' if result.is_profitable else 'לא'}")
        print(f"💡 {result.reasoning}")
