"""
Fix Express Labs — Deal Hunter Orchestrator
Runs the full pipeline: scrape → evaluate → notify.

Usage:
    python main.py                    # run once
    python main.py --interval 3600    # run every hour
    python main.py --keywords "iPhone 15 שבור" "PS5 לחלקים"
"""

import asyncio
import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from scraper import run_scraper, KEYWORDS
from evaluator import evaluate_item, PROFIT_THRESHOLD
from notifier import send_deal_alert, send_startup_ping


RESULTS_DIR = Path("results")
SEEN_URLS: set[str] = set()   # In-memory deduplication (resets on restart)


def load_seen_urls() -> set[str]:
    """Load previously seen URLs from disk to avoid re-alerting."""
    seen_file = RESULTS_DIR / "seen_urls.json"
    if seen_file.exists():
        try:
            with open(seen_file, encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()


def save_seen_urls(urls: set[str]) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / "seen_urls.json", "w", encoding="utf-8") as f:
        json.dump(list(urls), f)


def save_results(items: list[dict], run_id: str) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"deals_{run_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"[Main] Results saved to {path}")


async def run_pipeline(
    keywords: list[str],
    use_llm: bool = True,
    headless: bool = True,
) -> list[dict]:
    """Full pipeline: scrape → evaluate → notify profitable deals."""
    global SEEN_URLS

    print(f"\n[Main] {'='*50}")
    print(f"[Main] 🚀 Deal Hunt started at {datetime.now().strftime('%H:%M:%S')}")
    print(f"[Main] Keywords: {len(keywords)} | LLM: {use_llm}")

    # 1. Scrape
    scraped = await run_scraper(keywords=keywords, headless=headless)
    print(f"[Main] 📥 Scraped {len(scraped)} raw items")

    profitable_deals = []

    for item in scraped:
        # Skip seen
        if item.url in SEEN_URLS:
            continue
        SEEN_URLS.add(item.url)

        # Skip items with no price (can't calculate flip score reliably)
        if not item.price:
            continue

        # 2. Evaluate
        analysis = evaluate_item(
            item_title=item.title,
            item_desc=item.description,
            price=item.price,
            use_llm=use_llm,
        )

        deal_data = {
            "item": asdict(item),
            "analysis": {
                "flip_score_min": analysis.flip_score_min,
                "flip_score_max": analysis.flip_score_max,
                "estimated_market_value": analysis.estimated_market_value,
                "repair_cost_range": [analysis.estimated_repair_min, analysis.estimated_repair_max],
                "detected_issues": analysis.detected_issues,
                "confidence": analysis.confidence,
                "is_profitable": analysis.is_profitable,
                "reasoning": analysis.reasoning,
            },
        }

        profitable_deals.append(deal_data)

        # 3. Notify if profitable
        if analysis.is_profitable:
            print(f"[Main] 💰 PROFITABLE: {item.title} | Flip ₪{analysis.flip_score_min}–{analysis.flip_score_max}")
            send_deal_alert(analysis, item.url)
        else:
            print(f"[Main] ⏭️  Skip: {item.title} | Score ₪{analysis.flip_score_min} < ₪{PROFIT_THRESHOLD}")

    save_seen_urls(SEEN_URLS)
    save_results(profitable_deals, datetime.now().strftime("%Y%m%d_%H%M%S"))

    print(f"[Main] ✅ Run complete. {len(profitable_deals)} deals evaluated, {sum(1 for d in profitable_deals if d['analysis']['is_profitable'])} profitable.")
    return profitable_deals


async def main_loop(
    keywords: list[str],
    interval_seconds: int,
    use_llm: bool,
    headless: bool,
) -> None:
    """Continuous loop — runs pipeline every `interval_seconds`."""
    global SEEN_URLS
    SEEN_URLS = load_seen_urls()

    send_startup_ping()

    while True:
        try:
            await run_pipeline(keywords, use_llm=use_llm, headless=headless)
        except Exception as e:
            print(f"[Main] Pipeline error: {e}")

        print(f"[Main] 💤 Sleeping {interval_seconds}s until next run...")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix Express Labs Deal Hunter")
    parser.add_argument("--interval", type=int, default=0,
                        help="Re-run interval in seconds (0 = run once)")
    parser.add_argument("--keywords", nargs="*", default=None,
                        help="Custom search keywords")
    parser.add_argument("--no-llm", action="store_true",
                        help="Use local DB instead of LLM for evaluation")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show browser window (for debugging)")
    args = parser.parse_args()

    kws = args.keywords or KEYWORDS
    use_llm = not args.no_llm
    headless = not args.no_headless

    if args.interval > 0:
        asyncio.run(main_loop(kws, args.interval, use_llm, headless))
    else:
        asyncio.run(run_pipeline(kws, use_llm=use_llm, headless=headless))
