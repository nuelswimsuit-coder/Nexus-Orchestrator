"""
Fix Express Labs — Deal Hunter Stealth Scraper
Uses Playwright with stealth settings to search Facebook Marketplace
and Israeli classifieds (Yad2) for broken tech items worth flipping.

Requirements:
    pip install playwright playwright-stealth python-dotenv
    playwright install chromium
"""

import asyncio
import json
import re
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
from playwright.async_api import async_playwright, Page, BrowserContext

# ─── Target keywords (Hebrew + device combos) ────────────────────────────────
KEYWORDS = [
    "iPhone 15 שבור",
    "iPhone 14 לא נדלק",
    "iPhone תקלה בלוח",
    "MacBook לא נדלק",
    "MacBook שבור למכירה לחלקים",
    "PS5 תקלה",
    "PS5 HDMI שבור",
    "Samsung S24 מסך שבור",
    "Samsung לוח שבור",
    "מקבוק לא עובד",
    "לפטופ לחלקים",
]

PLATFORMS = {
    "facebook": "https://www.facebook.com/marketplace/search/?query={query}&exact=false",
    "yad2": "https://www.yad2.co.il/s/cellphones?text={query}",
}

# ─── Stealth headers ──────────────────────────────────────────────────────────
STEALTH_HEADERS = {
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


@dataclass
class ScrapedItem:
    title: str
    price: Optional[int]
    description: str
    url: str
    platform: str
    keyword: str
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    location: str = ""
    seller: str = ""
    images: list[str] = field(default_factory=list)


async def apply_stealth(page: Page) -> None:
    """Apply JS patches to evade common bot-detection checks."""
    await page.add_init_script("""
        // Overwrite the 'webdriver' property
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

        // Overwrite plugins length to appear as a real browser
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });

        // Overwrite languages
        Object.defineProperty(navigator, 'languages', { get: () => ['he-IL', 'he', 'en-US', 'en'] });

        // Fake permissions API
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
        );

        // Chrome runtime
        window.chrome = { runtime: {} };
    """)


async def random_delay(min_ms: int = 800, max_ms: int = 2500) -> None:
    """Human-like delay between actions."""
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


async def scroll_gradually(page: Page, steps: int = 5) -> None:
    """Simulate human scrolling behaviour."""
    for _ in range(steps):
        await page.evaluate("window.scrollBy(0, window.innerHeight * 0.6)")
        await random_delay(400, 900)


async def scrape_facebook_marketplace(
    context: BrowserContext, keyword: str
) -> list[ScrapedItem]:
    """Scrape Facebook Marketplace for a given keyword."""
    page = await context.new_page()
    await apply_stealth(page)

    url = PLATFORMS["facebook"].format(query=keyword.replace(" ", "%20"))
    items: list[ScrapedItem] = []

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await random_delay(2000, 4000)
        await scroll_gradually(page, steps=4)

        # FB Marketplace item cards selector (subject to change as FB updates)
        cards = await page.query_selector_all('[data-testid="marketplace_feed_item"]')
        if not cards:
            # Fallback selector
            cards = await page.query_selector_all('a[href*="/marketplace/item/"]')

        for card in cards[:20]:
            try:
                title_el = await card.query_selector('[class*="title"], span[class*="x1lliihq"]')
                price_el = await card.query_selector('[class*="price"], span[class*="x193iq5w"]')
                href = await card.get_attribute("href")

                raw_title = await title_el.inner_text() if title_el else ""
                raw_price = await price_el.inner_text() if price_el else ""

                # Parse price — extract digits only
                price_digits = re.findall(r'\d+', raw_price.replace(",", ""))
                price = int("".join(price_digits[:4])) if price_digits else None

                full_url = f"https://www.facebook.com{href}" if href and href.startswith("/") else href or url

                if raw_title:
                    items.append(ScrapedItem(
                        title=raw_title.strip(),
                        price=price,
                        description=raw_title.strip(),
                        url=full_url,
                        platform="facebook",
                        keyword=keyword,
                    ))
            except Exception:
                continue

    except Exception as e:
        print(f"[FB] Error scraping '{keyword}': {e}")
    finally:
        await page.close()

    return items


async def scrape_yad2(
    context: BrowserContext, keyword: str
) -> list[ScrapedItem]:
    """Scrape Yad2 classifieds for a given keyword."""
    page = await context.new_page()
    await apply_stealth(page)

    url = PLATFORMS["yad2"].format(query=keyword.replace(" ", "+"))
    items: list[ScrapedItem] = []

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await random_delay(1500, 3000)
        await scroll_gradually(page, steps=3)

        cards = await page.query_selector_all('.feed-item, [class*="feedItem"], [data-test="feed-item"]')

        for card in cards[:20]:
            try:
                title_el = await card.query_selector('h2, [class*="title"], [data-test="item-title"]')
                price_el = await card.query_selector('[class*="price"], [data-test="price"]')
                link_el = await card.query_selector('a')

                raw_title = await title_el.inner_text() if title_el else ""
                raw_price = await price_el.inner_text() if price_el else ""
                href = await link_el.get_attribute("href") if link_el else ""

                price_digits = re.findall(r'\d+', raw_price.replace(",", ""))
                price = int("".join(price_digits[:5])) if price_digits else None

                full_url = f"https://www.yad2.co.il{href}" if href and href.startswith("/") else href or url

                if raw_title:
                    items.append(ScrapedItem(
                        title=raw_title.strip(),
                        price=price,
                        description=raw_title.strip(),
                        url=full_url,
                        platform="yad2",
                        keyword=keyword,
                    ))
            except Exception:
                continue

    except Exception as e:
        print(f"[Yad2] Error scraping '{keyword}': {e}")
    finally:
        await page.close()

    return items


async def run_scraper(
    keywords: list[str] | None = None,
    max_per_keyword: int = 20,
    headless: bool = True,
) -> list[ScrapedItem]:
    """Main scraper entry point. Returns all scraped items."""
    keywords = keywords or KEYWORDS
    all_items: list[ScrapedItem] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-extensions",
                "--disable-dev-shm-usage",
                "--no-first-run",
                "--ignore-certificate-errors",
            ],
        )

        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768},
            locale="he-IL",
            timezone_id="Asia/Jerusalem",
            extra_http_headers=STEALTH_HEADERS,
        )

        for keyword in keywords:
            print(f"[Scraper] Searching: {keyword}")

            fb_items = await scrape_facebook_marketplace(context, keyword)
            yad2_items = await scrape_yad2(context, keyword)

            for item in (fb_items + yad2_items)[:max_per_keyword]:
                all_items.append(item)

            # Respectful delay between keywords to avoid rate-limiting
            await random_delay(3000, 6000)

        await browser.close()

    print(f"[Scraper] Done. Found {len(all_items)} items total.")
    return all_items


if __name__ == "__main__":
    items = asyncio.run(run_scraper(headless=False))  # headless=False for debugging
    with open("scraped_items.json", "w", encoding="utf-8") as f:
        json.dump([asdict(i) for i in items], f, ensure_ascii=False, indent=2)
    print("Results saved to scraped_items.json")
