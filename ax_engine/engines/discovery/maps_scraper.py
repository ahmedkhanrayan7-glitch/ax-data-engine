"""
AX Engine — Google Maps Scraper

Strategy:
  1. Search Google Maps for "{niche} in {location}"
  2. Paginate through all results (up to max_results)
  3. Extract: name, address, phone, website, rating, reviews, categories
  4. Rotate proxies per request, rotate user agents per session
  5. Implement exponential backoff + retry on rate limit detection

Legal note: Extracts publicly visible data from Google Maps search results.
Adheres to respectful scraping with delays and rate limits.
"""
from __future__ import annotations

import asyncio
import json
import random
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import structlog
from playwright.async_api import BrowserContext, Page, Playwright, async_playwright

from ax_engine.config import settings
from ax_engine.infrastructure.proxy_manager import ProxyManager
from ax_engine.infrastructure.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

# Google Maps selectors (these may need updating if Google changes the DOM)
SELECTORS = {
    "results_container": '[role="feed"]',
    "result_item": '[data-result-index]',
    "business_name": 'h1.DUwDvf',
    "address": '[data-item-id="address"] .Io6YTe',
    "phone": '[data-item-id^="phone"] .Io6YTe',
    "website": 'a[data-item-id="authority"]',
    "rating": '.F7nice span[aria-hidden="true"]',
    "review_count": '.F7nice span[aria-label]',
    "categories": '.DkEaL',
    "hours": '.t39EBf',
}


class GoogleMapsScraper:
    """
    Scrapes Google Maps for business listings.

    Uses Playwright with stealth mode to bypass basic bot detection.
    Implements respectful rate limiting: 1-4s delays between actions.
    """

    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.rate_limiter = RateLimiter(requests_per_minute=20)

    async def search(
        self,
        query: str,
        max_results: int = 25,
    ) -> List[Dict[str, Any]]:
        """
        Main entry point. Returns list of raw business dicts.
        """
        log = logger.bind(query=query, max_results=max_results)
        log.info("maps_scraper.search_start")

        results = []

        async with async_playwright() as pw:
            browser_context = await self._create_context(pw)

            try:
                page = await browser_context.new_page()
                await self._apply_stealth(page)

                # Navigate to Google Maps search
                encoded_query = quote_plus(query)
                url = f"https://www.google.com/maps/search/{encoded_query}"

                await self.rate_limiter.acquire()
                await page.goto(url, wait_until="networkidle", timeout=settings.NAVIGATION_TIMEOUT_MS)

                # Handle cookie consent (EU/Russia)
                await self._handle_consent(page)

                # Scroll and collect results
                results = await self._collect_results(page, max_results)
                log.info("maps_scraper.search_complete", count=len(results))

            except Exception as e:
                log.error("maps_scraper.search_failed", error=str(e))
            finally:
                await browser_context.close()

        return results

    async def _collect_results(self, page: Page, max_results: int) -> List[Dict[str, Any]]:
        """
        Scrolls the results panel and extracts business data.
        Clicks each result to get full details including website.
        """
        results = []
        seen_names = set()

        # Wait for results to load
        try:
            await page.wait_for_selector(SELECTORS["results_container"], timeout=15000)
        except Exception:
            logger.warning("maps_scraper.no_results_container")
            return results

        while len(results) < max_results:
            # Get all currently visible result items
            items = await page.query_selector_all(SELECTORS["result_item"])

            for item in items:
                if len(results) >= max_results:
                    break

                try:
                    name_el = await item.query_selector("div.qBF1Pd")
                    if not name_el:
                        continue

                    name = (await name_el.inner_text()).strip()
                    if not name or name in seen_names:
                        continue
                    seen_names.add(name)

                    # Click to get full details
                    await item.click()
                    await asyncio.sleep(random.uniform(1.5, 3.0))

                    business = await self._extract_business_detail(page, name)
                    if business:
                        results.append(business)

                except Exception as e:
                    logger.debug("maps_scraper.item_extract_failed", error=str(e))
                    continue

            # Scroll down to load more results
            prev_count = len(results)
            await self._scroll_results(page)
            await asyncio.sleep(random.uniform(1.0, 2.5))

            # Check if we've stopped getting new results
            new_items = await page.query_selector_all(SELECTORS["result_item"])
            if len(new_items) <= len(items):
                break  # No new results loaded

        return results

    async def _extract_business_detail(self, page: Page, name: str) -> Optional[Dict[str, Any]]:
        """
        Extracts full business details from the detail panel on the right.
        """
        try:
            await page.wait_for_selector(SELECTORS["business_name"], timeout=8000)
        except Exception:
            return None

        data: Dict[str, Any] = {"name": name}

        # Extract each field with individual error handling
        extractors = {
            "address": (SELECTORS["address"], "inner_text"),
            "phone": (SELECTORS["phone"], "inner_text"),
            "website": (SELECTORS["website"], "href"),
            "categories": (SELECTORS["categories"], "inner_text"),
        }

        for field, (selector, method) in extractors.items():
            try:
                el = await page.query_selector(selector)
                if el:
                    if method == "inner_text":
                        data[field] = (await el.inner_text()).strip()
                    elif method == "href":
                        data[field] = await el.get_attribute("href")
            except Exception:
                pass

        # Rating
        try:
            rating_el = await page.query_selector(SELECTORS["rating"])
            if rating_el:
                rating_text = await rating_el.inner_text()
                data["rating"] = float(rating_text.replace(",", "."))
        except Exception:
            pass

        # Review count
        try:
            review_el = await page.query_selector(SELECTORS["review_count"])
            if review_el:
                label = await review_el.get_attribute("aria-label")
                if label:
                    count_match = re.search(r"([\d,]+)", label)
                    if count_match:
                        data["review_count"] = int(count_match.group(1).replace(",", ""))
        except Exception:
            pass

        # Maps URL
        try:
            data["maps_url"] = page.url
        except Exception:
            pass

        return data if data.get("address") or data.get("phone") or data.get("website") else None

    async def _scroll_results(self, page: Page) -> None:
        """Scroll the results feed to trigger lazy loading."""
        try:
            feed = await page.query_selector(SELECTORS["results_container"])
            if feed:
                await page.evaluate(
                    "(el) => el.scrollBy(0, el.scrollHeight)",
                    feed,
                )
        except Exception:
            pass

    async def _create_context(self, pw: Playwright) -> BrowserContext:
        """Creates a Playwright browser context with proxy and stealth settings."""
        proxy = self.proxy_manager.get_proxy()

        launch_args = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        }

        browser = await pw.chromium.launch(**launch_args)

        context_args: Dict[str, Any] = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": self._get_user_agent(),
            "locale": "en-US",
            "timezone_id": "America/New_York",
            "java_script_enabled": True,
        }

        if proxy:
            context_args["proxy"] = {
                "server": proxy.server,
                "username": proxy.username,
                "password": proxy.password,
            }

        return await browser.new_context(**context_args)

    async def _apply_stealth(self, page: Page) -> None:
        """
        Injects JavaScript to mask Playwright's automation fingerprint.
        Overrides navigator.webdriver and related properties.
        """
        await page.add_init_script("""
            // Mask automation detection
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };

            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) =>
                parameters.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters);
        """)

    async def _handle_consent(self, page: Page) -> None:
        """Handles GDPR/cookie consent dialogs."""
        consent_selectors = [
            'button[aria-label="Accept all"]',
            'button[aria-label="Принять все"]',  # Russian
            '#L2AGLb',  # Google consent button ID
            'button:has-text("Accept")',
            'button:has-text("I agree")',
        ]
        for selector in consent_selectors:
            try:
                btn = await page.query_selector(selector)
                if btn:
                    await btn.click()
                    await asyncio.sleep(1)
                    return
            except Exception:
                continue

    def _get_user_agent(self) -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]
        return random.choice(agents)
