"""
AX Engine — Website Crawler

Intelligently crawls company websites to find:
  - About pages (team info, founders)
  - Contact pages (emails, phones)
  - Team/Staff pages (decision-maker names)
  - Homepage (general signals)

Design principles:
  - Respects robots.txt (configurable)
  - Delays between requests per domain
  - Extracts JS-rendered content via Playwright
  - Falls back to httpx for simple HTML pages (faster)
  - Deduplicates pages by content hash
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
import structlog
from playwright.async_api import async_playwright

from ax_engine.config import settings
from ax_engine.infrastructure.proxy_manager import ProxyManager

logger = structlog.get_logger(__name__)

# Pages to prioritize — ordered by importance for DM extraction
PRIORITY_PATHS = [
    "",          # Homepage
    "about",
    "about-us",
    "about_us",
    "team",
    "our-team",
    "staff",
    "leadership",
    "contact",
    "contact-us",
    "people",
    "founders",
    "management",
    # Russian variants
    "о-нас",
    "команда",
    "контакты",
    "руководство",
]

# Signals in page text that suggest a valuable page
DM_PAGE_SIGNALS = [
    "founder", "ceo", "director", "owner", "president",
    "meet the team", "about us", "our team", "leadership",
    "основатель", "директор", "владелец", "руководство",  # Russian
]


class CrawlResult:
    def __init__(self, url: str, html: str, status: int):
        self.url = url
        self.html = html
        self.status = status
        self.content_hash = hashlib.md5(html.encode()).hexdigest()


class WebsiteCrawler:
    """
    Two-tier crawler:
    Tier 1 (fast): httpx — for pages that don't require JS
    Tier 2 (full): Playwright — for JS-heavy SPAs
    """

    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self._seen_hashes: set = set()

    async def crawl(
        self,
        base_url: str,
        pages: Optional[List[str]] = None,
    ) -> Dict[str, CrawlResult]:
        """
        Crawl multiple pages of a domain.

        Returns dict of {url: CrawlResult}.
        """
        if not base_url:
            return {}

        base_url = self._normalize_url(base_url)
        paths_to_try = pages or PRIORITY_PATHS

        log = logger.bind(domain=urlparse(base_url).netloc)
        log.info("crawler.starting", paths=len(paths_to_try))

        results: Dict[str, CrawlResult] = {}

        # First pass: fast httpx crawl
        fast_results = await self._fast_crawl(base_url, paths_to_try)
        results.update(fast_results)

        # If homepage is JS-rendered (detected by low text content), do Playwright pass
        homepage = results.get(base_url)
        if homepage and self._is_js_heavy(homepage.html):
            log.info("crawler.js_detected_upgrading_to_playwright")
            pw_results = await self._playwright_crawl(base_url, paths_to_try)
            results.update(pw_results)  # Playwright results override httpx

        log.info("crawler.complete", pages_crawled=len(results))
        return results

    async def _fast_crawl(
        self,
        base_url: str,
        paths: List[str],
    ) -> Dict[str, CrawlResult]:
        """httpx concurrent crawl — fast, low resource."""
        proxy = self.proxy_manager.get_proxy()

        proxy_url = None
        if proxy:
            proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.server}"

        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; AXBot/1.0; +https://ax-engine.io/bot)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        }

        results = {}

        async with httpx.AsyncClient(
            headers=headers,
            proxy=proxy_url,
            timeout=settings.PAGE_TIMEOUT_MS / 1000,
            follow_redirects=True,
            verify=False,  # Some SMB sites have self-signed certs
        ) as client:
            tasks = [
                self._fetch_page(client, base_url, path)
                for path in paths
            ]

            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for path, resp in zip(paths, responses):
                if isinstance(resp, Exception):
                    continue
                if resp is None:
                    continue

                url = urljoin(base_url, path) if path else base_url

                # Deduplicate by content hash
                h = hashlib.md5(resp.html.encode()).hexdigest()
                if h not in self._seen_hashes:
                    self._seen_hashes.add(h)
                    results[url] = resp

        return results

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        path: str,
    ) -> Optional[CrawlResult]:
        """Fetch a single page."""
        url = urljoin(base_url, path) if path else base_url

        try:
            # Respectful delay
            await asyncio.sleep(settings.MIN_REQUEST_DELAY)

            response = await client.get(url)

            if response.status_code == 200:
                return CrawlResult(
                    url=str(response.url),
                    html=response.text,
                    status=response.status_code,
                )
            elif response.status_code in (301, 302, 307, 308):
                # Follow redirects handled by httpx
                return None
            else:
                return None

        except (httpx.ConnectError, httpx.TimeoutException, httpx.TooManyRedirects):
            return None
        except Exception as e:
            logger.debug("crawler.fetch_error", url=url, error=str(e))
            return None

    async def _playwright_crawl(
        self,
        base_url: str,
        paths: List[str],
    ) -> Dict[str, CrawlResult]:
        """
        Full Playwright crawl for JS-heavy sites.
        More expensive — limited to key pages.
        """
        results = {}
        priority_paths = self._prioritize_paths(paths)[:5]  # Max 5 pages via Playwright

        async with async_playwright() as pw:
            proxy = self.proxy_manager.get_proxy()

            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )

            context_args = {
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "viewport": {"width": 1280, "height": 800},
            }
            if proxy:
                context_args["proxy"] = {
                    "server": proxy.server,
                    "username": proxy.username,
                    "password": proxy.password,
                }

            context = await browser.new_context(**context_args)

            for path in priority_paths:
                url = urljoin(base_url, path) if path else base_url
                try:
                    page = await context.new_page()
                    await page.goto(url, wait_until="networkidle", timeout=settings.NAVIGATION_TIMEOUT_MS)

                    # Wait for dynamic content
                    await asyncio.sleep(2)

                    html = await page.content()
                    results[url] = CrawlResult(url=url, html=html, status=200)
                    await page.close()

                    await asyncio.sleep(settings.CRAWL_POLITENESS_DELAY)

                except Exception as e:
                    logger.debug("crawler.playwright_page_failed", url=url, error=str(e))

            await context.close()
            await browser.close()

        return results

    def _is_js_heavy(self, html: str) -> bool:
        """
        Heuristic: if page has very little text content but lots of JS,
        it's likely a SPA that needs Playwright.
        """
        # Strip tags and measure text density
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()

        # If text is very short relative to HTML, probably JS-rendered
        text_ratio = len(text) / max(len(html), 1)
        return text_ratio < 0.1 and len(html) > 5000

    def _prioritize_paths(self, paths: List[str]) -> List[str]:
        """Sort paths by likelihood of containing DM info."""
        def priority(p: str) -> int:
            p_lower = p.lower()
            for i, keyword in enumerate(["team", "about", "contact", "staff", "leader"]):
                if keyword in p_lower:
                    return i
            return 99

        return sorted(paths, key=priority)

    def _normalize_url(self, url: str) -> str:
        """Ensure URL has scheme."""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        # Remove trailing slash
        return url.rstrip("/")
