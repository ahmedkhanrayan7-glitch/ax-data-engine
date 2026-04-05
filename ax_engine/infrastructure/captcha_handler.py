"""
AX Engine — CAPTCHA Handler

Strategy:
  1. Avoid triggering CAPTCHAs first (stealth mode, delays, proxy rotation)
  2. If triggered, use external solving service (2Captcha, AntiCaptcha, CapSolver)
  3. Support reCAPTCHA v2, v3, hCaptcha, and image CAPTCHAs

Detection:
  - Monitor for reCAPTCHA iframes
  - Monitor for hCaptcha elements
  - Monitor for Cloudflare challenge pages

Solving flow:
  1. Extract site key from page
  2. Submit to solver API
  3. Poll for solution (async, 10-120s)
  4. Inject token into form
  5. Retry original action

Note: CAPTCHA solving is used reactively only, not proactively.
Primary defense is stealth configuration.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx
import structlog
from playwright.async_api import Page

from ax_engine.config import settings

logger = structlog.get_logger(__name__)

CLOUDFLARE_PATTERNS = [
    "Checking if the site connection is secure",
    "DDoS protection by Cloudflare",
    "Please Wait... | Cloudflare",
    "Just a moment...",
]

RECAPTCHA_SELECTORS = [
    'iframe[src*="recaptcha"]',
    '#g-recaptcha',
    '.g-recaptcha',
]

HCAPTCHA_SELECTORS = [
    'iframe[src*="hcaptcha"]',
    '.h-captcha',
]


class CaptchaHandler:
    """
    Detects and solves CAPTCHAs encountered during scraping.
    """

    def __init__(self):
        self.api_key = settings.CAPTCHA_API_KEY
        self.service = settings.CAPTCHA_SERVICE
        self.enabled = bool(self.api_key)

    async def check_and_solve(self, page: Page) -> bool:
        """
        Check if current page has a CAPTCHA and attempt to solve it.
        Returns True if CAPTCHA was solved or not present.
        Returns False if unsolvable.
        """
        content = await page.content()

        # Cloudflare detection
        if self._is_cloudflare(content):
            logger.warning("captcha.cloudflare_detected")
            return await self._wait_for_cloudflare(page)

        # reCAPTCHA detection
        for selector in RECAPTCHA_SELECTORS:
            if await page.query_selector(selector):
                logger.info("captcha.recaptcha_detected")
                if not self.enabled:
                    logger.warning("captcha.no_api_key_configured")
                    return False
                return await self._solve_recaptcha(page)

        # hCaptcha detection
        for selector in HCAPTCHA_SELECTORS:
            if await page.query_selector(selector):
                logger.info("captcha.hcaptcha_detected")
                if not self.enabled:
                    return False
                return await self._solve_hcaptcha(page)

        return True  # No CAPTCHA found

    async def _wait_for_cloudflare(self, page: Page, max_wait: int = 15) -> bool:
        """
        Cloudflare challenges often auto-resolve within 5-10 seconds.
        Wait and check if challenge passes.
        """
        for _ in range(max_wait):
            await asyncio.sleep(1)
            content = await page.content()
            if not self._is_cloudflare(content):
                logger.info("captcha.cloudflare_resolved")
                return True
        logger.warning("captcha.cloudflare_timeout")
        return False

    async def _solve_recaptcha(self, page: Page) -> bool:
        """Solve reCAPTCHA v2 via external service."""
        try:
            # Extract site key
            site_key = await self._extract_recaptcha_key(page)
            if not site_key:
                return False

            url = page.url

            # Submit to solver
            token = await self._submit_to_solver(
                captcha_type="recaptcha_v2",
                site_key=site_key,
                page_url=url,
            )

            if not token:
                return False

            # Inject token into page
            await page.evaluate(f"""
                document.getElementById('g-recaptcha-response').innerHTML = '{token}';
                if (typeof ___grecaptcha_cfg !== 'undefined') {{
                    Object.entries(___grecaptcha_cfg.clients).forEach(([key, client]) => {{
                        if (client && client.callback) client.callback('{token}');
                    }});
                }}
            """)

            return True

        except Exception as e:
            logger.error("captcha.solve_failed", error=str(e))
            return False

    async def _solve_hcaptcha(self, page: Page) -> bool:
        """Solve hCaptcha via external service."""
        try:
            site_key_el = await page.query_selector('[data-sitekey]')
            if not site_key_el:
                return False

            site_key = await site_key_el.get_attribute("data-sitekey")
            if not site_key:
                return False

            token = await self._submit_to_solver(
                captcha_type="hcaptcha",
                site_key=site_key,
                page_url=page.url,
            )

            if not token:
                return False

            await page.evaluate(f"""
                document.querySelector('[name="h-captcha-response"]').value = '{token}';
                document.querySelector('[name="g-recaptcha-response"]').value = '{token}';
            """)

            return True

        except Exception as e:
            logger.error("captcha.hcaptcha_failed", error=str(e))
            return False

    async def _submit_to_solver(
        self,
        captcha_type: str,
        site_key: str,
        page_url: str,
    ) -> Optional[str]:
        """
        Submit CAPTCHA to solving service and wait for solution.
        Supports 2captcha API format (most compatible).
        """
        if self.service == "2captcha":
            return await self._solve_2captcha(captcha_type, site_key, page_url)
        return None

    async def _solve_2captcha(
        self,
        captcha_type: str,
        site_key: str,
        page_url: str,
    ) -> Optional[str]:
        """2Captcha API integration."""
        method = "userrecaptcha" if "recaptcha" in captcha_type else "hcaptcha"

        async with httpx.AsyncClient(timeout=settings.CAPTCHA_TIMEOUT) as client:
            # Step 1: Submit
            submit_resp = await client.post(
                "https://2captcha.com/in.php",
                data={
                    "key": self.api_key,
                    "method": method,
                    "googlekey": site_key,
                    "pageurl": page_url,
                    "json": 1,
                },
            )
            result = submit_resp.json()
            if result.get("status") != 1:
                logger.warning("captcha.2captcha_submit_failed", response=result)
                return None

            task_id = result["request"]

            # Step 2: Poll for result
            for _ in range(settings.CAPTCHA_TIMEOUT // 5):
                await asyncio.sleep(5)
                poll_resp = await client.get(
                    "https://2captcha.com/res.php",
                    params={"key": self.api_key, "action": "get", "id": task_id, "json": 1},
                )
                poll_result = poll_resp.json()

                if poll_result.get("status") == 1:
                    return poll_result["request"]
                elif poll_result.get("request") != "CAPCHA_NOT_READY":
                    logger.warning("captcha.2captcha_error", response=poll_result)
                    return None

        return None

    async def _extract_recaptcha_key(self, page: Page) -> Optional[str]:
        """Extract reCAPTCHA site key from page."""
        selectors = [
            '[data-sitekey]',
            '.g-recaptcha[data-sitekey]',
        ]
        for selector in selectors:
            el = await page.query_selector(selector)
            if el:
                key = await el.get_attribute("data-sitekey")
                if key:
                    return key

        # Try to find in page source
        content = await page.content()
        import re
        m = re.search(r"data-sitekey=['\"]([^'\"]+)['\"]", content)
        if m:
            return m.group(1)

        return None

    def _is_cloudflare(self, content: str) -> bool:
        return any(pattern in content for pattern in CLOUDFLARE_PATTERNS)
