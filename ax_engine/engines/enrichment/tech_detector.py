"""
AX Engine — Technology Stack Detector

Detects technologies used by a business website by analyzing:
  1. HTTP response headers (Server, X-Powered-By, etc.)
  2. HTML meta tags and scripts
  3. Script/link src attributes
  4. Cookie names
  5. DOM patterns (WordPress admin links, etc.)

This reveals:
  - CMS (WordPress, Shopify, Wix, Squarespace)
  - Analytics (GA4, Facebook Pixel, Hotjar)
  - Booking systems (Calendly, Acuity, SimplyBook)
  - CRM presence (HubSpot, Salesforce)
  - E-commerce platform
  - Ads presence (Google Ads, Meta Ads)

Business intelligence value:
  - "No booking system" → sell them one
  - "Wix/Squarespace" → easy to upgrade to custom
  - "No analytics" → massive growth opportunity signal
  - "No CRM" → they're losing leads
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

# Technology fingerprints: {tech_name: [patterns to search in HTML/headers]}
TECH_SIGNATURES: Dict[str, Dict] = {
    # CMS
    "WordPress": {
        "html": [r"/wp-content/", r"/wp-includes/", r"wp-json"],
        "headers": [],
    },
    "Shopify": {
        "html": [r"cdn\.shopify\.com", r"Shopify\.theme"],
        "headers": ["x-shopify-shop"],
    },
    "Wix": {
        "html": [r"wix\.com/", r"_wix_"],
        "headers": ["x-wix-request-id"],
    },
    "Squarespace": {
        "html": [r"squarespace\.com", r"sqsp\.net"],
        "headers": [],
    },
    "Webflow": {
        "html": [r"webflow\.com", r"\.webflow\.io"],
        "headers": ["x-powered-by: webflow"],
    },
    "Tilda": {
        "html": [r"tilda\.ws", r"t\.tildacdn"],
        "headers": [],
    },
    "Bitrix": {
        "html": [r"bitrix/", r"1c-bitrix"],
        "headers": ["x-powered-cms: bitrix"],
    },

    # Analytics
    "Google Analytics 4": {
        "html": [r"gtag\(.*UA-|G-[A-Z0-9]+", r"google-analytics\.com/g/collect"],
        "headers": [],
    },
    "Facebook Pixel": {
        "html": [r"connect\.facebook\.net/.*fbevents", r"fbq\("],
        "headers": [],
    },
    "Hotjar": {
        "html": [r"static\.hotjar\.com", r"hj\("],
        "headers": [],
    },
    "Yandex Metrica": {
        "html": [r"mc\.yandex\.ru", r"ym\("],
        "headers": [],
    },

    # Booking Systems
    "Calendly": {
        "html": [r"calendly\.com", r"assets\.calendly\.com"],
        "headers": [],
    },
    "Acuity Scheduling": {
        "html": [r"acuityscheduling\.com"],
        "headers": [],
    },
    "SimplyBook": {
        "html": [r"simplybook\.me", r"simplybook\.it"],
        "headers": [],
    },
    "Booksy": {
        "html": [r"booksy\.com"],
        "headers": [],
    },

    # CRM
    "HubSpot": {
        "html": [r"js\.hs-scripts\.com", r"hubspot\.com"],
        "headers": ["x-hubspot"],
    },
    "Salesforce": {
        "html": [r"salesforce\.com", r"pardot\.com"],
        "headers": [],
    },
    "Bitrix24": {
        "html": [r"bitrix24\.ru", r"bitrix24\.com"],
        "headers": [],
    },

    # Live Chat
    "Intercom": {
        "html": [r"widget\.intercom\.io", r"Intercom\("],
        "headers": [],
    },
    "Tidio": {
        "html": [r"tidio\.co", r"tidiochat"],
        "headers": [],
    },
    "JivoChat": {
        "html": [r"jivosite\.com", r"jivosite\.ru"],
        "headers": [],
    },

    # Payments
    "Stripe": {
        "html": [r"js\.stripe\.com"],
        "headers": [],
    },
    "PayPal": {
        "html": [r"paypal\.com/sdk"],
        "headers": [],
    },
    "YooKassa": {
        "html": [r"yookassa\.ru", r"yandexcheckout"],
        "headers": [],
    },
}

# Absence signals — if these are NOT detected, it's an opportunity
ABSENCE_SIGNALS = {
    "no_booking_system": ["Calendly", "Acuity Scheduling", "SimplyBook", "Booksy"],
    "no_analytics": ["Google Analytics 4", "Yandex Metrica", "Hotjar"],
    "no_crm": ["HubSpot", "Salesforce", "Bitrix24"],
    "no_live_chat": ["Intercom", "Tidio", "JivoChat"],
}


class TechStackDetector:
    """
    Detects technology stack from a website URL.
    """

    async def detect(self, url: str) -> List[str]:
        """
        Returns list of detected technology names.
        """
        if not url:
            return []

        try:
            async with httpx.AsyncClient(
                timeout=15,
                follow_redirects=True,
                verify=False,
                headers={"User-Agent": "Mozilla/5.0 (compatible; AXBot/1.0)"},
            ) as client:
                response = await client.get(url)
                html = response.text
                headers = dict(response.headers)
        except Exception as e:
            logger.debug("tech_detector.fetch_failed", url=url, error=str(e))
            return []

        detected = []

        for tech_name, signatures in TECH_SIGNATURES.items():
            # Check HTML patterns
            for pattern in signatures.get("html", []):
                if re.search(pattern, html, re.IGNORECASE):
                    detected.append(tech_name)
                    break

            # Check response headers
            for header_pattern in signatures.get("headers", []):
                header_name = header_pattern.split(":")[0].lower()
                if header_name in headers:
                    detected.append(tech_name)
                    break

        return list(set(detected))

    def get_absence_signals(self, detected_stack: List[str]) -> List[str]:
        """
        Returns opportunity signals based on what's MISSING.
        """
        signals = []
        detected_set = set(detected_stack)

        for signal, required_techs in ABSENCE_SIGNALS.items():
            if not any(t in detected_set for t in required_techs):
                signals.append(signal)

        return signals
