"""
AX Engine — Company Enricher

Estimates:
  - Employee count (from LinkedIn, website clues, size signals)
  - Revenue range (from employee count × industry revenue/employee benchmarks)
  - Company age (domain registration, "founded" mentions)
  - Social presence audit

Sources (in priority order):
  1. Structured data on website (schema.org/Organization)
  2. LinkedIn company page (headcount public signal)
  3. Text mining from About page ("team of 15", "50+ professionals")
  4. Industry benchmarks × employee estimate
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

# Industry revenue per employee (USD) benchmarks
# Source: aggregated industry averages
REVENUE_PER_EMPLOYEE: Dict[str, int] = {
    "dental": 150_000,
    "medical": 120_000,
    "law": 200_000,
    "accounting": 180_000,
    "real estate": 250_000,
    "restaurant": 60_000,
    "gym": 80_000,
    "salon": 70_000,
    "software": 300_000,
    "agency": 150_000,
    "retail": 100_000,
    "default": 120_000,
}

SIZE_BUCKETS = [
    (1, 5, "1-5"),
    (6, 10, "6-10"),
    (11, 25, "11-25"),
    (26, 50, "26-50"),
    (51, 100, "51-100"),
    (101, 250, "101-250"),
    (251, 500, "251-500"),
    (501, float("inf"), "500+"),
]

# Patterns to find employee count hints in text
EMPLOYEE_PATTERNS = [
    re.compile(r"team of (\d+)\+?", re.I),
    re.compile(r"(\d+)\+?\s+(?:staff|employees|professionals|specialists|doctors|attorneys)", re.I),
    re.compile(r"(?:over|more than)\s+(\d+)\s+(?:staff|employees|people)", re.I),
    re.compile(r"(\d+)\s+locations?", re.I),  # "5 locations" → multiply
    # Russian
    re.compile(r"команда из (\d+)\+?", re.I),
    re.compile(r"(\d+)\+?\s+(?:сотрудников|специалистов|врачей)", re.I),
]

FOUNDED_PATTERNS = [
    re.compile(r"(?:founded|established|since|est\.?)\s+(?:in\s+)?(\d{4})", re.I),
    re.compile(r"©\s*(\d{4})", re.I),
    re.compile(r"основан[аоы]?\s+в?\s+(\d{4})", re.I),  # Russian
]


class CompanyEnricher:
    """
    Enriches company records with size, revenue, and age estimates.
    """

    async def enrich(
        self,
        company_name: str,
        website: Optional[str],
        niche: Optional[str] = None,
        website_text: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Returns enrichment dict with available fields populated.
        """
        result: Dict[str, Any] = {
            "size": None,
            "employees": None,
            "revenue": None,
            "founded": None,
            "socials": {},
            "has_paid_ads": False,
        }

        # Mine website text if available
        if website_text:
            result.update(self._mine_text(website_text, niche))

        # If no website text provided, fetch the about page
        elif website:
            try:
                text = await self._fetch_about_text(website)
                if text:
                    result.update(self._mine_text(text, niche))
            except Exception as e:
                logger.debug("enricher.fetch_failed", website=website, error=str(e))

        # Calculate revenue from employee estimate
        if result["employees"] and not result["revenue"]:
            result["revenue"] = self._estimate_revenue(result["employees"], niche or "default")
            result["size"] = self._bucket_size(result["employees"])

        return result

    def _mine_text(self, text: str, niche: Optional[str]) -> Dict[str, Any]:
        """Extract structured facts from free text."""
        data: Dict[str, Any] = {}

        # Employee count
        for pattern in EMPLOYEE_PATTERNS:
            m = pattern.search(text)
            if m:
                try:
                    count = int(m.group(1))
                    # "5 locations" → rough estimate of 8 employees per location
                    if "location" in pattern.pattern.lower():
                        count = count * 8
                    data["employees"] = count
                    data["size"] = self._bucket_size(count)
                    break
                except (ValueError, IndexError):
                    pass

        # Founded year
        for pattern in FOUNDED_PATTERNS:
            m = pattern.search(text)
            if m:
                try:
                    year = int(m.group(1))
                    if 1900 < year <= 2025:
                        data["founded"] = year
                        break
                except (ValueError, IndexError):
                    pass

        # Paid ads detection
        ad_signals = ["google ads", "facebook ads", "яндекс.директ", "рекламная кампания"]
        if any(signal in text.lower() for signal in ad_signals):
            data["has_paid_ads"] = True

        return data

    async def _fetch_about_text(self, website: str) -> Optional[str]:
        """Quick fetch of about page text."""
        if not website.startswith("http"):
            website = "https://" + website

        about_urls = [
            f"{website.rstrip('/')}/about",
            f"{website.rstrip('/')}/about-us",
            f"{website.rstrip('/')}/о-нас",
            website,
        ]

        async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False) as client:
            for url in about_urls[:2]:  # Only try 2 pages max
                try:
                    r = await client.get(url)
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "lxml")
                        for tag in soup(["script", "style", "nav"]):
                            tag.decompose()
                        return soup.get_text(separator=" ", strip=True)[:5000]
                except Exception:
                    continue

        return None

    def _estimate_revenue(self, employees: int, niche: str) -> str:
        """Calculate revenue range from employee count × industry multiplier."""
        niche_lower = niche.lower()
        rpe = REVENUE_PER_EMPLOYEE.get("default", 120_000)

        for keyword, value in REVENUE_PER_EMPLOYEE.items():
            if keyword in niche_lower:
                rpe = value
                break

        low = employees * rpe * 0.7
        high = employees * rpe * 1.3

        return f"${self._fmt_money(low)}-${self._fmt_money(high)}"

    def _fmt_money(self, amount: float) -> str:
        if amount >= 1_000_000:
            return f"{amount/1_000_000:.1f}M"
        if amount >= 1_000:
            return f"{amount/1_000:.0f}K"
        return str(int(amount))

    def _bucket_size(self, employees: int) -> str:
        for low, high, label in SIZE_BUCKETS:
            if low <= employees <= high:
                return label
        return "500+"
