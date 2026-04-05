"""
AX Engine — Website Content Parser

Transforms raw HTML pages into structured data:
  - Raw text for NLP processing
  - All links (for social discovery)
  - Structured data (JSON-LD, microdata, OpenGraph)
  - Contact info snippets
  - Meta information
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ax_engine.engines.website.crawler import CrawlResult


class WebsiteParser:
    """
    Parses a dict of {url: CrawlResult} into a unified data structure.
    Aggregates all pages into a single searchable text corpus.
    """

    def parse(self, crawl_results: Dict[str, CrawlResult]) -> Dict[str, Any]:
        if not crawl_results:
            return {}

        aggregated_text = []
        all_links = []
        all_emails = []
        all_phones = []
        structured_data = []
        meta_info = {}

        for url, result in crawl_results.items():
            if not result or not result.html:
                continue

            soup = BeautifulSoup(result.html, "lxml")
            page_data = self._parse_page(soup, url)

            aggregated_text.append(page_data["text"])
            all_links.extend(page_data["links"])
            all_emails.extend(page_data["emails"])
            all_phones.extend(page_data["phones"])
            structured_data.extend(page_data["structured_data"])

            # Use homepage meta info preferentially
            if not meta_info or url.count("/") <= 3:
                meta_info = page_data["meta"]

        return {
            "raw_text": "\n\n".join(aggregated_text),
            "links": list(set(all_links)),
            "emails": list(set(all_emails)),
            "phones": list(set(all_phones)),
            "structured_data": structured_data,
            "meta": meta_info,
            "page_count": len(crawl_results),
        }

    def _parse_page(self, soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
        # Remove noise elements
        for tag in soup(["script", "style", "nav", "footer", "iframe", "noscript"]):
            tag.decompose()

        text = self._extract_text(soup)
        links = self._extract_links(soup, base_url)
        emails = self._extract_emails_from_html(soup)
        phones = self._extract_phones_from_html(soup)
        structured = self._extract_structured_data(soup)
        meta = self._extract_meta(soup)

        return {
            "text": text,
            "links": links,
            "emails": emails,
            "phones": phones,
            "structured_data": structured,
            "meta": meta,
        }

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """
        Extracts text with semantic prioritization.
        H1/H2 text gets weighted higher (appears first) since
        names/titles are often in headings.
        """
        priority_text = []

        # Headings first — most likely to contain names/titles
        for tag in soup.find_all(["h1", "h2", "h3"]):
            t = tag.get_text(separator=" ", strip=True)
            if t:
                priority_text.append(t)

        # Then paragraph text
        body_text = soup.get_text(separator=" ", strip=True)
        body_text = re.sub(r"\s+", " ", body_text)

        return " ".join(priority_text) + " " + body_text

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links = []
        domain = urlparse(base_url).netloc

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("mailto:") or href.startswith("tel:"):
                links.append(href)
            elif href.startswith("http"):
                links.append(href)
            elif href.startswith("/"):
                links.append(urljoin(base_url, href))

        return links[:200]  # Cap to avoid bloat

    def _extract_emails_from_html(self, soup: BeautifulSoup) -> List[str]:
        """Extract emails from mailto: links and visible text."""
        emails = []

        # From mailto links
        for a in soup.find_all("a", href=re.compile(r"^mailto:", re.I)):
            email = a["href"].replace("mailto:", "").split("?")[0].strip().lower()
            if "@" in email:
                emails.append(email)

        # From text (obfuscated or visible)
        text = soup.get_text()
        pattern = r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
        emails.extend(re.findall(pattern, text))

        return list(set(emails))

    def _extract_phones_from_html(self, soup: BeautifulSoup) -> List[str]:
        """Extract phone numbers from tel: links and text."""
        phones = []

        for a in soup.find_all("a", href=re.compile(r"^tel:", re.I)):
            phone = a["href"].replace("tel:", "").strip()
            phones.append(phone)

        return phones

    def _extract_structured_data(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract JSON-LD structured data — gold mine for:
        - Organization schema (address, phone, email)
        - Person schema (name, role)
        - LocalBusiness schema
        """
        data = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                parsed = json.loads(script.string or "")
                if isinstance(parsed, list):
                    data.extend(parsed)
                else:
                    data.append(parsed)
            except (json.JSONDecodeError, TypeError):
                pass

        return data

    def _extract_meta(self, soup: BeautifulSoup) -> Dict[str, str]:
        meta = {}

        title = soup.find("title")
        if title:
            meta["title"] = title.get_text(strip=True)

        for m in soup.find_all("meta"):
            name = m.get("name") or m.get("property", "")
            content = m.get("content", "")
            if name and content:
                meta[name] = content

        return meta
