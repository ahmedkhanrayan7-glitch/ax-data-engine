"""
AX Engine — Social Profile Finder

Finds social media and professional profiles from:
  - Website links
  - Raw text (username patterns)

Platforms tracked:
  - LinkedIn (most valuable for B2B)
  - Instagram
  - Facebook
  - Twitter/X
  - YouTube
  - VKontakte (Russia)
  - WhatsApp Business
  - Telegram
"""
from __future__ import annotations

import re
from typing import Dict, List
from urllib.parse import urlparse

SOCIAL_PATTERNS: Dict[str, re.Pattern] = {
    "linkedin": re.compile(r"linkedin\.com/(?:in|company)/[A-Za-z0-9\-_%.]+", re.I),
    "instagram": re.compile(r"instagram\.com/[A-Za-z0-9_.]+", re.I),
    "facebook": re.compile(r"facebook\.com/(?:pages/)?[A-Za-z0-9_.+\-]+", re.I),
    "twitter": re.compile(r"(?:twitter|x)\.com/[A-Za-z0-9_]+", re.I),
    "youtube": re.compile(r"youtube\.com/(?:channel|c|@)[A-Za-z0-9_\-]+", re.I),
    "vk": re.compile(r"vk\.com/[A-Za-z0-9_.]+", re.I),
    "telegram": re.compile(r"t\.me/[A-Za-z0-9_]+", re.I),
    "whatsapp": re.compile(r"wa\.me/\d+", re.I),
    "tiktok": re.compile(r"tiktok\.com/@[A-Za-z0-9_.]+", re.I),
}

# Noise patterns to exclude
EXCLUDE_PATTERNS = [
    re.compile(r"share\.(facebook|twitter|linkedin)\.com"),
    re.compile(r"platform\.linkedin\.com"),
    re.compile(r"connect\.(facebook)\.net"),
]


class SocialProfileFinder:
    def find(self, text: str, links: List[str]) -> List[str]:
        """
        Find social profile URLs from text and link list.
        Returns deduplicated, normalized URLs.
        """
        found = set()

        # Search in links
        for link in links:
            if self._is_noise(link):
                continue
            for platform, pattern in SOCIAL_PATTERNS.items():
                if pattern.search(link):
                    normalized = self._normalize(link, platform)
                    if normalized:
                        found.add(normalized)

        # Search in raw text
        combined_text = " ".join(links) + " " + text
        for platform, pattern in SOCIAL_PATTERNS.items():
            for match in pattern.finditer(combined_text):
                url = "https://" + match.group().lstrip("/")
                if not self._is_noise(url):
                    normalized = self._normalize(url, platform)
                    if normalized:
                        found.add(normalized)

        return list(found)

    def _normalize(self, url: str, platform: str) -> str:
        """Clean up URL — remove tracking params, ensure https."""
        if not url.startswith("http"):
            url = "https://" + url

        parsed = urlparse(url)
        # Rebuild without query params or fragments
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        return clean

    def _is_noise(self, url: str) -> bool:
        return any(p.search(url) for p in EXCLUDE_PATTERNS)
