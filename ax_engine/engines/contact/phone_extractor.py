"""
AX Engine — Phone Number Extractor

Uses the libphonenumber library for:
  - Accurate international format parsing
  - Country-aware validation
  - E.164 format normalization
  - National format output
"""
from __future__ import annotations

import re
from typing import List, Optional

import phonenumbers
from phonenumbers import PhoneNumberMatcher, PhoneNumberFormat
import structlog

logger = structlog.get_logger(__name__)

# Country code to region map for common locations
COUNTRY_REGIONS = {
    "RU": "RU", "US": "US", "GB": "GB", "DE": "DE",
    "FR": "FR", "AE": "AE", "SA": "SA", "TR": "TR",
    "UA": "UA", "KZ": "KZ",
}


class PhoneExtractor:
    """
    Extracts and normalizes phone numbers from text.
    """

    def extract(
        self,
        text: str,
        country_hint: Optional[str] = None,
    ) -> List[str]:
        """
        Extract all phone numbers from text.

        Args:
            text: Raw text to search
            country_hint: ISO 3166-1 alpha-2 country code for context

        Returns:
            List of E.164 formatted phone numbers (e.g., "+79991234567")
        """
        if not text:
            return []

        results = set()
        region = COUNTRY_REGIONS.get(country_hint or "", None)

        # Use libphonenumber's built-in text matcher
        for match in PhoneNumberMatcher(text, region or "US"):
            try:
                number = match.number
                if phonenumbers.is_valid_number(number):
                    formatted = phonenumbers.format_number(
                        number, PhoneNumberFormat.E164
                    )
                    results.add(formatted)
            except Exception:
                continue

        # Additional pass with common Russian/CIS patterns if region is RU
        if country_hint in ("RU", "KZ", "UA"):
            cis_numbers = self._extract_cis_patterns(text)
            results.update(cis_numbers)

        return list(results)

    def _extract_cis_patterns(self, text: str) -> List[str]:
        """
        Specialized extraction for Russian/CIS phone format patterns.
        E.g.: +7 (999) 123-45-67, 8-800-123-45-67
        """
        patterns = [
            r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}",
            r"\+7\d{10}",
        ]

        results = []
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                raw = match.group().strip()
                try:
                    # Normalize to +7XXXXXXXXXX
                    digits = re.sub(r"[^\d]", "", raw)
                    if digits.startswith("8") and len(digits) == 11:
                        digits = "7" + digits[1:]
                    if len(digits) == 11 and digits.startswith("7"):
                        results.append(f"+{digits}")
                except Exception:
                    pass

        return results

    def format_national(self, e164: str, region: str = "US") -> str:
        """Convert E.164 to local national format."""
        try:
            number = phonenumbers.parse(e164, region)
            return phonenumbers.format_number(number, PhoneNumberFormat.NATIONAL)
        except Exception:
            return e164
