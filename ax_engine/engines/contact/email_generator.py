"""
AX Engine — Email Pattern Generator

When no email is found directly, generate likely email addresses
using common corporate naming conventions, then validate via SMTP.

Patterns (ordered by prevalence in B2B):
  1. firstname.lastname@domain.com         (most common ~35%)
  2. firstname@domain.com                  (~20%)
  3. flastname@domain.com                  (~15%)
  4. firstnamelastname@domain.com          (~8%)
  5. f.lastname@domain.com                 (~8%)
  6. lastname@domain.com                   (~5%)
  7. firstname_lastname@domain.com         (~4%)
  8. info@domain.com / contact@domain.com  (role-based fallback)

Source: Analysis of 10M+ B2B email patterns.
"""
from __future__ import annotations

import re
from typing import List

import structlog

from ax_engine.api.models.responses import DecisionMaker

logger = structlog.get_logger(__name__)


def _slugify_name(name: str) -> str:
    """Convert a name to ASCII-safe lowercase slug."""
    # Handle common transliteration for Russian names
    ru_map = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
        "е": "e", "ё": "yo", "ж": "zh", "з": "z", "и": "i",
        "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
        "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
        "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch",
        "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
        "э": "e", "ю": "yu", "я": "ya",
    }
    result = []
    for char in name.lower():
        result.append(ru_map.get(char, char))
    slug = "".join(result)
    # Keep only alphanumeric and common separators
    slug = re.sub(r"[^a-z0-9]", "", slug)
    return slug


class EmailPatternGenerator:
    """
    Generates candidate email addresses for decision-makers.
    """

    PATTERNS = [
        "firstname.lastname",
        "firstname",
        "flastname",            # f = first initial
        "firstnamelastname",
        "f.lastname",
        "lastname",
        "firstname_lastname",
        "lastname.firstname",
        "firstnamelast",        # firstname + first 4 chars of lastname
    ]

    ROLE_BASED = ["info", "contact", "hello", "office", "reception"]

    async def generate(
        self,
        domain: str,
        decision_makers: List[DecisionMaker],
    ) -> List[str]:
        """
        Generate candidate emails for each decision maker.
        Returns combined list across all patterns and DMs.
        """
        generated = []

        for dm in decision_makers[:3]:  # Max 3 DMs per domain to limit validation load
            name_parts = self._split_name(dm.name)
            if not name_parts:
                continue

            first = name_parts.get("first", "")
            last = name_parts.get("last", "")

            if not first:
                continue

            # Generate pattern-based emails
            for pattern in self.PATTERNS:
                email = self._apply_pattern(pattern, first, last, domain)
                if email:
                    generated.append(email)

        # Always include role-based fallbacks
        for prefix in self.ROLE_BASED:
            generated.append(f"{prefix}@{domain}")

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for email in generated:
            if email not in seen:
                seen.add(email)
                unique.append(email)

        return unique

    def _split_name(self, full_name: str) -> dict:
        """
        Split a full name into first/last components.
        Handles:
          - "Dr. John Smith" → first=john, last=smith
          - "John" → first=john, last=""
          - "John Michael Smith" → first=john, last=smith (middle ignored)
        """
        # Remove titles
        clean = re.sub(r"^(dr|mr|mrs|ms|prof|др|г-н|г-жа)\.?\s+", "", full_name, flags=re.IGNORECASE)
        parts = clean.split()

        if not parts:
            return {}

        first = _slugify_name(parts[0])
        last = _slugify_name(parts[-1]) if len(parts) > 1 else ""

        return {"first": first, "last": last}

    def _apply_pattern(self, pattern: str, first: str, last: str, domain: str) -> str:
        """Apply a naming pattern to generate an email."""
        if not first:
            return ""

        local = None

        if pattern == "firstname.lastname" and last:
            local = f"{first}.{last}"
        elif pattern == "firstname":
            local = first
        elif pattern == "flastname" and last:
            local = f"{first[0]}{last}"
        elif pattern == "firstnamelastname" and last:
            local = f"{first}{last}"
        elif pattern == "f.lastname" and last:
            local = f"{first[0]}.{last}"
        elif pattern == "lastname" and last:
            local = last
        elif pattern == "firstname_lastname" and last:
            local = f"{first}_{last}"
        elif pattern == "lastname.firstname" and last:
            local = f"{last}.{first}"
        elif pattern == "firstnamelast" and last:
            local = f"{first}{last[:4]}"

        if not local or len(local) < 2:
            return ""

        return f"{local}@{domain}"
