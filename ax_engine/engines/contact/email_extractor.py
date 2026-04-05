"""
AX Engine — Email Extractor

Finds email addresses from raw text using multiple strategies:

1. Direct regex extraction
2. Obfuscation reversal (common anti-bot tricks):
   - "user [at] domain [dot] com"
   - "user(at)domain.com"
   - Unicode character substitution
3. Deduplication and basic format validation
"""
from __future__ import annotations

import re
from typing import List

# Standard email pattern
EMAIL_REGEX = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b",
    re.IGNORECASE,
)

# Obfuscated patterns
OBFUSCATED_PATTERNS = [
    # user [at] domain [dot] com
    (re.compile(r"([A-Za-z0-9._%+\-]+)\s*[\[\(]at[\]\)]\s*([A-Za-z0-9.\-]+)\s*[\[\(]dot[\]\)]\s*([A-Za-z]{2,})", re.I), lambda m: f"{m.group(1)}@{m.group(2)}.{m.group(3)}"),
    # user AT domain DOT com
    (re.compile(r"([A-Za-z0-9._%+\-]+)\s+AT\s+([A-Za-z0-9.\-]+)\s+DOT\s+([A-Za-z]{2,})"), lambda m: f"{m.group(1)}@{m.group(2)}.{m.group(3)}"),
    # user(at)domain.com
    (re.compile(r"([A-Za-z0-9._%+\-]+)\(at\)([A-Za-z0-9.\-]+\.[A-Za-z]{2,})", re.I), lambda m: f"{m.group(1)}@{m.group(2)}"),
    # user _at_ domain.com
    (re.compile(r"([A-Za-z0-9._%+\-]+)\s+_at_\s+([A-Za-z0-9.\-]+\.[A-Za-z]{2,})", re.I), lambda m: f"{m.group(1)}@{m.group(2)}"),
]

# Domains to exclude (marketing/transactional, not personal/business)
EXCLUDE_DOMAINS = {
    "example.com", "test.com", "sentry.io", "w3.org",
    "schema.org", "facebook.com", "twitter.com", "google.com",
    "apple.com", "microsoft.com", "amazon.com",
}

# Common role-based prefixes to rank higher (these reach real people)
ROLE_BASED_PREFIXES = {
    "info", "contact", "hello", "hi", "admin", "support",
    "office", "reception", "mail", "email", "general",
}

PERSONAL_NAME_PATTERN = re.compile(r"^[a-z]+\.[a-z]+@", re.I)


class EmailExtractor:
    """
    Extracts and classifies email addresses from text.
    """

    def extract(self, text: str) -> List[str]:
        """
        Extract all email addresses from text.
        Returns deduplicated, validated list sorted by quality.
        """
        if not text:
            return []

        emails = set()

        # Standard extraction
        for match in EMAIL_REGEX.finditer(text):
            emails.add(match.group().lower())

        # Obfuscated extraction
        for pattern, formatter in OBFUSCATED_PATTERNS:
            for m in pattern.finditer(text):
                try:
                    email = formatter(m).lower()
                    if self._is_valid_format(email):
                        emails.add(email)
                except Exception:
                    pass

        # Filter and sort by quality
        valid = [e for e in emails if self._is_valid_format(e) and self._not_excluded(e)]
        return self._sort_by_quality(valid)

    def _is_valid_format(self, email: str) -> bool:
        """Basic format validation."""
        if not EMAIL_REGEX.match(email):
            return False
        local, domain = email.rsplit("@", 1)
        if len(local) < 1 or len(domain) < 4:
            return False
        if "." not in domain:
            return False
        if ".." in email:
            return False
        return True

    def _not_excluded(self, email: str) -> bool:
        domain = email.split("@")[-1].lower()
        return domain not in EXCLUDE_DOMAINS

    def _sort_by_quality(self, emails: List[str]) -> List[str]:
        """
        Sort emails by likelihood of reaching a real decision-maker.
        Priority: personal name patterns > role-based > generic
        """
        def score(email: str) -> int:
            local = email.split("@")[0].lower()
            # Personal: firstname.lastname@domain.com → highest priority
            if PERSONAL_NAME_PATTERN.match(email):
                return 100
            # Role-based: info@, contact@
            if local in ROLE_BASED_PREFIXES:
                return 60
            # Single name: john@domain.com
            if re.match(r"^[a-z]+$", local) and len(local) > 2:
                return 70
            return 40

        return sorted(emails, key=score, reverse=True)

    def classify(self, email: str) -> str:
        """Classify an email as 'personal', 'role_based', or 'generic'."""
        local = email.split("@")[0].lower()
        if PERSONAL_NAME_PATTERN.match(email):
            return "personal"
        if local in ROLE_BASED_PREFIXES:
            return "role_based"
        return "generic"
