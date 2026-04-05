"""
AX Engine — Email Validator

Validation pipeline (no actual emails sent):

STEP 1: Syntax check (regex)
STEP 2: Disposable email domain check
STEP 3: MX record lookup (does domain accept email?)
STEP 4: SMTP handshake simulation
  - Connect to mail server
  - EHLO → MAIL FROM → RCPT TO
  - Observe response codes WITHOUT sending DATA
  - 250 = valid, 550 = invalid, 421/450 = catch-all or greylisted
STEP 5: Catch-all detection
  - Test with a random address on same domain
  - If random address also returns 250, domain is catch-all

Status codes:
  "valid"     — SMTP confirmed deliverable
  "catch_all" — Domain accepts all emails (can't confirm individual)
  "invalid"   — SMTP rejected (550 User Unknown)
  "risky"     — Syntax ok, MX found, but SMTP check failed/inconclusive
  "unknown"   — Could not complete check (timeout, firewall, etc.)
"""
from __future__ import annotations

import asyncio
import random
import re
import smtplib
import socket
import string
from functools import lru_cache
from typing import Dict, List, Optional

import dns.resolver
import structlog

from ax_engine.config import settings

logger = structlog.get_logger(__name__)

# Cache MX lookups to avoid repeated DNS queries
_mx_cache: Dict[str, Optional[str]] = {}

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


class EmailValidator:
    """
    Validates email addresses via SMTP simulation.
    Uses async execution for bulk validation.
    """

    def __init__(self):
        self._catch_all_cache: Dict[str, bool] = {}

    async def validate_bulk(self, emails: List[str]) -> Dict[str, str]:
        """
        Validate multiple emails concurrently.
        Returns {email: status} dict.
        """
        if not emails:
            return {}

        # Limit concurrency to avoid SMTP server bans
        semaphore = asyncio.Semaphore(5)

        async def validate_one(email: str) -> tuple:
            async with semaphore:
                status = await self.validate(email)
                return email, status

        tasks = [validate_one(email) for email in emails[:20]]  # Max 20 per batch
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return {
            email: status
            for email, status in results
            if not isinstance((email, status), Exception)
        }

    async def validate(self, email: str) -> str:
        """Full validation pipeline for a single email."""
        email = email.lower().strip()

        # STEP 1: Syntax check
        if not EMAIL_REGEX.match(email):
            return "invalid"

        domain = email.split("@")[-1]

        # STEP 2: Disposable domain check
        if self._is_disposable(domain):
            return "invalid"

        # STEP 3: MX record lookup
        mx_host = await self._get_mx(domain)
        if not mx_host:
            return "invalid"

        # STEP 4 & 5: SMTP simulation
        loop = asyncio.get_event_loop()
        try:
            status = await asyncio.wait_for(
                loop.run_in_executor(None, self._smtp_check, email, mx_host, domain),
                timeout=settings.SMTP_VERIFY_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return "unknown"
        except Exception:
            return "unknown"

        return status

    def _smtp_check(self, email: str, mx_host: str, domain: str) -> str:
        """
        SMTP handshake simulation.
        Connects to MX server and probes RCPT TO response.
        Does NOT send any email — disconnects after RCPT TO.
        """
        try:
            with smtplib.SMTP(timeout=settings.SMTP_VERIFY_TIMEOUT) as smtp:
                smtp.connect(mx_host, 25)
                smtp.ehlo("verify.ax-engine.io")
                smtp.mail("verify@ax-engine.io")

                code, _ = smtp.rcpt(email)

                # Check catch-all before returning
                if code == 250:
                    is_catch_all = self._check_catch_all(smtp, domain)
                    if is_catch_all:
                        return "catch_all"
                    return "valid"
                elif code in (550, 551, 553):
                    return "invalid"
                elif code in (421, 450, 451, 452):
                    return "risky"
                else:
                    return "unknown"

        except smtplib.SMTPConnectError:
            return "unknown"
        except smtplib.SMTPServerDisconnected:
            return "unknown"
        except socket.gaierror:
            return "unknown"
        except Exception as e:
            logger.debug("smtp_check.error", email=email, error=str(e))
            return "unknown"

    def _check_catch_all(self, smtp: smtplib.SMTP, domain: str) -> bool:
        """
        Test if domain is catch-all by sending a random address.
        If random@domain.com returns 250, it's catch-all.
        """
        if domain in self._catch_all_cache:
            return self._catch_all_cache[domain]

        random_local = "".join(random.choices(string.ascii_lowercase, k=16))
        test_email = f"{random_local}@{domain}"

        try:
            code, _ = smtp.rcpt(test_email)
            is_catch_all = code == 250
            self._catch_all_cache[domain] = is_catch_all
            return is_catch_all
        except Exception:
            return False

    async def _get_mx(self, domain: str) -> Optional[str]:
        """Get the primary MX record for a domain."""
        if domain in _mx_cache:
            return _mx_cache[domain]

        loop = asyncio.get_event_loop()
        mx = await loop.run_in_executor(None, self._lookup_mx, domain)
        _mx_cache[domain] = mx
        return mx

    def _lookup_mx(self, domain: str) -> Optional[str]:
        """Synchronous DNS MX lookup."""
        try:
            records = dns.resolver.resolve(domain, "MX", lifetime=5)
            if records:
                # Get highest priority (lowest preference number)
                mx = min(records, key=lambda r: r.preference)
                return str(mx.exchange).rstrip(".")
        except Exception:
            pass

        # Fallback: try A record (some domains handle mail on same host)
        try:
            dns.resolver.resolve(domain, "A", lifetime=3)
            return domain
        except Exception:
            return None

    def _is_disposable(self, domain: str) -> bool:
        """Check against known disposable email domain list."""
        DISPOSABLE_DOMAINS = {
            "mailinator.com", "guerrillamail.com", "tempmail.com",
            "throwam.com", "yopmail.com", "sharklasers.com",
            "10minutemail.com", "trashmail.com", "maildrop.cc",
        }
        return domain.lower() in DISPOSABLE_DOMAINS
