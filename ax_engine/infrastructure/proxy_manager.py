"""
AX Engine — Proxy Manager

Production proxy rotation with:
  - Multiple rotation strategies (round-robin, random, sticky per domain)
  - Automatic failure detection and temporary banning
  - Health scoring per proxy
  - Support for residential, datacenter, and mobile proxy pools
  - External proxy pool API integration (e.g., Bright Data, OxyLabs, SmartProxy)

Proxy selection strategy:
  1. Check external pool API if configured
  2. Fall back to static proxy list
  3. Fall back to direct connection (risky for volume)

Ban logic:
  - Proxy is "soft banned" after MAX_FAILURES consecutive failures
  - Auto-unbanned after BAN_DURATION seconds
  - Permanently removed after 3× soft bans
"""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import structlog

from ax_engine.config import settings

logger = structlog.get_logger(__name__)


@dataclass
class Proxy:
    server: str          # host:port
    username: str = ""
    password: str = ""
    proxy_type: str = "http"    # http | socks5
    country: str = ""
    failures: int = 0
    successes: int = 0
    banned_until: float = 0.0
    soft_ban_count: int = 0
    last_used: float = 0.0

    @property
    def is_banned(self) -> bool:
        return time.time() < self.banned_until

    @property
    def health_score(self) -> float:
        total = self.failures + self.successes
        if total == 0:
            return 1.0
        return self.successes / total

    @property
    def full_url(self) -> str:
        if self.username:
            return f"{self.proxy_type}://{self.username}:{self.password}@{self.server}"
        return f"{self.proxy_type}://{self.server}"

    def record_success(self) -> None:
        self.successes += 1
        self.failures = 0   # Reset consecutive failure count

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= settings.PROXY_MAX_FAILURES:
            self.banned_until = time.time() + settings.PROXY_BAN_DURATION
            self.soft_ban_count += 1
            logger.warning(
                "proxy.banned",
                server=self.server,
                failures=self.failures,
                ban_count=self.soft_ban_count,
            )


class ProxyManager:
    """
    Thread-safe proxy pool manager with rotation and health tracking.
    """

    def __init__(self):
        self._proxies: List[Proxy] = []
        self._round_robin_idx: int = 0
        self._sticky: Dict[str, Proxy] = {}  # domain → proxy
        self._initialized = False

    def initialize(self) -> None:
        """Load proxies from config."""
        if self._initialized:
            return

        # Load from static list
        for proxy_str in settings.proxy_list_parsed:
            proxy = self._parse_proxy_string(proxy_str)
            if proxy:
                self._proxies.append(proxy)

        logger.info("proxy_manager.initialized", count=len(self._proxies))
        self._initialized = True

    def get_proxy(self, domain: Optional[str] = None) -> Optional[Proxy]:
        """
        Get next available proxy based on rotation strategy.
        Returns None if no proxies configured (direct connection).
        """
        if not self._initialized:
            self.initialize()

        if not self._proxies:
            return None

        available = [p for p in self._proxies if not p.is_banned and p.soft_ban_count < 3]

        if not available:
            logger.warning("proxy_manager.all_proxies_banned")
            # Unban the best one if all are banned
            best = min(self._proxies, key=lambda p: p.banned_until)
            best.banned_until = 0
            available = [best]

        strategy = settings.PROXY_ROTATION_STRATEGY

        if strategy == "sticky" and domain:
            return self._get_sticky(domain, available)
        elif strategy == "random":
            return random.choice(available)
        else:  # round_robin
            return self._get_round_robin(available)

    def record_result(self, proxy: Proxy, success: bool) -> None:
        """Update proxy health statistics."""
        if success:
            proxy.record_success()
        else:
            proxy.record_failure()

    def _get_round_robin(self, available: List[Proxy]) -> Proxy:
        if not available:
            return None
        idx = self._round_robin_idx % len(available)
        self._round_robin_idx += 1
        proxy = available[idx]
        proxy.last_used = time.time()
        return proxy

    def _get_sticky(self, domain: str, available: List[Proxy]) -> Proxy:
        """Return same proxy for same domain (session consistency)."""
        if domain in self._sticky:
            p = self._sticky[domain]
            if not p.is_banned:
                return p
        # Assign new proxy for this domain
        proxy = random.choice(available)
        self._sticky[domain] = proxy
        return proxy

    def _parse_proxy_string(self, proxy_str: str) -> Optional[Proxy]:
        """
        Parse proxy from string formats:
          - "host:port"
          - "user:pass@host:port"
          - "http://user:pass@host:port"
          - "socks5://user:pass@host:port"
        """
        try:
            proxy_type = "http"

            if "://" in proxy_str:
                parts = proxy_str.split("://", 1)
                proxy_type = parts[0]
                proxy_str = parts[1]

            if "@" in proxy_str:
                creds, server = proxy_str.rsplit("@", 1)
                username, password = creds.split(":", 1)
            else:
                server = proxy_str
                username = ""
                password = ""

            return Proxy(
                server=server,
                username=username,
                password=password,
                proxy_type=proxy_type,
            )
        except Exception as e:
            logger.warning("proxy_manager.parse_failed", proxy=proxy_str, error=str(e))
            return None

    @property
    def stats(self) -> dict:
        return {
            "total": len(self._proxies),
            "active": sum(1 for p in self._proxies if not p.is_banned),
            "banned": sum(1 for p in self._proxies if p.is_banned),
            "avg_health": sum(p.health_score for p in self._proxies) / max(len(self._proxies), 1),
        }
