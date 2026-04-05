"""
AX Engine — Search Orchestrator

The orchestrator is the pipeline controller. It:
  1. Fans out to the Discovery Engine to find businesses
  2. For each business, runs the full enrichment pipeline concurrently
  3. Collects, deduplicates, scores, and returns ranked leads

Architecture decision: asyncio.gather() with bounded semaphores to
prevent overwhelming target sites or proxy pool. Each stage is a
coroutine that can be independently retried or skipped on failure
without killing the whole pipeline.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Callable, List, Optional

import structlog

from ax_engine.api.models.requests import SearchDepth, SearchRequest
from ax_engine.api.models.responses import Contacts, Enrichment, LeadResult
from ax_engine.config import settings
from ax_engine.engines.contact.email_extractor import EmailExtractor
from ax_engine.engines.contact.email_generator import EmailPatternGenerator
from ax_engine.engines.contact.email_validator import EmailValidator
from ax_engine.engines.contact.phone_extractor import PhoneExtractor
from ax_engine.engines.contact.social_finder import SocialProfileFinder
from ax_engine.engines.decision_maker.extractor import DecisionMakerExtractor
from ax_engine.engines.decision_maker.nlp_engine import NLPEngine
from ax_engine.engines.discovery.maps_scraper import GoogleMapsScraper
from ax_engine.engines.enrichment.company_enricher import CompanyEnricher
from ax_engine.engines.enrichment.tech_detector import TechStackDetector
from ax_engine.engines.intent.opportunity_engine import OpportunityEngine
from ax_engine.engines.scoring.lead_scorer import LeadScorer
from ax_engine.engines.website.crawler import WebsiteCrawler
from ax_engine.engines.website.parser import WebsiteParser
from ax_engine.infrastructure.proxy_manager import ProxyManager

logger = structlog.get_logger(__name__)


class SearchOrchestrator:
    """
    Orchestrates the full intelligence extraction pipeline.

    Pipeline stages per business:
      STAGE 1 (ALWAYS):   Discovery   → raw business list
      STAGE 2 (ALWAYS):   Basic parse → website, phone, address
      STAGE 3 (STANDARD): Crawl       → website HTML
      STAGE 4 (STANDARD): Extract     → decision makers, emails, phones
      STAGE 5 (DEEP):     Enrich      → company size, revenue, tech stack
      STAGE 6 (DEEP):     Intent      → opportunity signals
      STAGE 7 (ALWAYS):   Score       → composite lead score
    """

    def __init__(
        self,
        nlp: Optional[NLPEngine] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ):
        self.nlp = nlp or NLPEngine()
        self.progress_callback = progress_callback
        self.proxy_manager = ProxyManager()

        # Pipeline components
        self.maps_scraper = GoogleMapsScraper(self.proxy_manager)
        self.crawler = WebsiteCrawler(self.proxy_manager)
        self.parser = WebsiteParser()
        self.dm_extractor = DecisionMakerExtractor(self.nlp)
        self.email_extractor = EmailExtractor()
        self.phone_extractor = PhoneExtractor()
        self.email_generator = EmailPatternGenerator()
        self.email_validator = EmailValidator()
        self.social_finder = SocialProfileFinder()
        self.company_enricher = CompanyEnricher()
        self.tech_detector = TechStackDetector()
        self.opportunity_engine = OpportunityEngine(self.nlp)
        self.lead_scorer = LeadScorer()

    async def run(self, request: SearchRequest) -> List[LeadResult]:
        start_time = time.perf_counter()
        log = logger.bind(niche=request.niche, location=request.location, depth=request.depth)
        log.info("orchestrator.pipeline_start")

        # ── STAGE 1: Business Discovery ───────────────────────────
        raw_businesses = await self.maps_scraper.search(
            query=f"{request.niche} in {request.location}",
            max_results=request.max_results,
        )
        log.info("orchestrator.discovery_complete", found=len(raw_businesses))

        if not raw_businesses:
            return []

        # ── Deduplication ─────────────────────────────────────────
        raw_businesses = self._deduplicate(raw_businesses)

        # ── Filter chains if requested ────────────────────────────
        if request.exclude_chains:
            raw_businesses = [b for b in raw_businesses if not self._is_chain(b)]

        # ── Parallel pipeline execution ───────────────────────────
        # Bounded semaphore prevents hammering proxies/targets
        semaphore = asyncio.Semaphore(8)

        tasks = [
            self._process_business(b, request, semaphore, idx, len(raw_businesses))
            for idx, b in enumerate(raw_businesses)
        ]

        results_raw = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions (failed businesses) and None (skipped)
        results: List[LeadResult] = []
        for r in results_raw:
            if isinstance(r, Exception):
                log.warning("orchestrator.business_failed", error=str(r))
            elif r is not None:
                results.append(r)

        # ── Require website filter ────────────────────────────────
        if request.require_website:
            results = [r for r in results if r.website]

        # ── Final sort by lead_score descending ───────────────────
        results.sort(key=lambda r: r.lead_score, reverse=True)

        elapsed = time.perf_counter() - start_time
        log.info(
            "orchestrator.pipeline_complete",
            results=len(results),
            elapsed_s=round(elapsed, 2),
        )

        return results

    async def _process_business(
        self,
        raw: dict,
        request: SearchRequest,
        semaphore: asyncio.Semaphore,
        idx: int,
        total: int,
    ) -> Optional[LeadResult]:
        """Full per-business pipeline — all stages."""
        async with semaphore:
            log = logger.bind(business=raw.get("name"), idx=idx)

            try:
                lead = LeadResult(
                    id=str(uuid.uuid4()),
                    company_name=raw.get("name", "Unknown"),
                    location=raw.get("address", request.location),
                    website=raw.get("website"),
                    phone=raw.get("phone"),
                    address=raw.get("address"),
                    google_maps_url=raw.get("maps_url"),
                    lead_score=0,
                )

                # ── STAGE 2: Website crawl (standard+) ───────────
                website_data = {}
                if request.depth in (SearchDepth.STANDARD, SearchDepth.DEEP) and lead.website:
                    try:
                        html_pages = await self.crawler.crawl(
                            lead.website,
                            pages=["", "about", "team", "contact", "staff"],
                        )
                        website_data = self.parser.parse(html_pages)
                        lead.data_sources.append("website_crawl")
                    except Exception as e:
                        log.warning("stage.crawl_failed", error=str(e))

                # ── STAGE 3: Decision maker extraction ───────────
                if request.depth in (SearchDepth.STANDARD, SearchDepth.DEEP):
                    target_roles = [r.value for r in request.roles]
                    decision_makers = await self.dm_extractor.extract(
                        website_data=website_data,
                        company_name=lead.company_name,
                        niche=request.niche,
                        target_roles=target_roles,
                    )
                    lead.decision_makers = decision_makers
                    if decision_makers:
                        lead.data_sources.append("nlp_extraction")

                # ── STAGE 4: Contact extraction ───────────────────
                if request.depth in (SearchDepth.STANDARD, SearchDepth.DEEP):
                    emails = self.email_extractor.extract(website_data.get("raw_text", ""))
                    phones = self.phone_extractor.extract(
                        website_data.get("raw_text", ""),
                        country_hint=self._guess_country(request.location),
                    )
                    socials = self.social_finder.find(website_data.get("raw_text", ""), website_data.get("links", []))

                    # Generate pattern-based emails for decision makers
                    if lead.website and lead.decision_makers:
                        generated = await self.email_generator.generate(
                            domain=self._extract_domain(lead.website),
                            decision_makers=lead.decision_makers,
                        )
                        emails.extend(generated)

                    # Validate all emails (SMTP simulation)
                    validated = await self.email_validator.validate_bulk(list(set(emails)))
                    valid_emails = [e for e, status in validated.items() if status in ("valid", "catch_all")]

                    lead.contacts = Contacts(
                        emails=valid_emails,
                        phones=list(set(phones)),
                        socials=socials,
                        email_status=self._best_email_status(validated),
                        primary_email=valid_emails[0] if valid_emails else None,
                    )

                # ── STAGE 5: Enrichment (deep only) ──────────────
                if request.depth == SearchDepth.DEEP:
                    enrichment_data = await asyncio.gather(
                        self.company_enricher.enrich(lead.company_name, lead.website),
                        self.tech_detector.detect(lead.website) if lead.website else asyncio.coroutine(lambda: [])(),
                        return_exceptions=True,
                    )

                    company_info = enrichment_data[0] if not isinstance(enrichment_data[0], Exception) else {}
                    tech_stack = enrichment_data[1] if not isinstance(enrichment_data[1], Exception) else []

                    lead.enrichment = Enrichment(
                        company_size=company_info.get("size"),
                        employee_count_estimate=company_info.get("employees"),
                        revenue_estimate=company_info.get("revenue"),
                        year_founded=company_info.get("founded"),
                        tech_stack=tech_stack,
                        social_presence=company_info.get("socials", {}),
                        has_paid_ads=company_info.get("has_paid_ads", False),
                        google_rating=raw.get("rating"),
                        review_count=raw.get("review_count"),
                        categories=raw.get("categories", []),
                    )

                # ── STAGE 6: Opportunity signals (deep only) ──────
                if request.depth == SearchDepth.DEEP:
                    signals = await self.opportunity_engine.analyze(
                        lead=lead,
                        website_data=website_data,
                        niche=request.niche,
                    )
                    lead.opportunity_details = signals
                    lead.opportunity_signals = [s.signal for s in signals]

                # ── STAGE 7: Scoring (always) ─────────────────────
                score, breakdown = self.lead_scorer.score(lead)
                lead.lead_score = score
                lead.score_breakdown = breakdown

                if self.progress_callback:
                    self.progress_callback(idx + 1, total)

                return lead

            except Exception as e:
                log.error("orchestrator.business_error", error=str(e), exc_info=True)
                return None

    def _deduplicate(self, businesses: list) -> list:
        seen = set()
        unique = []
        for b in businesses:
            key = (b.get("name", "").lower().strip(), b.get("address", "").lower().strip())
            if key not in seen:
                seen.add(key)
                unique.append(b)
        return unique

    CHAIN_KEYWORDS = {
        "mcdonald", "starbucks", "subway", "kfc", "domino",
        "pizza hut", "burger king", "walmart", "target", "costco",
        "cvs", "walgreen", "7-eleven", "dunkin", "wendy",
    }

    def _is_chain(self, business: dict) -> bool:
        name = business.get("name", "").lower()
        return any(chain in name for chain in self.CHAIN_KEYWORDS)

    def _extract_domain(self, url: str) -> str:
        import tldextract
        ext = tldextract.extract(url)
        return f"{ext.domain}.{ext.suffix}"

    def _guess_country(self, location: str) -> Optional[str]:
        """Simple country code guesser from location string."""
        country_map = {
            "russia": "RU", "usa": "US", "united states": "US",
            "uk": "GB", "germany": "DE", "france": "FR",
            "uae": "AE", "dubai": "AE", "saudi": "SA",
        }
        loc_lower = location.lower()
        for keyword, code in country_map.items():
            if keyword in loc_lower:
                return code
        return None

    def _best_email_status(self, validated: dict) -> str:
        statuses = list(validated.values())
        if "valid" in statuses:
            return "valid"
        if "catch_all" in statuses:
            return "catch_all"
        if "invalid" in statuses:
            return "invalid"
        return "unknown"
