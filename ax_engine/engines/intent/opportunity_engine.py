"""
AX Engine — Intent & Opportunity Engine

THE MOST DIFFERENTIATED COMPONENT.

Analyzes a lead to generate "why this lead is valuable" signals.
Outputs human-readable opportunity descriptions that tell a salesperson
exactly WHY to reach out and WHAT to offer.

Signal categories:

1. HIRING SIGNALS
   - Actively hiring → growing business, needs systems
   - Hiring in specific roles → pain points reveal

2. REVIEW SENTIMENT
   - Low rating (<4.0) → problems worth solving
   - Negative review themes → recurring pain points
   - No response to reviews → not monitoring reputation

3. MISSING SYSTEMS
   - No booking system → losing appointments
   - No CRM → losing leads
   - No live chat → losing website visitors
   - No analytics → flying blind

4. DIGITAL PRESENCE GAPS
   - No website or bad website
   - No social media presence
   - Not running ads (competitor is)
   - Outdated site (old copyright year)

5. GROWTH SIGNALS
   - Multiple locations
   - Expanding (hiring, new office)
   - High review volume + good rating → scale opportunity

6. PAIN POINTS IN TEXT
   - Mentions of manual processes
   - "Call us" instead of online booking
   - "Email us" as primary contact method
"""
from __future__ import annotations

import re
from typing import Any, Dict, List

import structlog

from ax_engine.api.models.responses import LeadResult, OpportunitySignal
from ax_engine.engines.decision_maker.nlp_engine import NLPEngine

logger = structlog.get_logger(__name__)

# Hiring keywords in job postings
HIRING_PATTERNS = [
    re.compile(r"we(?:'re| are) hiring", re.I),
    re.compile(r"join our team", re.I),
    re.compile(r"open positions?", re.I),
    re.compile(r"career opportunities", re.I),
    re.compile(r"apply now", re.I),
    re.compile(r"job openings?", re.I),
    re.compile(r"вакансии", re.I),         # Russian: vacancies
    re.compile(r"мы ищем", re.I),          # Russian: we are looking for
    re.compile(r"требуется", re.I),        # Russian: required
]

# Low-tech / manual process signals
MANUAL_PROCESS_PATTERNS = [
    re.compile(r"call us to (book|schedule|make an appointment)", re.I),
    re.compile(r"call (for|to make) (an )?appointment", re.I),
    re.compile(r"phone us (to|for)", re.I),
    re.compile(r"contact us (to|for) (book|schedule)", re.I),
    re.compile(r"позвоните.*(записат|забронир)", re.I),  # Russian: call to book
]

# Missing online presence signals
OUTDATED_SITE_PATTERN = re.compile(r"©\s*(200[0-9]|201[0-5])", re.I)

# Pain point phrases in reviews / about text
PAIN_POINT_KEYWORDS = [
    "hard to reach", "never answered", "couldn't get through",
    "no response", "waited too long", "outdated",
    "не отвечают", "трудно дозвониться", "долго ждать",
]

POSITIVE_REVIEW_THRESHOLD = 4.0
CONCERNING_REVIEW_THRESHOLD = 3.5


class OpportunityEngine:
    """
    Detects and describes commercial opportunities for each lead.
    """

    def __init__(self, nlp: NLPEngine):
        self.nlp = nlp

    async def analyze(
        self,
        lead: LeadResult,
        website_data: Dict[str, Any],
        niche: str,
    ) -> List[OpportunitySignal]:
        """
        Run all opportunity detectors and return ranked signal list.
        """
        signals: List[OpportunitySignal] = []
        text = website_data.get("raw_text", "")
        tech_stack = lead.enrichment.tech_stack if lead.enrichment else []
        rating = lead.enrichment.google_rating if lead.enrichment else None
        review_count = lead.enrichment.review_count if lead.enrichment else None

        # Run all detectors
        signals.extend(self._detect_hiring_signals(text, niche))
        signals.extend(self._detect_review_signals(rating, review_count, text))
        signals.extend(self._detect_missing_systems(tech_stack, lead))
        signals.extend(self._detect_digital_gaps(lead, text))
        signals.extend(self._detect_manual_processes(text))
        signals.extend(self._detect_growth_signals(lead, text))

        # Sort by severity: high → medium → low
        severity_order = {"high": 0, "medium": 1, "low": 2}
        signals.sort(key=lambda s: severity_order.get(s.severity, 99))

        return signals[:10]  # Cap at 10 signals per lead

    def _detect_hiring_signals(self, text: str, niche: str) -> List[OpportunitySignal]:
        signals = []

        is_hiring = any(p.search(text) for p in HIRING_PATTERNS)

        if is_hiring:
            # Extract what roles they're hiring for
            role_context = self._extract_hiring_roles(text)
            detail = f"Currently hiring: {role_context}" if role_context else "Active hiring detected"

            signals.append(OpportunitySignal(
                signal="Actively hiring — business is growing and needs systems",
                category="hiring",
                severity="high",
                detail=detail,
            ))

        return signals

    def _detect_review_signals(
        self,
        rating: float | None,
        review_count: int | None,
        text: str,
    ) -> List[OpportunitySignal]:
        signals = []

        if rating is not None:
            if rating < CONCERNING_REVIEW_THRESHOLD:
                signals.append(OpportunitySignal(
                    signal=f"Low Google rating ({rating:.1f}★) — reputation management opportunity",
                    category="reviews",
                    severity="high",
                    detail="Business has significant reputation issues. High opportunity for review management, customer experience, or marketing services.",
                ))
            elif rating < POSITIVE_REVIEW_THRESHOLD:
                signals.append(OpportunitySignal(
                    signal=f"Below-average rating ({rating:.1f}★) — room for improvement",
                    category="reviews",
                    severity="medium",
                    detail="Rating below 4.0 suggests unresolved customer pain points.",
                ))

        if review_count is not None and review_count < 10:
            signals.append(OpportunitySignal(
                signal="Very few online reviews — local SEO and review generation opportunity",
                category="reviews",
                severity="medium",
                detail=f"Only {review_count} reviews. Competitors likely have more social proof.",
            ))

        # Check for pain point mentions in text
        pain_found = [kw for kw in PAIN_POINT_KEYWORDS if kw.lower() in text.lower()]
        if pain_found:
            signals.append(OpportunitySignal(
                signal="Customer complaints detected on website or reviews",
                category="reviews",
                severity="medium",
                detail=f"Pain points found: {', '.join(pain_found[:3])}",
            ))

        return signals

    def _detect_missing_systems(
        self,
        tech_stack: List[str],
        lead: LeadResult,
    ) -> List[OpportunitySignal]:
        signals = []
        tech_set = set(tech_stack)

        BOOKING_TOOLS = {"Calendly", "Acuity Scheduling", "SimplyBook", "Booksy"}
        CRM_TOOLS = {"HubSpot", "Salesforce", "Bitrix24"}
        CHAT_TOOLS = {"Intercom", "Tidio", "JivoChat"}
        ANALYTICS_TOOLS = {"Google Analytics 4", "Yandex Metrica", "Hotjar"}

        if not tech_set & BOOKING_TOOLS:
            signals.append(OpportunitySignal(
                signal="No online booking system detected — losing appointment bookings 24/7",
                category="missing_system",
                severity="high",
                detail="Customers can't self-book. Business is losing revenue to competitors who offer online booking.",
            ))

        if not tech_set & CRM_TOOLS:
            signals.append(OpportunitySignal(
                signal="No CRM system detected — leads likely falling through the cracks",
                category="missing_system",
                severity="high",
                detail="Without a CRM, lead follow-up is manual. Industry average: 79% of leads never convert without automated follow-up.",
            ))

        if not tech_set & ANALYTICS_TOOLS:
            signals.append(OpportunitySignal(
                signal="No web analytics detected — business has no visibility into website performance",
                category="missing_system",
                severity="medium",
                detail="No Google Analytics or equivalent. They cannot track conversions, traffic sources, or user behavior.",
            ))

        if not tech_set & CHAT_TOOLS:
            signals.append(OpportunitySignal(
                signal="No live chat tool — losing website visitors who don't call",
                category="missing_system",
                severity="low",
                detail="Live chat converts 3-5x more visitors than contact forms alone.",
            ))

        return signals

    def _detect_digital_gaps(self, lead: LeadResult, text: str) -> List[OpportunitySignal]:
        signals = []

        # No website
        if not lead.website:
            signals.append(OpportunitySignal(
                signal="No website found — massive digital presence gap",
                category="pain_point",
                severity="high",
                detail="Business has no web presence. Every competitor with a website is capturing their potential customers.",
            ))

        # Outdated website
        if text and OUTDATED_SITE_PATTERN.search(text):
            year_match = OUTDATED_SITE_PATTERN.search(text)
            if year_match:
                signals.append(OpportunitySignal(
                    signal=f"Website appears outdated (© {year_match.group(1)})",
                    category="pain_point",
                    severity="medium",
                    detail="Old copyright year suggests website hasn't been updated in years. Performance and security issues likely.",
                ))

        # No social media
        if not lead.contacts.socials:
            signals.append(OpportunitySignal(
                signal="No social media presence detected",
                category="pain_point",
                severity="low",
                detail="Business is invisible on social channels. Missing significant organic reach opportunities.",
            ))

        return signals

    def _detect_manual_processes(self, text: str) -> List[OpportunitySignal]:
        signals = []

        manual_detected = [p.search(text) for p in MANUAL_PROCESS_PATTERNS]
        if any(manual_detected):
            signals.append(OpportunitySignal(
                signal="Manual booking process — customers must call to schedule",
                category="pain_point",
                severity="high",
                detail="Website explicitly tells customers to call for appointments. This creates friction and loses bookings outside business hours.",
            ))

        return signals

    def _detect_growth_signals(self, lead: LeadResult, text: str) -> List[OpportunitySignal]:
        signals = []

        # High review count + good rating = growing business = scale opportunity
        if (
            lead.enrichment
            and lead.enrichment.review_count
            and lead.enrichment.review_count > 100
            and lead.enrichment.google_rating
            and lead.enrichment.google_rating >= 4.0
        ):
            signals.append(OpportunitySignal(
                signal=f"High-volume, well-rated business ({lead.enrichment.review_count}+ reviews, {lead.enrichment.google_rating}★) — prime for scaling",
                category="growth",
                severity="high",
                detail="This business has proven product-market fit. Ready for automation and scale. High LTV customer.",
            ))

        # Multiple locations
        location_pattern = re.compile(r"(\d+)\+?\s+locations?", re.I)
        m = location_pattern.search(text)
        if m and int(m.group(1)) > 1:
            signals.append(OpportunitySignal(
                signal=f"Multi-location business ({m.group(1)} locations) — enterprise-tier opportunity",
                category="growth",
                severity="high",
                detail="Multi-location businesses have higher budgets and greater need for centralized systems.",
            ))

        return signals

    def _extract_hiring_roles(self, text: str, max_chars: int = 200) -> str:
        """Extract the context around hiring mentions."""
        for pattern in HIRING_PATTERNS:
            m = pattern.search(text)
            if m:
                start = max(0, m.start() - 30)
                end = min(len(text), m.end() + 150)
                snippet = text[start:end].strip()
                return snippet[:max_chars]
        return ""
