"""
AX Engine — Lead Scoring Engine

Computes a composite 0-100 lead score from 4 weighted dimensions:

DIMENSION 1: Data Completeness (25%)
  What data did we successfully extract?
  Signals: has website, has decision maker, has email, has phone, has enrichment

DIMENSION 2: Contact Availability (30%)
  How reachable is this lead?
  Signals: email validity, phone confirmed, LinkedIn found, email is personal

DIMENSION 3: Opportunity Signals (30%)
  How valuable is the opportunity?
  Signals: number and severity of detected opportunities

DIMENSION 4: Decision Maker Confidence (15%)
  How confident are we in the decision-maker identity?
  Signals: DM confidence scores, multi-source confirmation

Total: 0-100 with integer output.

Scoring philosophy:
  80-100: Hot lead — full contact info, high-value opportunities
  60-79:  Warm lead — partial info, some opportunities
  40-59:  Qualified lead — basic info, worth prospecting
  20-39:  Cold lead — limited data, needs manual research
  0-19:   Poor lead — minimal data, low priority
"""
from __future__ import annotations

from typing import Dict, Tuple

from ax_engine.api.models.responses import LeadResult
from ax_engine.config import settings


class LeadScorer:
    """
    Stateless scorer — pure function over a LeadResult.
    """

    def score(self, lead: LeadResult) -> Tuple[int, Dict[str, int]]:
        """
        Returns (composite_score, breakdown_dict).

        breakdown_dict keys match dimension names.
        """
        d1 = self._score_data_completeness(lead)
        d2 = self._score_contact_availability(lead)
        d3 = self._score_opportunity_signals(lead)
        d4 = self._score_dm_confidence(lead)

        w1 = settings.SCORE_WEIGHT_DATA_COMPLETENESS
        w2 = settings.SCORE_WEIGHT_CONTACT_AVAILABILITY
        w3 = settings.SCORE_WEIGHT_OPPORTUNITY_SIGNALS
        w4 = settings.SCORE_WEIGHT_DECISION_MAKER_CONFIDENCE

        composite = int(
            d1 * w1 + d2 * w2 + d3 * w3 + d4 * w4
        )

        breakdown = {
            "data_completeness": d1,
            "contact_availability": d2,
            "opportunity_signals": d3,
            "dm_confidence": d4,
        }

        return max(0, min(composite, 100)), breakdown

    def _score_data_completeness(self, lead: LeadResult) -> int:
        """
        0-100 score for how much data we have.
        """
        score = 0
        max_score = 100

        # Website presence (20 points)
        if lead.website:
            score += 20

        # Decision makers found (30 points)
        dm_count = len(lead.decision_makers)
        if dm_count >= 3:
            score += 30
        elif dm_count == 2:
            score += 22
        elif dm_count == 1:
            score += 15

        # Contact data (25 points)
        if lead.contacts.emails:
            score += 15
        if lead.contacts.phones:
            score += 10

        # Enrichment data (25 points)
        if lead.enrichment:
            if lead.enrichment.company_size:
                score += 8
            if lead.enrichment.revenue_estimate:
                score += 8
            if lead.enrichment.tech_stack:
                score += 5
            if lead.enrichment.year_founded:
                score += 4

        return min(score, 100)

    def _score_contact_availability(self, lead: LeadResult) -> int:
        """
        0-100 score for contact reachability.
        """
        score = 0

        if not lead.contacts:
            return 0

        # Email quality scoring
        email_status = lead.contacts.email_status
        if email_status == "valid":
            score += 50
        elif email_status == "catch_all":
            score += 30
        elif email_status == "risky":
            score += 15

        # Personal email vs. generic
        if lead.contacts.primary_email:
            local = lead.contacts.primary_email.split("@")[0].lower()
            if "." in local and any(c.isalpha() for c in local):
                score += 15  # firstname.lastname pattern

        # Phone number
        if lead.contacts.phones:
            score += 20

        # LinkedIn profile (DM reachable via LinkedIn)
        has_linkedin = any("linkedin.com" in s for s in (lead.contacts.socials or []))
        if has_linkedin:
            score += 15

        return min(score, 100)

    def _score_opportunity_signals(self, lead: LeadResult) -> int:
        """
        0-100 score based on detected opportunities.
        More + higher-severity opportunities = higher score.
        """
        if not lead.opportunity_details:
            return 10  # Baseline: some opportunity always exists

        severity_weights = {"high": 25, "medium": 15, "low": 8}
        raw_score = sum(
            severity_weights.get(signal.severity, 5)
            for signal in lead.opportunity_details
        )

        # Normalize to 0-100
        return min(raw_score, 100)

    def _score_dm_confidence(self, lead: LeadResult) -> int:
        """
        0-100 score based on decision-maker extraction confidence.
        """
        if not lead.decision_makers:
            return 0

        # Average confidence of top 3 DMs (weighted by position)
        top_dms = sorted(
            lead.decision_makers,
            key=lambda dm: dm.confidence_score,
            reverse=True,
        )[:3]

        if not top_dms:
            return 0

        weights = [0.6, 0.3, 0.1]
        weighted_score = sum(
            dm.confidence_score * w
            for dm, w in zip(top_dms, weights[:len(top_dms)])
        )

        return int(weighted_score)
