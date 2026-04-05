"""
AX Engine — Decision Maker Extractor

Multi-signal extraction pipeline:

SIGNAL 1: NLP (spaCy NER)
  - Find PERSON entities in website text
  - Score by mention count and proximity to role keywords

SIGNAL 2: Pattern Recognition
  - Regex patterns for "Founder & CEO: John Smith"
  - "Dr. Sarah Jones, Owner"
  - Schema.org Person markup

SIGNAL 3: Structured Data
  - JSON-LD Person/Employee schemas
  - LinkedIn profile patterns in links

SIGNAL 4: Niche Heuristics
  - Dental clinic → "Dr. [Name]" patterns
  - Law firm → "Attorney [Name]"
  - Restaurant → chef/owner patterns

SIGNAL 5: Domain-based inference
  - "johnsmith.com" → John Smith likely owner

All signals are merged and deduplicated.
Confidence scores are composited across signals.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

import structlog

from ax_engine.api.models.responses import DecisionMaker
from ax_engine.engines.decision_maker.nlp_engine import NLPEngine

logger = structlog.get_logger(__name__)

# Role keywords in multiple languages
ROLE_PATTERNS: Dict[str, List[str]] = {
    "owner": ["owner", "proprietor", "владелец", "хозяин"],
    "founder": ["founder", "co-founder", "cofounder", "основатель", "соучредитель"],
    "ceo": ["ceo", "chief executive", "executive director", "генеральный директор", "гендиректор"],
    "director": ["director", "директор", "управляющий"],
    "partner": ["partner", "партнёр", "партнер"],
    "president": ["president", "президент"],
    "manager": ["manager", "managing director", "менеджер", "управляющий"],
}

ALL_ROLE_KEYWORDS = [kw for keywords in ROLE_PATTERNS.values() for kw in keywords]

# Niche-specific honorific patterns
NICHE_HONORIFICS: Dict[str, List[str]] = {
    "dental": ["dr.", "doctor", "dds", "dmd", "врач", "доктор"],
    "medical": ["dr.", "doctor", "md", "врач", "доктор"],
    "law": ["attorney", "counsel", "esq", "адвокат", "юрист"],
    "accounting": ["cpa", "cfa", "бухгалтер"],
    "real estate": ["realtor", "broker", "агент"],
}

# Patterns that strongly indicate a decision-maker mention
# Group 1: optional title, Group 2: name, Group 3: role
DM_INLINE_PATTERNS = [
    # "CEO John Smith" or "John Smith, CEO"
    r"(?:^|\b)((?:dr\.?|mr\.?|mrs\.?|ms\.?|prof\.?)\s+)?([A-ZА-Я][a-zA-Zа-яА-Я\-']+(?:\s+[A-ZА-Я][a-zA-Zа-яА-Я\-']+){1,3})\s*[,\-–—]\s*(" + "|".join(ALL_ROLE_KEYWORDS[:20]) + r")",
    # "Founder: John Smith"
    r"(" + "|".join(ALL_ROLE_KEYWORDS[:20]) + r")[:\s]+([A-ZА-Я][a-zA-Zа-яА-Я\-']+(?:\s+[A-ZА-Я][a-zA-Zа-яА-Я\-']+){1,3})",
    # "Meet our CEO, Dr. Jane Doe"
    r"(?:meet|our|founded by|led by|owned by|run by)\s+(?:dr\.?\s+)?([A-ZА-Я][a-zA-Zа-яА-Я\-']+(?:\s+[A-ZА-Я][a-zA-Zа-яА-Я\-']+){1,2})",
]


class DecisionMakerExtractor:
    """
    Extracts and ranks decision-makers from website data.
    """

    def __init__(self, nlp: NLPEngine):
        self.nlp = nlp

    async def extract(
        self,
        website_data: Dict[str, Any],
        company_name: str,
        niche: str,
        target_roles: List[str],
    ) -> List[DecisionMaker]:
        """
        Main extraction entry point.
        Runs all signals in parallel and merges results.
        """
        if not website_data:
            return []

        text = website_data.get("raw_text", "")
        structured_data = website_data.get("structured_data", [])
        links = website_data.get("links", [])

        if not text:
            return []

        # Detect language for NLP model selection
        lang = self.nlp.detect_language(text[:500])

        # Run all extraction signals
        nlp_candidates = self._extract_via_nlp(text, lang)
        pattern_candidates = self._extract_via_patterns(text)
        schema_candidates = self._extract_via_schema(structured_data)
        niche_candidates = self._extract_via_niche_heuristics(text, niche, lang)

        # Merge all candidates
        all_candidates = (
            nlp_candidates + pattern_candidates + schema_candidates + niche_candidates
        )

        # Deduplicate and merge confidence scores
        merged = self._merge_candidates(all_candidates)

        # Filter to target roles if not "any"
        if "any" not in target_roles:
            merged = [dm for dm in merged if dm.role.lower() in target_roles or not dm.role]

        # Exclude company name (sometimes picked up as a person)
        merged = [dm for dm in merged if not self._is_company_name(dm.name, company_name)]

        # Sort by confidence
        merged.sort(key=lambda x: x.confidence_score, reverse=True)

        # Return top 5 decision makers max
        return merged[:5]

    def _extract_via_nlp(self, text: str, lang: str) -> List[DecisionMaker]:
        """
        Use spaCy NER to find PERSON entities, then check context
        for nearby role keywords.
        """
        persons = self.nlp.extract_persons(text, lang)
        results = []

        for name, base_confidence in persons:
            # Look for role keywords near this name in text
            role, role_confidence_boost = self._find_role_near_name(text, name)

            confidence = int((base_confidence + role_confidence_boost) * 100)
            confidence = min(confidence, 95)

            results.append(DecisionMaker(
                name=name,
                role=role,
                confidence_score=confidence,
                source="nlp_ner",
            ))

        return results

    def _extract_via_patterns(self, text: str) -> List[DecisionMaker]:
        """Regex pattern matching for explicit role-name co-occurrences."""
        results = []

        for pattern in DM_INLINE_PATTERNS:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
                for match in matches:
                    groups = [g for g in match.groups() if g]
                    if not groups:
                        continue

                    # Extract name and role from the match groups
                    name = None
                    role = None

                    for g in groups:
                        g_lower = g.lower().strip()
                        if any(kw in g_lower for kw in ALL_ROLE_KEYWORDS):
                            role = self._normalize_role(g_lower)
                        elif len(g.split()) >= 2 and g[0].isupper():
                            name = g.strip()

                    if name and len(name) > 4:
                        results.append(DecisionMaker(
                            name=name,
                            role=role or "unknown",
                            confidence_score=85,  # Pattern match = high confidence
                            source="regex_pattern",
                        ))
            except re.error:
                continue

        return results

    def _extract_via_schema(self, structured_data: List[Dict]) -> List[DecisionMaker]:
        """
        Extract from JSON-LD Person/Employee schemas.
        Schema.org data is the most reliable signal.
        """
        results = []

        for item in structured_data:
            if not isinstance(item, dict):
                continue

            schema_type = item.get("@type", "")
            if isinstance(schema_type, list):
                schema_type = schema_type[0] if schema_type else ""

            # Direct Person schema
            if schema_type == "Person":
                name = item.get("name", "").strip()
                job_title = item.get("jobTitle", "").strip()
                if name and len(name) > 3:
                    results.append(DecisionMaker(
                        name=name,
                        role=self._normalize_role(job_title) if job_title else "unknown",
                        confidence_score=92,  # Schema.org = very high confidence
                        source="schema_org",
                    ))

            # Organization with employees/founders
            if schema_type in ("Organization", "LocalBusiness"):
                for key in ("founder", "employee", "member"):
                    person = item.get(key)
                    if isinstance(person, dict) and person.get("name"):
                        results.append(DecisionMaker(
                            name=person["name"].strip(),
                            role=key if key in ROLE_PATTERNS else "unknown",
                            confidence_score=90,
                            source="schema_org",
                        ))
                    elif isinstance(person, list):
                        for p in person:
                            if isinstance(p, dict) and p.get("name"):
                                results.append(DecisionMaker(
                                    name=p["name"].strip(),
                                    role=key,
                                    confidence_score=90,
                                    source="schema_org",
                                ))

        return results

    def _extract_via_niche_heuristics(
        self,
        text: str,
        niche: str,
        lang: str,
    ) -> List[DecisionMaker]:
        """
        Niche-specific patterns.
        Example: dental clinics → "Dr. Smith is our owner"
        """
        results = []
        niche_lower = niche.lower()

        # Find which niche honorifics apply
        relevant_honorifics = []
        for niche_key, honorifics in NICHE_HONORIFICS.items():
            if niche_key in niche_lower:
                relevant_honorifics.extend(honorifics)

        if not relevant_honorifics:
            return results

        # Build pattern: "Dr. John Smith" or "Dr. Smith"
        for honorific in relevant_honorifics:
            pattern = rf"\b{re.escape(honorific)}\s+([A-ZА-Я][a-zA-Zа-яА-Я\-']+(?:\s+[A-ZА-Я][a-zA-Zа-яА-Я\-']+)?)"
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for m in matches:
                name = f"{honorific.title()} {m.group(1).strip()}"
                role, _ = self._find_role_near_name(text, m.group(1))
                results.append(DecisionMaker(
                    name=name,
                    role=role or "owner",  # In clinics, the Dr is usually the owner
                    confidence_score=80,
                    source="niche_heuristic",
                ))

        return results

    def _find_role_near_name(
        self,
        text: str,
        name: str,
        window: int = 100,
    ) -> Tuple[str, float]:
        """
        Search for role keywords within ±window characters of a name.
        Returns (role, confidence_boost).
        """
        # Find all occurrences of the name in text
        text_lower = text.lower()
        name_lower = name.lower()

        pos = text_lower.find(name_lower)
        if pos == -1:
            return "unknown", 0.0

        context = text[max(0, pos - window) : pos + len(name) + window].lower()

        for role, keywords in ROLE_PATTERNS.items():
            for kw in keywords:
                if kw in context:
                    return role, 0.15

        return "unknown", 0.0

    def _normalize_role(self, role_text: str) -> str:
        """Map raw role text to a canonical role name."""
        role_lower = role_text.lower().strip()
        for canonical, keywords in ROLE_PATTERNS.items():
            if any(kw in role_lower for kw in keywords):
                return canonical
        return role_text[:50]  # Keep original if no match

    def _merge_candidates(self, candidates: List[DecisionMaker]) -> List[DecisionMaker]:
        """
        Deduplicate candidates by name (fuzzy match).
        When duplicates found, merge confidence scores and sources.
        """
        merged: Dict[str, DecisionMaker] = {}

        for dm in candidates:
            key = self._normalize_name(dm.name)
            if not key or len(key) < 4:
                continue

            if key in merged:
                existing = merged[key]
                # Boost confidence if multiple signals agree
                new_conf = min(existing.confidence_score + 8, 98)
                merged[key] = DecisionMaker(
                    name=existing.name,
                    role=dm.role if dm.role != "unknown" else existing.role,
                    confidence_score=new_conf,
                    source=f"{existing.source},{dm.source}",
                )
            else:
                merged[key] = dm

        return list(merged.values())

    def _normalize_name(self, name: str) -> str:
        """Normalize name for deduplication."""
        # Remove titles
        name = re.sub(r"^(dr|mr|mrs|ms|prof)\.?\s+", "", name, flags=re.IGNORECASE)
        return " ".join(name.lower().split())

    def _is_company_name(self, person_name: str, company_name: str) -> bool:
        """Check if a detected 'person' is actually the company name."""
        person_lower = person_name.lower().strip()
        company_lower = company_name.lower().strip()
        return (
            person_lower in company_lower
            or company_lower in person_lower
            or person_lower == company_lower
        )
