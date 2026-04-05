"""
AX Engine — NLP Engine

Wraps spaCy models for:
  1. Named Entity Recognition (PERSON, ORG)
  2. Multilingual support (English + Russian + more)
  3. Model warm-up at boot (not per-request)

Design: Singleton loaded once at FastAPI startup and passed
to all extractors via dependency injection.
"""
from __future__ import annotations

import asyncio
from typing import List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)


class NLPEngine:
    """
    Manages spaCy model instances.
    Models are heavy (~500MB) — load once, reuse everywhere.
    Thread-safe: spaCy pipelines are stateless per call.
    """

    def __init__(self):
        self._en_model = None
        self._multi_model = None
        self._initialized = False

    async def initialize(self) -> None:
        """
        Load models in a thread pool to avoid blocking the event loop.
        spaCy model loading is CPU-bound.
        """
        if self._initialized:
            return

        loop = asyncio.get_event_loop()

        logger.info("nlp_engine.loading_models")

        self._en_model = await loop.run_in_executor(None, self._load_en)
        self._multi_model = await loop.run_in_executor(None, self._load_multi)

        self._initialized = True
        logger.info("nlp_engine.models_ready")

    def _load_en(self):
        import spacy
        try:
            return spacy.load("en_core_web_lg")
        except OSError:
            logger.warning("nlp.en_model_not_found_using_sm")
            return spacy.load("en_core_web_sm")

    def _load_multi(self):
        import spacy
        try:
            return spacy.load("xx_ent_wiki_sm")
        except OSError:
            logger.warning("nlp.multi_model_not_found_falling_back_to_en")
            return self._en_model

    def extract_persons(
        self,
        text: str,
        lang: str = "en",
    ) -> List[Tuple[str, float]]:
        """
        Extract PERSON entities from text.

        Returns list of (name, confidence) tuples.
        Confidence is based on entity score when available.
        """
        if not self._initialized:
            raise RuntimeError("NLPEngine not initialized. Call await initialize() first.")

        model = self._multi_model if lang != "en" else self._en_model
        doc = model(text[:50_000])  # Cap input to avoid OOM

        persons = []
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                # spaCy doesn't expose confidence scores for NER directly
                # Use heuristics: full name (2+ tokens) gets higher confidence
                tokens = ent.text.split()
                base_confidence = 0.7 if len(tokens) >= 2 else 0.5

                # Boost if name appears multiple times
                mention_count = text.lower().count(ent.text.lower())
                boosted = min(base_confidence + (mention_count - 1) * 0.05, 0.95)

                persons.append((ent.text.strip(), round(boosted, 2)))

        # Deduplicate by normalized name
        seen = set()
        unique = []
        for name, conf in persons:
            normalized = " ".join(name.lower().split())
            if normalized not in seen and len(normalized) > 3:
                seen.add(normalized)
                unique.append((name, conf))

        return unique

    def extract_organizations(self, text: str) -> List[str]:
        """Extract ORG entities."""
        if not self._initialized:
            raise RuntimeError("NLPEngine not initialized.")

        doc = self._en_model(text[:30_000])
        return list({ent.text.strip() for ent in doc.ents if ent.label_ == "ORG"})

    def detect_language(self, text: str) -> str:
        """Detect text language. Returns ISO 639-1 code."""
        try:
            from langdetect import detect
            return detect(text[:1000])
        except Exception:
            return "en"
