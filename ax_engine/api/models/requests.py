"""
AX Engine — API Request Models
Pydantic v2 models with strict validation.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class TargetRole(str, Enum):
    OWNER = "owner"
    FOUNDER = "founder"
    CEO = "ceo"
    DIRECTOR = "director"
    PARTNER = "partner"
    MANAGER = "manager"
    PRESIDENT = "president"
    COO = "coo"
    CTO = "cto"
    ANY = "any"


class SearchDepth(str, Enum):
    SHALLOW = "shallow"    # Business discovery only (~5s per lead)
    STANDARD = "standard"  # + website crawl + contacts (~15s)
    DEEP = "deep"          # + full enrichment + opportunity signals (~45s)


class SearchRequest(BaseModel):
    """
    Primary search request. Drives the entire pipeline.

    Example:
        {
            "niche": "dental clinics",
            "location": "Dagestan, Russia",
            "roles": ["owner", "founder"],
            "depth": "deep",
            "max_results": 50
        }
    """
    niche: str = Field(
        ...,
        min_length=2,
        max_length=200,
        description="Business niche (e.g. 'dental clinics', 'law firms', 'gyms')",
        examples=["dental clinics", "software agencies", "restaurants"],
    )
    location: str = Field(
        ...,
        min_length=2,
        max_length=300,
        description="Target location — city, region, or country",
        examples=["Dagestan, Russia", "New York, USA", "Dubai, UAE"],
    )
    roles: List[TargetRole] = Field(
        default=[TargetRole.OWNER, TargetRole.FOUNDER, TargetRole.CEO],
        description="Decision-maker roles to target",
    )
    depth: SearchDepth = Field(
        default=SearchDepth.STANDARD,
        description="Extraction depth — affects latency vs. data richness",
    )
    max_results: int = Field(
        default=25,
        ge=1,
        le=100,
        description="Maximum number of business leads to return",
    )
    language_hint: Optional[str] = Field(
        default=None,
        description="ISO 639-1 language code hint for NLP (e.g. 'ru', 'ar', 'en')",
        examples=["en", "ru", "ar", "de"],
    )
    exclude_chains: bool = Field(
        default=True,
        description="Exclude large chain businesses (Starbucks, McDonalds, etc.)",
    )
    require_website: bool = Field(
        default=False,
        description="Only return leads that have a discoverable website",
    )
    webhook_url: Optional[str] = Field(
        default=None,
        description="Optional webhook to POST results to when job completes",
    )

    @field_validator("niche", "location")
    @classmethod
    def strip_and_clean(cls, v: str) -> str:
        return v.strip()

    @field_validator("roles")
    @classmethod
    def roles_not_empty(cls, v: List[TargetRole]) -> List[TargetRole]:
        if not v:
            raise ValueError("At least one role must be specified")
        return v

    @model_validator(mode="after")
    def validate_webhook(self) -> "SearchRequest":
        if self.webhook_url and not self.webhook_url.startswith(("http://", "https://")):
            raise ValueError("webhook_url must be a valid HTTP/HTTPS URL")
        return self


class JobStatusRequest(BaseModel):
    job_id: str = Field(..., description="Job ID returned from POST /search")


class ExportRequest(BaseModel):
    job_id: str
    format: str = Field(default="json", pattern="^(json|csv|xlsx)$")
