"""
AX Engine — API Response Models
Exactly matches the required output JSON schema.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class DecisionMaker(BaseModel):
    name: str
    role: str
    confidence_score: int = Field(ge=0, le=100)
    source: Optional[str] = None             # Where this was found
    profile_url: Optional[str] = None        # LinkedIn or other social
    verified: bool = False


class Contacts(BaseModel):
    emails: List[str] = Field(default_factory=list)
    phones: List[str] = Field(default_factory=list)
    socials: List[str] = Field(default_factory=list)
    email_status: Optional[str] = None      # valid | catch_all | invalid | unknown
    primary_email: Optional[str] = None     # Highest-confidence email


class Enrichment(BaseModel):
    company_size: Optional[str] = None       # "1-10" | "11-50" | "51-200" | "201-500" | "500+"
    employee_count_estimate: Optional[int] = None
    revenue_estimate: Optional[str] = None   # "$100K-$500K" etc.
    year_founded: Optional[int] = None
    tech_stack: List[str] = Field(default_factory=list)
    social_presence: Dict[str, str] = Field(default_factory=dict)  # platform -> url
    has_paid_ads: bool = False
    google_rating: Optional[float] = None
    review_count: Optional[int] = None
    categories: List[str] = Field(default_factory=list)


class OpportunitySignal(BaseModel):
    signal: str                              # Human-readable signal description
    category: str                            # hiring | reviews | missing_system | growth | pain_point
    severity: str = "medium"                 # low | medium | high
    detail: Optional[str] = None


class LeadResult(BaseModel):
    """
    Complete lead record — the primary output unit of the system.
    """
    id: str                                  # UUID
    company_name: str
    location: str
    website: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    google_maps_url: Optional[str] = None

    decision_makers: List[DecisionMaker] = Field(default_factory=list)
    contacts: Contacts = Field(default_factory=Contacts)
    enrichment: Enrichment = Field(default_factory=Enrichment)

    opportunity_signals: List[str] = Field(default_factory=list)  # Summary strings
    opportunity_details: List[OpportunitySignal] = Field(default_factory=list)

    lead_score: int = Field(ge=0, le=100)    # Composite score
    score_breakdown: Dict[str, int] = Field(default_factory=dict)

    data_sources: List[str] = Field(default_factory=list)
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    processing_time_ms: Optional[int] = None


class SearchJobResponse(BaseModel):
    """Returned immediately when a search job is submitted."""
    job_id: str
    status: JobStatus = JobStatus.PENDING
    message: str = "Job queued successfully"
    estimated_completion_seconds: Optional[int] = None
    poll_url: str                             # GET /api/v1/jobs/{job_id}
    webhook_url: Optional[str] = None
    submitted_at: datetime = Field(default_factory=datetime.utcnow)


class JobResultResponse(BaseModel):
    """Returned when polling a job status."""
    job_id: str
    status: JobStatus
    progress: int = Field(ge=0, le=100, default=0)
    total_found: int = 0
    processed: int = 0
    results: List[LeadResult] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    processing_time_seconds: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: float
    database: str
    cache: str
    workers: Dict[str, Any]
