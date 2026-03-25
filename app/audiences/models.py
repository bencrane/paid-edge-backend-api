"""Audience segment Pydantic models (BJC-81)."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Request models ---


class AudienceCreate(BaseModel):
    name: str
    description: str | None = None
    filter_config: dict[str, Any] = Field(
        ...,
        description="Structured filter definition for segment membership criteria",
    )
    priority: str = "normal"  # normal, high


class AudienceUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    filter_config: dict[str, Any] | None = None
    priority: str | None = None


class AudienceExportRequest(BaseModel):
    platform: str = Field(
        ...,
        description="Ad platform format: linkedin, meta, or google",
    )


# --- Response models ---


class AudienceResponse(BaseModel):
    id: str
    organization_id: str
    name: str
    description: str | None = None
    filter_config: dict[str, Any]
    status: str  # active, paused, archived
    priority: str
    member_count: int = 0
    last_refreshed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class AudienceDetailResponse(AudienceResponse):
    """Segment detail with member count — same shape, explicitly typed."""
    pass


class AudienceListResponse(BaseModel):
    data: list[AudienceResponse]
    total: int


class AudienceMember(BaseModel):
    entity_id: str
    entity_type: str = "person"
    full_name: str | None = None
    work_email: str | None = None
    title: str | None = None
    company_name: str | None = None
    linkedin_url: str | None = None
    added_at: datetime | None = None


class AudienceMemberListResponse(BaseModel):
    data: list[AudienceMember]
    total: int


class SignalCard(BaseModel):
    segment_id: str
    segment_name: str
    signal_type: str
    member_count: int = 0
    trend: str = "stable"  # up, down, stable
    last_refreshed_at: datetime | None = None


class SignalListResponse(BaseModel):
    data: list[SignalCard]
