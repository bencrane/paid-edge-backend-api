from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel


class CampaignSchedule(BaseModel):
    start_date: date | None = None
    end_date: date | None = None


class CampaignCreate(BaseModel):
    name: str
    description: str | None = None
    platforms: list[str]
    audience_segment_id: str | None = None
    budget: Decimal | None = None
    schedule: CampaignSchedule | None = None
    angle: str | None = None
    objective: str | None = None


class CampaignUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    platforms: list[str] | None = None
    audience_segment_id: str | None = None
    budget: Decimal | None = None
    schedule: CampaignSchedule | None = None
    angle: str | None = None
    objective: str | None = None


class CampaignResponse(BaseModel):
    id: str
    organization_id: str
    name: str
    description: str | None = None
    status: str
    platforms: list[str]
    audience_segment_id: str | None = None
    budget: Decimal | None = None
    schedule: CampaignSchedule | None = None
    angle: str | None = None
    objective: str | None = None
    tracked_link_url: str | None = None
    created_at: datetime
    updated_at: datetime


class CampaignListResponse(BaseModel):
    data: list[CampaignResponse]
    total: int
