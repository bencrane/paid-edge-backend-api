from datetime import date

from pydantic import BaseModel


class LinkedInAdAccount(BaseModel):
    id: int
    name: str
    currency: str
    status: str
    reference_org_urn: str | None = None


class LinkedInCampaignGroup(BaseModel):
    id: int
    name: str
    status: str
    account_urn: str
    total_budget: dict | None = None
    run_schedule: dict | None = None


class LinkedInCampaign(BaseModel):
    id: int
    name: str
    status: str
    type: str
    objective_type: str | None = None
    cost_type: str
    daily_budget: dict | None = None
    total_budget: dict | None = None
    unit_cost: dict | None = None
    targeting_criteria: dict | None = None
    run_schedule: dict | None = None
    offsite_delivery_enabled: bool = False
    campaign_group_urn: str | None = None
    account_urn: str | None = None


class LinkedInCampaignCreate(BaseModel):
    name: str
    campaign_type: str
    objective: str
    cost_type: str
    daily_budget_amount: str
    daily_budget_currency: str = "USD"
    unit_cost_amount: str | None = None
    offsite_delivery: bool = False
    start_date: date | None = None
    end_date: date | None = None
    targeting: dict  # Pre-built targeting criteria


class LinkedInCreative(BaseModel):
    id: int
    campaign_urn: str
    content_reference: str  # post URN
    intended_status: str
    review_status: str | None = None
    serving_statuses: list[str] | None = None


class LinkedInCreativeCreate(BaseModel):
    campaign_id: int
    media_type: str  # image, document, video, inmail
    media_url: str | None = None
    commentary: str
    title: str
    lead_gen_form_id: int | None = None


class LinkedInDMPSegment(BaseModel):
    id: str
    name: str
    type: str  # COMPANY, USER
    status: str  # BUILDING, READY, UPDATING, FAILED, ARCHIVED, EXPIRED
    matched_member_count: int | None = None
    destination_segment_id: str | None = None  # adSegment URN for targeting
    account_urn: str


class LinkedInAudienceSyncResult(BaseModel):
    segment_id: str
    segment_type: str
    total_uploaded: int
    batches_completed: int
    status: str
    matched_count: int | None = None
    ad_segment_urn: str | None = None  # Available when status=READY


class LinkedInAPIErrorDetail(BaseModel):
    status: int
    service_error_code: int | None = None
    message: str
