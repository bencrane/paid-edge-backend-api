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


class LinkedInAPIErrorDetail(BaseModel):
    status: int
    service_error_code: int | None = None
    message: str
