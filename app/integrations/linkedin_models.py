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


class LinkedInAPIErrorDetail(BaseModel):
    status: int
    service_error_code: int | None = None
    message: str
