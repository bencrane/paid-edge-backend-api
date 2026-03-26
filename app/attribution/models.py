"""Attribution response models (PEX-67)."""

from pydantic import BaseModel

# --- Funnel ---


class FunnelStage(BaseModel):
    campaign_id: str
    platform: str
    total_spend: float
    lead_count: int
    opportunity_count: int
    closed_won_count: int
    lead_to_opportunity_rate: float
    opportunity_to_won_rate: float


class FunnelTotals(BaseModel):
    total_spend: float
    total_leads: int
    total_opportunities: int
    total_closed_won: int
    overall_lead_to_opp_rate: float
    overall_opp_to_won_rate: float


class FunnelResponse(BaseModel):
    campaigns: list[FunnelStage]
    totals: FunnelTotals


# --- Cost metrics ---


class CostPerOpportunity(BaseModel):
    campaign_id: str
    platform: str
    total_spend: float
    opportunity_count: int
    cost_per_opportunity: float


class CostPerOpportunityResponse(BaseModel):
    campaigns: list[CostPerOpportunity]


class CostPerClosedWon(BaseModel):
    campaign_id: str
    platform: str
    total_spend: float
    closed_won_count: int
    cost_per_closed_won: float


class CostPerClosedWonResponse(BaseModel):
    campaigns: list[CostPerClosedWon]


# --- Pipeline influenced ---


class PipelineCampaign(BaseModel):
    campaign_id: str
    opportunity_count: int
    pipeline_value: float
    closed_won_value: float
    closed_won_count: int


class PipelineInfluencedResponse(BaseModel):
    campaigns: list[PipelineCampaign]
    total_pipeline_value: float
    total_closed_won_value: float


# --- Lookalike profile ---


class LookalikeCompany(BaseModel):
    company_domain: str
    company_name: str
    deal_count: int
    total_revenue: float
    avg_deal_size: float


class LookalikeProfileResponse(BaseModel):
    companies: list[LookalikeCompany]
    total_companies: int
    total_revenue: float
