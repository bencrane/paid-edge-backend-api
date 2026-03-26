"""Analytics response models (PEX-68)."""

from datetime import date

from pydantic import BaseModel

# --- Overview ---


class KPITrend(BaseModel):
    """A KPI with current value and % change from previous period."""

    value: float
    previous_value: float
    change_pct: float | None = None


class OverviewResponse(BaseModel):
    total_spend: KPITrend
    avg_cac: KPITrend
    total_conversions: KPITrend
    total_leads: KPITrend


# --- Campaign performance ---


class CampaignPerformance(BaseModel):
    campaign_id: str
    platform: str
    total_spend: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    total_leads: int
    ctr: float
    cpc: float
    cpm: float
    cost_per_conversion: float


class CampaignPerformanceResponse(BaseModel):
    campaigns: list[CampaignPerformance]
    total: int


# --- Platform comparison ---


class PlatformBreakdown(BaseModel):
    platform: str
    campaign_count: int
    total_spend: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    total_leads: int
    ctr: float
    cpc: float
    cpm: float
    cost_per_conversion: float


class PlatformComparisonResponse(BaseModel):
    platforms: list[PlatformBreakdown]


# --- Time series ---


class TimeSeriesPoint(BaseModel):
    period: date
    spend: float
    impressions: int
    clicks: int
    conversions: int
    leads: int
    ctr: float
    cpc: float


class TimeSeriesResponse(BaseModel):
    granularity: str
    data: list[TimeSeriesPoint]
