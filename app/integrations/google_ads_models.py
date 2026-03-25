"""Pydantic models for Google Ads integration (BJC-141)."""

from pydantic import BaseModel


class GoogleAdsAccount(BaseModel):
    customer_id: str  # No hyphens: "1234567890"
    name: str
    currency: str
    timezone: str
    is_manager: bool
    is_test: bool
    status: str | None = None


class GoogleAdsAPIErrorDetail(BaseModel):
    error_code: str
    message: str
    request_id: str | None = None


class GoogleAdsCampaign(BaseModel):
    id: str
    name: str
    status: str
    channel_type: str
    budget_resource: str | None = None
    daily_budget_dollars: float | None = None
    bidding_strategy: str | None = None
    start_date: str | None = None
    end_date: str | None = None


class GoogleAdsCampaignCreate(BaseModel):
    name: str
    daily_budget_dollars: float
    bidding_strategy: str = "maximize_conversions"
    bidding_params: dict | None = None
    target_google_search: bool = True
    target_search_network: bool = False
    start_date: str | None = None
    end_date: str | None = None
    geo_target_ids: list[int] | None = None


class GoogleAdsAdGroup(BaseModel):
    id: str
    name: str
    status: str
    campaign_resource: str | None = None
    cpc_bid_dollars: float | None = None


class GoogleAdsKeyword(BaseModel):
    criterion_id: str
    text: str
    match_type: str  # BROAD, PHRASE, EXACT
    status: str
    cpc_bid_dollars: float | None = None
    impressions: int | None = None
    clicks: int | None = None
    cost_dollars: float | None = None
    conversions: float | None = None


class GoogleAdsSearchTerm(BaseModel):
    search_term: str
    status: str  # ADDED, EXCLUDED, NONE
    campaign_name: str
    ad_group_name: str
    impressions: int
    clicks: int
    cost_dollars: float
    conversions: float


class KeywordInput(BaseModel):
    text: str
    match_type: str = "BROAD"  # BROAD, PHRASE, EXACT


class GoogleAdsRSA(BaseModel):
    ad_id: str
    ad_group_id: str
    status: str
    headlines: list[str]
    descriptions: list[str]
    final_urls: list[str]
    ad_strength: str | None = None
    approval_status: str | None = None
    impressions: int | None = None
    clicks: int | None = None
    cost_dollars: float | None = None
    conversions: float | None = None


class RSAInput(BaseModel):
    headlines: list[str]
    descriptions: list[str]
    final_url: str
    path1: str | None = None
    path2: str | None = None
    pinned_headlines: dict[int, int] | None = None
    pinned_descriptions: dict[int, int] | None = None
