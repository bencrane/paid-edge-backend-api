"""Meta Campaign CRUD + Campaign Budget Optimization (BJC-151)."""

import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# --- Objective mapping ---

META_OBJECTIVE_MAP = {
    "lead_generation": "OUTCOME_LEADS",
    "website_traffic": "OUTCOME_TRAFFIC",
    "brand_awareness": "OUTCOME_AWARENESS",
    "engagement": "OUTCOME_ENGAGEMENT",
    "conversions": "OUTCOME_SALES",
    "app_installs": "OUTCOME_APP_PROMOTION",
}

# --- Status mapping ---

PAIDEDGE_STATUS_TO_META = {
    "draft": "PAUSED",
    "active": "ACTIVE",
    "paused": "PAUSED",
    "completed": "ARCHIVED",
    "archived": "ARCHIVED",
}


def map_paidedge_status_to_meta(status: str) -> str:
    """Map PaidEdge campaign status to Meta status."""
    return PAIDEDGE_STATUS_TO_META.get(status.lower(), "PAUSED")


# --- Pydantic models ---


class MetaCampaignCreate(BaseModel):
    name: str
    objective: str
    special_ad_categories: list[str] = []
    daily_budget: int | None = None  # cents
    lifetime_budget: int | None = None  # cents
    bid_strategy: str = "LOWEST_COST_WITHOUT_CAP"
    status: str = "PAUSED"


class MetaCampaign(BaseModel):
    id: str
    name: str
    objective: str
    status: str
    effective_status: str
    daily_budget: int | None = None
    lifetime_budget: int | None = None
    bid_strategy: str | None = None
    special_ad_categories: list[str] = []
    created_time: str = ""
    updated_time: str = ""


# --- Campaign CRUD (methods on MetaAdsClient, imported as mixin pattern) ---


class MetaCampaignsMixin:
    """Campaign CRUD methods for MetaAdsClient."""

    async def create_campaign(
        self,
        name: str,
        objective: str,
        special_ad_categories: list[str] | None = None,
        daily_budget: int | None = None,
        lifetime_budget: int | None = None,
        bid_strategy: str | None = None,
        status: str = "PAUSED",
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/campaigns

        Always create as PAUSED. Activate via separate status update.
        special_ad_categories must always be present (empty array if none).
        """
        payload = {
            "name": name,
            "objective": objective,
            "special_ad_categories": special_ad_categories or [],
            "status": status,
        }
        if daily_budget is not None:
            payload["daily_budget"] = daily_budget
        if lifetime_budget is not None:
            payload["lifetime_budget"] = lifetime_budget
        if bid_strategy:
            payload["bid_strategy"] = bid_strategy

        return await self._request(
            "POST", f"{self.ad_account_id}/campaigns", data=payload
        )

    async def get_campaign(self, campaign_id: str) -> dict:
        """GET /{CAMPAIGN_ID} with full fields."""
        return await self._request(
            "GET",
            campaign_id,
            params={
                "fields": "name,objective,status,daily_budget,lifetime_budget,"
                "bid_strategy,special_ad_categories,effective_status,"
                "created_time,updated_time"
            },
        )

    async def update_campaign(self, campaign_id: str, **fields) -> dict:
        """POST /{CAMPAIGN_ID} with updated fields."""
        return await self._request("POST", campaign_id, data=fields)

    async def list_campaigns(
        self,
        status_filter: list[str] | None = None,
        limit: int = 25,
    ) -> list[dict]:
        """GET /act_{AD_ACCOUNT_ID}/campaigns with optional status filtering."""
        params = {
            "fields": "name,objective,status,effective_status,daily_budget,"
            "lifetime_budget,bid_strategy,special_ad_categories,"
            "created_time,updated_time",
            "limit": limit,
        }
        if status_filter:
            params["effective_status"] = json.dumps(status_filter)

        return await self._paginate(
            f"{self.ad_account_id}/campaigns", params=params, limit=limit
        )

    async def delete_campaign(self, campaign_id: str) -> None:
        """DELETE /{CAMPAIGN_ID}. Prefer archiving for data retention."""
        await self._request("DELETE", campaign_id)

    async def set_campaign_status(self, campaign_id: str, status: str) -> dict:
        """Update campaign status."""
        return await self._request("POST", campaign_id, data={"status": status})


# Attach methods to MetaAdsClient via import in meta_client or direct mixin
import json  # noqa: E402 — needed for list_campaigns status filter
