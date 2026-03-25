"""Meta Ad Set CRUD (BJC-153)."""

import json
import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)


# --- Pydantic models ---


class MetaAdSetCreate(BaseModel):
    campaign_id: str
    name: str
    targeting: dict
    optimization_goal: str
    billing_event: str = "IMPRESSIONS"
    daily_budget: int | None = None
    lifetime_budget: int | None = None
    bid_strategy: str | None = None
    bid_amount: int | None = None
    start_time: str | None = None
    end_time: str | None = None
    status: str = "PAUSED"


class MetaAdSet(BaseModel):
    id: str
    name: str
    campaign_id: str
    targeting: dict
    optimization_goal: str
    billing_event: str = "IMPRESSIONS"
    daily_budget: int | None = None
    lifetime_budget: int | None = None
    bid_strategy: str | None = None
    status: str = ""
    effective_status: str = ""
    start_time: str | None = None
    end_time: str | None = None


# --- Ad Set CRUD methods (mixin for MetaAdsClient) ---


class MetaAdSetsMixin:
    """Ad Set CRUD methods for MetaAdsClient."""

    async def create_ad_set(
        self,
        campaign_id: str,
        name: str,
        targeting: dict,
        optimization_goal: str,
        billing_event: str = "IMPRESSIONS",
        daily_budget: int | None = None,
        lifetime_budget: int | None = None,
        bid_strategy: str | None = None,
        bid_amount: int | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        status: str = "PAUSED",
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/adsets"""
        payload = {
            "campaign_id": campaign_id,
            "name": name,
            "targeting": json.dumps(targeting),
            "optimization_goal": optimization_goal,
            "billing_event": billing_event,
            "status": status,
        }
        if daily_budget is not None:
            payload["daily_budget"] = daily_budget
        if lifetime_budget is not None:
            payload["lifetime_budget"] = lifetime_budget
        if bid_strategy:
            payload["bid_strategy"] = bid_strategy
        if bid_amount is not None:
            payload["bid_amount"] = bid_amount
        if start_time:
            payload["start_time"] = start_time
        if end_time:
            payload["end_time"] = end_time

        return await self._request(
            "POST", f"{self.ad_account_id}/adsets", data=payload
        )

    async def get_ad_set(self, adset_id: str) -> dict:
        """GET /{ADSET_ID} with full fields."""
        return await self._request(
            "GET",
            adset_id,
            params={
                "fields": "name,campaign_id,targeting,optimization_goal,"
                "billing_event,daily_budget,lifetime_budget,bid_strategy,"
                "start_time,end_time,status,effective_status,promoted_object"
            },
        )

    async def update_ad_set(self, adset_id: str, **fields) -> dict:
        """POST /{ADSET_ID} with updated fields.

        Note: budget changes limited to 4 per hour per ad set.
        """
        if "targeting" in fields and isinstance(fields["targeting"], dict):
            fields["targeting"] = json.dumps(fields["targeting"])
        return await self._request("POST", adset_id, data=fields)

    async def list_ad_sets(
        self,
        campaign_id: str | None = None,
        limit: int = 25,
    ) -> list[dict]:
        """GET /act_{AD_ACCOUNT_ID}/adsets or GET /{CAMPAIGN_ID}/adsets"""
        path = (
            f"{campaign_id}/adsets" if campaign_id
            else f"{self.ad_account_id}/adsets"
        )
        params = {
            "fields": "name,campaign_id,targeting,optimization_goal,"
            "billing_event,daily_budget,lifetime_budget,bid_strategy,"
            "start_time,end_time,status,effective_status",
            "limit": limit,
        }
        return await self._paginate(path, params=params, limit=limit)

    async def delete_ad_set(self, adset_id: str) -> None:
        """DELETE /{ADSET_ID}"""
        await self._request("DELETE", adset_id)
