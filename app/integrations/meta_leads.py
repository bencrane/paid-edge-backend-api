"""Meta Lead Ads — form management + real-time webhook integration (BJC-165)."""

import hashlib
import hmac
import json
import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)


# --- Models ---


class MetaLeadForm(BaseModel):
    id: str
    name: str = ""
    status: str = ""
    questions: list[dict] = []


class MetaLead(BaseModel):
    id: str
    created_time: str = ""
    ad_id: str = ""
    form_id: str = ""
    field_data: dict = {}


# --- Lead field parser ---


def parse_lead_field_data(field_data: list[dict]) -> dict:
    """Parse Meta's field_data array into flat dict.

    Input: [{"name": "full_name", "values": ["Jane Smith"]}, ...]
    Output: {"full_name": "Jane Smith", ...}
    """
    result = {}
    for field in field_data:
        name = field.get("name", "")
        values = field.get("values", [])
        result[name] = values[0] if values else ""
    return result


# --- Webhook signature verification ---


def verify_meta_webhook_signature(
    payload: bytes,
    signature: str,
    app_secret: str,
) -> bool:
    """Verify HMAC-SHA256 signature of webhook payload."""
    expected = hmac.new(
        app_secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)


# --- Lead form + retrieval methods (mixin for MetaAdsClient) ---


class MetaLeadsMixin:
    """Lead form and retrieval methods for MetaAdsClient."""

    async def create_lead_form(
        self,
        page_id: str,
        name: str,
        questions: list[dict],
        privacy_policy_url: str,
        is_optimized_for_quality: bool = True,
        tracking_parameters: dict | None = None,
        page_access_token: str | None = None,
    ) -> dict:
        """POST /{PAGE_ID}/leadgen_forms — requires page access token."""
        data = {
            "name": name,
            "questions": json.dumps(questions),
            "privacy_policy": json.dumps({"url": privacy_policy_url}),
            "is_optimized_for_quality": str(is_optimized_for_quality).lower(),
        }
        if tracking_parameters:
            data["tracking_parameters"] = json.dumps(tracking_parameters)

        # Override token if page token provided
        params = {}
        if page_access_token:
            params["access_token"] = page_access_token

        return await self._request("POST", f"{page_id}/leadgen_forms", data=data, params=params)

    async def get_lead_form(self, form_id: str) -> dict:
        """GET /{FORM_ID} with fields."""
        return await self._request(
            "GET", form_id,
            params={"fields": "name,status,questions,privacy_policy"},
        )

    async def list_lead_forms(self, page_id: str) -> list[dict]:
        """GET /{PAGE_ID}/leadgen_forms"""
        return await self._paginate(
            f"{page_id}/leadgen_forms",
            params={"fields": "name,status,questions"},
        )

    async def get_leads_by_form(
        self,
        form_id: str,
        since: int | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """GET /{FORM_ID}/leads — leads from a specific form."""
        params = {
            "fields": "created_time,id,ad_id,form_id,field_data",
            "limit": limit,
        }
        if since:
            params["filtering"] = json.dumps([{
                "field": "time_created",
                "operator": "GREATER_THAN",
                "value": since,
            }])
        return await self._paginate(f"{form_id}/leads", params=params, limit=limit)

    async def get_leads_by_ad(self, ad_id: str, limit: int = 50) -> list[dict]:
        """GET /{AD_ID}/leads — leads from a specific ad."""
        return await self._paginate(
            f"{ad_id}/leads",
            params={
                "fields": "created_time,id,ad_id,form_id,field_data",
                "limit": limit,
            },
            limit=limit,
        )

    async def get_lead(self, lead_id: str) -> dict:
        """GET /{LEAD_ID} — individual lead details."""
        return await self._request(
            "GET", lead_id,
            params={"fields": "created_time,id,ad_id,form_id,field_data"},
        )

    async def subscribe_to_lead_webhooks(
        self,
        page_id: str,
        page_access_token: str,
    ) -> dict:
        """POST /{PAGE_ID}/subscribed_apps — subscribe for real-time lead notifications."""
        return await self._request(
            "POST",
            f"{page_id}/subscribed_apps",
            data={"subscribed_fields": "leadgen"},
            params={"access_token": page_access_token},
        )


# --- Periodic polling backup ---


async def poll_leads_for_tenant(
    tenant_id: str,
    since_timestamp: int,
    supabase,
    meta_client,
) -> list[dict]:
    """Backup polling for leads in case webhook misses events."""
    # Get active form IDs from provider_configs
    res = (
        supabase.table("provider_configs")
        .select("config")
        .eq("organization_id", tenant_id)
        .eq("provider", "meta_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        return []

    config = res.data.get("config", {})
    form_ids = config.get("lead_gen_forms", [])
    all_leads = []

    for form_id in form_ids:
        leads = await meta_client.get_leads_by_form(
            form_id, since=since_timestamp
        )
        for lead in leads:
            field_data = lead.get("field_data", [])
            parsed = parse_lead_field_data(field_data) if isinstance(field_data, list) else field_data
            all_leads.append({
                "lead_id": lead.get("id"),
                "form_id": form_id,
                "ad_id": lead.get("ad_id"),
                "created_time": lead.get("created_time"),
                "fields": parsed,
            })

    return all_leads
