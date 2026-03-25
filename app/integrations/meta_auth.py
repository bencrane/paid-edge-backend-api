"""Meta system user token management and auth utilities (BJC-147)."""

import hashlib
import hmac
import logging
from datetime import UTC, datetime, timedelta

import httpx
from supabase import Client

from app.config import settings

logger = logging.getLogger(__name__)

META_GRAPH_URL = f"https://graph.facebook.com/{settings.META_API_VERSION}"


class MetaReauthRequiredError(Exception):
    """Raised when the Meta token has expired and re-authorization is needed."""

    def __init__(self, org_id: str):
        self.org_id = org_id
        super().__init__(
            f"Meta access token expired for org {org_id}. Re-authorization required."
        )


def compute_appsecret_proof(app_secret: str, access_token: str) -> str:
    """HMAC-SHA256 of access_token using app_secret as key."""
    return hmac.new(
        app_secret.encode("utf-8"),
        access_token.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def get_valid_meta_token(org_id: str, supabase: Client) -> str:
    """Get a valid Meta access token for a tenant.

    If system user token exists and is non-expiring → return it.
    If 60-day system user token → check expiry, refresh if < 7 days remaining.
    If only long-lived user token → return it, warn about expiry.
    """
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .eq("provider", "meta_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise MetaReauthRequiredError(org_id)

    config = res.data["config"]
    token_type = config.get("token_type", "long_lived_user")

    # Non-expiring system user tokens always valid
    if token_type == "system_user_non_expiring":
        return config["access_token"]

    # Check expiry
    token_expires_at_str = config.get("token_expires_at")
    if not token_expires_at_str:
        raise MetaReauthRequiredError(org_id)

    now = datetime.now(UTC)
    token_expires_at = datetime.fromisoformat(token_expires_at_str)

    if token_expires_at <= now:
        raise MetaReauthRequiredError(org_id)

    # Proactive refresh if < 7 days remaining
    if (token_expires_at - now).days < 7:
        config = await _refresh_meta_token(org_id, config, supabase)

    return config["access_token"]


async def _refresh_meta_token(
    org_id: str, config: dict, supabase: Client
) -> dict:
    """Refresh Meta access token by exchanging for a new 60-day token."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{META_GRAPH_URL}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": settings.META_APP_ID,
                "client_secret": settings.META_APP_SECRET,
                "fb_exchange_token": config["access_token"],
            },
        )

    if resp.status_code != 200:
        logger.error("Meta token refresh failed for org %s: %s", org_id, resp.text)
        raise MetaReauthRequiredError(org_id)

    token_data = resp.json()
    now = datetime.now(UTC).replace(microsecond=0)

    config["access_token"] = token_data["access_token"]
    expires_in = token_data.get("expires_in", 5184000)
    config["token_expires_at"] = (
        now + timedelta(seconds=expires_in)
    ).isoformat()

    # Persist updated token
    supabase.table("provider_configs").update({"config": config}).eq(
        "organization_id", org_id
    ).eq("provider", "meta_ads").execute()

    logger.info("Meta access token refreshed for org %s", org_id)
    return config


async def generate_system_user_token(
    org_id: str,
    supabase: Client,
) -> str:
    """Generate a system user token for ongoing API calls.

    Steps:
    1. Install app on system user
    2. Compute appsecret_proof
    3. Generate token
    4. Update provider_configs
    """
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .eq("provider", "meta_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise MetaReauthRequiredError(org_id)

    config = res.data["config"]
    admin_token = config["access_token"]
    system_user_id = settings.META_SYSTEM_USER_ID
    proof = compute_appsecret_proof(settings.META_APP_SECRET, admin_token)

    async with httpx.AsyncClient() as client:
        # Install app on system user
        await client.post(
            f"{META_GRAPH_URL}/{system_user_id}/applications",
            data={
                "business_app": settings.META_APP_ID,
                "access_token": admin_token,
            },
        )

        # Generate system user token
        resp = await client.post(
            f"{META_GRAPH_URL}/{system_user_id}/access_tokens",
            data={
                "business_app": settings.META_APP_ID,
                "appsecret_proof": proof,
                "scope": "ads_management,ads_read,business_management,leads_retrieval",
                "set_token_expires_in_60_days": "true",
                "access_token": admin_token,
            },
        )

    if resp.status_code != 200:
        logger.error(
            "System user token generation failed for org %s: %s",
            org_id,
            resp.text,
        )
        raise MetaReauthRequiredError(org_id)

    token_data = resp.json()
    new_token = token_data["access_token"]
    now = datetime.now(UTC).replace(microsecond=0)

    config["access_token"] = new_token
    config["token_type"] = "system_user"
    config["system_user_id"] = system_user_id
    config["token_expires_at"] = (now + timedelta(days=60)).isoformat()

    supabase.table("provider_configs").update({"config": config}).eq(
        "organization_id", org_id
    ).eq("provider", "meta_ads").execute()

    logger.info("System user token generated for org %s", org_id)
    return new_token


async def acquire_page_token(
    org_id: str, page_id: str, supabase: Client
) -> str:
    """Get a non-expiring page access token from a long-lived user token.

    Required for lead gen forms, webhook subscriptions, and ad creatives with page_id.
    """
    token = await get_valid_meta_token(org_id, supabase)

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{META_GRAPH_URL}/{page_id}",
            params={
                "fields": "access_token",
                "access_token": token,
            },
        )

    if resp.status_code != 200:
        logger.error("Page token acquisition failed for org %s: %s", org_id, resp.text)
        raise MetaReauthRequiredError(org_id)

    page_access_token = resp.json()["access_token"]

    # Update provider_configs with page info
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .eq("provider", "meta_ads")
        .maybe_single()
        .execute()
    )
    if res.data:
        config = res.data["config"]
        config["page_id"] = page_id
        config["page_access_token"] = page_access_token
        supabase.table("provider_configs").update({"config": config}).eq(
            "organization_id", org_id
        ).eq("provider", "meta_ads").execute()

    logger.info("Page token acquired for org %s, page %s", org_id, page_id)
    return page_access_token
