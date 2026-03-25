import logging
from datetime import UTC, datetime, timedelta

import httpx
from supabase import Client

from app.config import settings

logger = logging.getLogger(__name__)

LINKEDIN_TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"


class LinkedInReauthRequiredError(Exception):
    """Raised when the LinkedIn refresh token has expired and re-authorization is needed."""

    def __init__(self, org_id: str):
        self.org_id = org_id
        super().__init__(
            f"LinkedIn refresh token expired for org {org_id}. Re-authorization required."
        )


async def get_valid_linkedin_token(org_id: str, supabase: Client) -> str:
    """Get a valid LinkedIn access token for a tenant, refreshing if needed."""
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .eq("provider", "linkedin_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise LinkedInReauthRequiredError(org_id)

    config = res.data["config"]
    now = datetime.now(UTC)

    # Check refresh token expiry
    refresh_expires_at = datetime.fromisoformat(config["refresh_token_expires_at"])
    if refresh_expires_at <= now:
        raise LinkedInReauthRequiredError(org_id)

    # Warn if refresh token is expiring within 14 days
    days_until_refresh_expiry = (refresh_expires_at - now).days
    if days_until_refresh_expiry < 14:
        logger.warning(
            "LinkedIn refresh token for org %s expires in %d days. "
            "Tenant must re-authorize soon.",
            org_id,
            days_until_refresh_expiry,
        )

    # Proactive refresh if access token expires within 7 days
    access_expires_at = datetime.fromisoformat(config["access_token_expires_at"])
    if access_expires_at <= now or (access_expires_at - now).days < 7:
        config = await _refresh_access_token(org_id, config, supabase)

    return config["access_token"]


async def _refresh_access_token(
    org_id: str, config: dict, supabase: Client
) -> dict:
    """Refresh the LinkedIn access token using the refresh token."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            LINKEDIN_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": config["refresh_token"],
                "client_id": settings.LINKEDIN_CLIENT_ID,
                "client_secret": settings.LINKEDIN_CLIENT_SECRET,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if resp.status_code != 200:
        logger.error(
            "LinkedIn token refresh failed for org %s: %s", org_id, resp.text
        )
        raise LinkedInReauthRequiredError(org_id)

    token_data = resp.json()
    now = datetime.now(UTC)

    config["access_token"] = token_data["access_token"]
    config["access_token_expires_at"] = (
        now.replace(microsecond=0) + timedelta(seconds=token_data["expires_in"])
    ).isoformat()

    if "refresh_token" in token_data:
        config["refresh_token"] = token_data["refresh_token"]
        config["refresh_token_expires_at"] = (
            now.replace(microsecond=0)
            + timedelta(seconds=token_data["refresh_token_expires_in"])
        ).isoformat()

    # Persist updated tokens
    supabase.table("provider_configs").update({"config": config}).eq(
        "organization_id", org_id
    ).eq("provider", "linkedin_ads").execute()

    logger.info("LinkedIn access token refreshed for org %s", org_id)
    return config
