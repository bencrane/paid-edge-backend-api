"""Google Ads token refresh + credential management (BJC-140)."""

import logging

import httpx
from supabase import Client

from app.config import settings

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


class GoogleAdsReauthRequiredError(Exception):
    """Raised when the Google Ads refresh token is invalid and re-authorization is needed."""

    def __init__(self, org_id: str):
        self.org_id = org_id
        super().__init__(
            f"Google Ads refresh token invalid for org {org_id}. Re-authorization required."
        )


async def get_google_ads_credentials(org_id: str, supabase: Client) -> dict:
    """Get valid Google Ads credentials for a tenant, refreshing the access token.

    Returns dict with: access_token, refresh_token, customer_id, developer_token, mcc_id
    """
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .eq("provider", "google_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise GoogleAdsReauthRequiredError(org_id)

    config = res.data["config"]
    refresh_token = config.get("refresh_token")
    if not refresh_token:
        raise GoogleAdsReauthRequiredError(org_id)

    # Exchange refresh token for a fresh access token (access tokens expire after 1 hour)
    access_token = await _refresh_access_token(org_id, refresh_token)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "customer_id": config.get("selected_customer_id"),
        "developer_token": settings.GOOGLE_ADS_DEVELOPER_TOKEN,
        "mcc_id": settings.GOOGLE_ADS_MCC_ID,
    }


async def _refresh_access_token(org_id: str, refresh_token: str) -> str:
    """Exchange a refresh token for a fresh access token."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": settings.GOOGLE_ADS_CLIENT_ID,
                "client_secret": settings.GOOGLE_ADS_CLIENT_SECRET,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if resp.status_code != 200:
        error_data = resp.json() if resp.status_code < 500 else {}
        error_code = error_data.get("error", "")
        if error_code == "invalid_grant":
            logger.error(
                "Google Ads refresh token revoked for org %s: %s",
                org_id,
                resp.text,
            )
            raise GoogleAdsReauthRequiredError(org_id)
        logger.error(
            "Google Ads token refresh failed for org %s: %s",
            org_id,
            resp.text,
        )
        raise GoogleAdsReauthRequiredError(org_id)

    token_data = resp.json()
    logger.info("Google Ads access token refreshed for org %s", org_id)
    return token_data["access_token"]
