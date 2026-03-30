import logging
import secrets
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import httpx
import jwt
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from app.config import settings
from app.dependencies import get_current_user, get_supabase, get_tenant
from app.shared.errors import BadRequestError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth/linkedin", tags=["linkedin-auth"])

LINKEDIN_AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"
LINKEDIN_TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
LINKEDIN_API_BASE = "https://api.linkedin.com/rest"

LINKEDIN_SCOPES = (
    "r_ads rw_ads r_ads_reporting r_organization_social "
    "rw_dmp_segments rw_conversions r_marketing_leadgen_automation r_basicprofile"
)

# JWT algorithm for state tokens
STATE_JWT_ALGORITHM = "HS256"
STATE_JWT_EXPIRY_MINUTES = 30


# --- Response models ---


class LinkedInStatusResponse(BaseModel):
    connected: bool
    expires_in_days: int | None = None
    ad_accounts: list[dict] = []
    needs_reauth: bool = False
    selected_ad_account_id: int | None = None


# --- Routes ---


@router.get("/authorize")
async def linkedin_authorize(
    request: Request,
    org_id: str = Query(..., description="Organization ID connecting LinkedIn"),
    user=Depends(get_current_user),
):
    """Generate LinkedIn OAuth authorization URL and redirect."""
    nonce = secrets.token_urlsafe(32)
    state_payload = {
        "org_id": org_id,
        "user_id": user.id,
        "nonce": nonce,
        "exp": datetime.now(UTC) + timedelta(minutes=STATE_JWT_EXPIRY_MINUTES),
    }
    state = jwt.encode(
        state_payload,
        settings.SUPABASE_SERVICE_ROLE_KEY,
        algorithm=STATE_JWT_ALGORITHM,
    )

    params = {
        "response_type": "code",
        "client_id": settings.LINKEDIN_CLIENT_ID,
        "redirect_uri": settings.LINKEDIN_REDIRECT_URI,
        "scope": LINKEDIN_SCOPES,
        "state": state,
    }
    auth_url = f"{LINKEDIN_AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url=auth_url)


@router.get("/callback")
async def linkedin_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    error_description: str | None = Query(None),
    supabase=Depends(get_supabase),
):
    """Handle OAuth callback from LinkedIn."""
    # Handle LinkedIn error responses
    if isinstance(error, str) and error:
        logger.warning("LinkedIn OAuth error: %s — %s", error, error_description)
        redirect_url = (
            f"{settings.FRONTEND_URL}/settings/integrations"
            f"?linkedin_error={error}"
        )
        return RedirectResponse(url=redirect_url)

    if not isinstance(code, str) or not isinstance(state, str) or not code or not state:
        raise BadRequestError(detail="Missing code or state parameter")

    # Validate state JWT
    try:
        state_payload = jwt.decode(
            state,
            settings.SUPABASE_SERVICE_ROLE_KEY,
            algorithms=[STATE_JWT_ALGORITHM],
        )
    except jwt.ExpiredSignatureError:
        raise BadRequestError(detail="OAuth state expired. Please try again.")
    except jwt.InvalidTokenError:
        raise BadRequestError(detail="Invalid OAuth state.")

    org_id = state_payload["org_id"]
    user_id = state_payload["user_id"]

    # Exchange authorization code for tokens
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            LINKEDIN_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": settings.LINKEDIN_CLIENT_ID,
                "client_secret": settings.LINKEDIN_CLIENT_SECRET,
                "redirect_uri": settings.LINKEDIN_REDIRECT_URI,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if token_resp.status_code != 200:
        logger.error("LinkedIn token exchange failed: %s", token_resp.text)
        raise BadRequestError(detail="Failed to exchange authorization code for tokens.")

    token_data = token_resp.json()
    now = datetime.now(UTC).replace(microsecond=0)

    access_token = token_data["access_token"]
    access_token_expires_at = (
        now + timedelta(seconds=token_data["expires_in"])
    ).isoformat()
    refresh_token = token_data.get("refresh_token", "")
    refresh_token_expires_at = (
        now + timedelta(seconds=token_data.get("refresh_token_expires_in", 31536000))
    ).isoformat()
    scope = token_data.get("scope", "")

    # Fetch member URN
    li_headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": settings.LINKEDIN_API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }

    member_urn = ""
    async with httpx.AsyncClient() as client:
        me_resp = await client.get(f"{LINKEDIN_API_BASE}/me", headers=li_headers)
        if me_resp.status_code == 200:
            me_data = me_resp.json()
            member_urn = me_data.get("id", "")
            if not member_urn.startswith("urn:"):
                member_urn = f"urn:li:person:{member_urn}"
        else:
            logger.warning("Failed to fetch LinkedIn member profile: %s", me_resp.text)

    # Discover ad accounts
    ad_accounts = []
    async with httpx.AsyncClient() as client:
        accounts_resp = await client.get(
            f"{LINKEDIN_API_BASE}/adAccountUsers",
            params={"q": "authenticatedUser"},
            headers=li_headers,
        )
        if accounts_resp.status_code == 200:
            accounts_data = accounts_resp.json()
            for element in accounts_data.get("elements", []):
                account_urn = element.get("account", "")
                account_id = int(account_urn.split(":")[-1]) if account_urn else 0
                ad_accounts.append({
                    "id": account_id,
                    "name": element.get("account", ""),
                    "role": element.get("role", ""),
                })
        else:
            logger.warning(
                "Failed to fetch LinkedIn ad accounts: %s", accounts_resp.text
            )

    # Upsert provider config
    config_payload = {
        "access_token": access_token,
        "access_token_expires_at": access_token_expires_at,
        "refresh_token": refresh_token,
        "refresh_token_expires_at": refresh_token_expires_at,
        "member_urn": member_urn,
        "scope": scope,
        "ad_accounts": ad_accounts,
        "selected_ad_account_id": None,
        "connected_by": user_id,
    }

    supabase.table("provider_configs").upsert(
        {
            "organization_id": org_id,
            "provider": "linkedin_ads",
            "config": config_payload,
            "is_active": True,
        },
        on_conflict="organization_id,provider",
    ).execute()

    logger.info(
        "LinkedIn OAuth completed for org %s by user %s — %d ad accounts found",
        org_id,
        user_id,
        len(ad_accounts),
    )

    redirect_url = (
        f"{settings.FRONTEND_URL}/settings/integrations?linkedin_connected=true"
    )
    return RedirectResponse(url=redirect_url)


@router.get("/status", response_model=LinkedInStatusResponse)
async def linkedin_status(
    tenant=Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    """Return LinkedIn connection health for the current tenant."""
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", tenant.id)
        .eq("provider", "linkedin_ads")
        .maybe_single()
        .execute()
    )

    if not res.data or not res.data.get("is_active"):
        return LinkedInStatusResponse(connected=False)

    config = res.data["config"]
    now = datetime.now(UTC)

    access_expires_at = datetime.fromisoformat(config["access_token_expires_at"])
    refresh_expires_at = datetime.fromisoformat(config["refresh_token_expires_at"])

    expires_in_days = max(0, (access_expires_at - now).days)
    needs_reauth = refresh_expires_at <= now

    return LinkedInStatusResponse(
        connected=True,
        expires_in_days=expires_in_days,
        ad_accounts=config.get("ad_accounts", []),
        needs_reauth=needs_reauth,
        selected_ad_account_id=config.get("selected_ad_account_id"),
    )
