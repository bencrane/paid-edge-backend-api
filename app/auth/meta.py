"""Meta OAuth 2.0 + Business Manager auth endpoints (BJC-147)."""

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

router = APIRouter(prefix="/auth/meta", tags=["meta-auth"])

META_OAUTH_URL = "https://www.facebook.com/v25.0/dialog/oauth"
META_TOKEN_URL = "https://graph.facebook.com/v25.0/oauth/access_token"
META_GRAPH_URL = "https://graph.facebook.com/v25.0"

META_SCOPES = (
    "ads_management,ads_read,business_management,leads_retrieval,"
    "pages_manage_metadata,pages_read_engagement,pages_show_list"
)

STATE_JWT_ALGORITHM = "HS256"
STATE_JWT_EXPIRY_MINUTES = 30


# --- Response models ---


class MetaStatusResponse(BaseModel):
    connected: bool
    token_type: str | None = None
    expires_in_days: int | None = None
    ad_accounts: list[dict] = []
    selected_ad_account_id: str | None = None
    page_connected: bool = False
    needs_reauth: bool = False


# --- Routes ---


@router.get("/authorize")
async def meta_authorize(
    request: Request,
    org_id: str = Query(..., description="Organization ID connecting Meta"),
    user=Depends(get_current_user),
):
    """Generate Meta OAuth authorization URL and redirect."""
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
        "client_id": settings.META_APP_ID,
        "redirect_uri": settings.META_REDIRECT_URI,
        "scope": META_SCOPES,
        "state": state,
    }
    auth_url = f"{META_OAUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url=auth_url)


@router.get("/callback")
async def meta_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    error_description: str | None = Query(None),
    supabase=Depends(get_supabase),
):
    """Handle OAuth callback from Meta."""
    if isinstance(error, str) and error:
        logger.warning("Meta OAuth error: %s — %s", error, error_description)
        redirect_url = (
            f"{settings.FRONTEND_URL}/settings/integrations"
            f"?meta_error={error}"
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

    # Step 1: Exchange code for short-lived token
    async with httpx.AsyncClient() as client:
        token_resp = await client.get(
            META_TOKEN_URL,
            params={
                "client_id": settings.META_APP_ID,
                "redirect_uri": settings.META_REDIRECT_URI,
                "client_secret": settings.META_APP_SECRET,
                "code": code,
            },
        )

    if token_resp.status_code != 200:
        logger.error("Meta token exchange failed: %s", token_resp.text)
        raise BadRequestError(detail="Failed to exchange authorization code.")

    short_lived_token = token_resp.json()["access_token"]

    # Step 2: Exchange short-lived for long-lived token (60-day)
    async with httpx.AsyncClient() as client:
        ll_resp = await client.get(
            META_TOKEN_URL,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": settings.META_APP_ID,
                "client_secret": settings.META_APP_SECRET,
                "fb_exchange_token": short_lived_token,
            },
        )

    if ll_resp.status_code != 200:
        logger.error("Meta long-lived token exchange failed: %s", ll_resp.text)
        raise BadRequestError(detail="Failed to obtain long-lived token.")

    ll_data = ll_resp.json()
    access_token = ll_data["access_token"]
    expires_in = ll_data.get("expires_in", 5184000)  # Default 60 days
    now = datetime.now(UTC).replace(microsecond=0)
    token_expires_at = (now + timedelta(seconds=expires_in)).isoformat()

    # Step 3: Discover accessible ad accounts
    ad_accounts = []
    async with httpx.AsyncClient() as client:
        accounts_resp = await client.get(
            f"{META_GRAPH_URL}/me/adaccounts",
            params={
                "access_token": access_token,
                "fields": "id,name,currency,timezone_name,account_status",
                "limit": 100,
            },
        )
        if accounts_resp.status_code == 200:
            for acct in accounts_resp.json().get("data", []):
                ad_accounts.append({
                    "id": acct["id"],
                    "name": acct.get("name", ""),
                    "currency": acct.get("currency", "USD"),
                    "timezone_name": acct.get("timezone_name", ""),
                    "account_status": acct.get("account_status", 0),
                })
        else:
            logger.warning("Failed to fetch Meta ad accounts: %s", accounts_resp.text)

    # Upsert provider config
    config_payload = {
        "access_token": access_token,
        "token_type": "long_lived_user",
        "token_expires_at": token_expires_at,
        "business_id": None,
        "ad_accounts": ad_accounts,
        "selected_ad_account_id": None,
        "page_id": None,
        "page_access_token": None,
        "system_user_id": None,
        "app_id": settings.META_APP_ID,
        "connected_by": user_id,
    }

    supabase.table("provider_configs").upsert(
        {
            "organization_id": org_id,
            "provider": "meta_ads",
            "config": config_payload,
            "is_active": True,
        },
        on_conflict="organization_id,provider",
    ).execute()

    logger.info(
        "Meta OAuth completed for org %s by user %s — %d ad accounts found",
        org_id,
        user_id,
        len(ad_accounts),
    )

    redirect_url = (
        f"{settings.FRONTEND_URL}/settings/integrations?meta_connected=true"
    )
    return RedirectResponse(url=redirect_url)


@router.get("/status", response_model=MetaStatusResponse)
async def meta_status(
    tenant=Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    """Return Meta connection health for the current tenant."""
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", tenant.id)
        .eq("provider", "meta_ads")
        .maybe_single()
        .execute()
    )

    if not res.data or not res.data.get("is_active"):
        return MetaStatusResponse(connected=False)

    config = res.data["config"]
    now = datetime.now(UTC)

    token_type = config.get("token_type", "long_lived_user")
    expires_in_days = None
    needs_reauth = False

    token_expires_at_str = config.get("token_expires_at")
    if token_expires_at_str:
        token_expires_at = datetime.fromisoformat(token_expires_at_str)
        if token_type != "system_user_non_expiring":
            expires_in_days = max(0, (token_expires_at - now).days)
            needs_reauth = token_expires_at <= now

    return MetaStatusResponse(
        connected=True,
        token_type=token_type,
        expires_in_days=expires_in_days,
        ad_accounts=config.get("ad_accounts", []),
        selected_ad_account_id=config.get("selected_ad_account_id"),
        page_connected=bool(config.get("page_id")),
        needs_reauth=needs_reauth,
    )
