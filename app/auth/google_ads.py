"""Google Ads OAuth 2.0 flow + account management (BJC-140)."""

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
from app.shared.errors import BadRequestError, NotFoundError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth/google-ads", tags=["google-ads-auth"])

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_ADS_SCOPE = "https://www.googleapis.com/auth/adwords"

STATE_JWT_ALGORITHM = "HS256"
STATE_JWT_EXPIRY_MINUTES = 30


# --- Response models ---


class GoogleAdsAccount(BaseModel):
    customer_id: str
    name: str
    currency: str
    timezone: str
    is_manager: bool
    is_test: bool


class GoogleAdsStatusResponse(BaseModel):
    connected: bool
    accessible_accounts: list[dict] = []
    selected_customer_id: str | None = None
    needs_reauth: bool = False


class AccountSelectRequest(BaseModel):
    customer_id: str


# --- Routes ---


@router.get("/authorize")
async def google_ads_authorize(
    request: Request,
    org_id: str = Query(..., description="Organization ID connecting Google Ads"),
    user=Depends(get_current_user),
):
    """Generate Google OAuth authorization URL and redirect."""
    nonce = secrets.token_urlsafe(32)
    state_payload = {
        "org_id": org_id,
        "user_id": user.id,
        "nonce": nonce,
        "exp": datetime.now(UTC) + timedelta(minutes=STATE_JWT_EXPIRY_MINUTES),
    }
    state = jwt.encode(
        state_payload, settings.SUPABASE_JWT_SECRET, algorithm=STATE_JWT_ALGORITHM
    )

    params = {
        "client_id": settings.GOOGLE_ADS_CLIENT_ID,
        "redirect_uri": settings.GOOGLE_ADS_REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_ADS_SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
        "include_granted_scopes": "true",
    }
    auth_url = f"{GOOGLE_AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url=auth_url)


@router.get("/callback")
async def google_ads_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    error_description: str | None = Query(None),
    supabase=Depends(get_supabase),
):
    """Handle OAuth callback from Google."""
    if error:
        logger.warning("Google Ads OAuth error: %s — %s", error, error_description)
        redirect_url = (
            f"{settings.FRONTEND_URL}/settings/integrations"
            f"?google_ads_error={error}"
        )
        return RedirectResponse(url=redirect_url)

    if not code or not state:
        raise BadRequestError(detail="Missing code or state parameter")

    # Validate state JWT
    try:
        state_payload = jwt.decode(
            state,
            settings.SUPABASE_JWT_SECRET,
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
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.GOOGLE_ADS_CLIENT_ID,
                "client_secret": settings.GOOGLE_ADS_CLIENT_SECRET,
                "redirect_uri": settings.GOOGLE_ADS_REDIRECT_URI,
                "grant_type": "authorization_code",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if token_resp.status_code != 200:
        logger.error("Google Ads token exchange failed: %s", token_resp.text)
        raise BadRequestError(
            detail="Failed to exchange authorization code for tokens."
        )

    token_data = token_resp.json()
    refresh_token = token_data.get("refresh_token")
    access_token = token_data["access_token"]

    if not refresh_token:
        logger.error("No refresh token in Google Ads token response for org %s", org_id)
        raise BadRequestError(
            detail="No refresh token received. Please ensure you grant offline access."
        )

    # Discover accessible Google Ads accounts via listAccessibleCustomers
    accessible_accounts = await _discover_accounts(access_token)

    now = datetime.now(UTC).replace(microsecond=0)
    config_payload = {
        "refresh_token": refresh_token,
        "connected_at": now.isoformat(),
        "accessible_accounts": accessible_accounts,
        "selected_customer_id": None,
        "connected_by": user_id,
    }

    supabase.table("provider_configs").upsert(
        {
            "organization_id": org_id,
            "provider": "google_ads",
            "config": config_payload,
            "is_active": True,
        },
        on_conflict="organization_id,provider",
    ).execute()

    logger.info(
        "Google Ads OAuth completed for org %s by user %s — %d accounts found",
        org_id,
        user_id,
        len(accessible_accounts),
    )

    redirect_url = (
        f"{settings.FRONTEND_URL}/settings/integrations?google_ads_connected=true"
    )
    return RedirectResponse(url=redirect_url)


@router.get("/status", response_model=GoogleAdsStatusResponse)
async def google_ads_status(
    tenant=Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    """Return Google Ads connection health for the current tenant."""
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", tenant.id)
        .eq("provider", "google_ads")
        .maybe_single()
        .execute()
    )

    if not res.data or not res.data.get("is_active"):
        return GoogleAdsStatusResponse(connected=False)

    config = res.data["config"]

    return GoogleAdsStatusResponse(
        connected=True,
        accessible_accounts=config.get("accessible_accounts", []),
        selected_customer_id=config.get("selected_customer_id"),
        needs_reauth=False,
    )


@router.put("/account")
async def google_ads_select_account(
    body: AccountSelectRequest,
    tenant=Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    """Set the active Google Ads customer account for the current tenant."""
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", tenant.id)
        .eq("provider", "google_ads")
        .maybe_single()
        .execute()
    )

    if not res.data:
        raise NotFoundError(detail="Google Ads not connected for this organization.")

    config = res.data["config"]
    accessible = config.get("accessible_accounts", [])

    # Validate customer_id is in accessible accounts
    customer_id = body.customer_id.replace("-", "")
    valid_ids = [a["customer_id"] for a in accessible]
    if customer_id not in valid_ids:
        raise BadRequestError(
            detail=f"Customer ID {customer_id} not in accessible accounts."
        )

    config["selected_customer_id"] = customer_id
    supabase.table("provider_configs").update({"config": config}).eq(
        "organization_id", tenant.id
    ).eq("provider", "google_ads").execute()

    return {"selected_customer_id": customer_id}


# --- Helpers ---


async def _discover_accounts(access_token: str) -> list[dict]:
    """Discover accessible Google Ads accounts using the REST API."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": settings.GOOGLE_ADS_DEVELOPER_TOKEN,
        "login-customer-id": settings.GOOGLE_ADS_MCC_ID,
    }

    accounts = []
    async with httpx.AsyncClient() as client:
        # List accessible customers
        resp = await client.get(
            "https://googleads.googleapis.com/v18/customers:listAccessibleCustomers",
            headers=headers,
        )

        if resp.status_code != 200:
            logger.warning(
                "Failed to list accessible Google Ads customers: %s", resp.text
            )
            return accounts

        resource_names = resp.json().get("resourceNames", [])

        # For each customer, get account details via GAQL
        for resource_name in resource_names:
            customer_id = resource_name.split("/")[-1]
            account_info = await _get_account_details(
                client, headers, customer_id
            )
            if account_info and not account_info.get("is_manager", False):
                accounts.append(account_info)

    return accounts


async def _get_account_details(
    client: httpx.AsyncClient,
    headers: dict,
    customer_id: str,
) -> dict | None:
    """Get details for a specific Google Ads account via GAQL."""
    query = (
        "SELECT customer.id, customer.descriptive_name, "
        "customer.currency_code, customer.time_zone, "
        "customer.manager, customer.test_account "
        "FROM customer LIMIT 1"
    )

    try:
        resp = await client.post(
            f"https://googleads.googleapis.com/v18/customers/{customer_id}/googleAds:searchStream",
            headers={**headers, "Content-Type": "application/json"},
            json={"query": query},
        )

        if resp.status_code != 200:
            logger.debug(
                "Failed to get details for Google Ads customer %s: %s",
                customer_id,
                resp.text,
            )
            return None

        results = resp.json()
        if not results or not results[0].get("results"):
            return None

        row = results[0]["results"][0]
        customer = row.get("customer", {})
        return {
            "customer_id": str(customer.get("id", customer_id)).replace("-", ""),
            "name": customer.get("descriptiveName", ""),
            "currency": customer.get("currencyCode", "USD"),
            "timezone": customer.get("timeZone", ""),
            "is_manager": customer.get("manager", False),
            "is_test": customer.get("testAccount", False),
        }
    except Exception as e:
        logger.debug("Error fetching Google Ads customer %s: %s", customer_id, e)
        return None
