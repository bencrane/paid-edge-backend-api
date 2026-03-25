import asyncio
import logging
from typing import Any

import httpx
from supabase import Client

from app.config import settings
from app.integrations.linkedin_auth import get_valid_linkedin_token
from app.integrations.linkedin_models import (
    LinkedInAdAccount,
    LinkedInAPIErrorDetail,
    LinkedInCampaignGroup,
)

logger = logging.getLogger(__name__)


# --- Custom exceptions ---


class LinkedInAPIError(Exception):
    """Base exception for LinkedIn API errors."""

    def __init__(self, status_code: int, service_error_code: int | None, message: str):
        self.status_code = status_code
        self.service_error_code = service_error_code
        self.message = message
        super().__init__(f"LinkedIn API error {status_code}: {message}")


class LinkedInRateLimitError(LinkedInAPIError):
    """429 Too Many Requests."""

    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(429, None, message)


class LinkedInPermissionError(LinkedInAPIError):
    """403 Forbidden."""

    def __init__(self, service_error_code: int | None = None, message: str = "Permission denied"):
        super().__init__(403, service_error_code, message)


class LinkedInNotFoundError(LinkedInAPIError):
    """404 Not Found."""

    def __init__(self, service_error_code: int | None = None, message: str = "Resource not found"):
        super().__init__(404, service_error_code, message)


class LinkedInVersionError(LinkedInAPIError):
    """Version-related errors (400 VERSION_MISSING or 426 NONEXISTENT_VERSION)."""

    def __init__(self, status_code: int = 400, message: str = "API version error"):
        super().__init__(status_code, None, message)


# --- URN utilities ---


def extract_id_from_urn(urn: str) -> int:
    """Extract numeric ID from LinkedIn URN string.

    'urn:li:sponsoredAccount:507404993' -> 507404993
    """
    return int(urn.split(":")[-1])


def make_account_urn(account_id: int) -> str:
    return f"urn:li:sponsoredAccount:{account_id}"


def make_campaign_urn(campaign_id: int) -> str:
    return f"urn:li:sponsoredCampaign:{campaign_id}"


def make_org_urn(org_id: int) -> str:
    return f"urn:li:organization:{org_id}"


# --- Base client ---


class LinkedInAdsClient:
    BASE_URL = "https://api.linkedin.com/rest"
    API_VERSION = settings.LINKEDIN_API_VERSION

    MAX_RETRIES = 5
    BACKOFF_BASE = 2  # seconds
    BACKOFF_CAP = 300  # seconds

    def __init__(self, org_id: str, supabase: Client):
        self.org_id = org_id
        self.supabase = supabase
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def _get_headers(self) -> dict[str, str]:
        token = await get_valid_linkedin_token(self.org_id, self.supabase)
        return {
            "Authorization": f"Bearer {token}",
            "LinkedIn-Version": self.API_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Core request method with retry + rate limit handling."""
        url = f"{self.BASE_URL}{path}"
        headers = await self._get_headers()

        for attempt in range(self.MAX_RETRIES + 1):
            resp = await self._client.request(
                method, url, headers=headers, params=params, json=json
            )

            if resp.status_code == 429:
                if attempt >= self.MAX_RETRIES:
                    logger.error(
                        "LinkedIn rate limit exceeded after %d retries: %s %s",
                        self.MAX_RETRIES,
                        method,
                        path,
                    )
                    raise LinkedInRateLimitError()
                delay = min(self.BACKOFF_BASE * (2**attempt), self.BACKOFF_CAP)
                logger.warning(
                    "LinkedIn rate limited on %s %s — retrying in %ds (attempt %d/%d)",
                    method,
                    path,
                    delay,
                    attempt + 1,
                    self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                # Re-fetch headers in case token was refreshed
                headers = await self._get_headers()
                continue

            if resp.status_code >= 400:
                self._raise_for_status(resp)

            if resp.status_code == 204:
                return {}

            return resp.json()

        # Should not reach here, but just in case
        raise LinkedInAPIError(500, None, "Unexpected retry exhaustion")

    def _raise_for_status(self, resp: httpx.Response) -> None:
        """Parse LinkedIn error response and raise appropriate exception."""
        try:
            body = resp.json()
            error_detail = LinkedInAPIErrorDetail(
                status=body.get("status", resp.status_code),
                service_error_code=body.get("serviceErrorCode"),
                message=body.get("message", resp.text),
            )
        except Exception:
            error_detail = LinkedInAPIErrorDetail(
                status=resp.status_code,
                service_error_code=None,
                message=resp.text,
            )

        if resp.status_code == 403:
            raise LinkedInPermissionError(
                error_detail.service_error_code, error_detail.message
            )
        if resp.status_code == 404:
            raise LinkedInNotFoundError(
                error_detail.service_error_code, error_detail.message
            )
        if resp.status_code in (400, 426) and (
            "VERSION" in error_detail.message.upper()
            or (
                error_detail.service_error_code
                and "VERSION" in str(error_detail.service_error_code)
            )
        ):
            raise LinkedInVersionError(resp.status_code, error_detail.message)

        raise LinkedInAPIError(
            error_detail.status,
            error_detail.service_error_code,
            error_detail.message,
        )

    async def get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._request("GET", path, params=params)

    async def post(
        self, path: str, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._request("POST", path, json=json)

    async def patch(
        self, path: str, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._request("PATCH", path, json=json)

    async def delete(self, path: str) -> None:
        await self._request("DELETE", path)

    # --- Account structure methods ---

    async def get_ad_accounts(self, status: str = "ACTIVE") -> list[LinkedInAdAccount]:
        """List all ad accounts accessible to the authenticated member."""
        resp = await self.get(
            "/adAccounts",
            params={
                "q": "search",
                "search": f"(status:(values:List({status})))",
            },
        )
        accounts = []
        for element in resp.get("elements", []):
            account_id = extract_id_from_urn(element.get("id", element.get("urn", "")))
            accounts.append(
                LinkedInAdAccount(
                    id=account_id,
                    name=element.get("name", ""),
                    currency=element.get("currency", "USD"),
                    status=element.get("status", ""),
                    reference_org_urn=element.get("reference"),
                )
            )
        return accounts

    async def get_ad_account_users(self) -> list[dict[str, Any]]:
        """Discover accounts via authenticated user's roles."""
        resp = await self.get(
            "/adAccountUsers",
            params={"q": "authenticatedUser"},
        )
        return resp.get("elements", [])

    async def get_campaign_groups(
        self, account_id: int, statuses: list[str] | None = None
    ) -> list[LinkedInCampaignGroup]:
        """List campaign groups for an ad account."""
        if statuses is None:
            statuses = ["ACTIVE", "PAUSED", "DRAFT"]
        status_values = ",".join(statuses)
        resp = await self.get(
            f"/adAccounts/{account_id}/adCampaignGroups",
            params={
                "q": "search",
                "search": f"(status:(values:List({status_values})))",
            },
        )
        groups = []
        for element in resp.get("elements", []):
            group_id = extract_id_from_urn(element.get("id", element.get("urn", "")))
            groups.append(
                LinkedInCampaignGroup(
                    id=group_id,
                    name=element.get("name", ""),
                    status=element.get("status", ""),
                    account_urn=element.get("account", ""),
                    total_budget=element.get("totalBudget"),
                    run_schedule=element.get("runSchedule"),
                )
            )
        return groups

    async def create_campaign_group(
        self,
        account_id: int,
        name: str,
        budget: dict | None = None,
        schedule: dict | None = None,
    ) -> dict[str, Any]:
        """Create a campaign group."""
        body: dict[str, Any] = {
            "account": make_account_urn(account_id),
            "name": name,
            "status": "DRAFT",
        }
        if budget:
            body["totalBudget"] = budget
        if schedule:
            body["runSchedule"] = schedule

        return await self.post(f"/adAccounts/{account_id}/adCampaignGroups", json=body)

    async def get_selected_account_id(self) -> int:
        """Get the tenant's selected LinkedIn ad account ID from provider_configs."""
        res = (
            self.supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", self.org_id)
            .eq("provider", "linkedin_ads")
            .maybe_single()
            .execute()
        )
        if not res.data:
            raise LinkedInAPIError(
                400, None, "LinkedIn not connected for this organization."
            )
        selected = res.data["config"].get("selected_ad_account_id")
        if not selected:
            raise LinkedInAPIError(
                400, None, "No LinkedIn ad account selected. Please select one in settings."
            )
        return int(selected)
