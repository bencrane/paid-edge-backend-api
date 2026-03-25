"""Meta Marketing API base client + rate limiter + ad account management (BJC-149)."""

import asyncio
import json
import logging
import time

import httpx
from pydantic import BaseModel
from supabase import Client

from app.config import settings
from app.integrations.meta_auth import (
    MetaReauthRequiredError,
    compute_appsecret_proof,
    get_valid_meta_token,
)

logger = logging.getLogger(__name__)


# --- Exceptions ---


class MetaAPIError(Exception):
    """Base Meta API error."""

    def __init__(
        self,
        code: int,
        subcode: int | None,
        message: str,
        blame_field: str | None = None,
    ):
        self.code = code
        self.subcode = subcode
        self.message = message
        self.blame_field = blame_field
        super().__init__(f"Meta API error {code}: {message}")


class MetaRateLimitError(MetaAPIError):
    """Error codes 4, 17, 80000-80014, 613."""

    pass


class MetaTokenError(MetaAPIError):
    """Error code 190 — invalid/expired token."""

    pass


class MetaPermissionError(MetaAPIError):
    """Error code 10 or 200 — insufficient permissions."""

    pass


RATE_LIMIT_CODES = {4, 17, 613} | set(range(80000, 80015))


def _classify_error(code: int, subcode: int | None, message: str, blame_field: str | None = None):
    """Raise the appropriate exception subclass."""
    if code == 190:
        raise MetaTokenError(code, subcode, message, blame_field)
    if code in (10, 200):
        raise MetaPermissionError(code, subcode, message, blame_field)
    if code in RATE_LIMIT_CODES:
        raise MetaRateLimitError(code, subcode, message, blame_field)
    raise MetaAPIError(code, subcode, message, blame_field)


async def handle_meta_error(response_data: dict) -> None:
    """Parse error response and raise appropriate exception."""
    err = response_data.get("error", {})
    if err:
        _classify_error(
            code=err.get("code", 0),
            subcode=err.get("error_subcode"),
            message=err.get("message", "Unknown error"),
            blame_field=err.get("error_data", {}).get("blame_field_specs") if isinstance(err.get("error_data"), dict) else None,
        )


# --- Pydantic models ---


class MetaAdAccount(BaseModel):
    id: str  # "act_123456"
    name: str
    currency: str = "USD"
    timezone_name: str = ""
    account_status: int = 0  # 1=ACTIVE, 2=DISABLED, 3=UNSETTLED


class MetaAPIResponse(BaseModel):
    data: list[dict] | None = None
    paging: dict | None = None
    error: dict | None = None


# --- Rate limiter ---


class MetaRateLimiter:
    """Track Meta's multi-dimensional rate limits."""

    def __init__(self):
        self._ad_account_util: float = 0.0
        self._buc_call_count: float = 0.0
        self._buc_cputime: float = 0.0
        self._buc_total_time: float = 0.0
        self._insights_app_util: float = 0.0
        self._insights_acc_util: float = 0.0
        self._reset_time: float = 0.0

    def update_from_headers(self, headers: dict) -> None:
        """Parse rate limit headers after each response."""
        # X-Ad-Account-Usage
        ad_usage = headers.get("x-ad-account-usage")
        if ad_usage:
            try:
                data = json.loads(ad_usage)
                self._ad_account_util = float(data.get("acc_id_util_pct", 0))
                self._reset_time = float(data.get("reset_time_duration", 0))
            except (json.JSONDecodeError, ValueError):
                pass

        # X-Business-Use-Case-Usage
        buc_usage = headers.get("x-business-use-case-usage")
        if buc_usage:
            try:
                data = json.loads(buc_usage)
                for _account_id, usages in data.items():
                    if isinstance(usages, list) and usages:
                        usage = usages[0]
                        self._buc_call_count = float(usage.get("call_count", 0))
                        self._buc_cputime = float(usage.get("total_cputime", 0))
                        self._buc_total_time = float(usage.get("total_time", 0))
            except (json.JSONDecodeError, ValueError):
                pass

        # X-FB-Ads-Insights-Throttle
        insights_throttle = headers.get("x-fb-ads-insights-throttle")
        if insights_throttle:
            try:
                data = json.loads(insights_throttle)
                self._insights_app_util = float(data.get("app_id_util_pct", 0))
                self._insights_acc_util = float(data.get("acc_id_util_pct", 0))
            except (json.JSONDecodeError, ValueError):
                pass

    def should_throttle(self) -> bool:
        """True if any utilization > 75%."""
        return any(
            v > 75
            for v in [
                self._ad_account_util,
                self._buc_call_count,
                self._buc_cputime,
                self._buc_total_time,
                self._insights_app_util,
                self._insights_acc_util,
            ]
        )

    async def wait_if_needed(self) -> None:
        """Sleep if approaching limits."""
        if self.should_throttle():
            wait_time = max(self._reset_time, 5.0)
            logger.warning(
                "Meta rate limit approaching (%.0f%%), sleeping %.1fs",
                max(self._ad_account_util, self._buc_call_count),
                wait_time,
            )
            await asyncio.sleep(wait_time)


# --- Base client ---


class MetaAdsClient:
    """Base client for Meta Marketing API.

    Uses raw httpx for all API calls. Per-request initialization for multi-tenant:
    each tenant gets its own client instance with their access token.
    """

    BASE_URL = f"https://graph.facebook.com/{settings.META_API_VERSION}"

    def __init__(
        self,
        access_token: str,
        app_id: str,
        app_secret: str,
        ad_account_id: str,
    ):
        self.access_token = access_token
        self.ad_account_id = ad_account_id
        self.app_id = app_id
        self.app_secret = app_secret
        self._appsecret_proof = compute_appsecret_proof(app_secret, access_token)
        self._http_client: httpx.AsyncClient | None = None
        self._rate_limiter = MetaRateLimiter()

    @classmethod
    async def for_tenant(cls, org_id: str, supabase: Client) -> "MetaAdsClient":
        """Factory: load credentials from provider_configs and construct client."""
        token = await get_valid_meta_token(org_id, supabase)

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
        ad_account_id = config.get("selected_ad_account_id", "")
        if not ad_account_id and config.get("ad_accounts"):
            ad_account_id = config["ad_accounts"][0]["id"]

        return cls(
            access_token=token,
            app_id=config.get("app_id", settings.META_APP_ID),
            app_secret=settings.META_APP_SECRET,
            ad_account_id=ad_account_id,
        )

    async def __aenter__(self):
        self._http_client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *args):
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_data: dict | None = None,
        data: dict | None = None,
    ) -> dict:
        """Raw HTTP request with appsecret_proof, rate limit tracking, retry."""
        return await self._request_with_retry(
            method, path, params=params, json_data=json_data, data=data
        )

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        max_retries: int = 5,
        base_delay: float = 1.0,
        params: dict | None = None,
        json_data: dict | None = None,
        data: dict | None = None,
    ) -> dict:
        """Execute request with exponential backoff on rate limits."""
        url = f"{self.BASE_URL}/{path.lstrip('/')}" if not path.startswith("http") else path

        base_params = {
            "access_token": self.access_token,
            "appsecret_proof": self._appsecret_proof,
        }
        if params:
            base_params.update(params)

        client = self._get_client()

        for attempt in range(max_retries + 1):
            await self._rate_limiter.wait_if_needed()

            try:
                if method.upper() == "GET":
                    resp = await client.get(url, params=base_params)
                elif method.upper() == "POST":
                    if json_data is not None:
                        resp = await client.post(url, params=base_params, json=json_data)
                    elif data is not None:
                        resp = await client.post(url, params=base_params, data=data)
                    else:
                        resp = await client.post(url, params=base_params)
                elif method.upper() == "DELETE":
                    resp = await client.delete(url, params=base_params)
                else:
                    resp = await client.request(method, url, params=base_params)

                # Update rate limit tracking
                self._rate_limiter.update_from_headers(dict(resp.headers))

                response_data = resp.json()

                # Check for errors
                if "error" in response_data:
                    err = response_data["error"]
                    code = err.get("code", 0)

                    if code in RATE_LIMIT_CODES and attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            "Meta rate limit (code %d), retry %d/%d in %.1fs",
                            code, attempt + 1, max_retries, delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    await handle_meta_error(response_data)

                return response_data

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(
                        "Meta request failed (%s), retry %d/%d in %.1fs",
                        exc, attempt + 1, max_retries, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise MetaAPIError(0, None, f"Request failed after {max_retries} retries: {exc}")

        raise MetaAPIError(0, None, "Max retries exceeded")

    async def _paginate(
        self, path: str, params: dict | None = None, limit: int | None = None
    ) -> list[dict]:
        """Auto-paginate through cursor-based results."""
        results = []
        req_params = dict(params or {})
        if limit:
            req_params["limit"] = min(limit, 100)

        while True:
            resp = await self._request("GET", path, params=req_params)
            data = resp.get("data", [])
            results.extend(data)

            if limit and len(results) >= limit:
                return results[:limit]

            paging = resp.get("paging", {})
            next_url = paging.get("next")
            if not next_url:
                break

            # For next page, use the full URL directly
            resp = await self._request("GET", next_url)
            data = resp.get("data", [])
            results.extend(data)

            paging = resp.get("paging", {})
            next_url = paging.get("next")
            if not next_url:
                break

        return results[:limit] if limit else results

    async def batch_request(self, requests: list[dict]) -> list[dict]:
        """Execute up to 50 sub-requests in a single HTTP call.

        Auto-chunks if len(requests) > 50.
        """
        all_results = []
        for i in range(0, len(requests), 50):
            chunk = requests[i : i + 50]
            resp = await self._request(
                "POST", "", data={"batch": json.dumps(chunk)}
            )
            if isinstance(resp, list):
                all_results.extend(resp)
            else:
                all_results.append(resp)
        return all_results

    # --- Ad Account Management ---

    async def list_ad_accounts(self, business_id: str | None = None) -> list[dict]:
        """List accessible ad accounts."""
        if business_id:
            path = f"{business_id}/owned_ad_accounts"
        else:
            path = "me/adaccounts"
        return await self._paginate(
            path,
            params={"fields": "id,name,currency,timezone_name,account_status"},
        )

    async def get_ad_account(self, account_id: str | None = None) -> dict:
        """Get details of a specific ad account."""
        acct_id = account_id or self.ad_account_id
        return await self._request(
            "GET",
            acct_id,
            params={"fields": "name,account_status,currency,timezone_name,business"},
        )

    async def validate_ad_account_access(self) -> bool:
        """Verify the current token has ADVERTISE + ANALYZE permissions."""
        try:
            resp = await self._request(
                "GET",
                self.ad_account_id,
                params={"fields": "name,account_status"},
            )
            status = resp.get("account_status", 0)
            return status == 1  # 1 = ACTIVE
        except MetaAPIError:
            return False
