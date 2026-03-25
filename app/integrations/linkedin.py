import asyncio
import hashlib
import logging
from datetime import UTC, date, datetime
from typing import Any

import httpx
from supabase import Client

from app.config import settings
from app.integrations.linkedin_auth import get_valid_linkedin_token
from app.integrations.linkedin_models import (
    LinkedInAdAccount,
    LinkedInAPIErrorDetail,
    LinkedInCampaign,
    LinkedInCampaignGroup,
    LinkedInCreative,
    LinkedInDMPSegment,
)
from app.integrations.linkedin_targeting import validate_campaign_config

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


def make_campaign_group_urn(group_id: int) -> str:
    return f"urn:li:sponsoredCampaignGroup:{group_id}"


def hash_email_for_linkedin(email: str) -> str:
    """Lowercase, strip whitespace, then SHA256 hash.

    CRITICAL: order matters — lowercase + strip BEFORE hashing.
    Wrong order = 200 response, 0 matches.
    """
    normalized = email.lower().strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def to_linkedin_date(d: date) -> dict:
    """Convert Python date to LinkedIn date format."""
    return {"year": d.year, "month": d.month, "day": d.day}


def from_linkedin_date(d: dict) -> date:
    """Convert LinkedIn date format back to Python date."""
    return date(d["year"], d["month"], d["day"])


# --- Default analytics fields ---

_DEFAULT_ANALYTICS_FIELDS = [
    "impressions",
    "clicks",
    "costInLocalCurrency",
    "costInUsd",
    "externalWebsiteConversions",
    "leadGenerationMailContactInfoShares",
    "oneClickLeads",
    "videoViews",
    "videoCompletions",
    "totalEngagements",
    "likes",
    "comments",
    "shares",
    "textUrlClicks",
    "landingPageClicks",
    "dateRange",
    "pivotValue",
]


# Valid status transitions for campaigns
_VALID_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "DRAFT": {"ACTIVE", "ARCHIVED"},
    "ACTIVE": {"PAUSED", "ARCHIVED"},
    "PAUSED": {"ACTIVE", "ARCHIVED"},
    "COMPLETED": {"ARCHIVED"},
    "CANCELED": {"ARCHIVED"},
}


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

    async def _request_raw(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Like _request but returns the raw httpx.Response (for headers)."""
        url = f"{self.BASE_URL}{path}"
        headers = await self._get_headers()

        for attempt in range(self.MAX_RETRIES + 1):
            resp = await self._client.request(
                method, url, headers=headers, params=params, json=json
            )

            if resp.status_code == 429:
                if attempt >= self.MAX_RETRIES:
                    raise LinkedInRateLimitError()
                delay = min(
                    self.BACKOFF_BASE * (2**attempt), self.BACKOFF_CAP
                )
                await asyncio.sleep(delay)
                headers = await self._get_headers()
                continue

            if resp.status_code >= 400:
                self._raise_for_status(resp)

            return resp

        raise LinkedInAPIError(500, None, "Unexpected retry exhaustion")

    async def _upload_binary(
        self, upload_url: str, data: bytes
    ) -> None:
        """PUT binary data to a LinkedIn upload URL."""
        headers = await self._get_headers()
        headers["Content-Type"] = "application/octet-stream"
        resp = await self._client.put(
            upload_url, headers=headers, content=data
        )
        if resp.status_code >= 400:
            self._raise_for_status(resp)

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
                400,
                None,
                "No LinkedIn ad account selected. "
                "Please select one in settings.",
            )
        return int(selected)

    # --- Campaign methods ---

    async def create_campaign(
        self,
        account_id: int,
        campaign_group_id: int,
        name: str,
        campaign_type: str,
        objective: str,
        targeting: dict,
        daily_budget: dict,
        cost_type: str,
        unit_cost: dict | None = None,
        run_schedule: dict | None = None,
        offsite_delivery: bool = False,
        status: str = "DRAFT",
    ) -> dict[str, Any]:
        """Create a LinkedIn campaign."""
        validate_campaign_config(
            objective, campaign_type, cost_type, offsite_delivery
        )

        body: dict[str, Any] = {
            "account": make_account_urn(account_id),
            "campaignGroup": make_campaign_group_urn(campaign_group_id),
            "name": name,
            "type": campaign_type,
            "objectiveType": objective,
            "costType": cost_type,
            "status": status,
            "locale": {"country": "US", "language": "en"},
            "offsiteDeliveryEnabled": offsite_delivery,
            "dailyBudget": daily_budget,
            "targetingCriteria": targeting,
        }
        if unit_cost:
            body["unitCost"] = unit_cost
        if run_schedule:
            body["runSchedule"] = run_schedule

        return await self.post(
            f"/adAccounts/{account_id}/adCampaigns", json=body
        )

    async def get_campaigns(
        self,
        account_id: int,
        statuses: list[str] | None = None,
    ) -> list[LinkedInCampaign]:
        """List campaigns for an ad account, filtered by status."""
        if statuses is None:
            statuses = ["ACTIVE", "PAUSED", "DRAFT"]
        status_values = ",".join(statuses)
        resp = await self.get(
            f"/adAccounts/{account_id}/adCampaigns",
            params={
                "q": "search",
                "search": f"(status:(values:List({status_values})))",
            },
        )
        campaigns = []
        for el in resp.get("elements", []):
            cid = extract_id_from_urn(
                el.get("id", el.get("urn", ""))
            )
            campaigns.append(
                LinkedInCampaign(
                    id=cid,
                    name=el.get("name", ""),
                    status=el.get("status", ""),
                    type=el.get("type", ""),
                    objective_type=el.get("objectiveType"),
                    cost_type=el.get("costType", ""),
                    daily_budget=el.get("dailyBudget"),
                    total_budget=el.get("totalBudget"),
                    unit_cost=el.get("unitCost"),
                    targeting_criteria=el.get("targetingCriteria"),
                    run_schedule=el.get("runSchedule"),
                    offsite_delivery_enabled=el.get(
                        "offsiteDeliveryEnabled", False
                    ),
                    campaign_group_urn=el.get("campaignGroup"),
                    account_urn=el.get("account"),
                )
            )
        return campaigns

    async def get_campaign(
        self, account_id: int, campaign_id: int
    ) -> dict[str, Any]:
        """Get a single campaign's full details."""
        return await self.get(
            f"/adAccounts/{account_id}/adCampaigns/{campaign_id}"
        )

    async def update_campaign(
        self,
        account_id: int,
        campaign_id: int,
        updates: dict[str, Any],
    ) -> None:
        """Update campaign fields using Rest.li PATCH $set format."""
        await self.patch(
            f"/adAccounts/{account_id}/adCampaigns/{campaign_id}",
            json={"patch": {"$set": updates}},
        )

    async def update_campaign_status(
        self, account_id: int, campaign_id: int, status: str
    ) -> None:
        """Convenience method for status transitions with validation.

        Valid: DRAFT→ACTIVE, ACTIVE→PAUSED, PAUSED→ACTIVE, *→ARCHIVED
        """
        # Fetch current status to validate the transition
        campaign = await self.get_campaign(account_id, campaign_id)
        current = campaign.get("status", "")

        allowed = _VALID_STATUS_TRANSITIONS.get(current)
        if allowed is None or status not in allowed:
            raise LinkedInAPIError(
                400,
                None,
                f"Invalid status transition: {current} → {status}. "
                f"Allowed from {current}: "
                f"{sorted(allowed) if allowed else 'none'}",
            )

        await self.update_campaign(
            account_id, campaign_id, {"status": status}
        )

    # --- Media upload methods ---

    async def upload_image(self, org_id: int, image_bytes: bytes) -> str:
        """Upload image and return image URN.

        1. POST /images?action=initializeUpload → get uploadUrl + image URN
        2. PUT {uploadUrl} with binary data
        Returns: urn:li:image:{id}
        """
        init_resp = await self.post(
            "/images?action=initializeUpload",
            json={
                "initializeUploadRequest": {
                    "owner": make_org_urn(org_id),
                }
            },
        )
        upload_url = init_resp["value"]["uploadUrl"]
        image_urn = init_resp["value"]["image"]

        await self._upload_binary(upload_url, image_bytes)
        return image_urn

    async def upload_document(
        self, org_id: int, doc_bytes: bytes
    ) -> str:
        """Upload document and return document URN.

        1. POST /documents?action=initializeUpload → get uploadUrl + doc URN
        2. PUT {uploadUrl} with binary data
        Returns: urn:li:document:{id}
        """
        init_resp = await self.post(
            "/documents?action=initializeUpload",
            json={
                "initializeUploadRequest": {
                    "owner": make_org_urn(org_id),
                }
            },
        )
        upload_url = init_resp["value"]["uploadUrl"]
        doc_urn = init_resp["value"]["document"]

        await self._upload_binary(upload_url, doc_bytes)
        return doc_urn

    async def upload_video(
        self, org_id: int, video_bytes: bytes, file_size: int
    ) -> str:
        """Upload video and return video URN.

        1. POST /videos?action=initializeUpload → get uploadInstructions
        2. PUT each chunk to its uploadUrl
        3. POST /videos?action=finalizeUpload with uploadedPartIds
        Returns: urn:li:video:{id}
        """
        init_resp = await self.post(
            "/videos?action=initializeUpload",
            json={
                "initializeUploadRequest": {
                    "owner": make_org_urn(org_id),
                    "fileSizeBytes": file_size,
                    "uploadCaptions": False,
                    "uploadThumbnail": False,
                }
            },
        )
        video_urn = init_resp["value"]["video"]
        instructions = init_resp["value"]["uploadInstructions"]

        # Upload chunks according to instructions
        uploaded_part_ids = []
        offset = 0
        for instruction in instructions:
            chunk_url = instruction["uploadUrl"]
            # Each instruction covers a segment of the file
            last_byte = instruction.get("lastByte", file_size - 1)
            first_byte = instruction.get("firstByte", offset)
            chunk = video_bytes[first_byte : last_byte + 1]

            headers = await self._get_headers()
            headers["Content-Type"] = "application/octet-stream"
            resp = await self._client.put(
                chunk_url, headers=headers, content=chunk
            )
            if resp.status_code >= 400:
                self._raise_for_status(resp)

            # Collect the ETag or part ID from response
            etag = resp.headers.get("etag", "")
            uploaded_part_ids.append(etag)
            offset = last_byte + 1

        # Finalize
        await self.post(
            "/videos?action=finalizeUpload",
            json={
                "finalizeUploadRequest": {
                    "video": video_urn,
                    "uploadToken": "",
                    "uploadedPartIds": uploaded_part_ids,
                }
            },
        )
        return video_urn

    # --- Post creation ---

    async def create_sponsored_post(
        self,
        org_id: int,
        commentary: str,
        media_urn: str,
        media_title: str,
    ) -> str:
        """Create a post suitable for sponsored content.

        Sets distribution.feedDistribution = NONE (ad-only, not organic).
        Returns post URN from x-restli-id header.
        """
        resp = await self._request_raw(
            "POST",
            "/posts",
            json={
                "author": make_org_urn(org_id),
                "commentary": commentary,
                "visibility": "PUBLIC",
                "distribution": {
                    "feedDistribution": "NONE",
                    "targetEntities": [],
                    "thirdPartyDistributionChannels": [],
                },
                "content": {
                    "media": {
                        "title": media_title,
                        "id": media_urn,
                    }
                },
                "lifecycleState": "PUBLISHED",
                "isReshareDisabledByAuthor": False,
            },
        )
        # Post URN returned in x-restli-id header
        post_urn = resp.headers.get("x-restli-id", "")
        if not post_urn:
            # Fallback: try response body
            body = resp.json() if resp.status_code != 204 else {}
            post_urn = body.get("id", "")
        return post_urn

    # --- Creative CRUD ---

    async def create_creative(
        self,
        account_id: int,
        campaign_id: int,
        post_urn: str,
        lead_gen_form_urn: str | None = None,
        status: str = "ACTIVE",
    ) -> dict[str, Any]:
        """Create a creative referencing a post."""
        content: dict[str, Any] = {"reference": post_urn}
        if lead_gen_form_urn:
            content["leadGenerationContext"] = {
                "leadGenerationFormUrn": lead_gen_form_urn,
            }

        return await self.post(
            f"/adAccounts/{account_id}/adCreatives",
            json={
                "campaign": make_campaign_urn(campaign_id),
                "content": content,
                "intendedStatus": status,
            },
        )

    async def get_creatives(
        self,
        account_id: int,
        campaign_id: int | None = None,
    ) -> list[LinkedInCreative]:
        """List creatives, optionally filtered by campaign."""
        params: dict[str, str] = {"q": "search"}
        if campaign_id:
            campaign_urn = make_campaign_urn(campaign_id)
            params["search"] = (
                f"(campaign:(values:List({campaign_urn})))"
            )

        resp = await self.get(
            f"/adAccounts/{account_id}/adCreatives",
            params=params,
        )
        creatives = []
        for el in resp.get("elements", []):
            cid = extract_id_from_urn(
                el.get("id", el.get("urn", ""))
            )
            creatives.append(
                LinkedInCreative(
                    id=cid,
                    campaign_urn=el.get("campaign", ""),
                    content_reference=el.get(
                        "content", {}
                    ).get("reference", ""),
                    intended_status=el.get("intendedStatus", ""),
                    review_status=el.get("reviewStatus"),
                    serving_statuses=el.get("servingStatuses"),
                )
            )
        return creatives

    async def update_creative_status(
        self, account_id: int, creative_id: int, status: str
    ) -> None:
        """Update creative status (ACTIVE, PAUSED, ARCHIVED)."""
        await self.patch(
            f"/adAccounts/{account_id}/adCreatives/{creative_id}",
            json={
                "patch": {"$set": {"intendedStatus": status}}
            },
        )

    # --- InMail content ---

    async def create_inmail_content(
        self,
        account_id: int,
        name: str,
        subject: str,
        html_body: str,
        sender_urn: str,
        cta_label: str,
        cta_url: str,
    ) -> str:
        """Create InMail content for message ads.

        Returns adInMailContent URN.
        Validates: subject max 60 chars, body max 1500 chars,
        CTA label max 20 chars.
        """
        if len(subject) > 60:
            raise LinkedInAPIError(
                400, None,
                f"InMail subject exceeds 60 chars ({len(subject)})",
            )
        if len(html_body) > 1500:
            raise LinkedInAPIError(
                400, None,
                f"InMail body exceeds 1500 chars ({len(html_body)})",
            )
        if len(cta_label) > 20:
            raise LinkedInAPIError(
                400, None,
                f"InMail CTA label exceeds 20 chars ({len(cta_label)})",
            )

        resp = await self._request_raw(
            "POST",
            "/adInMailContents",
            json={
                "account": make_account_urn(account_id),
                "name": name,
                "subject": subject,
                "htmlBody": html_body,
                "sender": sender_urn,
                "ctaLabel": cta_label,
                "ctaUrl": cta_url,
            },
        )
        inmail_urn = resp.headers.get("x-restli-id", "")
        if not inmail_urn:
            body = resp.json() if resp.status_code != 204 else {}
            inmail_urn = body.get("id", "")
        return inmail_urn

    # --- End-to-end creative pipeline helpers ---

    async def create_image_ad(
        self,
        account_id: int,
        campaign_id: int,
        org_id: int,
        image_bytes: bytes,
        headline: str,
        intro_text: str,
        lead_gen_form_urn: str | None = None,
    ) -> dict[str, Any]:
        """Convenience: upload image → create post → create creative."""
        image_urn = await self.upload_image(org_id, image_bytes)
        post_urn = await self.create_sponsored_post(
            org_id=org_id,
            commentary=intro_text,
            media_urn=image_urn,
            media_title=headline,
        )
        return await self.create_creative(
            account_id=account_id,
            campaign_id=campaign_id,
            post_urn=post_urn,
            lead_gen_form_urn=lead_gen_form_urn,
        )

    async def create_document_ad(
        self,
        account_id: int,
        campaign_id: int,
        org_id: int,
        pdf_bytes: bytes,
        title: str,
        commentary: str,
    ) -> dict[str, Any]:
        """Convenience: upload document → create post → create creative."""
        doc_urn = await self.upload_document(org_id, pdf_bytes)
        post_urn = await self.create_sponsored_post(
            org_id=org_id,
            commentary=commentary,
            media_urn=doc_urn,
            media_title=title,
        )
        return await self.create_creative(
            account_id=account_id,
            campaign_id=campaign_id,
            post_urn=post_urn,
        )

    # --- DMP Segment (Matched Audiences) methods ---

    async def create_dmp_segment(
        self,
        account_id: int,
        name: str,
        segment_type: str,
    ) -> dict[str, Any]:
        """Create a DMP segment (staging bucket for audience data).

        POST /dmpSegments
        segment_type: "COMPANY" or "USER"
        Returns segment with id, status (BUILDING initially).
        """
        return await self.post(
            "/dmpSegments",
            json={
                "name": name,
                "type": segment_type,
                "account": make_account_urn(account_id),
                "sources": ["FIRST_PARTY"],
            },
        )

    async def _batch_upload(
        self,
        segment_id: str,
        items: list,
        upload_fn: Any,
        batch_size: int = 5000,
        delay_between_batches: float = 0.2,
    ) -> dict[str, Any]:
        """Split items into batches and upload sequentially with delay.

        Returns: {total_sent, batches_completed, errors}.
        """
        total_sent = 0
        batches_completed = 0
        errors: list[str] = []

        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            try:
                await upload_fn(segment_id, batch)
                total_sent += len(batch)
                batches_completed += 1
            except LinkedInAPIError as e:
                errors.append(f"Batch {batches_completed + 1}: {e.message}")

            if i + batch_size < len(items):
                await asyncio.sleep(delay_between_batches)

        return {
            "total_sent": total_sent,
            "batches_completed": batches_completed,
            "errors": errors,
        }

    async def _upload_company_batch(
        self, segment_id: str, companies: list[dict]
    ) -> dict[str, Any]:
        """Upload a single batch of companies to a DMP segment."""
        elements = [
            {
                "action": companies[0].get("_action", "ADD"),
                "companyIdentifiers": {
                    k: v
                    for k, v in company.items()
                    if k != "_action"
                },
            }
            for company in companies
        ]
        return await self.post(
            f"/dmpSegments/{segment_id}/companies",
            json={"elements": elements},
        )

    async def _upload_contact_batch(
        self, segment_id: str, hashed_elements: list[dict]
    ) -> dict[str, Any]:
        """Upload a single batch of hashed contacts to a DMP segment."""
        return await self.post(
            f"/dmpSegments/{segment_id}/users",
            json={"elements": hashed_elements},
        )

    async def stream_companies(
        self,
        segment_id: str,
        companies: list[dict],
        action: str = "ADD",
    ) -> dict[str, Any]:
        """Stream company list to a DMP segment.

        POST /dmpSegments/{segmentId}/companies
        Max 5,000 records per batch. Auto-batches if more.
        Rate limit: 300 requests/minute.
        """
        # Tag each company with the action for batch processing
        tagged = [{**c, "_action": action} for c in companies]

        return await self._batch_upload(
            segment_id=segment_id,
            items=tagged,
            upload_fn=self._upload_company_batch,
            batch_size=5000,
            delay_between_batches=0.2,
        )

    async def stream_contacts(
        self,
        segment_id: str,
        emails: list[str],
        action: str = "ADD",
    ) -> dict[str, Any]:
        """Stream hashed email list to a DMP segment.

        POST /dmpSegments/{segmentId}/users
        Emails are lowercased, stripped, then SHA256 hashed before sending.
        Max 5,000 records per batch. Auto-batches if more.
        Rate limit: 600 requests/minute.
        """
        elements = [
            {
                "action": action,
                "userIdentifiers": {
                    "hashedEmail": {
                        "hashType": "SHA256",
                        "hashValue": hash_email_for_linkedin(email),
                    }
                },
            }
            for email in emails
        ]

        return await self._batch_upload(
            segment_id=segment_id,
            items=elements,
            upload_fn=self._upload_contact_batch,
            batch_size=5000,
            delay_between_batches=0.2,
        )

    async def get_dmp_segment_status(
        self, segment_id: str
    ) -> LinkedInDMPSegment:
        """GET /dmpSegments/{segmentId}

        Returns parsed segment with status, matchedMemberCount,
        destinationSegmentId.
        """
        resp = await self.get(f"/dmpSegments/{segment_id}")
        return LinkedInDMPSegment(
            id=resp.get("id", segment_id),
            name=resp.get("name", ""),
            type=resp.get("type", ""),
            status=resp.get("status", ""),
            matched_member_count=resp.get("matchedMemberCount"),
            destination_segment_id=resp.get("destinationSegmentId"),
            account_urn=resp.get("account", ""),
        )

    async def delete_dmp_segment(self, segment_id: str) -> None:
        """DELETE /dmpSegments/{segmentId}"""
        await self.delete(f"/dmpSegments/{segment_id}")

    async def list_dmp_segments(
        self, account_id: int
    ) -> list[LinkedInDMPSegment]:
        """GET /dmpSegments?q=account&account=urn:li:sponsoredAccount:{id}"""
        resp = await self.get(
            "/dmpSegments",
            params={
                "q": "account",
                "account": make_account_urn(account_id),
            },
        )
        segments = []
        for el in resp.get("elements", []):
            segments.append(
                LinkedInDMPSegment(
                    id=el.get("id", ""),
                    name=el.get("name", ""),
                    type=el.get("type", ""),
                    status=el.get("status", ""),
                    matched_member_count=el.get("matchedMemberCount"),
                    destination_segment_id=el.get(
                        "destinationSegmentId"
                    ),
                    account_urn=el.get("account", ""),
                )
            )
        return segments

    async def wait_for_segment_ready(
        self,
        segment_id: str,
        max_wait_minutes: int = 60,
        poll_interval_seconds: int = 30,
    ) -> LinkedInDMPSegment:
        """Poll segment status until READY or timeout.

        Initial matching takes up to 48 hours — this is for subsequent
        updates which are near-real-time.
        """
        terminal_statuses = {"READY", "FAILED", "ARCHIVED", "EXPIRED"}
        elapsed = 0
        max_seconds = max_wait_minutes * 60

        while elapsed < max_seconds:
            segment = await self.get_dmp_segment_status(segment_id)
            if segment.status in terminal_statuses:
                return segment
            await asyncio.sleep(poll_interval_seconds)
            elapsed += poll_interval_seconds

        # Return last known status on timeout
        return await self.get_dmp_segment_status(segment_id)

    # --- Analytics methods ---

    async def get_campaign_analytics(
        self,
        account_id: int,
        start_date: date,
        end_date: date,
        campaign_ids: list[int] | None = None,
        pivot: str = "CAMPAIGN",
        granularity: str = "DAILY",
        fields: list[str] | None = None,
    ) -> list[dict]:
        """GET /adAnalytics for campaign performance.

        start_date inclusive, end_date exclusive.
        Max range: 730 days for DAILY granularity.
        """
        s = to_linkedin_date(start_date)
        e = to_linkedin_date(end_date)
        field_list = ",".join(fields or _DEFAULT_ANALYTICS_FIELDS)

        params: dict[str, str] = {
            "q": "analytics",
            "pivot": pivot,
            "dateRange": (
                f"(start:(year:{s['year']},month:{s['month']},"
                f"day:{s['day']}),"
                f"end:(year:{e['year']},month:{e['month']},"
                f"day:{e['day']}))"
            ),
            "timeGranularity": granularity,
            "fields": field_list,
        }

        if campaign_ids:
            urns = ",".join(
                f"urn%3Ali%3AsponsoredCampaign%3A{cid}"
                for cid in campaign_ids
            )
            params["campaigns"] = f"List({urns})"
        else:
            acct_urn = (
                f"urn%3Ali%3AsponsoredAccount%3A{account_id}"
            )
            params["accounts"] = f"List({acct_urn})"

        resp = await self.get("/adAnalytics", params=params)
        elements = resp.get("elements", [])
        if not elements:
            logger.info(
                "No analytics data for account %d (%s to %s)",
                account_id,
                start_date,
                end_date,
            )
        return elements

    async def get_creative_analytics(
        self,
        account_id: int,
        start_date: date,
        end_date: date,
        campaign_id: int | None = None,
        granularity: str = "DAILY",
    ) -> list[dict]:
        """Analytics pivoted by CREATIVE."""
        s = to_linkedin_date(start_date)
        e = to_linkedin_date(end_date)
        field_list = ",".join(_DEFAULT_ANALYTICS_FIELDS)

        params: dict[str, str] = {
            "q": "analytics",
            "pivot": "CREATIVE",
            "dateRange": (
                f"(start:(year:{s['year']},month:{s['month']},"
                f"day:{s['day']}),"
                f"end:(year:{e['year']},month:{e['month']},"
                f"day:{e['day']}))"
            ),
            "timeGranularity": granularity,
            "fields": field_list,
        }

        if campaign_id:
            urn = f"urn%3Ali%3AsponsoredCampaign%3A{campaign_id}"
            params["campaigns"] = f"List({urn})"
        else:
            acct_urn = (
                f"urn%3Ali%3AsponsoredAccount%3A{account_id}"
            )
            params["accounts"] = f"List({acct_urn})"

        resp = await self.get("/adAnalytics", params=params)
        return resp.get("elements", [])

    async def get_demographic_analytics(
        self,
        account_id: int,
        start_date: date,
        end_date: date,
        pivot: str,
    ) -> list[dict]:
        """Audience demographic breakdown for a given pivot dimension.

        Pivot: MEMBER_INDUSTRY, MEMBER_SENIORITY, MEMBER_JOB_FUNCTION,
        MEMBER_COMPANY_SIZE, etc.
        """
        s = to_linkedin_date(start_date)
        e = to_linkedin_date(end_date)
        field_list = ",".join(_DEFAULT_ANALYTICS_FIELDS)

        acct_urn = (
            f"urn%3Ali%3AsponsoredAccount%3A{account_id}"
        )
        params: dict[str, str] = {
            "q": "analytics",
            "pivot": pivot,
            "dateRange": (
                f"(start:(year:{s['year']},month:{s['month']},"
                f"day:{s['day']}),"
                f"end:(year:{e['year']},month:{e['month']},"
                f"day:{e['day']}))"
            ),
            "timeGranularity": "ALL",
            "accounts": f"List({acct_urn})",
            "fields": field_list,
        }

        resp = await self.get("/adAnalytics", params=params)
        return resp.get("elements", [])

    # --- Conversion rule methods (CAPI) ---

    async def create_conversion_rule(
        self,
        account_id: int,
        name: str,
        conversion_type: str,
        post_click_window: int = 30,
        view_through_window: int = 7,
        attribution_type: str = "LAST_TOUCH_BY_CAMPAIGN",
    ) -> dict:
        """POST /conversions — create a conversion rule on the ad account.

        Returns conversion with URN (urn:lla:llaPartnerConversion:{id}).
        """
        return await self.post(
            "/conversions",
            json={
                "name": name,
                "account": make_account_urn(account_id),
                "conversionMethod": "CONVERSIONS_API",
                "postClickAttributionWindowSize": post_click_window,
                "viewThroughAttributionWindowSize": view_through_window,
                "attributionType": attribution_type,
                "type": conversion_type,
            },
        )

    async def list_conversion_rules(
        self, account_id: int
    ) -> list[dict]:
        """List all conversion rules for the account."""
        resp = await self.get(
            "/conversions",
            params={
                "q": "account",
                "account": make_account_urn(account_id),
            },
        )
        return resp.get("elements", [])

    async def delete_conversion_rule(
        self, conversion_id: str
    ) -> None:
        """Delete a conversion rule."""
        await self.delete(f"/conversions/{conversion_id}")

    # --- Conversion event sending ---

    async def send_conversion_event(
        self,
        conversion_urn: str,
        email: str,
        event_id: str,
        value_usd: str | None = None,
        user_info: dict | None = None,
        happened_at: datetime | None = None,
    ) -> dict:
        """POST /conversionEvents — send a single conversion event.

        Hashes email (SHA256) before sending.
        event_id enables deduplication with Insight Tag pixel.
        happened_at defaults to now() if not provided.
        """
        ts = happened_at or datetime.now(UTC)
        ts_ms = int(ts.timestamp() * 1000)
        hashed_email = hash_email_for_linkedin(email)

        body: dict[str, Any] = {
            "conversion": conversion_urn,
            "conversionHappenedAt": ts_ms,
            "eventId": event_id,
            "user": {
                "userIds": [
                    {
                        "idType": "SHA256_EMAIL",
                        "idValue": hashed_email,
                    }
                ],
            },
        }

        if value_usd:
            body["conversionValue"] = {
                "currencyCode": "USD",
                "amount": value_usd,
            }
        if user_info:
            body["user"]["userInfo"] = user_info

        return await self.post("/conversionEvents", json=body)

    async def send_conversion_events_batch(
        self,
        conversion_urn: str,
        events: list[dict],
    ) -> dict:
        """Batch send multiple conversion events.

        Each event: {email, event_id, value_usd?, user_info?, happened_at?}
        """
        elements = []
        for evt in events:
            ts = evt.get("happened_at") or datetime.now(UTC)
            ts_ms = int(ts.timestamp() * 1000)
            hashed_email = hash_email_for_linkedin(evt["email"])

            element: dict[str, Any] = {
                "conversion": conversion_urn,
                "conversionHappenedAt": ts_ms,
                "eventId": evt["event_id"],
                "user": {
                    "userIds": [
                        {
                            "idType": "SHA256_EMAIL",
                            "idValue": hashed_email,
                        }
                    ],
                },
            }
            if evt.get("value_usd"):
                element["conversionValue"] = {
                    "currencyCode": "USD",
                    "amount": evt["value_usd"],
                }
            if evt.get("user_info"):
                element["user"]["userInfo"] = evt["user_info"]
            elements.append(element)

        return await self.post(
            "/conversionEvents",
            json={"elements": elements},
        )

    # --- Lead Gen Form methods ---

    async def create_lead_gen_form(
        self,
        account_id: int,
        name: str,
        headline: str,
        description: str,
        privacy_policy_url: str,
        thank_you_message: str,
        thank_you_landing_page_url: str | None = None,
        questions: list[dict] | None = None,
    ) -> dict:
        """POST /leadGenerationForms — create a lead gen form.

        Standard field types (auto-filled from profile): FIRST_NAME,
        LAST_NAME, EMAIL, PHONE_NUMBER, COMPANY_NAME, JOB_TITLE, etc.
        Custom: fieldType='CUSTOM' with customQuestionText and answerType.
        """
        body: dict[str, Any] = {
            "account": make_account_urn(account_id),
            "name": name,
            "headline": headline,
            "description": description,
            "privacyPolicyUrl": privacy_policy_url,
            "thankYouMessage": thank_you_message,
        }
        if thank_you_landing_page_url:
            body["thankYouLandingPageUrl"] = thank_you_landing_page_url
        if questions:
            body["questions"] = questions

        return await self.post("/leadGenerationForms", json=body)

    async def list_lead_gen_forms(
        self, account_id: int
    ) -> list[dict]:
        """List all lead gen forms for the account."""
        resp = await self.get(
            "/leadGenerationForms",
            params={
                "q": "account",
                "account": make_account_urn(account_id),
            },
        )
        return resp.get("elements", [])

    async def get_lead_gen_form(self, form_id: int) -> dict:
        """Get form details."""
        return await self.get(f"/leadGenerationForms/{form_id}")

    async def get_lead_submissions(
        self,
        account_id: int,
        form_id: int,
        submitted_after: int | None = None,
    ) -> list[dict]:
        """GET /leadFormResponses — retrieve lead submissions.

        Requires r_marketing_leadgen_automation scope.
        submitted_after in epoch milliseconds for incremental polling.
        """
        params: dict[str, str] = {
            "q": "owner",
            "owner": make_account_urn(account_id),
            "leadGenerationForm": (
                f"urn:li:leadGenerationForm:{form_id}"
            ),
        }
        if submitted_after is not None:
            params["submittedAfter"] = str(submitted_after)

        resp = await self.get("/leadFormResponses", params=params)
        return resp.get("elements", [])
