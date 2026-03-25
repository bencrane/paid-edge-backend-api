"""dub.co tracked link integration (BJC-64).

API client for dub.co short link management:
- create_link(destination_url, ...) → TrackedLink
- get_link_analytics(link_id, ...) → LinkAnalytics

Flow: Campaign creation auto-generates a dub.co short link pointing to the
campaign's landing page. Short link used in ad copy. Click analytics supplement
ad platform metrics.

Ref: docs/DUBCO_API_REFERENCE.md
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from pydantic import BaseModel

from app.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dub.co"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TrackedLink(BaseModel):
    """Response from dub.co link creation."""

    id: str
    short_link: str
    domain: str
    key: str
    url: str
    external_id: str | None = None
    tenant_id: str | None = None
    clicks: int = 0
    leads: int = 0
    sales: int = 0
    created_at: str | None = None


class LinkAnalytics(BaseModel):
    """Aggregated click analytics for a link."""

    clicks: int = 0
    leads: int = 0
    sales: int = 0
    sale_amount: float = 0
    timeseries: list[dict[str, Any]] | None = None
    countries: list[dict[str, Any]] | None = None
    devices: list[dict[str, Any]] | None = None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class DubCoClient:
    """httpx-based client for the dub.co REST API."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.DUBCO_API_KEY
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Link CRUD
    # ------------------------------------------------------------------

    async def create_link(
        self,
        destination_url: str,
        *,
        domain: str | None = None,
        slug: str | None = None,
        external_id: str | None = None,
        tenant_id: str | None = None,
        tag_names: list[str] | None = None,
        track_conversion: bool = True,
        utm_source: str | None = None,
        utm_medium: str | None = None,
        utm_campaign: str | None = None,
    ) -> TrackedLink:
        """Create a short link on dub.co.

        Args:
            destination_url: The landing page / destination URL.
            domain: Custom short link domain (e.g., go.clientdomain.com).
            slug: Custom slug (e.g., "cmmc-q1"). Random if omitted.
            external_id: PaidEdge campaign ID — unique per workspace.
            tenant_id: PaidEdge tenant/org ID for multi-tenant isolation.
            tag_names: Labels for filtering (e.g., ["q1-2026", "cmmc"]).
            track_conversion: Enable conversion tracking (default True).
            utm_source, utm_medium, utm_campaign: UTM parameters.

        Returns:
            TrackedLink with short_link URL and metadata.
        """
        payload: dict[str, Any] = {
            "url": destination_url,
            "trackConversion": track_conversion,
        }
        if domain:
            payload["domain"] = domain
        if slug:
            payload["key"] = slug
        if external_id:
            payload["externalId"] = external_id
        if tenant_id:
            payload["tenantId"] = tenant_id
        if tag_names:
            payload["tagNames"] = tag_names
        if utm_source:
            payload["utm_source"] = utm_source
        if utm_medium:
            payload["utm_medium"] = utm_medium
        if utm_campaign:
            payload["utm_campaign"] = utm_campaign

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{BASE_URL}/links",
                headers=self._headers,
                json=payload,
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()

        return TrackedLink(
            id=data["id"],
            short_link=data["shortLink"],
            domain=data.get("domain", ""),
            key=data.get("key", ""),
            url=data.get("url", destination_url),
            external_id=data.get("externalId"),
            tenant_id=data.get("tenantId"),
            clicks=data.get("clicks", 0),
            leads=data.get("leads", 0),
            sales=data.get("sales", 0),
            created_at=data.get("createdAt"),
        )

    async def get_link(self, external_id: str) -> TrackedLink:
        """Retrieve a link by PaidEdge external ID."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/links/info",
                headers=self._headers,
                params={"externalId": f"ext_{external_id}"},
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()

        return TrackedLink(
            id=data["id"],
            short_link=data["shortLink"],
            domain=data.get("domain", ""),
            key=data.get("key", ""),
            url=data.get("url", ""),
            external_id=data.get("externalId"),
            tenant_id=data.get("tenantId"),
            clicks=data.get("clicks", 0),
            leads=data.get("leads", 0),
            sales=data.get("sales", 0),
            created_at=data.get("createdAt"),
        )

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    async def get_link_analytics(
        self,
        link_id: str,
        *,
        interval: str = "30d",
        group_by: str = "count",
    ) -> LinkAnalytics:
        """Get click analytics for a link.

        Args:
            link_id: dub.co link ID or ext_{externalId}.
            interval: Time window — 24h, 7d, 30d, 90d, 1y, all.
            group_by: Dimension — count, timeseries, countries, devices.

        Returns:
            LinkAnalytics with aggregated click data.
        """
        params: dict[str, str] = {
            "externalId": f"ext_{link_id}",
            "interval": interval,
            "groupBy": group_by,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/analytics",
                headers=self._headers,
                params=params,
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()

        # Shape depends on groupBy
        if group_by == "count":
            return LinkAnalytics(
                clicks=data.get("clicks", 0),
                leads=data.get("leads", 0),
                sales=data.get("sales", 0),
                sale_amount=data.get("saleAmount", 0),
            )

        if group_by == "timeseries":
            return LinkAnalytics(timeseries=data if isinstance(data, list) else [])

        if group_by == "countries":
            return LinkAnalytics(countries=data if isinstance(data, list) else [])

        if group_by == "devices":
            return LinkAnalytics(devices=data if isinstance(data, list) else [])

        # Fallback: return raw count-style
        return LinkAnalytics(
            clicks=data.get("clicks", 0) if isinstance(data, dict) else 0,
        )


# ---------------------------------------------------------------------------
# Campaign integration helper
# ---------------------------------------------------------------------------


async def create_campaign_tracked_link(
    campaign_id: str,
    campaign_name: str,
    landing_page_url: str,
    tenant_id: str,
    *,
    domain: str | None = None,
    dubco_client: DubCoClient | None = None,
) -> TrackedLink | None:
    """Auto-generate a tracked short link for a campaign.

    Called during campaign creation or when a landing page is attached.
    Returns None if dub.co is not configured.
    """
    api_key = settings.DUBCO_API_KEY
    if not api_key:
        logger.info("DUBCO_API_KEY not set — skipping tracked link generation")
        return None

    client = dubco_client or DubCoClient(api_key=api_key)

    # Generate a URL-safe slug from campaign name
    slug = campaign_name.lower().replace(" ", "-")[:30]

    try:
        link = await client.create_link(
            destination_url=landing_page_url,
            domain=domain,
            slug=slug,
            external_id=campaign_id,
            tenant_id=tenant_id,
            tag_names=[campaign_name[:50]],
            track_conversion=True,
            utm_source="paidedge",
            utm_medium="campaign",
            utm_campaign=campaign_id,
        )
        logger.info(
            "Created tracked link %s → %s for campaign %s",
            link.short_link,
            landing_page_url,
            campaign_id,
        )
        return link
    except Exception:
        logger.exception("Failed to create tracked link for campaign %s", campaign_id)
        return None
