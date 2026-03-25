"""Tests for dub.co tracked link integration (BJC-64).

Covers: DubCoClient CRUD, analytics, campaign helper, edge cases.
All httpx calls are mocked — no live credentials needed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.integrations.dubco import (
    DubCoClient,
    LinkAnalytics,
    TrackedLink,
    create_campaign_tracked_link,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SAMPLE_LINK_RESPONSE = {
    "id": "link-abc123",
    "shortLink": "https://dub.sh/cmmc-q1",
    "domain": "dub.sh",
    "key": "cmmc-q1",
    "url": "https://lp.example.com/cmmc",
    "externalId": "camp-001",
    "tenantId": "org-1",
    "clicks": 42,
    "leads": 5,
    "sales": 1,
    "createdAt": "2026-01-15T10:00:00Z",
}


def _mock_httpx_response(json_data: dict | list, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# DubCoClient.create_link
# ---------------------------------------------------------------------------


class TestCreateLink:
    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_create_link_basic(self, mock_post):
        mock_post.return_value = _mock_httpx_response(SAMPLE_LINK_RESPONSE)

        client = DubCoClient(api_key="test-key")
        link = await client.create_link("https://lp.example.com/cmmc")

        assert isinstance(link, TrackedLink)
        assert link.id == "link-abc123"
        assert link.short_link == "https://dub.sh/cmmc-q1"
        assert link.domain == "dub.sh"
        assert link.key == "cmmc-q1"
        assert link.url == "https://lp.example.com/cmmc"
        assert link.clicks == 42
        assert link.leads == 5
        assert link.sales == 1

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["url"] == "https://lp.example.com/cmmc"
        assert payload["trackConversion"] is True

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_create_link_all_params(self, mock_post):
        mock_post.return_value = _mock_httpx_response(SAMPLE_LINK_RESPONSE)

        client = DubCoClient(api_key="test-key")
        await client.create_link(
            "https://lp.example.com/cmmc",
            domain="go.client.com",
            slug="cmmc-q1",
            external_id="camp-001",
            tenant_id="org-1",
            tag_names=["q1-2026", "cmmc"],
            track_conversion=True,
            utm_source="paidedge",
            utm_medium="campaign",
            utm_campaign="camp-001",
        )

        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["domain"] == "go.client.com"
        assert payload["key"] == "cmmc-q1"
        assert payload["externalId"] == "camp-001"
        assert payload["tenantId"] == "org-1"
        assert payload["tagNames"] == ["q1-2026", "cmmc"]
        assert payload["utm_source"] == "paidedge"
        assert payload["utm_medium"] == "campaign"
        assert payload["utm_campaign"] == "camp-001"

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_create_link_uses_auth_header(self, mock_post):
        mock_post.return_value = _mock_httpx_response(SAMPLE_LINK_RESPONSE)

        client = DubCoClient(api_key="my-secret-key")
        await client.create_link("https://example.com")

        call_kwargs = mock_post.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert headers["Authorization"] == "Bearer my-secret-key"

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_create_link_api_error_raises(self, mock_post):
        mock_post.return_value = _mock_httpx_response({}, status_code=422)
        mock_post.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "422 Unprocessable Entity",
            request=MagicMock(),
            response=mock_post.return_value,
        )

        client = DubCoClient(api_key="test-key")
        with pytest.raises(httpx.HTTPStatusError):
            await client.create_link("https://example.com")

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_create_link_optional_fields_omitted(self, mock_post):
        """When optional params are None, they should not appear in the payload."""
        mock_post.return_value = _mock_httpx_response(SAMPLE_LINK_RESPONSE)

        client = DubCoClient(api_key="test-key")
        await client.create_link("https://example.com")

        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "domain" not in payload
        assert "key" not in payload
        assert "externalId" not in payload
        assert "tenantId" not in payload
        assert "tagNames" not in payload
        assert "utm_source" not in payload


# ---------------------------------------------------------------------------
# DubCoClient.get_link
# ---------------------------------------------------------------------------


class TestGetLink:
    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_get_link_by_external_id(self, mock_get):
        mock_get.return_value = _mock_httpx_response(SAMPLE_LINK_RESPONSE)

        client = DubCoClient(api_key="test-key")
        link = await client.get_link("camp-001")

        assert isinstance(link, TrackedLink)
        assert link.id == "link-abc123"
        assert link.external_id == "camp-001"

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["externalId"] == "ext_camp-001"

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_get_link_not_found_raises(self, mock_get):
        mock_get.return_value = _mock_httpx_response({}, status_code=404)
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=MagicMock(),
            response=mock_get.return_value,
        )

        client = DubCoClient(api_key="test-key")
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_link("nonexistent")


# ---------------------------------------------------------------------------
# DubCoClient.get_link_analytics
# ---------------------------------------------------------------------------


class TestGetLinkAnalytics:
    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_analytics_count(self, mock_get):
        mock_get.return_value = _mock_httpx_response({
            "clicks": 100,
            "leads": 10,
            "sales": 3,
            "saleAmount": 1500.50,
        })

        client = DubCoClient(api_key="test-key")
        analytics = await client.get_link_analytics("camp-001")

        assert isinstance(analytics, LinkAnalytics)
        assert analytics.clicks == 100
        assert analytics.leads == 10
        assert analytics.sales == 3
        assert analytics.sale_amount == 1500.50

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_analytics_timeseries(self, mock_get):
        ts_data = [
            {"start": "2026-01-01", "clicks": 10},
            {"start": "2026-01-02", "clicks": 15},
        ]
        mock_get.return_value = _mock_httpx_response(ts_data)

        client = DubCoClient(api_key="test-key")
        analytics = await client.get_link_analytics(
            "camp-001", group_by="timeseries"
        )

        assert analytics.timeseries == ts_data

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_analytics_countries(self, mock_get):
        country_data = [{"country": "US", "clicks": 50}, {"country": "UK", "clicks": 30}]
        mock_get.return_value = _mock_httpx_response(country_data)

        client = DubCoClient(api_key="test-key")
        analytics = await client.get_link_analytics(
            "camp-001", group_by="countries"
        )

        assert analytics.countries == country_data

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_analytics_devices(self, mock_get):
        device_data = [{"device": "Desktop", "clicks": 60}, {"device": "Mobile", "clicks": 40}]
        mock_get.return_value = _mock_httpx_response(device_data)

        client = DubCoClient(api_key="test-key")
        analytics = await client.get_link_analytics(
            "camp-001", group_by="devices"
        )

        assert analytics.devices == device_data

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_analytics_passes_interval(self, mock_get):
        mock_get.return_value = _mock_httpx_response({"clicks": 5})

        client = DubCoClient(api_key="test-key")
        await client.get_link_analytics("camp-001", interval="7d")

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["interval"] == "7d"
        assert params["groupBy"] == "count"

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_analytics_unknown_groupby_fallback(self, mock_get):
        """Unknown groupBy returns count-style fallback."""
        mock_get.return_value = _mock_httpx_response({"clicks": 7})

        client = DubCoClient(api_key="test-key")
        analytics = await client.get_link_analytics(
            "camp-001", group_by="unknown"
        )

        assert analytics.clicks == 7


# ---------------------------------------------------------------------------
# create_campaign_tracked_link helper
# ---------------------------------------------------------------------------


class TestCreateCampaignTrackedLink:
    @pytest.mark.asyncio
    @patch("app.integrations.dubco.settings")
    async def test_returns_none_when_no_api_key(self, mock_settings):
        mock_settings.DUBCO_API_KEY = ""

        result = await create_campaign_tracked_link(
            campaign_id="camp-001",
            campaign_name="CMMC Q1 Campaign",
            landing_page_url="https://lp.example.com/cmmc",
            tenant_id="org-1",
        )

        assert result is None

    @pytest.mark.asyncio
    @patch("app.integrations.dubco.settings")
    async def test_creates_link_with_correct_params(self, mock_settings):
        mock_settings.DUBCO_API_KEY = "test-key"

        mock_client = MagicMock(spec=DubCoClient)
        expected_link = TrackedLink(
            id="link-123",
            short_link="https://dub.sh/cmmc-q1-campaign",
            domain="dub.sh",
            key="cmmc-q1-campaign",
            url="https://lp.example.com/cmmc",
            external_id="camp-001",
            tenant_id="org-1",
        )
        mock_client.create_link = AsyncMock(return_value=expected_link)

        result = await create_campaign_tracked_link(
            campaign_id="camp-001",
            campaign_name="CMMC Q1 Campaign",
            landing_page_url="https://lp.example.com/cmmc",
            tenant_id="org-1",
            dubco_client=mock_client,
        )

        assert result is not None
        assert result.short_link == "https://dub.sh/cmmc-q1-campaign"
        assert result.external_id == "camp-001"

        mock_client.create_link.assert_called_once()
        call_kwargs = mock_client.create_link.call_args
        assert call_kwargs.kwargs["external_id"] == "camp-001"
        assert call_kwargs.kwargs["tenant_id"] == "org-1"
        assert call_kwargs.kwargs["utm_source"] == "paidedge"
        assert call_kwargs.kwargs["utm_medium"] == "campaign"
        assert call_kwargs.kwargs["utm_campaign"] == "camp-001"
        assert call_kwargs.kwargs["track_conversion"] is True

    @pytest.mark.asyncio
    @patch("app.integrations.dubco.settings")
    async def test_slug_derived_from_campaign_name(self, mock_settings):
        mock_settings.DUBCO_API_KEY = "test-key"

        mock_client = MagicMock(spec=DubCoClient)
        mock_client.create_link = AsyncMock(
            return_value=TrackedLink(
                id="x", short_link="https://dub.sh/x", domain="dub.sh", key="x", url="https://example.com"
            )
        )

        await create_campaign_tracked_link(
            campaign_id="camp-001",
            campaign_name="CMMC Q1 Campaign With Long Name",
            landing_page_url="https://lp.example.com",
            tenant_id="org-1",
            dubco_client=mock_client,
        )

        call_kwargs = mock_client.create_link.call_args
        slug = call_kwargs.kwargs["slug"]
        # Should be lowercase, hyphens instead of spaces, max 30 chars
        assert slug == "cmmc-q1-campaign-with-long-nam"
        assert len(slug) <= 30
        assert " " not in slug

    @pytest.mark.asyncio
    @patch("app.integrations.dubco.settings")
    async def test_custom_domain_passed_through(self, mock_settings):
        mock_settings.DUBCO_API_KEY = "test-key"

        mock_client = MagicMock(spec=DubCoClient)
        mock_client.create_link = AsyncMock(
            return_value=TrackedLink(
                id="x", short_link="https://go.client.com/x", domain="go.client.com", key="x", url="https://example.com"
            )
        )

        await create_campaign_tracked_link(
            campaign_id="camp-001",
            campaign_name="Test",
            landing_page_url="https://lp.example.com",
            tenant_id="org-1",
            domain="go.client.com",
            dubco_client=mock_client,
        )

        call_kwargs = mock_client.create_link.call_args
        assert call_kwargs.kwargs["domain"] == "go.client.com"

    @pytest.mark.asyncio
    @patch("app.integrations.dubco.settings")
    async def test_returns_none_on_api_error(self, mock_settings):
        """API failures are caught and logged, not raised."""
        mock_settings.DUBCO_API_KEY = "test-key"

        mock_client = MagicMock(spec=DubCoClient)
        mock_client.create_link = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "500 Internal Server Error",
                request=MagicMock(),
                response=MagicMock(),
            )
        )

        result = await create_campaign_tracked_link(
            campaign_id="camp-001",
            campaign_name="Test",
            landing_page_url="https://lp.example.com",
            tenant_id="org-1",
            dubco_client=mock_client,
        )

        assert result is None

    @pytest.mark.asyncio
    @patch("app.integrations.dubco.settings")
    async def test_tag_names_capped_at_50_chars(self, mock_settings):
        mock_settings.DUBCO_API_KEY = "test-key"

        mock_client = MagicMock(spec=DubCoClient)
        mock_client.create_link = AsyncMock(
            return_value=TrackedLink(
                id="x", short_link="https://dub.sh/x", domain="dub.sh", key="x", url="https://example.com"
            )
        )

        long_name = "A" * 100
        await create_campaign_tracked_link(
            campaign_id="camp-001",
            campaign_name=long_name,
            landing_page_url="https://lp.example.com",
            tenant_id="org-1",
            dubco_client=mock_client,
        )

        call_kwargs = mock_client.create_link.call_args
        tag = call_kwargs.kwargs["tag_names"][0]
        assert len(tag) <= 50
