"""Tests for LinkedIn analytics client + metrics mapping to ClickHouse (BJC-136)."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import (
    LinkedInAdsClient,
    from_linkedin_date,
    to_linkedin_date,
)
from app.integrations.linkedin_metrics import (
    build_campaign_id_map,
    insert_linkedin_metrics,
    map_linkedin_analytics_to_campaign_metrics,
)

# --- Helpers ---


def _mock_resp(status_code: int, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


# --- Date conversion utilities ---


class TestDateConversion:
    def test_to_linkedin_date(self):
        result = to_linkedin_date(date(2026, 3, 25))
        assert result == {"year": 2026, "month": 3, "day": 25}

    def test_from_linkedin_date(self):
        result = from_linkedin_date(
            {"year": 2026, "month": 1, "day": 15}
        )
        assert result == date(2026, 1, 15)

    def test_roundtrip(self):
        d = date(2025, 12, 31)
        assert from_linkedin_date(to_linkedin_date(d)) == d


# --- Campaign analytics ---


class TestGetCampaignAnalytics:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_campaign_analytics_default_params(self, client):
        """Should query /adAnalytics with correct date format and params."""
        api_resp = {
            "elements": [
                {
                    "pivotValue": "urn:li:sponsoredCampaign:111",
                    "impressions": 1000,
                    "clicks": 50,
                    "costInLocalCurrency": 75.50,
                    "dateRange": {
                        "start": {"year": 2026, "month": 3, "day": 22},
                        "end": {"year": 2026, "month": 3, "day": 23},
                    },
                }
            ]
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            result = await client.get_campaign_analytics(
                account_id=507404993,
                start_date=date(2026, 3, 22),
                end_date=date(2026, 3, 25),
            )

        assert len(result) == 1
        assert result[0]["impressions"] == 1000

        params = mock_req.call_args.kwargs["params"]
        assert params["q"] == "analytics"
        assert params["pivot"] == "CAMPAIGN"
        assert params["timeGranularity"] == "DAILY"
        assert "year:2026" in params["dateRange"]
        assert "month:3" in params["dateRange"]
        assert "507404993" in params["accounts"]
        assert "impressions" in params["fields"]
        assert "clicks" in params["fields"]

    @pytest.mark.asyncio
    async def test_campaign_analytics_with_ids(self, client):
        """Should filter by campaign IDs when provided."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ) as mock_req,
        ):
            await client.get_campaign_analytics(
                account_id=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 31),
                campaign_ids=[111, 222],
            )

        params = mock_req.call_args.kwargs["params"]
        assert "campaigns" in params
        assert "111" in params["campaigns"]
        assert "222" in params["campaigns"]
        assert "accounts" not in params

    @pytest.mark.asyncio
    async def test_campaign_analytics_empty_response(self, client):
        """Should return empty list and log when no data."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ),
        ):
            result = await client.get_campaign_analytics(
                account_id=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_campaign_analytics_custom_fields(self, client):
        """Should use custom fields when provided."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ) as mock_req,
        ):
            await client.get_campaign_analytics(
                account_id=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
                fields=["impressions", "clicks"],
            )

        params = mock_req.call_args.kwargs["params"]
        assert params["fields"] == "impressions,clicks"


# --- Creative analytics ---


class TestGetCreativeAnalytics:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_creative_analytics_pivot(self, client):
        """Should use CREATIVE pivot."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ) as mock_req,
        ):
            await client.get_creative_analytics(
                account_id=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
            )

        params = mock_req.call_args.kwargs["params"]
        assert params["pivot"] == "CREATIVE"

    @pytest.mark.asyncio
    async def test_creative_analytics_campaign_filter(self, client):
        """Should filter by campaign when provided."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ) as mock_req,
        ):
            await client.get_creative_analytics(
                account_id=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
                campaign_id=12345,
            )

        params = mock_req.call_args.kwargs["params"]
        assert "12345" in params["campaigns"]


# --- Demographic analytics ---


class TestGetDemographicAnalytics:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_demographic_analytics_uses_all_granularity(self, client):
        """Should use ALL granularity for demographic breakdowns."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ) as mock_req,
        ):
            await client.get_demographic_analytics(
                account_id=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                pivot="MEMBER_INDUSTRY",
            )

        params = mock_req.call_args.kwargs["params"]
        assert params["pivot"] == "MEMBER_INDUSTRY"
        assert params["timeGranularity"] == "ALL"


# --- Metrics mapping ---


class TestMapLinkedInAnalytics:
    def test_maps_all_fields_correctly(self):
        """Should map LinkedIn analytics to ClickHouse schema."""
        raw = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "impressions": 1000,
                "clicks": 50,
                "costInLocalCurrency": 75.50,
                "costInUsd": 75.50,
                "externalWebsiteConversions": 5,
                "leadGenerationMailContactInfoShares": 3,
                "oneClickLeads": 2,
                "videoViews": 200,
                "videoCompletions": 100,
                "totalEngagements": 80,
                "likes": 30,
                "comments": 10,
                "shares": 5,
                "textUrlClicks": 20,
                "landingPageClicks": 15,
                "dateRange": {
                    "start": {"year": 2026, "month": 3, "day": 22},
                    "end": {"year": 2026, "month": 3, "day": 23},
                },
            }
        ]
        campaign_id_map = {111: "uuid-campaign-1"}

        result = map_linkedin_analytics_to_campaign_metrics(
            raw, "tenant-1", campaign_id_map
        )

        assert len(result) == 1
        row = result[0]
        assert row["tenant_id"] == "tenant-1"
        assert row["campaign_id"] == "uuid-campaign-1"
        assert row["platform"] == "linkedin"
        assert row["platform_campaign_id"] == "111"
        assert row["date"] == date(2026, 3, 22)
        assert row["spend"] == 75.50
        assert row["impressions"] == 1000
        assert row["clicks"] == 50
        assert row["conversions"] == 5
        assert row["leads"] == 5  # 3 + 2
        assert row["ctr"] == round(50 / 1000, 6)
        assert row["cpc"] == round(75.50 / 50, 2)
        assert row["cpm"] == round((75.50 / 1000) * 1000, 2)
        assert row["roas"] == 0.0

    def test_division_by_zero_impressions(self):
        """CTR and CPM should be 0 when impressions is 0."""
        raw = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "impressions": 0,
                "clicks": 0,
                "costInLocalCurrency": 0,
                "dateRange": {
                    "start": {"year": 2026, "month": 3, "day": 22},
                    "end": {"year": 2026, "month": 3, "day": 23},
                },
            }
        ]
        campaign_id_map = {111: "uuid-1"}

        result = map_linkedin_analytics_to_campaign_metrics(
            raw, "tenant-1", campaign_id_map
        )

        assert len(result) == 1
        assert result[0]["ctr"] == 0.0
        assert result[0]["cpc"] == 0.0
        assert result[0]["cpm"] == 0.0

    def test_division_by_zero_clicks(self):
        """CPC should be 0 when clicks is 0."""
        raw = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "impressions": 1000,
                "clicks": 0,
                "costInLocalCurrency": 50.0,
                "dateRange": {
                    "start": {"year": 2026, "month": 1, "day": 1},
                    "end": {"year": 2026, "month": 1, "day": 2},
                },
            }
        ]
        campaign_id_map = {111: "uuid-1"}

        result = map_linkedin_analytics_to_campaign_metrics(
            raw, "t", campaign_id_map
        )

        assert result[0]["cpc"] == 0.0
        assert result[0]["ctr"] == 0.0

    def test_skips_unmapped_campaigns(self):
        """Should skip elements with no PaidEdge campaign mapping."""
        raw = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:999",
                "impressions": 100,
                "clicks": 10,
                "costInLocalCurrency": 20,
                "dateRange": {
                    "start": {"year": 2026, "month": 1, "day": 1},
                    "end": {"year": 2026, "month": 1, "day": 2},
                },
            }
        ]
        campaign_id_map = {}  # No mappings

        result = map_linkedin_analytics_to_campaign_metrics(
            raw, "t", campaign_id_map
        )

        assert result == []

    def test_handles_missing_fields_gracefully(self):
        """Missing fields should default to 0."""
        raw = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "dateRange": {
                    "start": {"year": 2026, "month": 1, "day": 1},
                    "end": {"year": 2026, "month": 1, "day": 2},
                },
            }
        ]
        campaign_id_map = {111: "uuid-1"}

        result = map_linkedin_analytics_to_campaign_metrics(
            raw, "t", campaign_id_map
        )

        assert len(result) == 1
        assert result[0]["impressions"] == 0
        assert result[0]["clicks"] == 0
        assert result[0]["spend"] == 0.0

    def test_multiple_elements(self):
        """Should process multiple elements correctly."""
        raw = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "impressions": 1000,
                "clicks": 50,
                "costInLocalCurrency": 75,
                "dateRange": {
                    "start": {"year": 2026, "month": 3, "day": 22},
                    "end": {"year": 2026, "month": 3, "day": 23},
                },
            },
            {
                "pivotValue": "urn:li:sponsoredCampaign:222",
                "impressions": 500,
                "clicks": 25,
                "costInLocalCurrency": 40,
                "dateRange": {
                    "start": {"year": 2026, "month": 3, "day": 22},
                    "end": {"year": 2026, "month": 3, "day": 23},
                },
            },
        ]
        campaign_id_map = {111: "uuid-1", 222: "uuid-2"}

        result = map_linkedin_analytics_to_campaign_metrics(
            raw, "t", campaign_id_map
        )

        assert len(result) == 2
        assert result[0]["platform_campaign_id"] == "111"
        assert result[1]["platform_campaign_id"] == "222"


# --- Campaign ID map building ---


class TestBuildCampaignIdMap:
    @pytest.mark.asyncio
    async def test_builds_map_from_platforms_jsonb(self):
        """Should extract LinkedIn campaign IDs from platforms column."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = [
            {
                "id": "uuid-campaign-1",
                "platforms": {
                    "linkedin": {"campaign_id": 507404993}
                },
            },
            {
                "id": "uuid-campaign-2",
                "platforms": {
                    "linkedin": {"campaign_id": 111222333}
                },
            },
            {
                "id": "uuid-campaign-3",
                "platforms": {"meta": {"campaign_id": "meta-123"}},
            },
        ]
        (
            mock_sb.table.return_value
            .select.return_value
            .eq.return_value
            .execute.return_value
        ) = mock_result

        result = await build_campaign_id_map(mock_sb, "tenant-1")

        assert result == {
            507404993: "uuid-campaign-1",
            111222333: "uuid-campaign-2",
        }

    @pytest.mark.asyncio
    async def test_empty_campaigns(self):
        """Should return empty map when no campaigns."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = []
        (
            mock_sb.table.return_value
            .select.return_value
            .eq.return_value
            .execute.return_value
        ) = mock_result

        result = await build_campaign_id_map(mock_sb, "tenant-1")

        assert result == {}


# --- ClickHouse insert ---


class TestInsertLinkedInMetrics:
    @pytest.mark.asyncio
    async def test_inserts_metrics_to_clickhouse(self):
        """Should call clickhouse.insert with correct table and data."""
        mock_ch = MagicMock()
        metrics = [
            {
                "tenant_id": "t1",
                "campaign_id": "c1",
                "platform": "linkedin",
                "platform_campaign_id": "111",
                "platform_ad_group_id": "",
                "platform_ad_id": "",
                "date": date(2026, 3, 22),
                "spend": 75.50,
                "impressions": 1000,
                "clicks": 50,
                "conversions": 5,
                "leads": 3,
                "ctr": 0.05,
                "cpc": 1.51,
                "cpm": 75.50,
                "roas": 0.0,
            }
        ]

        result = await insert_linkedin_metrics(mock_ch, metrics)

        assert result == 1
        mock_ch.insert.assert_called_once()
        call_args = mock_ch.insert.call_args
        assert call_args.args[0] == "paid_edge.campaign_metrics"
        # Verify data structure
        data = call_args.args[1]
        assert len(data) == 1
        assert data[0][0] == "t1"  # tenant_id

    @pytest.mark.asyncio
    async def test_empty_metrics_returns_zero(self):
        """Should not call insert for empty metrics list."""
        mock_ch = MagicMock()

        result = await insert_linkedin_metrics(mock_ch, [])

        assert result == 0
        mock_ch.insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_rows(self):
        """Should insert all rows in one batch."""
        mock_ch = MagicMock()
        metrics = [
            {
                "tenant_id": "t1",
                "campaign_id": f"c{i}",
                "platform": "linkedin",
                "platform_campaign_id": str(i),
                "platform_ad_group_id": "",
                "platform_ad_id": "",
                "date": date(2026, 3, 22),
                "spend": 50.0,
                "impressions": 500,
                "clicks": 25,
                "conversions": 2,
                "leads": 1,
                "ctr": 0.05,
                "cpc": 2.0,
                "cpm": 100.0,
                "roas": 0.0,
            }
            for i in range(5)
        ]

        result = await insert_linkedin_metrics(mock_ch, metrics)

        assert result == 5
        data = mock_ch.insert.call_args.args[1]
        assert len(data) == 5
