"""Tests for Google Ads campaign CRUD + budget + bidding (BJC-142)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService, dollars_to_micros
from app.integrations.google_ads_campaigns import (
    BIDDING_STRATEGIES,
    GoogleAdsCampaignService,
    apply_bidding_strategy,
)


@pytest.fixture
def mock_service():
    service = MagicMock(spec=GoogleAdsService)
    service.customer_id = "1234567890"
    service.enums = MagicMock()
    service._get_type = MagicMock()
    service.mutate = AsyncMock()
    service.search_stream = AsyncMock(return_value=[])
    service.client = MagicMock()
    return service


@pytest.fixture
def campaign_service(mock_service):
    return GoogleAdsCampaignService(mock_service)


class TestBiddingStrategies:
    def test_all_strategies_defined(self):
        assert "manual_cpc" in BIDDING_STRATEGIES
        assert "maximize_clicks" in BIDDING_STRATEGIES
        assert "maximize_conversions" in BIDDING_STRATEGIES
        assert "target_cpa" in BIDDING_STRATEGIES
        assert "target_roas" in BIDDING_STRATEGIES
        assert "maximize_conversion_value" in BIDDING_STRATEGIES

    def test_apply_manual_cpc(self):
        campaign = MagicMock()
        enums = MagicMock()
        apply_bidding_strategy(campaign, "manual_cpc", {}, enums)
        assert campaign.manual_cpc.enhanced_cpc_enabled is False

    def test_apply_maximize_clicks(self):
        campaign = MagicMock()
        enums = MagicMock()
        apply_bidding_strategy(
            campaign, "maximize_clicks", {"cpc_bid_ceiling_micros": 2_000_000}, enums
        )
        assert campaign.maximize_clicks.cpc_bid_ceiling_micros == 2_000_000

    def test_apply_maximize_conversions(self):
        campaign = MagicMock()
        enums = MagicMock()
        apply_bidding_strategy(
            campaign, "maximize_conversions", {"target_cpa_micros": 10_000_000}, enums
        )
        assert campaign.maximize_conversions.target_cpa_micros == 10_000_000

    def test_apply_unknown_raises(self):
        campaign = MagicMock()
        enums = MagicMock()
        with pytest.raises(ValueError, match="Unknown bidding strategy"):
            apply_bidding_strategy(campaign, "unknown_strategy", {}, enums)


class TestCreateBudget:
    @pytest.mark.asyncio
    async def test_create_budget_converts_dollars_to_micros(self, campaign_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="customers/123/campaignBudgets/456")]
        mock_service.mutate.return_value = mock_response

        mock_operation = MagicMock()
        mock_service._get_type.return_value = mock_operation

        result = await campaign_service.create_campaign_budget(50.0, name="Test Budget")

        assert result == "customers/123/campaignBudgets/456"
        assert mock_operation.create.amount_micros == 50_000_000


class TestCreateCampaign:
    @pytest.mark.asyncio
    async def test_create_search_campaign(self, campaign_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="customers/123/campaigns/789")]
        mock_service.mutate.return_value = mock_response

        mock_operation = MagicMock()
        mock_service._get_type.return_value = mock_operation

        result = await campaign_service.create_search_campaign(
            name="Test Campaign",
            budget_resource="customers/123/campaignBudgets/456",
            bidding_strategy="maximize_conversions",
        )

        assert result == "customers/123/campaigns/789"
        mock_service.mutate.assert_called_once()


class TestGetCampaigns:
    @pytest.mark.asyncio
    async def test_get_campaigns_returns_list(self, campaign_service, mock_service):
        mock_row = MagicMock()
        mock_row.campaign.id = 789
        mock_row.campaign.name = "Test"
        mock_row.campaign.status.name = "ENABLED"
        mock_row.campaign.advertising_channel_type.name = "SEARCH"
        mock_row.campaign.campaign_budget = "customers/123/campaignBudgets/456"
        mock_row.metrics.cost_micros = 5_000_000
        mock_row.metrics.impressions = 1000
        mock_row.metrics.clicks = 50
        mock_service.search_stream.return_value = [mock_row]

        result = await campaign_service.get_campaigns()

        assert len(result) == 1
        assert result[0]["id"] == "789"
        assert result[0]["name"] == "Test"
        assert result[0]["status"] == "ENABLED"
        assert result[0]["cost_dollars"] == 5.0

    @pytest.mark.asyncio
    async def test_get_campaigns_with_status_filter(self, campaign_service, mock_service):
        mock_service.search_stream.return_value = []
        await campaign_service.get_campaigns(statuses=["ENABLED", "PAUSED"])
        query = mock_service.search_stream.call_args[0][0]
        assert "ENABLED" in query
        assert "PAUSED" in query


class TestUpdateCampaignStatus:
    @pytest.mark.asyncio
    async def test_update_status(self, campaign_service, mock_service):
        mock_operation = MagicMock()
        mock_service._get_type.return_value = mock_operation
        mock_service.mutate.return_value = MagicMock()

        await campaign_service.update_campaign_status("789", "PAUSED")

        mock_service.mutate.assert_called_once()


class TestCreateAdGroup:
    @pytest.mark.asyncio
    async def test_create_ad_group(self, campaign_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="customers/123/adGroups/111")
        ]
        mock_service.mutate.return_value = mock_response
        mock_operation = MagicMock()
        mock_service._get_type.return_value = mock_operation

        result = await campaign_service.create_ad_group(
            campaign_resource="customers/123/campaigns/789",
            name="Test Ad Group",
            cpc_bid_micros=1_500_000,
        )

        assert result == "customers/123/adGroups/111"
        assert mock_operation.create.cpc_bid_micros == 1_500_000


class TestGeoTargeting:
    @pytest.mark.asyncio
    async def test_add_location_targeting(self, campaign_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="customers/123/campaignCriteria/1"),
            MagicMock(resource_name="customers/123/campaignCriteria/2"),
        ]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        result = await campaign_service.add_location_targeting("789", [2840, 1014221])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_add_location_exclusion(self, campaign_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="customers/123/campaignCriteria/3")]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        result = await campaign_service.add_location_exclusion("789", [9999])
        assert len(result) == 1
