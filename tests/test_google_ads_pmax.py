"""Tests for Google Ads Performance Max campaign support (BJC-158)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_pmax import (
    PMAX_BIDDING_STRATEGIES,
    PMAX_LIMITS,
    GoogleAdsPMaxService,
    PMaxValidationError,
    validate_pmax_assets,
)


@pytest.fixture
def mock_service():
    service = MagicMock(spec=GoogleAdsService)
    service.customer_id = "1234567890"
    service.enums = MagicMock()
    service._get_type = MagicMock()
    service._get_service = MagicMock()
    service.mutate = AsyncMock()
    service.search_stream = AsyncMock(return_value=[])
    return service


@pytest.fixture
def pmax_service(mock_service):
    return GoogleAdsPMaxService(mock_service)


# --- Validation ---


class TestValidation:
    def test_valid_assets(self):
        errors = validate_pmax_assets(
            headlines=["H1 short", "H2 short", "H3 short"],
            long_headlines=["A longer headline for PMax display"],
            descriptions=["Description one for PMax ads", "Description two text"],
        )
        assert errors == []

    def test_too_few_headlines(self):
        errors = validate_pmax_assets(
            headlines=["H1", "H2"],
            long_headlines=["Long H1"],
            descriptions=["D1", "D2"],
        )
        assert any("headlines" in e.lower() for e in errors)

    def test_too_many_headlines(self):
        errors = validate_pmax_assets(
            headlines=[f"H{i}" for i in range(6)],
            long_headlines=["Long H1"],
            descriptions=["D1", "D2"],
        )
        assert any("Max 5 headlines" in e for e in errors)

    def test_headline_too_long(self):
        errors = validate_pmax_assets(
            headlines=["x" * 31, "H2", "H3"],
            long_headlines=["Long H1"],
            descriptions=["D1", "D2"],
        )
        assert any("Headline 1 is 31 chars" in e for e in errors)

    def test_too_few_long_headlines(self):
        errors = validate_pmax_assets(
            headlines=["H1", "H2", "H3"],
            long_headlines=[],
            descriptions=["D1", "D2"],
        )
        assert any("long headlines" in e.lower() for e in errors)

    def test_long_headline_too_long(self):
        errors = validate_pmax_assets(
            headlines=["H1", "H2", "H3"],
            long_headlines=["x" * 91],
            descriptions=["D1", "D2"],
        )
        assert any("Long headline 1 is 91 chars" in e for e in errors)

    def test_too_few_descriptions(self):
        errors = validate_pmax_assets(
            headlines=["H1", "H2", "H3"],
            long_headlines=["Long H1"],
            descriptions=["D1"],
        )
        assert any("descriptions" in e.lower() for e in errors)

    def test_description_too_long(self):
        errors = validate_pmax_assets(
            headlines=["H1", "H2", "H3"],
            long_headlines=["Long H1"],
            descriptions=["x" * 91, "D2"],
        )
        assert any("Description 1 is 91 chars" in e for e in errors)

    def test_at_exact_limits(self):
        errors = validate_pmax_assets(
            headlines=["x" * 30] * 5,
            long_headlines=["y" * 90] * 5,
            descriptions=["z" * 90] * 5,
        )
        assert errors == []

    def test_multiple_errors(self):
        errors = validate_pmax_assets(
            headlines=["H1"],
            long_headlines=[],
            descriptions=["D1"],
        )
        assert len(errors) >= 3


# --- Create PMax campaign ---


class TestCreatePMaxCampaign:
    @pytest.mark.asyncio
    async def test_create_pmax_campaign(self, pmax_service, mock_service):
        budget_response = MagicMock()
        budget_response.results = [
            MagicMock(resource_name="customers/123/campaignBudgets/1")
        ]
        campaign_response = MagicMock()
        campaign_response.results = [
            MagicMock(resource_name="customers/123/campaigns/2")
        ]
        mock_service.mutate.side_effect = [budget_response, campaign_response]

        result = await pmax_service.create_pmax_campaign(
            campaign_name="PMax B2B Campaign",
            daily_budget_dollars=100.0,
        )

        assert result["budget_resource_name"] == "customers/123/campaignBudgets/1"
        assert result["campaign_resource_name"] == "customers/123/campaigns/2"
        assert mock_service.mutate.call_count == 2

    @pytest.mark.asyncio
    async def test_create_pmax_invalid_bidding(self, pmax_service):
        with pytest.raises(PMaxValidationError, match="conversion-based bidding"):
            await pmax_service.create_pmax_campaign(
                campaign_name="Bad PMax",
                daily_budget_dollars=50.0,
                bidding_strategy="manual_cpc",
            )

    @pytest.mark.asyncio
    async def test_create_pmax_maximize_conversion_value(
        self, pmax_service, mock_service
    ):
        mock_service.mutate.side_effect = [
            MagicMock(results=[MagicMock(resource_name="budget/1")]),
            MagicMock(results=[MagicMock(resource_name="campaigns/2")]),
        ]

        result = await pmax_service.create_pmax_campaign(
            campaign_name="ROAS PMax",
            daily_budget_dollars=200.0,
            bidding_strategy="maximize_conversion_value",
            target_roas=3.0,
        )

        assert result["campaign_resource_name"] == "campaigns/2"


# --- Create asset group ---


class TestCreateAssetGroup:
    @pytest.mark.asyncio
    async def test_create_asset_group(self, pmax_service, mock_service):
        # Mock: asset group creation + 7 link operations (3 headlines + 1 long + 2 desc + 1 biz name = 7 * 2 calls)
        mock_service.mutate.return_value = MagicMock(
            results=[MagicMock(resource_name="asset/1")]
        )

        result = await pmax_service.create_asset_group(
            campaign_resource_name="customers/123/campaigns/2",
            group_name="Asset Group 1",
            final_url="https://example.com",
            headlines=["H1 text", "H2 text", "H3 text"],
            long_headlines=["Long headline for display ads"],
            descriptions=["Description one", "Description two"],
            business_name="Acme Inc",
        )

        assert result == "asset/1"

    @pytest.mark.asyncio
    async def test_create_asset_group_validation_fails(self, pmax_service):
        with pytest.raises(PMaxValidationError, match="validation failed"):
            await pmax_service.create_asset_group(
                campaign_resource_name="customers/123/campaigns/2",
                group_name="Bad Group",
                final_url="https://example.com",
                headlines=["H1"],  # too few
                long_headlines=[],  # too few
                descriptions=["D1"],  # too few
            )


# --- Audience signals ---


class TestAudienceSignals:
    @pytest.mark.asyncio
    async def test_add_audience_signal(self, pmax_service, mock_service):
        mock_service.mutate.return_value = MagicMock()

        await pmax_service.add_audience_signal(
            asset_group_resource_name="customers/123/assetGroups/1",
            user_list_resource_name="customers/123/userLists/456",
        )

        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_audience_signal_no_user_list(self, pmax_service, mock_service):
        mock_service.mutate.return_value = MagicMock()

        await pmax_service.add_audience_signal(
            asset_group_resource_name="customers/123/assetGroups/1",
        )

        mock_service.mutate.assert_called_once()


# --- Get PMax campaigns ---


class TestGetPMaxCampaigns:
    @pytest.mark.asyncio
    async def test_get_pmax_campaigns(self, pmax_service, mock_service):
        mock_row = MagicMock()
        mock_row.campaign.id = 999
        mock_row.campaign.name = "PMax Campaign"
        mock_row.campaign.status.name = "ENABLED"
        mock_row.metrics.impressions = 5000
        mock_row.metrics.clicks = 200
        mock_row.metrics.cost_micros = 50_000_000
        mock_row.metrics.conversions = 15.0
        mock_row.metrics.conversions_value = 100_000_000
        mock_service.search_stream.return_value = [mock_row]

        result = await pmax_service.get_pmax_campaigns()

        assert len(result) == 1
        assert result[0]["id"] == "999"
        assert result[0]["name"] == "PMax Campaign"
        assert result[0]["cost_dollars"] == 50.0
        assert result[0]["conversions"] == 15.0

    @pytest.mark.asyncio
    async def test_get_pmax_campaigns_empty(self, pmax_service, mock_service):
        mock_service.search_stream.return_value = []
        result = await pmax_service.get_pmax_campaigns()
        assert result == []


# --- Asset group performance ---


class TestAssetGroupPerformance:
    @pytest.mark.asyncio
    async def test_get_asset_group_performance(self, pmax_service, mock_service):
        mock_row = MagicMock()
        mock_row.asset_group.id = 111
        mock_row.asset_group.name = "Asset Group 1"
        mock_row.asset_group.status.name = "ENABLED"
        mock_row.metrics.impressions = 1000
        mock_row.metrics.clicks = 50
        mock_row.metrics.cost_micros = 10_000_000
        mock_row.metrics.conversions = 3.0
        mock_service.search_stream.return_value = [mock_row]

        result = await pmax_service.get_asset_group_performance("999")

        assert len(result) == 1
        assert result[0]["name"] == "Asset Group 1"
        assert result[0]["cost_dollars"] == 10.0


# --- Constants ---


class TestConstants:
    def test_pmax_bidding_strategies(self):
        assert "maximize_conversions" in PMAX_BIDDING_STRATEGIES
        assert "maximize_conversion_value" in PMAX_BIDDING_STRATEGIES
        assert "manual_cpc" not in PMAX_BIDDING_STRATEGIES

    def test_pmax_limits(self):
        assert PMAX_LIMITS["headlines_min"] == 3
        assert PMAX_LIMITS["headlines_max"] == 5
        assert PMAX_LIMITS["headline_max_chars"] == 30
        assert PMAX_LIMITS["descriptions_min"] == 2
        assert PMAX_LIMITS["description_max_chars"] == 90
