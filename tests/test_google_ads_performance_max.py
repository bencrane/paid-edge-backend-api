"""Tests for Google Ads Performance Max campaign support (BJC-158)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_performance_max import (
    PMAX_ALLOWED_BIDDING,
    PMAX_DESCRIPTION_MAX,
    PMAX_DESCRIPTION_MAX_CHARS,
    PMAX_DESCRIPTION_MIN,
    PMAX_HEADLINE_MAX,
    PMAX_HEADLINE_MAX_CHARS,
    PMAX_HEADLINE_MIN,
    PMAX_LONG_HEADLINE_MIN,
    GoogleAdsPMaxClient,
    PMaxAssetValidator,
    PMaxValidationError,
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
def pmax_client(mock_service):
    return GoogleAdsPMaxClient(mock_service)


@pytest.fixture
def valid_headlines():
    return ["Headline One", "Headline Two", "Headline Three"]


@pytest.fixture
def valid_long_headlines():
    return ["This is a longer headline for Performance Max campaigns"]


@pytest.fixture
def valid_descriptions():
    return ["Description one for PMax ads.", "Description two for PMax ads."]


# --- Asset validation ---


class TestPMaxAssetValidator:
    def test_valid_assets(self, valid_headlines, valid_long_headlines, valid_descriptions):
        errors = PMaxAssetValidator.validate(
            valid_headlines, valid_long_headlines, valid_descriptions, "Acme Corp"
        )
        assert errors == []

    def test_too_few_headlines(self):
        errors = PMaxAssetValidator.validate(
            ["H1"], ["Long headline"], ["D1", "D2"], "Acme"
        )
        assert any("short headlines" in e for e in errors)

    def test_too_many_headlines(self):
        errors = PMaxAssetValidator.validate(
            [f"H{i}" for i in range(PMAX_HEADLINE_MAX + 1)],
            ["Long"],
            ["D1", "D2"],
            "Acme",
        )
        assert any("Max" in e and "short headlines" in e for e in errors)

    def test_headline_too_long(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "A" * (PMAX_HEADLINE_MAX_CHARS + 1)],
            ["Long"],
            ["D1", "D2"],
            "Acme",
        )
        assert any("Short headline 3" in e for e in errors)

    def test_too_few_long_headlines(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "H3"], [], ["D1", "D2"], "Acme"
        )
        assert any("long headline" in e for e in errors)

    def test_long_headline_too_long(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "H3"],
            ["A" * 91],
            ["D1", "D2"],
            "Acme",
        )
        assert any("Long headline" in e for e in errors)

    def test_too_few_descriptions(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "H3"], ["Long"], ["D1"], "Acme"
        )
        assert any("descriptions" in e for e in errors)

    def test_too_many_descriptions(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "H3"],
            ["Long"],
            [f"D{i}" for i in range(PMAX_DESCRIPTION_MAX + 1)],
            "Acme",
        )
        assert any("Max" in e and "descriptions" in e for e in errors)

    def test_description_too_long(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "H3"],
            ["Long"],
            ["D1", "B" * (PMAX_DESCRIPTION_MAX_CHARS + 1)],
            "Acme",
        )
        assert any("Description 2" in e for e in errors)

    def test_missing_business_name(self):
        errors = PMaxAssetValidator.validate(
            ["H1", "H2", "H3"], ["Long"], ["D1", "D2"], ""
        )
        assert any("Business name" in e for e in errors)

    def test_multiple_errors(self):
        errors = PMaxAssetValidator.validate([], [], [], "")
        assert len(errors) >= 4  # headlines, long headlines, descriptions, business name


# --- Campaign creation ---


class TestCreatePMaxCampaign:
    @pytest.mark.asyncio
    async def test_create_pmax_campaign(self, pmax_client, mock_service):
        budget_response = MagicMock()
        budget_response.results = [
            MagicMock(resource_name="customers/123/campaignBudgets/1")
        ]
        campaign_response = MagicMock()
        campaign_response.results = [
            MagicMock(resource_name="customers/123/campaigns/2")
        ]
        mock_service.mutate.side_effect = [budget_response, campaign_response]

        result = await pmax_client.create_pmax_campaign(
            campaign_name="PMax Test",
            daily_budget_dollars=50.0,
        )

        assert result["budget_resource_name"] == "customers/123/campaignBudgets/1"
        assert result["campaign_resource_name"] == "customers/123/campaigns/2"
        assert mock_service.mutate.call_count == 2

    @pytest.mark.asyncio
    async def test_create_pmax_with_target_cpa(self, pmax_client, mock_service):
        budget_response = MagicMock()
        budget_response.results = [
            MagicMock(resource_name="customers/123/campaignBudgets/1")
        ]
        campaign_response = MagicMock()
        campaign_response.results = [
            MagicMock(resource_name="customers/123/campaigns/2")
        ]
        mock_service.mutate.side_effect = [budget_response, campaign_response]

        result = await pmax_client.create_pmax_campaign(
            campaign_name="PMax CPA",
            daily_budget_dollars=100.0,
            target_cpa_dollars=25.0,
        )

        assert result["campaign_resource_name"] == "customers/123/campaigns/2"

    @pytest.mark.asyncio
    async def test_create_pmax_with_maximize_conversion_value(
        self, pmax_client, mock_service
    ):
        budget_response = MagicMock()
        budget_response.results = [
            MagicMock(resource_name="customers/123/campaignBudgets/1")
        ]
        campaign_response = MagicMock()
        campaign_response.results = [
            MagicMock(resource_name="customers/123/campaigns/2")
        ]
        mock_service.mutate.side_effect = [budget_response, campaign_response]

        result = await pmax_client.create_pmax_campaign(
            campaign_name="PMax ROAS",
            daily_budget_dollars=200.0,
            bidding_strategy="MAXIMIZE_CONVERSION_VALUE",
            target_roas=3.0,
        )

        assert result["campaign_resource_name"] == "customers/123/campaigns/2"

    @pytest.mark.asyncio
    async def test_invalid_bidding_strategy_raises(self, pmax_client):
        with pytest.raises(PMaxValidationError, match="conversion-based bidding"):
            await pmax_client.create_pmax_campaign(
                campaign_name="Bad",
                daily_budget_dollars=50.0,
                bidding_strategy="MANUAL_CPC",
            )


# --- Asset group creation ---


class TestCreateAssetGroup:
    @pytest.mark.asyncio
    async def test_create_asset_group(
        self,
        pmax_client,
        mock_service,
        valid_headlines,
        valid_long_headlines,
        valid_descriptions,
    ):
        ag_response = MagicMock()
        ag_response.results = [
            MagicMock(resource_name="customers/123/assetGroups/1")
        ]
        mock_service.mutate.return_value = ag_response

        # Mock _link_text_assets
        mock_asset_service = MagicMock()
        mock_asset_response = MagicMock()
        mock_asset_response.results = [
            MagicMock(resource_name="customers/123/assets/1")
        ]
        mock_asset_service.mutate_assets.return_value = mock_asset_response

        mock_aga_service = MagicMock()
        mock_service._get_service.side_effect = lambda name: {
            "AssetService": mock_asset_service,
            "AssetGroupAssetService": mock_aga_service,
        }.get(name, MagicMock())

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_asset_response
            )

            result = await pmax_client.create_asset_group(
                campaign_resource_name="customers/123/campaigns/2",
                group_name="Asset Group 1",
                final_url="https://example.com",
                headlines=valid_headlines,
                long_headlines=valid_long_headlines,
                descriptions=valid_descriptions,
                business_name="Acme Corp",
            )

        assert result == "customers/123/assetGroups/1"

    @pytest.mark.asyncio
    async def test_create_asset_group_validation_fails(self, pmax_client):
        with pytest.raises(PMaxValidationError, match="validation failed"):
            await pmax_client.create_asset_group(
                campaign_resource_name="customers/123/campaigns/2",
                group_name="AG",
                final_url="https://example.com",
                headlines=[],  # Too few
                long_headlines=[],
                descriptions=[],
                business_name="",
            )


# --- Audience signal ---


class TestAddAudienceSignal:
    @pytest.mark.asyncio
    async def test_add_audience_signal(self, pmax_client, mock_service):
        mock_service.mutate.return_value = MagicMock()

        await pmax_client.add_audience_signal(
            asset_group_resource_name="customers/123/assetGroups/1",
            user_list_resource_name="customers/123/userLists/456",
        )

        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_audience_signal_none_skips(self, pmax_client, mock_service):
        await pmax_client.add_audience_signal(
            asset_group_resource_name="customers/123/assetGroups/1",
            user_list_resource_name=None,
        )

        mock_service.mutate.assert_not_called()


# --- Listing campaigns ---


class TestGetPMaxCampaigns:
    @pytest.mark.asyncio
    async def test_list_pmax_campaigns(self, pmax_client, mock_service):
        mock_row = MagicMock()
        mock_row.campaign.id = 111
        mock_row.campaign.name = "PMax Campaign"
        mock_row.campaign.status.name = "ENABLED"
        mock_row.campaign_budget.amount_micros = 50000000
        mock_row.metrics.impressions = 1000
        mock_row.metrics.clicks = 50
        mock_row.metrics.cost_micros = 25000000
        mock_row.metrics.conversions = 3.0
        mock_service.search_stream.return_value = [mock_row]

        result = await pmax_client.get_pmax_campaigns()

        assert len(result) == 1
        assert result[0]["id"] == "111"
        assert result[0]["name"] == "PMax Campaign"
        assert result[0]["channel_type"] == "PERFORMANCE_MAX"
        assert result[0]["daily_budget_dollars"] == 50.0
        assert result[0]["cost_dollars"] == 25.0

    @pytest.mark.asyncio
    async def test_list_pmax_campaigns_empty(self, pmax_client, mock_service):
        mock_service.search_stream.return_value = []
        result = await pmax_client.get_pmax_campaigns()
        assert result == []


class TestGetAssetGroups:
    @pytest.mark.asyncio
    async def test_list_asset_groups(self, pmax_client, mock_service):
        mock_row = MagicMock()
        mock_row.asset_group.id = 222
        mock_row.asset_group.name = "AG 1"
        mock_row.asset_group.status.name = "ENABLED"
        mock_row.asset_group.campaign = "customers/123/campaigns/111"
        mock_service.search_stream.return_value = [mock_row]

        result = await pmax_client.get_asset_groups("111")

        assert len(result) == 1
        assert result[0]["id"] == "222"
        assert result[0]["name"] == "AG 1"


class TestGetAssetGroupPerformance:
    @pytest.mark.asyncio
    async def test_get_performance(self, pmax_client, mock_service):
        mock_row = MagicMock()
        mock_row.asset_group.id = 222
        mock_row.asset_group.name = "AG 1"
        mock_row.metrics.impressions = 500
        mock_row.metrics.clicks = 25
        mock_row.metrics.cost_micros = 12500000
        mock_row.metrics.conversions = 2.0
        mock_service.search_stream.return_value = [mock_row]

        result = await pmax_client.get_asset_group_performance(
            "111", "2026-01-01", "2026-01-07"
        )

        assert len(result) == 1
        assert result[0]["cost_dollars"] == 12.5
        assert result[0]["conversions"] == 2.0


# --- Campaign status ---


class TestUpdateCampaignStatus:
    @pytest.mark.asyncio
    async def test_pause_campaign(self, pmax_client, mock_service):
        mock_service.mutate.return_value = MagicMock()
        await pmax_client.update_campaign_status("111", "PAUSED")
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_enable_campaign(self, pmax_client, mock_service):
        mock_service.mutate.return_value = MagicMock()
        await pmax_client.update_campaign_status("111", "ENABLED")
        mock_service.mutate.assert_called_once()


# --- Constants ---


class TestConstants:
    def test_allowed_bidding_strategies(self):
        assert "MAXIMIZE_CONVERSIONS" in PMAX_ALLOWED_BIDDING
        assert "MAXIMIZE_CONVERSION_VALUE" in PMAX_ALLOWED_BIDDING
        assert "MANUAL_CPC" not in PMAX_ALLOWED_BIDDING

    def test_headline_limits(self):
        assert PMAX_HEADLINE_MIN == 3
        assert PMAX_HEADLINE_MAX == 5
        assert PMAX_HEADLINE_MAX_CHARS == 30

    def test_description_limits(self):
        assert PMAX_DESCRIPTION_MIN == 2
        assert PMAX_DESCRIPTION_MAX == 5
        assert PMAX_DESCRIPTION_MAX_CHARS == 90
