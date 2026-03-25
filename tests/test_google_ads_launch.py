"""Tests for Google Ads campaign launch orchestration (BJC-148)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_launch import (
    GoogleAdsLaunchOrchestrator,
    GoogleAdsLaunchRequest,
    LaunchValidationError,
    STATUS_MAP,
    PAIDEDGE_TO_GADS_STATUS,
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
def orchestrator(mock_service):
    return GoogleAdsLaunchOrchestrator(mock_service)


@pytest.fixture
def valid_request():
    return GoogleAdsLaunchRequest(
        org_id="org-123",
        campaign_id="camp-456",
        campaign_name="Test B2B Campaign",
        daily_budget_dollars=50.0,
        bidding_strategy="maximize_conversions",
        keywords=[
            {"text": "soc 2 compliance", "match_type": "EXACT"},
            {"text": "security platform", "match_type": "BROAD"},
        ],
        headlines=[
            "Enterprise Security Platform",
            "SOC 2 Compliance Fast",
            "Trusted by 500+ Firms",
        ],
        descriptions=[
            "Get compliant in weeks not months. Start free trial today.",
            "Automated SOC 2 compliance for modern SaaS companies.",
        ],
        final_url="https://example.com/security",
        path1="security",
        path2="demo",
    )


# --- Validation ---


class TestValidation:
    def test_valid_request_passes(self, orchestrator, valid_request):
        errors = orchestrator.validate_request(valid_request)
        assert errors == []

    def test_missing_name(self, orchestrator, valid_request):
        valid_request.campaign_name = ""
        errors = orchestrator.validate_request(valid_request)
        assert any("Campaign name" in e for e in errors)

    def test_zero_budget(self, orchestrator, valid_request):
        valid_request.daily_budget_dollars = 0
        errors = orchestrator.validate_request(valid_request)
        assert any("budget" in e.lower() for e in errors)

    def test_negative_budget(self, orchestrator, valid_request):
        valid_request.daily_budget_dollars = -10
        errors = orchestrator.validate_request(valid_request)
        assert any("budget" in e.lower() for e in errors)

    def test_missing_final_url(self, orchestrator, valid_request):
        valid_request.final_url = ""
        errors = orchestrator.validate_request(valid_request)
        assert any("Final URL" in e for e in errors)

    def test_no_keywords(self, orchestrator, valid_request):
        valid_request.keywords = []
        errors = orchestrator.validate_request(valid_request)
        assert any("keyword" in e.lower() for e in errors)

    def test_too_few_headlines(self, orchestrator, valid_request):
        valid_request.headlines = ["H1", "H2"]  # need 3
        errors = orchestrator.validate_request(valid_request)
        assert any("headline" in e.lower() for e in errors)

    def test_too_few_descriptions(self, orchestrator, valid_request):
        valid_request.descriptions = ["D1"]  # need 2
        errors = orchestrator.validate_request(valid_request)
        assert any("description" in e.lower() for e in errors)

    def test_multiple_errors(self, orchestrator):
        request = GoogleAdsLaunchRequest(
            org_id="org-123",
            campaign_id="camp-456",
            campaign_name="",
            daily_budget_dollars=0,
            keywords=[],
            headlines=[],
            descriptions=[],
            final_url="",
        )
        errors = orchestrator.validate_request(request)
        assert len(errors) >= 4


# --- Launch ---


class TestLaunch:
    @pytest.mark.asyncio
    async def test_full_launch_succeeds(self, orchestrator, mock_service, valid_request):
        # Mock all the service calls
        budget_response = MagicMock()
        budget_response.results = [MagicMock(resource_name="customers/123/campaignBudgets/1")]

        campaign_response = MagicMock()
        campaign_response.results = [MagicMock(resource_name="customers/123/campaigns/2")]

        ad_group_response = MagicMock()
        ad_group_response.results = [MagicMock(resource_name="customers/123/adGroups/3")]

        keyword_response = MagicMock()
        keyword_response.results = [
            MagicMock(resource_name="criteria/1"),
            MagicMock(resource_name="criteria/2"),
        ]

        rsa_response = MagicMock()
        rsa_response.results = [MagicMock(resource_name="customers/123/adGroupAds/3~4")]

        status_response = MagicMock()

        # mutate is called for: budget, campaign, ad_group, keywords, rsa, enable
        mock_service.mutate.side_effect = [
            budget_response,   # create budget
            campaign_response, # create campaign
            ad_group_response, # create ad group
            keyword_response,  # add keywords
            rsa_response,      # create RSA
            status_response,   # enable campaign
        ]
        mock_service._get_type.return_value = MagicMock()

        result = await orchestrator.launch(valid_request)

        assert result["status"] == "launched"
        assert result["resources"]["budget"] == "customers/123/campaignBudgets/1"
        assert result["resources"]["campaign"] == "customers/123/campaigns/2"
        assert result["resources"]["ad_group"] == "customers/123/adGroups/3"
        assert len(result["steps"]) >= 5
        assert result["elapsed_seconds"] >= 0

    @pytest.mark.asyncio
    async def test_launch_with_geo_targeting(self, orchestrator, mock_service, valid_request):
        valid_request.geo_target_ids = [2840, 1014221]

        mock_service.mutate.side_effect = [
            MagicMock(results=[MagicMock(resource_name="budget/1")]),    # budget
            MagicMock(results=[MagicMock(resource_name="campaigns/2")]), # campaign
            MagicMock(results=[MagicMock(resource_name="criteria/1"), MagicMock(resource_name="criteria/2")]),  # geo targeting
            MagicMock(results=[MagicMock(resource_name="adGroups/3")]),  # ad group
            MagicMock(results=[MagicMock(resource_name="criteria/k1"), MagicMock(resource_name="criteria/k2")]),  # keywords
            MagicMock(results=[MagicMock(resource_name="ads/4")]),       # RSA
            MagicMock(),  # enable
        ]
        mock_service._get_type.return_value = MagicMock()

        result = await orchestrator.launch(valid_request)
        assert result["status"] == "launched"
        assert any(s["step"] == "geo_targeting" for s in result["steps"])

    @pytest.mark.asyncio
    async def test_launch_with_negative_keywords(self, orchestrator, mock_service, valid_request):
        valid_request.negative_keywords = [
            {"text": "free", "match_type": "BROAD"},
            {"text": "jobs", "match_type": "BROAD"},
        ]

        mock_service.mutate.side_effect = [
            MagicMock(results=[MagicMock(resource_name="budget/1")]),
            MagicMock(results=[MagicMock(resource_name="campaigns/2")]),
            MagicMock(results=[MagicMock(resource_name="adGroups/3")]),
            MagicMock(results=[MagicMock(resource_name="criteria/k1"), MagicMock(resource_name="criteria/k2")]),
            MagicMock(results=[MagicMock(resource_name="neg/1"), MagicMock(resource_name="neg/2")]),
            MagicMock(results=[MagicMock(resource_name="ads/4")]),
            MagicMock(),
        ]
        mock_service._get_type.return_value = MagicMock()

        result = await orchestrator.launch(valid_request)
        assert result["status"] == "launched"
        assert any(s["step"] == "negative_keywords" for s in result["steps"])

    @pytest.mark.asyncio
    async def test_launch_validation_fails(self, orchestrator):
        bad_request = GoogleAdsLaunchRequest(
            org_id="org-123",
            campaign_id="camp-456",
            campaign_name="",
            daily_budget_dollars=0,
            keywords=[],
            headlines=["H1"],
            descriptions=["D1"],
            final_url="",
        )

        with pytest.raises(LaunchValidationError, match="validation failed"):
            await orchestrator.launch(bad_request)

    @pytest.mark.asyncio
    async def test_launch_api_failure(self, orchestrator, mock_service, valid_request):
        mock_service.mutate.side_effect = Exception("API error")

        with pytest.raises(Exception, match="API error"):
            await orchestrator.launch(valid_request)


# --- Status management ---


class TestStatusManagement:
    @pytest.mark.asyncio
    async def test_pause_campaign(self, orchestrator, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await orchestrator.pause_campaign("789")
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_resume_campaign(self, orchestrator, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await orchestrator.resume_campaign("789")
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_archive_campaign(self, orchestrator, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await orchestrator.archive_campaign("789")
        mock_service.mutate.assert_called_once()


# --- Status mapping ---


class TestStatusMapping:
    def test_map_to_paidedge(self):
        assert GoogleAdsLaunchOrchestrator.map_status_to_paidedge("ENABLED") == "active"
        assert GoogleAdsLaunchOrchestrator.map_status_to_paidedge("PAUSED") == "paused"
        assert GoogleAdsLaunchOrchestrator.map_status_to_paidedge("REMOVED") == "archived"
        assert GoogleAdsLaunchOrchestrator.map_status_to_paidedge("UNKNOWN") == "unknown"

    def test_map_to_google(self):
        assert GoogleAdsLaunchOrchestrator.map_status_to_google("active") == "ENABLED"
        assert GoogleAdsLaunchOrchestrator.map_status_to_google("paused") == "PAUSED"
        assert GoogleAdsLaunchOrchestrator.map_status_to_google("archived") == "REMOVED"
        assert GoogleAdsLaunchOrchestrator.map_status_to_google("unknown") == "PAUSED"
