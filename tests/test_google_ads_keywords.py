"""Tests for Google Ads keyword targeting + negative keywords (BJC-143)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_keywords import (
    DEFAULT_B2B_NEGATIVES,
    GoogleAdsKeywordService,
    build_b2b_keyword_set,
)


@pytest.fixture
def mock_service():
    service = MagicMock(spec=GoogleAdsService)
    service.customer_id = "1234567890"
    service.enums = MagicMock()
    service._get_type = MagicMock()
    service.mutate = AsyncMock()
    service.search_stream = AsyncMock(return_value=[])
    return service


@pytest.fixture
def keyword_service(mock_service):
    return GoogleAdsKeywordService(mock_service)


class TestAddKeywords:
    @pytest.mark.asyncio
    async def test_add_keywords_creates_operations(self, keyword_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="criteria/1"),
            MagicMock(resource_name="criteria/2"),
        ]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        result = await keyword_service.add_keywords(
            ad_group_resource="customers/123/adGroups/456",
            keywords=[("soc 2 compliance", "EXACT"), ("security platform", "BROAD")],
        )

        assert len(result) == 2
        # Should have created 2 operations
        assert mock_service.mutate.call_args[0][1].__class__.__name__ == "list"

    @pytest.mark.asyncio
    async def test_add_keywords_batch(self, keyword_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name=f"criteria/{i}") for i in range(5)]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        keywords = [(f"keyword {i}", "BROAD") for i in range(5)]
        result = await keyword_service.add_keywords(
            "customers/123/adGroups/456", keywords
        )
        assert len(result) == 5


class TestGetKeywords:
    @pytest.mark.asyncio
    async def test_get_keywords_returns_parsed_list(self, keyword_service, mock_service):
        mock_row = MagicMock()
        mock_row.ad_group_criterion.criterion_id = 12345
        mock_row.ad_group_criterion.keyword.text = "soc 2 compliance"
        mock_row.ad_group_criterion.keyword.match_type.name = "EXACT"
        mock_row.ad_group_criterion.status.name = "ENABLED"
        mock_row.ad_group_criterion.cpc_bid_micros = 2_000_000
        mock_row.metrics.impressions = 100
        mock_row.metrics.clicks = 10
        mock_row.metrics.cost_micros = 5_000_000
        mock_row.metrics.conversions = 2.0
        mock_service.search_stream.return_value = [mock_row]

        result = await keyword_service.get_keywords("456")

        assert len(result) == 1
        assert result[0]["text"] == "soc 2 compliance"
        assert result[0]["match_type"] == "EXACT"
        assert result[0]["cpc_bid_dollars"] == 2.0
        assert result[0]["cost_dollars"] == 5.0


class TestNegativeKeywords:
    @pytest.mark.asyncio
    async def test_add_negative_keywords_campaign(self, keyword_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="neg/1"),
            MagicMock(resource_name="neg/2"),
        ]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        result = await keyword_service.add_negative_keywords_campaign(
            "789",
            [("free", "BROAD"), ("jobs", "BROAD")],
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_add_negative_keywords_ad_group(self, keyword_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="neg/3")]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        result = await keyword_service.add_negative_keywords_ad_group(
            "customers/123/adGroups/456",
            [("salary", "BROAD")],
        )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_negative_keywords(self, keyword_service, mock_service):
        mock_row = MagicMock()
        mock_row.campaign_criterion.criterion_id = 99
        mock_row.campaign_criterion.keyword.text = "free"
        mock_row.campaign_criterion.keyword.match_type.name = "BROAD"
        mock_service.search_stream.return_value = [mock_row]

        result = await keyword_service.get_negative_keywords("789")
        assert len(result) == 1
        assert result[0]["text"] == "free"


class TestKeywordStatusChanges:
    @pytest.mark.asyncio
    async def test_remove_keyword(self, keyword_service, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await keyword_service.remove_keyword("456", "12345")
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_pause_keyword(self, keyword_service, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await keyword_service.pause_keyword("456", "12345")
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_keyword_bid(self, keyword_service, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await keyword_service.update_keyword_bid("456", "12345", 3_000_000)
        mock_service.mutate.assert_called_once()


class TestB2BKeywordSet:
    def test_build_basic_set(self):
        result = build_b2b_keyword_set(
            "endpoint security",
            ["EDR", "threat detection"],
        )
        assert "exact" in result
        assert "phrase" in result
        assert "broad" in result
        assert "negative" in result
        assert "endpoint security software" in result["exact"]
        assert "free" in result["negative"]
        assert "jobs" in result["negative"]

    def test_build_with_competitors(self):
        result = build_b2b_keyword_set(
            "crm",
            ["sales automation"],
            competitors=["Salesforce", "HubSpot"],
        )
        assert "Salesforce alternative" in result["phrase"]
        assert "HubSpot vs" in result["phrase"]

    def test_default_negatives_comprehensive(self):
        assert len(DEFAULT_B2B_NEGATIVES) >= 10
        assert "free" in DEFAULT_B2B_NEGATIVES
        assert "salary" in DEFAULT_B2B_NEGATIVES
        assert "tutorial" in DEFAULT_B2B_NEGATIVES
        assert "reddit" in DEFAULT_B2B_NEGATIVES
