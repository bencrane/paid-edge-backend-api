"""Tests for Google Ads RSA creation + ad management (BJC-144)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_ads import GoogleAdsAdService, RSAValidator


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
def ad_service(mock_service):
    return GoogleAdsAdService(mock_service)


# --- RSA Validator ---


class TestRSAValidator:
    def test_valid_rsa(self):
        errors = RSAValidator.validate(
            headlines=["H1 short text", "H2 short text", "H3 short text"],
            descriptions=["Description one that is somewhat longer", "Description two"],
        )
        assert errors == []

    def test_too_few_headlines(self):
        errors = RSAValidator.validate(
            headlines=["H1", "H2"],
            descriptions=["D1", "D2"],
        )
        assert any("at least 3 headlines" in e for e in errors)

    def test_too_many_headlines(self):
        errors = RSAValidator.validate(
            headlines=[f"H{i}" for i in range(16)],
            descriptions=["D1", "D2"],
        )
        assert any("Max 15 headlines" in e for e in errors)

    def test_too_few_descriptions(self):
        errors = RSAValidator.validate(
            headlines=["H1", "H2", "H3"],
            descriptions=["D1"],
        )
        assert any("at least 2 descriptions" in e for e in errors)

    def test_too_many_descriptions(self):
        errors = RSAValidator.validate(
            headlines=["H1", "H2", "H3"],
            descriptions=["D1", "D2", "D3", "D4", "D5"],
        )
        assert any("Max 4 descriptions" in e for e in errors)

    def test_headline_too_long(self):
        errors = RSAValidator.validate(
            headlines=["x" * 31, "H2", "H3"],
            descriptions=["D1", "D2"],
        )
        assert any("Headline 1 is 31 chars" in e for e in errors)

    def test_description_too_long(self):
        errors = RSAValidator.validate(
            headlines=["H1", "H2", "H3"],
            descriptions=["x" * 91, "D2"],
        )
        assert any("Description 1 is 91 chars" in e for e in errors)

    def test_path1_too_long(self):
        errors = RSAValidator.validate(
            headlines=["H1", "H2", "H3"],
            descriptions=["D1", "D2"],
            path1="x" * 16,
        )
        assert any("Path1 is 16 chars" in e for e in errors)

    def test_path2_too_long(self):
        errors = RSAValidator.validate(
            headlines=["H1", "H2", "H3"],
            descriptions=["D1", "D2"],
            path2="x" * 16,
        )
        assert any("Path2 is 16 chars" in e for e in errors)

    def test_exactly_at_limits(self):
        errors = RSAValidator.validate(
            headlines=["x" * 30, "y" * 30, "z" * 30],
            descriptions=["a" * 90, "b" * 90],
            path1="c" * 15,
            path2="d" * 15,
        )
        assert errors == []

    def test_multiple_errors(self):
        errors = RSAValidator.validate(
            headlines=["H1"],
            descriptions=[],
        )
        assert len(errors) >= 2


# --- RSA creation ---


class TestCreateRSA:
    @pytest.mark.asyncio
    async def test_create_rsa_succeeds(self, ad_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="customers/123/adGroupAds/456~789")]
        mock_service.mutate.return_value = mock_response

        mock_operation = MagicMock()
        mock_service._get_type.return_value = mock_operation

        result = await ad_service.create_responsive_search_ad(
            ad_group_id="456",
            headlines=["H1 test headline", "H2 test headline", "H3 test headline"],
            descriptions=["D1 test description text", "D2 test description text"],
            final_url="https://example.com",
        )

        assert result == "customers/123/adGroupAds/456~789"
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_rsa_validation_fails(self, ad_service, mock_service):
        with pytest.raises(ValueError, match="RSA validation failed"):
            await ad_service.create_responsive_search_ad(
                ad_group_id="456",
                headlines=["H1"],  # Too few
                descriptions=["D1"],  # Too few
                final_url="https://example.com",
            )

    @pytest.mark.asyncio
    async def test_create_rsa_with_pinning(self, ad_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="customers/123/adGroupAds/456~789")]
        mock_service.mutate.return_value = mock_response

        mock_operation = MagicMock()
        mock_ad_text_asset = MagicMock()
        mock_service._get_type.side_effect = [mock_operation, mock_ad_text_asset, mock_ad_text_asset, mock_ad_text_asset, mock_ad_text_asset, mock_ad_text_asset]

        result = await ad_service.create_responsive_search_ad(
            ad_group_id="456",
            headlines=["Brand Name Here", "Value Prop Two", "Value Prop Three"],
            descriptions=["Description one text", "Description two text"],
            final_url="https://example.com",
            pinned_headlines={0: 1},  # Pin first headline to position 1
            path1="products",
            path2="search",
        )

        assert result == "customers/123/adGroupAds/456~789"

    @pytest.mark.asyncio
    async def test_create_rsa_with_paths(self, ad_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [MagicMock(resource_name="customers/123/adGroupAds/456~789")]
        mock_service.mutate.return_value = mock_response
        mock_service._get_type.return_value = MagicMock()

        await ad_service.create_responsive_search_ad(
            ad_group_id="456",
            headlines=["H1 headline text", "H2 headline text", "H3 headline text"],
            descriptions=["D1 description text", "D2 description text"],
            final_url="https://example.com",
            path1="security",
            path2="solutions",
        )
        mock_service.mutate.assert_called_once()


# --- Get ads ---


class TestGetAds:
    @pytest.mark.asyncio
    async def test_get_ads_returns_list(self, ad_service, mock_service):
        mock_row = MagicMock()
        mock_row.ad_group_ad.ad.id = 999
        mock_row.ad_group_ad.status.name = "ENABLED"
        mock_row.ad_group_ad.ad.final_urls = ["https://example.com"]
        mock_row.ad_group_ad.ad_strength.name = "GOOD"
        mock_row.ad_group_ad.policy_summary.approval_status.name = "APPROVED"
        mock_headline = MagicMock()
        mock_headline.text = "Test Headline"
        mock_desc = MagicMock()
        mock_desc.text = "Test Description"
        mock_row.ad_group_ad.ad.responsive_search_ad.headlines = [mock_headline]
        mock_row.ad_group_ad.ad.responsive_search_ad.descriptions = [mock_desc]
        mock_row.metrics.impressions = 500
        mock_row.metrics.clicks = 25
        mock_row.metrics.cost_micros = 12_500_000
        mock_row.metrics.conversions = 3.0
        mock_service.search_stream.return_value = [mock_row]

        result = await ad_service.get_ads("456")

        assert len(result) == 1
        assert result[0]["ad_id"] == "999"
        assert result[0]["headlines"] == ["Test Headline"]
        assert result[0]["descriptions"] == ["Test Description"]
        assert result[0]["cost_dollars"] == 12.5
        assert result[0]["ad_strength"] == "GOOD"


# --- Ad status ---


class TestAdStatus:
    @pytest.mark.asyncio
    async def test_update_ad_status(self, ad_service, mock_service):
        mock_service._get_type.return_value = MagicMock()
        mock_service.mutate.return_value = MagicMock()

        await ad_service.update_ad_status("456", "789", "PAUSED")
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ad_strength(self, ad_service, mock_service):
        mock_row = MagicMock()
        mock_row.ad_group_ad.ad_strength.name = "EXCELLENT"
        mock_service.search_stream.return_value = [mock_row]

        result = await ad_service.get_ad_strength("456", "789")
        assert result["ad_strength"] == "EXCELLENT"

    @pytest.mark.asyncio
    async def test_get_ad_strength_no_results(self, ad_service, mock_service):
        mock_service.search_stream.return_value = []
        result = await ad_service.get_ad_strength("456", "789")
        assert result["ad_strength"] == "UNKNOWN"
