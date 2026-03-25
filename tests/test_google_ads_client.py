"""Tests for Google Ads base client factory + service wrapper (BJC-141)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import (
    GoogleAdsAPIError,
    GoogleAdsAuthError,
    GoogleAdsClientFactory,
    GoogleAdsNotFoundError,
    GoogleAdsPermissionError,
    GoogleAdsQuotaError,
    GoogleAdsService,
    customer_id_to_str,
    dollars_to_micros,
    micros_to_dollars,
)
from app.integrations.google_ads_models import GoogleAdsAccount


# --- Utility helpers ---


class TestHelpers:
    def test_customer_id_no_hyphens(self):
        assert customer_id_to_str("123-456-7890") == "1234567890"

    def test_customer_id_already_clean(self):
        assert customer_id_to_str("1234567890") == "1234567890"

    def test_customer_id_from_int(self):
        assert customer_id_to_str(1234567890) == "1234567890"

    def test_micros_to_dollars(self):
        assert micros_to_dollars(5_000_000) == 5.0
        assert micros_to_dollars(1_250_000) == 1.25
        assert micros_to_dollars(0) == 0.0

    def test_dollars_to_micros(self):
        assert dollars_to_micros(5.0) == 5_000_000
        assert dollars_to_micros(1.25) == 1_250_000
        assert dollars_to_micros(0) == 0

    def test_micros_roundtrip(self):
        for amount in [0.01, 0.50, 1.0, 10.99, 100.00, 999999.99]:
            assert micros_to_dollars(dollars_to_micros(amount)) == pytest.approx(
                amount, abs=0.01
            )


# --- Client factory ---


class TestGoogleAdsClientFactory:
    @pytest.mark.asyncio
    async def test_get_client_calls_credentials(self):
        """Should call get_google_ads_credentials and load_from_dict."""
        factory = GoogleAdsClientFactory()
        mock_sb = MagicMock()

        mock_creds = {
            "access_token": "tok",
            "refresh_token": "refresh-tok",
            "customer_id": "1234567890",
            "developer_token": "dev-tok",
            "mcc_id": "9876543210",
        }

        with (
            patch(
                "app.integrations.google_ads.get_google_ads_credentials",
                new_callable=AsyncMock,
                return_value=mock_creds,
            ),
            patch(
                "google.ads.googleads.client.GoogleAdsClient.load_from_dict"
            ) as mock_load,
        ):
            mock_load.return_value = MagicMock()
            client = await factory.get_client("org-1", mock_sb)

            mock_load.assert_called_once()
            config = mock_load.call_args[0][0]
            assert config["refresh_token"] == "refresh-tok"
            assert config["use_proto_plus"] is True
            assert client is not None

    @pytest.mark.asyncio
    async def test_get_customer_id_returns_selected(self):
        """Should return the selected customer_id from credentials."""
        factory = GoogleAdsClientFactory()
        mock_sb = MagicMock()

        mock_creds = {
            "customer_id": "1234567890",
            "refresh_token": "tok",
        }

        with patch(
            "app.integrations.google_ads.get_google_ads_credentials",
            new_callable=AsyncMock,
            return_value=mock_creds,
        ):
            cid = await factory.get_customer_id("org-1", mock_sb)
            assert cid == "1234567890"

    @pytest.mark.asyncio
    async def test_get_customer_id_raises_when_not_selected(self):
        """Should raise ValueError when no account is selected."""
        factory = GoogleAdsClientFactory()
        mock_sb = MagicMock()

        mock_creds = {
            "customer_id": None,
            "refresh_token": "tok",
        }

        with patch(
            "app.integrations.google_ads.get_google_ads_credentials",
            new_callable=AsyncMock,
            return_value=mock_creds,
        ):
            with pytest.raises(ValueError, match="No Google Ads account selected"):
                await factory.get_customer_id("org-1", mock_sb)


# --- Service wrapper ---


class TestGoogleAdsService:
    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def service(self, mock_client):
        return GoogleAdsService(mock_client, "1234567890")

    def test_customer_id_stored(self, service):
        assert service.customer_id == "1234567890"

    @pytest.mark.asyncio
    async def test_search_stream_executes_query(self, service, mock_client):
        """search_stream should execute GAQL and return results."""
        mock_row = MagicMock()
        mock_batch = MagicMock()
        mock_batch.results = [mock_row]

        mock_ga_service = MagicMock()
        mock_ga_service.search_stream.return_value = [mock_batch]
        mock_client.get_service.return_value = mock_ga_service

        results = await service.search_stream("SELECT campaign.id FROM campaign")

        assert len(results) == 1
        assert results[0] == mock_row
        mock_ga_service.search_stream.assert_called_once_with(
            customer_id="1234567890",
            query="SELECT campaign.id FROM campaign",
        )

    @pytest.mark.asyncio
    async def test_search_stream_retries_on_quota_error(self, service, mock_client):
        """Should retry with exponential backoff on quota errors."""
        mock_ga_service = MagicMock()
        call_count = 0

        def side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("RESOURCE_EXHAUSTED: quota exceeded")
            mock_batch = MagicMock()
            mock_batch.results = []
            return [mock_batch]

        mock_ga_service.search_stream.side_effect = side_effect
        mock_client.get_service.return_value = mock_ga_service

        with patch("app.integrations.google_ads.asyncio.sleep", new_callable=AsyncMock):
            results = await service.search_stream("SELECT campaign.id FROM campaign")

        assert call_count == 3
        assert results == []

    @pytest.mark.asyncio
    async def test_search_stream_raises_after_max_retries(self, service, mock_client):
        """Should raise GoogleAdsQuotaError after max retries."""
        mock_ga_service = MagicMock()
        mock_ga_service.search_stream.side_effect = Exception(
            "RESOURCE_EXHAUSTED: quota exceeded"
        )
        mock_client.get_service.return_value = mock_ga_service

        with patch("app.integrations.google_ads.asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(GoogleAdsQuotaError):
                await service.search_stream("SELECT campaign.id FROM campaign")

    @pytest.mark.asyncio
    async def test_mutate_executes_operations(self, service, mock_client):
        """mutate should call the appropriate service method."""
        mock_service = MagicMock()
        mock_response = MagicMock()
        mock_response.partial_failure_error = None
        mock_service.mutate_campaign_budgets.return_value = mock_response
        mock_client.get_service.return_value = mock_service

        operations = [MagicMock()]
        result = await service.mutate("CampaignBudgetService", operations)

        assert result == mock_response
        mock_service.mutate_campaign_budgets.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_accessible_customers(self, service, mock_client):
        """Should return customer IDs from resource names."""
        mock_customer_service = MagicMock()
        mock_response = MagicMock()
        mock_response.resource_names = [
            "customers/1234567890",
            "customers/9876543210",
        ]
        mock_customer_service.list_accessible_customers.return_value = mock_response
        mock_client.get_service.return_value = mock_customer_service

        result = await service.list_accessible_customers()
        assert result == ["1234567890", "9876543210"]


# --- Error mapping ---


class TestErrorMapping:
    def test_quota_error(self):
        e = GoogleAdsService._map_exception(Exception("RESOURCE_EXHAUSTED: quota"))
        assert isinstance(e, GoogleAdsQuotaError)

    def test_permission_error(self):
        e = GoogleAdsService._map_exception(Exception("PERMISSION_DENIED: authorization"))
        assert isinstance(e, GoogleAdsPermissionError)

    def test_not_found_error(self):
        e = GoogleAdsService._map_exception(Exception("NOT_FOUND: resource"))
        assert isinstance(e, GoogleAdsNotFoundError)

    def test_auth_error(self):
        e = GoogleAdsService._map_exception(Exception("UNAUTHENTICATED: bad token"))
        assert isinstance(e, GoogleAdsAuthError)

    def test_generic_error(self):
        e = GoogleAdsService._map_exception(Exception("something else"))
        assert isinstance(e, GoogleAdsAPIError)
        assert not isinstance(e, GoogleAdsQuotaError)

    def test_is_quota_error_true(self):
        assert GoogleAdsService._is_quota_error(Exception("resource_exhausted"))
        assert GoogleAdsService._is_quota_error(Exception("quota limit"))
        assert GoogleAdsService._is_quota_error(Exception("rate limited"))

    def test_is_quota_error_false(self):
        assert not GoogleAdsService._is_quota_error(Exception("not found"))
        assert not GoogleAdsService._is_quota_error(Exception("permission denied"))


# --- Mutate method name derivation ---


class TestMutateMethodName:
    def test_campaign_budget_service(self):
        assert (
            GoogleAdsService._get_mutate_method("CampaignBudgetService")
            == "mutate_campaign_budgets"
        )

    def test_campaign_service(self):
        assert (
            GoogleAdsService._get_mutate_method("CampaignService")
            == "mutate_campaigns"
        )

    def test_ad_group_service(self):
        assert (
            GoogleAdsService._get_mutate_method("AdGroupService")
            == "mutate_ad_groups"
        )

    def test_ad_group_ad_service(self):
        assert (
            GoogleAdsService._get_mutate_method("AdGroupAdService")
            == "mutate_ad_group_ads"
        )

    def test_ad_group_criterion_service(self):
        assert (
            GoogleAdsService._get_mutate_method("AdGroupCriterionService")
            == "mutate_ad_group_criterions"
        )


# --- Pydantic models ---


class TestGoogleAdsModels:
    def test_google_ads_account(self):
        account = GoogleAdsAccount(
            customer_id="1234567890",
            name="Test Account",
            currency="USD",
            timezone="America/New_York",
            is_manager=False,
            is_test=False,
        )
        assert account.customer_id == "1234567890"
        assert account.is_manager is False

    def test_google_ads_account_with_status(self):
        account = GoogleAdsAccount(
            customer_id="1234567890",
            name="Test",
            currency="USD",
            timezone="UTC",
            is_manager=False,
            is_test=True,
            status="ENABLED",
        )
        assert account.status == "ENABLED"
        assert account.is_test is True
