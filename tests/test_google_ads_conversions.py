"""Tests for Google Ads offline conversion import (BJC-156)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_conversions import (
    CRM_EVENT_CATEGORY_MAP,
    GCLID_MAX_AGE_DAYS,
    GoogleAdsConversionClient,
    _format_datetime,
    _hash_value,
    _normalize_phone,
    _parse_upload_response,
    build_dedup_key,
    extract_gclid,
    is_gclid_valid,
    map_crm_event_to_conversion,
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
def conversion_client(mock_service):
    return GoogleAdsConversionClient(mock_service)


# --- Click conversions ---


class TestUploadClickConversions:
    @pytest.mark.asyncio
    async def test_upload_empty_list(self, conversion_client):
        result = await conversion_client.upload_click_conversions([])
        assert result == {"uploaded": 0, "failed": 0, "errors": []}

    @pytest.mark.asyncio
    async def test_upload_click_conversions(self, conversion_client, mock_service):
        mock_upload_service = MagicMock()
        mock_response = MagicMock()
        mock_response.partial_failure_error = None
        mock_response.results = [MagicMock(), MagicMock()]
        mock_service._get_service.return_value = mock_upload_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_response
            )
            result = await conversion_client.upload_click_conversions([
                {
                    "gclid": "gclid_abc",
                    "conversion_action_id": "123",
                    "conversion_time": "2026-01-01T12:00:00",
                    "value": 100.0,
                    "currency": "USD",
                },
                {
                    "gclid": "gclid_def",
                    "conversion_action_id": "123",
                    "conversion_time": "2026-01-02T12:00:00",
                },
            ])

        assert result["uploaded"] == 2
        assert result["failed"] == 0

    @pytest.mark.asyncio
    async def test_upload_with_partial_failure(self, conversion_client, mock_service):
        mock_upload_service = MagicMock()
        mock_response = MagicMock()
        mock_response.partial_failure_error = MagicMock()
        mock_response.partial_failure_error.details = ["Error on row 1"]
        mock_response.results = [MagicMock(), MagicMock()]
        mock_service._get_service.return_value = mock_upload_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_response
            )
            result = await conversion_client.upload_click_conversions([
                {
                    "gclid": "gclid_1",
                    "conversion_action_id": "123",
                    "conversion_time": "2026-01-01",
                },
            ])

        assert result["failed"] == 1
        assert result["uploaded"] == 1
        assert len(result["errors"]) == 1


# --- Enhanced conversions ---


class TestUploadEnhancedConversions:
    @pytest.mark.asyncio
    async def test_upload_empty_list(self, conversion_client):
        result = await conversion_client.upload_enhanced_conversions([])
        assert result == {"uploaded": 0, "failed": 0, "errors": []}

    @pytest.mark.asyncio
    async def test_upload_enhanced_conversions(self, conversion_client, mock_service):
        mock_upload_service = MagicMock()
        mock_response = MagicMock()
        mock_response.partial_failure_error = None
        mock_response.results = [MagicMock()]
        mock_service._get_service.return_value = mock_upload_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value=mock_response
            )
            result = await conversion_client.upload_enhanced_conversions([
                {
                    "conversion_action_id": "456",
                    "conversion_time": "2026-01-01T12:00:00",
                    "value": 500.0,
                    "email": "test@example.com",
                    "phone": "+14155551234",
                    "order_id": "ORD-001",
                },
            ])

        assert result["uploaded"] == 1
        assert result["failed"] == 0


# --- Conversion actions ---


class TestCreateConversionAction:
    @pytest.mark.asyncio
    async def test_create_conversion_action(self, conversion_client, mock_service):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(
                resource_name="customers/123/conversionActions/456"
            )
        ]
        mock_service.mutate.return_value = mock_response

        resource = await conversion_client.create_conversion_action(
            name="Lead Form Submit",
            category="SUBMIT_LEAD_FORM",
        )

        assert resource == "customers/123/conversionActions/456"
        mock_service.mutate.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_conversion_action_with_value_settings(
        self, conversion_client, mock_service
    ):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="customers/123/conversionActions/789")
        ]
        mock_service.mutate.return_value = mock_response

        resource = await conversion_client.create_conversion_action(
            name="Deal Closed",
            category="PURCHASE",
            value_settings={"default_value": 1000, "always_use_default": False},
        )

        assert resource == "customers/123/conversionActions/789"


class TestListConversionActions:
    @pytest.mark.asyncio
    async def test_list_conversion_actions(self, conversion_client, mock_service):
        mock_row = MagicMock()
        mock_row.conversion_action.id = 123
        mock_row.conversion_action.name = "Lead Form"
        mock_row.conversion_action.type_.name = "UPLOAD_CLICKS"
        mock_row.conversion_action.category.name = "SUBMIT_LEAD_FORM"
        mock_row.conversion_action.status.name = "ENABLED"
        mock_service.search_stream.return_value = [mock_row]

        results = await conversion_client.list_conversion_actions()

        assert len(results) == 1
        assert results[0]["name"] == "Lead Form"
        assert results[0]["type"] == "UPLOAD_CLICKS"

    @pytest.mark.asyncio
    async def test_list_conversion_actions_empty(
        self, conversion_client, mock_service
    ):
        mock_service.search_stream.return_value = []
        results = await conversion_client.list_conversion_actions()
        assert results == []


class TestGetConversionAction:
    @pytest.mark.asyncio
    async def test_get_conversion_action(self, conversion_client, mock_service):
        mock_row = MagicMock()
        mock_row.conversion_action.id = 456
        mock_row.conversion_action.name = "Purchase"
        mock_row.conversion_action.type_.name = "UPLOAD_CLICKS"
        mock_row.conversion_action.category.name = "PURCHASE"
        mock_row.conversion_action.status.name = "ENABLED"
        mock_service.search_stream.return_value = [mock_row]

        result = await conversion_client.get_conversion_action("456")
        assert result["name"] == "Purchase"
        assert result["id"] == "456"

    @pytest.mark.asyncio
    async def test_get_conversion_action_not_found(
        self, conversion_client, mock_service
    ):
        mock_service.search_stream.return_value = []
        result = await conversion_client.get_conversion_action("999")
        assert result is None


# --- CRM event mapping ---


class TestMapCrmEventToConversion:
    def test_maps_lead_created(self):
        event = {
            "event_type": "lead_created",
            "gclid": "gclid_abc",
            "timestamp": "2026-01-01T12:00:00",
            "value": 50.0,
        }
        action_map = {"lead_created": "action_1"}
        result = map_crm_event_to_conversion(event, action_map)

        assert result is not None
        assert result["gclid"] == "gclid_abc"
        assert result["conversion_action_id"] == "action_1"
        assert result["value"] == 50.0

    def test_maps_deal_closed_won(self):
        event = {
            "event_type": "deal_closed_won",
            "gclid": "gclid_xyz",
            "timestamp": "2026-01-01",
            "value": 10000.0,
            "currency": "EUR",
            "order_id": "DEAL-001",
        }
        action_map = {"deal_closed_won": "action_2"}
        result = map_crm_event_to_conversion(event, action_map)

        assert result["currency"] == "EUR"
        assert result["order_id"] == "DEAL-001"

    def test_no_mapping_returns_none(self):
        event = {"event_type": "unknown_event", "gclid": "abc"}
        result = map_crm_event_to_conversion(event, {})
        assert result is None

    def test_no_gclid_but_has_email(self):
        event = {
            "event_type": "lead_created",
            "timestamp": "2026-01-01",
            "email": "test@example.com",
        }
        action_map = {"lead_created": "action_1"}
        result = map_crm_event_to_conversion(event, action_map)

        assert result is not None
        assert "gclid" not in result
        assert result["email"] == "test@example.com"

    def test_no_gclid_no_pii_returns_none(self):
        event = {"event_type": "lead_created", "timestamp": "2026-01-01"}
        action_map = {"lead_created": "action_1"}
        result = map_crm_event_to_conversion(event, action_map)
        assert result is None


# --- gclid helpers ---


class TestExtractGclid:
    def test_extract_from_url(self):
        url = "https://example.com/landing?gclid=abc123&utm_source=google"
        assert extract_gclid(url) == "abc123"

    def test_extract_only_param(self):
        url = "https://example.com/?gclid=xyz789"
        assert extract_gclid(url) == "xyz789"

    def test_no_gclid(self):
        url = "https://example.com/page"
        assert extract_gclid(url) is None

    def test_gclid_not_first_param(self):
        url = "https://example.com/?utm_source=google&gclid=test123"
        assert extract_gclid(url) == "test123"


class TestIsGclidValid:
    def test_fresh_gclid_is_valid(self):
        captured = datetime.now(timezone.utc) - timedelta(days=1)
        assert is_gclid_valid(captured) is True

    def test_gclid_at_boundary_is_valid(self):
        captured = datetime.now(timezone.utc) - timedelta(days=GCLID_MAX_AGE_DAYS)
        assert is_gclid_valid(captured) is True

    def test_expired_gclid_is_invalid(self):
        captured = datetime.now(timezone.utc) - timedelta(days=GCLID_MAX_AGE_DAYS + 1)
        assert is_gclid_valid(captured) is False

    def test_naive_datetime_treated_as_utc(self):
        captured = datetime.now() - timedelta(days=1)
        assert is_gclid_valid(captured) is True


# --- Dedup ---


class TestBuildDedupKey:
    def test_builds_key(self):
        key = build_dedup_key("gclid_abc", "action_1", "2026-01-01")
        assert key == "gclid_abc:action_1:2026-01-01"

    def test_unique_keys(self):
        key1 = build_dedup_key("gclid_abc", "action_1", "2026-01-01")
        key2 = build_dedup_key("gclid_abc", "action_1", "2026-01-02")
        assert key1 != key2


# --- Internal helpers ---


class TestFormatDatetime:
    def test_format_string(self):
        result = _format_datetime("2026-01-01T12:00:00")
        assert result == "2026-01-01 12:00:00+00:00"

    def test_format_datetime_object(self):
        dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _format_datetime(dt)
        assert result == "2026-01-01 12:00:00+00:00"

    def test_format_naive_datetime(self):
        dt = datetime(2026, 1, 1, 12, 0, 0)
        result = _format_datetime(dt)
        assert result == "2026-01-01 12:00:00+00:00"


class TestHashValue:
    def test_hash_deterministic(self):
        assert _hash_value("test") == _hash_value("test")

    def test_hash_is_sha256(self):
        result = _hash_value("test")
        assert len(result) == 64

    def test_different_inputs_different_hashes(self):
        assert _hash_value("a") != _hash_value("b")


class TestNormalizePhone:
    def test_us_default(self):
        assert _normalize_phone("5551234567") == "+15551234567"

    def test_with_country_code(self):
        assert _normalize_phone("+14155551234") == "+14155551234"

    def test_strips_formatting(self):
        assert _normalize_phone("(415) 555-1234") == "+14155551234"


class TestParseUploadResponse:
    def test_no_failures(self):
        response = MagicMock()
        response.partial_failure_error = None
        response.results = [MagicMock(), MagicMock()]
        result = _parse_upload_response(response)
        assert result["uploaded"] == 2
        assert result["failed"] == 0

    def test_with_partial_failures(self):
        response = MagicMock()
        response.partial_failure_error = MagicMock()
        response.partial_failure_error.details = ["Error 1", "Error 2"]
        response.results = [MagicMock(), MagicMock(), MagicMock()]
        result = _parse_upload_response(response)
        assert result["uploaded"] == 1
        assert result["failed"] == 2
        assert len(result["errors"]) == 2

    def test_all_failures(self):
        response = MagicMock()
        response.partial_failure_error = MagicMock()
        response.partial_failure_error.details = ["E1", "E2"]
        response.results = [MagicMock()]
        result = _parse_upload_response(response)
        assert result["uploaded"] == 0
        assert result["failed"] == 2


# --- Constants ---


class TestConstants:
    def test_gclid_max_age(self):
        assert GCLID_MAX_AGE_DAYS == 90

    def test_crm_event_categories(self):
        assert "lead_created" in CRM_EVENT_CATEGORY_MAP
        assert "deal_closed_won" in CRM_EVENT_CATEGORY_MAP
        assert CRM_EVENT_CATEGORY_MAP["deal_closed_won"] == "PURCHASE"
