"""Tests for Meta Conversions API (CAPI) (BJC-164)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.meta_conversions import (
    META_STANDARD_EVENTS,
    PAIDEDGE_CUSTOM_EVENTS,
    MetaConversionsMixin,
    build_data_processing_options,
    build_event,
    build_user_data,
    generate_event_id,
    get_tenant_pixel_id,
    send_landing_page_conversion,
)


class FakeClient(MetaConversionsMixin):
    def __init__(self):
        self._request = AsyncMock()


class TestBuildUserData:
    def test_hashes_pii_fields(self):
        ud = build_user_data(
            email="test@example.com",
            first_name="John",
            last_name="Doe",
        )
        assert "em" in ud
        assert len(ud["em"][0]) == 64  # SHA-256
        assert "fn" in ud
        assert len(ud["fn"]) == 64

    def test_raw_fields_not_hashed(self):
        ud = build_user_data(
            client_ip_address="1.2.3.4",
            client_user_agent="Mozilla/5.0",
            fbc="fb.1.1234567890.abcdef",
            fbp="fb.1.1234567890.abcdef",
        )
        assert ud["client_ip_address"] == "1.2.3.4"
        assert ud["client_user_agent"] == "Mozilla/5.0"
        assert ud["fbc"] == "fb.1.1234567890.abcdef"

    def test_empty_fields_omitted(self):
        ud = build_user_data(email="test@example.com")
        assert "ph" not in ud
        assert "fn" not in ud

    def test_phone_hashed(self):
        ud = build_user_data(phone="+1-555-123-4567")
        assert "ph" in ud
        assert len(ud["ph"][0]) == 64


class TestBuildEvent:
    def test_basic_event(self):
        ud = build_user_data(email="test@example.com")
        event = build_event(
            event_name="Lead",
            action_source="website",
            event_source_url="https://example.com/landing",
            user_data=ud,
        )
        assert event["event_name"] == "Lead"
        assert event["action_source"] == "website"
        assert "event_time" in event
        assert "user_data" in event

    def test_event_with_dedup_id(self):
        ud = build_user_data(email="test@example.com")
        event = build_event(
            event_name="Lead",
            action_source="website",
            event_source_url="https://example.com",
            user_data=ud,
            event_id="pe_abc123",
        )
        assert event["event_id"] == "pe_abc123"

    def test_event_with_custom_data(self):
        ud = build_user_data(email="test@example.com")
        event = build_event(
            event_name="Purchase",
            action_source="website",
            event_source_url="https://example.com",
            user_data=ud,
            custom_data={"currency": "USD", "value": 99.99},
        )
        assert event["custom_data"]["value"] == 99.99


class TestGenerateEventId:
    def test_format(self):
        eid = generate_event_id()
        assert eid.startswith("pe_")
        assert len(eid) == 15  # "pe_" + 12 hex chars

    def test_unique(self):
        ids = {generate_event_id() for _ in range(100)}
        assert len(ids) == 100

    def test_custom_prefix(self):
        eid = generate_event_id(prefix="test")
        assert eid.startswith("test_")


class TestDataProcessingOptions:
    def test_ldu_enabled(self):
        opts = build_data_processing_options(ldu=True)
        assert opts["data_processing_options"] == ["LDU"]
        assert opts["data_processing_options_country"] == 1

    def test_ldu_disabled(self):
        opts = build_data_processing_options(ldu=False)
        assert opts["data_processing_options"] == []


class TestSendEvents:
    @pytest.mark.asyncio
    async def test_send_events(self):
        client = FakeClient()
        client._request.return_value = {"events_received": 1, "messages": []}

        events = [build_event("Lead", "website", "https://x.com", {}, event_id="pe_123")]
        result = await client.send_events("pixel_123", events)
        assert result["events_received"] == 1

    @pytest.mark.asyncio
    async def test_send_test_events(self):
        client = FakeClient()
        client._request.return_value = {"events_received": 1}

        events = [build_event("Lead", "website", "https://x.com", {})]
        await client.send_events("pixel_123", events, test_event_code="TEST123")
        call_data = client._request.call_args[1]["data"]
        assert call_data["test_event_code"] == "TEST123"


class TestGetTenantPixelId:
    @pytest.mark.asyncio
    async def test_returns_pixel_id(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"config": {"pixel_id": "px_123"}}
        )
        result = await get_tenant_pixel_id("org-1", mock_supabase)
        assert result == "px_123"

    @pytest.mark.asyncio
    async def test_no_config(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(data=None)
        result = await get_tenant_pixel_id("org-1", mock_supabase)
        assert result == ""


class TestConstants:
    def test_standard_events(self):
        assert "Lead" in META_STANDARD_EVENTS
        assert "Purchase" in META_STANDARD_EVENTS

    def test_custom_events(self):
        assert "DemoRequested" in PAIDEDGE_CUSTOM_EVENTS
