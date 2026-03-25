"""Tests for LinkedIn Conversions API (CAPI) — BJC-138."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import (
    LinkedInAdsClient,
    hash_email_for_linkedin,
)
from app.integrations.linkedin_conversions import LinkedInConversionBridge

# --- Helpers ---


def _mock_resp(status_code: int, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


def _mock_supabase_chain(select_data=None):
    """Create a Supabase mock that supports select + update chains."""
    mock_supabase = MagicMock()
    mock_chain = MagicMock()
    mock_chain.eq.return_value = mock_chain
    mock_chain.maybe_single.return_value = mock_chain
    mock_chain.select.return_value = mock_chain
    mock_chain.update.return_value = mock_chain
    if select_data is not None:
        mock_chain.execute.return_value = SimpleNamespace(data=select_data)
    else:
        mock_chain.execute.return_value = SimpleNamespace(data=None)
    mock_supabase.table.return_value = mock_chain
    return mock_supabase, mock_chain


# --- Conversion rule CRUD ---


class TestCreateConversionRule:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_create_conversion_rule_default_params(self, client):
        """Should POST /conversions with correct payload."""
        expected_resp = {
            "id": "urn:lla:llaPartnerConversion:123456",
            "name": "Demo Request",
            "type": "LEAD",
        }
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, expected_resp)

            result = await client.create_conversion_rule(
                account_id=507404993,
                name="Demo Request",
                conversion_type="LEAD",
            )

        assert result["id"] == "urn:lla:llaPartnerConversion:123456"
        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["type"] == "LEAD"
        assert body["conversionMethod"] == "CONVERSIONS_API"
        assert body["postClickAttributionWindowSize"] == 30
        assert body["viewThroughAttributionWindowSize"] == 7
        assert body["attributionType"] == "LAST_TOUCH_BY_CAMPAIGN"

    @pytest.mark.asyncio
    async def test_create_conversion_rule_custom_windows(self, client):
        """Should respect custom attribution windows."""
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"id": "urn:test"})

            await client.create_conversion_rule(
                account_id=507404993,
                name="Purchase",
                conversion_type="PURCHASE",
                post_click_window=90,
                view_through_window=30,
                attribution_type="EACH_CAMPAIGN",
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["postClickAttributionWindowSize"] == 90
        assert body["viewThroughAttributionWindowSize"] == 30
        assert body["attributionType"] == "EACH_CAMPAIGN"
        assert body["type"] == "PURCHASE"


class TestListConversionRules:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_list_conversion_rules(self, client):
        """Should GET /conversions with account filter."""
        elements = [
            {"id": "urn:lla:llaPartnerConversion:1", "name": "Lead", "type": "LEAD"},
            {"id": "urn:lla:llaPartnerConversion:2", "name": "Purchase", "type": "PURCHASE"},
        ]
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(
                200, {"elements": elements}
            )

            result = await client.list_conversion_rules(account_id=507404993)

        assert len(result) == 2
        assert result[0]["type"] == "LEAD"
        call_kwargs = client._client.request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["q"] == "account"
        assert "507404993" in params["account"]


class TestDeleteConversionRule:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_delete_conversion_rule(self, client):
        """Should DELETE /conversions/{id}."""
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(204)

            await client.delete_conversion_rule("12345")

        call_args = client._client.request.call_args
        assert call_args[0][0] == "DELETE"
        assert "/conversions/12345" in call_args[0][1]


# --- Conversion event sending ---


class TestSendConversionEvent:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_send_event_with_hashed_email(self, client):
        """Should hash email with SHA256 before sending."""
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            ts = datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC)
            await client.send_conversion_event(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                email="Test@Example.com",
                event_id="evt-001",
                happened_at=ts,
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        expected_hash = hash_email_for_linkedin("Test@Example.com")
        assert body["user"]["userIds"][0]["idValue"] == expected_hash
        assert body["user"]["userIds"][0]["idType"] == "SHA256_EMAIL"
        assert body["eventId"] == "evt-001"
        assert body["conversionHappenedAt"] == int(ts.timestamp() * 1000)

    @pytest.mark.asyncio
    async def test_send_event_with_value(self, client):
        """Should include conversionValue when value_usd provided."""
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            await client.send_conversion_event(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                email="user@test.com",
                event_id="evt-002",
                value_usd="500.00",
                happened_at=datetime(2026, 3, 25, tzinfo=UTC),
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["conversionValue"]["currencyCode"] == "USD"
        assert body["conversionValue"]["amount"] == "500.00"

    @pytest.mark.asyncio
    async def test_send_event_with_user_info(self, client):
        """Should pass through user_info fields."""
        user_info = {
            "firstName": "John",
            "lastName": "Doe",
            "companyName": "Acme",
            "title": "VP Marketing",
            "countryCode": "US",
        }
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            await client.send_conversion_event(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                email="john@acme.com",
                event_id="evt-003",
                user_info=user_info,
                happened_at=datetime(2026, 3, 25, tzinfo=UTC),
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["user"]["userInfo"]["firstName"] == "John"
        assert body["user"]["userInfo"]["countryCode"] == "US"

    @pytest.mark.asyncio
    async def test_send_event_defaults_to_now(self, client):
        """Should default happened_at to now() when not provided."""
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            before = datetime.now(UTC)
            await client.send_conversion_event(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                email="user@test.com",
                event_id="evt-004",
            )
            after = datetime.now(UTC)

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        ts_ms = body["conversionHappenedAt"]
        assert int(before.timestamp() * 1000) <= ts_ms <= int(after.timestamp() * 1000)

    @pytest.mark.asyncio
    async def test_dedup_event_id(self, client):
        """eventId should be passed through for deduplication."""
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            await client.send_conversion_event(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                email="user@test.com",
                event_id="tenant1:form_submitted:user@test.com:1700000000",
                happened_at=datetime(2026, 3, 25, tzinfo=UTC),
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["eventId"] == "tenant1:form_submitted:user@test.com:1700000000"


class TestSendConversionEventsBatch:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_batch_events(self, client):
        """Should send multiple events in a single request."""
        ts = datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC)
        events = [
            {"email": "a@test.com", "event_id": "e1", "happened_at": ts},
            {
                "email": "b@test.com",
                "event_id": "e2",
                "value_usd": "100.00",
                "happened_at": ts,
            },
        ]
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            await client.send_conversion_events_batch(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                events=events,
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert len(body["elements"]) == 2
        assert body["elements"][0]["eventId"] == "e1"
        assert body["elements"][1]["conversionValue"]["amount"] == "100.00"

    @pytest.mark.asyncio
    async def test_batch_hashes_all_emails(self, client):
        """Each event in batch should have SHA256-hashed email."""
        ts = datetime(2026, 3, 25, tzinfo=UTC)
        events = [
            {"email": "User@A.com", "event_id": "e1", "happened_at": ts},
            {"email": "User@B.com", "event_id": "e2", "happened_at": ts},
        ]
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            await client.send_conversion_events_batch(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                events=events,
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        for i, evt in enumerate(events):
            expected = hash_email_for_linkedin(evt["email"])
            actual = body["elements"][i]["user"]["userIds"][0]["idValue"]
            assert actual == expected

    @pytest.mark.asyncio
    async def test_batch_with_user_info(self, client):
        """Batch events should pass through user_info."""
        ts = datetime(2026, 3, 25, tzinfo=UTC)
        events = [
            {
                "email": "j@co.com",
                "event_id": "e1",
                "happened_at": ts,
                "user_info": {"firstName": "Jane"},
            },
        ]
        with patch.object(
            client, "_get_headers", new_callable=AsyncMock
        ) as mock_headers:
            mock_headers.return_value = {"Authorization": "Bearer tok"}
            client._client = AsyncMock()
            client._client.request.return_value = _mock_resp(200, {"status": "ok"})

            await client.send_conversion_events_batch(
                conversion_urn="urn:lla:llaPartnerConversion:123",
                events=events,
            )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["elements"][0]["user"]["userInfo"]["firstName"] == "Jane"


# --- ConversionBridge ---


class TestConversionBridgeSetup:
    @pytest.mark.asyncio
    async def test_setup_creates_rules_for_all_event_types(self):
        """Should create one conversion rule per PaidEdge event type."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_conversion_rule.return_value = {
            "id": "urn:lla:llaPartnerConversion:999"
        }

        mock_supabase, mock_chain = _mock_supabase_chain(
            select_data={"config": {}}
        )

        bridge = LinkedInConversionBridge(
            client=mock_client, supabase=mock_supabase
        )

        result = await bridge.setup_conversion_rules(
            tenant_id="tenant-1", account_id=507404993
        )

        assert len(result) == 6
        assert "form_submitted" in result
        assert "closed_won" in result
        assert mock_client.create_conversion_rule.call_count == 6

    @pytest.mark.asyncio
    async def test_setup_stores_mapping_in_provider_configs(self):
        """Should persist conversion_rules mapping to Supabase."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_conversion_rule.return_value = {
            "id": "urn:lla:llaPartnerConversion:42"
        }

        mock_supabase, mock_chain = _mock_supabase_chain(
            select_data={"config": {"existing_key": "value"}}
        )

        bridge = LinkedInConversionBridge(
            client=mock_client, supabase=mock_supabase
        )

        await bridge.setup_conversion_rules(
            tenant_id="tenant-1", account_id=507404993
        )

        # Verify update was called with conversion_rules in config
        update_call = mock_chain.update.call_args[0][0]
        assert "conversion_rules" in update_call["config"]
        assert update_call["config"]["existing_key"] == "value"


class TestConversionBridgeSendEvent:
    @pytest.mark.asyncio
    async def test_send_paidedge_event_maps_type(self):
        """Should look up conversion URN and fire event."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)

        mock_supabase, _ = _mock_supabase_chain(
            select_data={
                "config": {
                    "conversion_rules": {
                        "form_submitted": "urn:lla:llaPartnerConversion:100",
                        "closed_won": "urn:lla:llaPartnerConversion:200",
                    }
                }
            }
        )

        bridge = LinkedInConversionBridge(
            client=mock_client, supabase=mock_supabase
        )

        await bridge.send_paidedge_event(
            tenant_id="tenant-1",
            event_type="form_submitted",
            email="user@test.com",
            event_id="evt-100",
        )

        mock_client.send_conversion_event.assert_called_once_with(
            conversion_urn="urn:lla:llaPartnerConversion:100",
            email="user@test.com",
            event_id="evt-100",
            value_usd=None,
            user_info=None,
        )

    @pytest.mark.asyncio
    async def test_send_event_no_config_skips(self):
        """Should skip gracefully when no LinkedIn config exists."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain(select_data=None)

        bridge = LinkedInConversionBridge(
            client=mock_client, supabase=mock_supabase
        )

        # Should not raise
        await bridge.send_paidedge_event(
            tenant_id="tenant-1",
            event_type="form_submitted",
            email="user@test.com",
            event_id="evt-100",
        )

        mock_client.send_conversion_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_event_unknown_type_skips(self):
        """Should skip when event type has no conversion rule."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain(
            select_data={"config": {"conversion_rules": {}}}
        )

        bridge = LinkedInConversionBridge(
            client=mock_client, supabase=mock_supabase
        )

        await bridge.send_paidedge_event(
            tenant_id="tenant-1",
            event_type="unknown_event",
            email="user@test.com",
            event_id="evt-100",
        )

        mock_client.send_conversion_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_event_with_value_and_user_info(self):
        """Should pass value_usd and user_info through to client."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain(
            select_data={
                "config": {
                    "conversion_rules": {
                        "closed_won": "urn:lla:llaPartnerConversion:200",
                    }
                }
            }
        )

        bridge = LinkedInConversionBridge(
            client=mock_client, supabase=mock_supabase
        )

        await bridge.send_paidedge_event(
            tenant_id="tenant-1",
            event_type="closed_won",
            email="buyer@co.com",
            event_id="evt-200",
            value_usd="50000.00",
            user_info={"firstName": "Jane", "companyName": "BigCorp"},
        )

        mock_client.send_conversion_event.assert_called_once_with(
            conversion_urn="urn:lla:llaPartnerConversion:200",
            email="buyer@co.com",
            event_id="evt-200",
            value_usd="50000.00",
            user_info={"firstName": "Jane", "companyName": "BigCorp"},
        )


class TestEventTypeMapping:
    def test_all_paidedge_types_have_linkedin_mapping(self):
        """Verify all six PaidEdge event types map to valid LinkedIn types."""
        valid_linkedin_types = {
            "LEAD", "PURCHASE", "ADD_TO_CART", "SIGN_UP",
            "DOWNLOAD", "KEY_PAGE_VIEW", "INSTALL", "OTHER",
        }
        mapping = LinkedInConversionBridge.PAIDEDGE_TO_LINKEDIN_TYPE
        assert len(mapping) == 6
        for paidedge_type, linkedin_type in mapping.items():
            assert linkedin_type in valid_linkedin_types, (
                f"{paidedge_type} maps to invalid type {linkedin_type}"
            )
