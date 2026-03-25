"""Tests for Meta Lead Ads — form management + webhooks (BJC-165)."""

import hmac
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.meta_leads import (
    MetaLeadsMixin,
    parse_lead_field_data,
    poll_leads_for_tenant,
    verify_meta_webhook_signature,
)


class FakeClient(MetaLeadsMixin):
    def __init__(self):
        self._request = AsyncMock()
        self._paginate = AsyncMock()


class TestParseLeadFieldData:
    def test_basic_parsing(self):
        field_data = [
            {"name": "full_name", "values": ["Jane Smith"]},
            {"name": "email", "values": ["jane@example.com"]},
            {"name": "phone_number", "values": ["+1555123456"]},
        ]
        result = parse_lead_field_data(field_data)
        assert result["full_name"] == "Jane Smith"
        assert result["email"] == "jane@example.com"
        assert result["phone_number"] == "+1555123456"

    def test_empty_values(self):
        field_data = [{"name": "email", "values": []}]
        result = parse_lead_field_data(field_data)
        assert result["email"] == ""

    def test_empty_list(self):
        result = parse_lead_field_data([])
        assert result == {}


class TestWebhookSignatureVerification:
    def test_valid_signature(self):
        payload = b'{"object":"page","entry":[]}'
        secret = "test_secret"
        expected = hmac.new(
            secret.encode("utf-8"), payload, hashlib.sha256
        ).hexdigest()
        signature = f"sha256={expected}"

        assert verify_meta_webhook_signature(payload, signature, secret) is True

    def test_invalid_signature(self):
        payload = b'{"object":"page"}'
        assert verify_meta_webhook_signature(payload, "sha256=invalid", "secret") is False


class TestLeadFormCRUD:
    @pytest.mark.asyncio
    async def test_create_lead_form(self):
        client = FakeClient()
        client._request.return_value = {"id": "form_123"}

        questions = [
            {"type": "FULL_NAME"},
            {"type": "EMAIL"},
            {"type": "COMPANY_NAME"},
        ]
        result = await client.create_lead_form(
            page_id="page_456",
            name="Lead Magnet Form",
            questions=questions,
            privacy_policy_url="https://example.com/privacy",
        )
        assert result["id"] == "form_123"
        call_data = client._request.call_args[1]["data"]
        assert "is_optimized_for_quality" in call_data
        assert call_data["is_optimized_for_quality"] == "true"

    @pytest.mark.asyncio
    async def test_get_lead_form(self):
        client = FakeClient()
        client._request.return_value = {
            "id": "form_123", "name": "Test Form", "status": "ACTIVE"
        }
        result = await client.get_lead_form("form_123")
        assert result["status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_list_lead_forms(self):
        client = FakeClient()
        client._paginate.return_value = [
            {"id": "form_1", "name": "Form 1"},
            {"id": "form_2", "name": "Form 2"},
        ]
        result = await client.list_lead_forms("page_456")
        assert len(result) == 2


class TestLeadRetrieval:
    @pytest.mark.asyncio
    async def test_get_leads_by_form(self):
        client = FakeClient()
        client._paginate.return_value = [
            {
                "id": "lead_1",
                "created_time": "2026-03-25T12:00:00+0000",
                "field_data": [{"name": "email", "values": ["a@b.com"]}],
            }
        ]
        result = await client.get_leads_by_form("form_123")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_leads_by_form_with_since(self):
        client = FakeClient()
        client._paginate.return_value = []
        await client.get_leads_by_form("form_123", since=1711324800)
        # Verify filtering param was passed
        call_params = client._paginate.call_args[1]["params"]
        assert "filtering" in call_params

    @pytest.mark.asyncio
    async def test_get_leads_by_ad(self):
        client = FakeClient()
        client._paginate.return_value = [{"id": "lead_1"}]
        result = await client.get_leads_by_ad("ad_456")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_lead(self):
        client = FakeClient()
        client._request.return_value = {
            "id": "lead_1", "field_data": [{"name": "email", "values": ["x@y.com"]}]
        }
        result = await client.get_lead("lead_1")
        assert result["id"] == "lead_1"


class TestWebhookSubscription:
    @pytest.mark.asyncio
    async def test_subscribe_to_webhooks(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        result = await client.subscribe_to_lead_webhooks(
            "page_456", "page_token_abc"
        )
        assert result["success"] is True


class TestPollLeads:
    @pytest.mark.asyncio
    async def test_poll_with_forms(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"config": {"lead_gen_forms": ["form_1"]}}
        )

        mock_meta_client = AsyncMock()
        mock_meta_client.get_leads_by_form.return_value = [
            {
                "id": "lead_1",
                "ad_id": "ad_1",
                "created_time": "2026-03-25T12:00:00",
                "field_data": [{"name": "email", "values": ["a@b.com"]}],
            }
        ]

        leads = await poll_leads_for_tenant(
            "org-1", 1711324800, mock_supabase, mock_meta_client
        )
        assert len(leads) == 1
        assert leads[0]["fields"]["email"] == "a@b.com"

    @pytest.mark.asyncio
    async def test_poll_no_config(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(data=None)

        leads = await poll_leads_for_tenant("org-1", 0, mock_supabase, AsyncMock())
        assert leads == []
