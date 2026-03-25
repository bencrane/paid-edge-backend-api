"""Tests for Meta Campaign CRUD + CBO (BJC-151)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.meta_campaigns import (
    META_OBJECTIVE_MAP,
    MetaCampaign,
    MetaCampaignCreate,
    MetaCampaignsMixin,
    map_paidedge_status_to_meta,
)


def _mock_resp(status_code, json_data, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = str(json_data)
    resp.headers = headers or {}
    return resp


class FakeClient(MetaCampaignsMixin):
    """Fake client for testing mixin methods."""

    def __init__(self):
        self.ad_account_id = "act_123"
        self._request = AsyncMock()
        self._paginate = AsyncMock()


class TestObjectiveMapping:
    def test_all_objectives_mapped(self):
        assert "lead_generation" in META_OBJECTIVE_MAP
        assert META_OBJECTIVE_MAP["lead_generation"] == "OUTCOME_LEADS"
        assert META_OBJECTIVE_MAP["conversions"] == "OUTCOME_SALES"

    def test_status_mapping(self):
        assert map_paidedge_status_to_meta("draft") == "PAUSED"
        assert map_paidedge_status_to_meta("active") == "ACTIVE"
        assert map_paidedge_status_to_meta("completed") == "ARCHIVED"
        assert map_paidedge_status_to_meta("unknown") == "PAUSED"


class TestCampaignModels:
    def test_campaign_create_model(self):
        m = MetaCampaignCreate(name="Test", objective="OUTCOME_LEADS")
        assert m.special_ad_categories == []
        assert m.status == "PAUSED"
        assert m.bid_strategy == "LOWEST_COST_WITHOUT_CAP"

    def test_campaign_model(self):
        m = MetaCampaign(
            id="123", name="Test", objective="OUTCOME_LEADS",
            status="PAUSED", effective_status="PAUSED",
        )
        assert m.id == "123"


class TestCampaignCRUD:
    @pytest.mark.asyncio
    async def test_create_campaign(self):
        client = FakeClient()
        client._request.return_value = {"id": "campaign_123"}

        result = await client.create_campaign(
            name="Test Campaign",
            objective="OUTCOME_LEADS",
            special_ad_categories=[],
            daily_budget=5000,
            status="PAUSED",
        )
        assert result["id"] == "campaign_123"
        call_args = client._request.call_args
        assert call_args[0][0] == "POST"
        assert "campaigns" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_create_campaign_with_cbo(self):
        """CBO campaigns set budget at campaign level."""
        client = FakeClient()
        client._request.return_value = {"id": "campaign_cbo"}

        await client.create_campaign(
            name="CBO Campaign",
            objective="OUTCOME_TRAFFIC",
            daily_budget=10000,
            bid_strategy="COST_CAP",
        )
        call_data = client._request.call_args[1]["data"]
        assert call_data["daily_budget"] == 10000
        assert call_data["bid_strategy"] == "COST_CAP"

    @pytest.mark.asyncio
    async def test_get_campaign(self):
        client = FakeClient()
        client._request.return_value = {
            "id": "123", "name": "Test", "objective": "OUTCOME_LEADS",
            "effective_status": "ACTIVE",
        }
        result = await client.get_campaign("123")
        assert result["effective_status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_update_campaign(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.update_campaign("123", name="New Name")
        assert client._request.call_count == 1

    @pytest.mark.asyncio
    async def test_list_campaigns(self):
        client = FakeClient()
        client._paginate.return_value = [
            {"id": "1", "name": "C1"}, {"id": "2", "name": "C2"}
        ]
        result = await client.list_campaigns()
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_delete_campaign(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.delete_campaign("123")
        call_args = client._request.call_args
        assert call_args[0][0] == "DELETE"

    @pytest.mark.asyncio
    async def test_set_campaign_status(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.set_campaign_status("123", "ACTIVE")
        call_data = client._request.call_args[1]["data"]
        assert call_data["status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_special_ad_categories_always_sent(self):
        """special_ad_categories should always be present, even if empty."""
        client = FakeClient()
        client._request.return_value = {"id": "123"}
        await client.create_campaign(name="Test", objective="OUTCOME_LEADS")
        call_data = client._request.call_args[1]["data"]
        assert "special_ad_categories" in call_data
        assert call_data["special_ad_categories"] == []
